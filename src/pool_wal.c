// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define _GNU_SOURCE
#include "a-table-store-library/pool_wal.h"
#include "a-memory-library/aml_alloc.h"

#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <time.h>

static inline void encode_u32_le(uint8_t *dst, uint32_t v) {
    dst[0] = v & 0xFF; dst[1] = (v >> 8) & 0xFF; dst[2] = (v >> 16) & 0xFF; dst[3] = (v >> 24) & 0xFF;
}
static inline uint32_t decode_u32_le(const uint8_t *src) {
    return (uint32_t)src[0] | ((uint32_t)src[1] << 8) | ((uint32_t)src[2] << 16) | ((uint32_t)src[3] << 24);
}

static uint32_t crc32(const void *buf, size_t size) {
    const uint8_t *p = buf;
    uint32_t crc = ~0U;
    while (size--) {
        crc ^= *p++;
        for (int i = 0; i < 8; i++) crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
    }
    return ~crc;
}

static int preallocate_file(int fd, uint64_t size) {
#ifdef __APPLE__
    fstore_t store = {F_ALLOCATECONTIG, F_PEOFPOSMODE, 0, size, 0};
    fcntl(fd, F_PREALLOCATE, &store);
    return ftruncate(fd, size);
#else
    return posix_fallocate(fd, 0, size);
#endif
}

static void get_segment_path(pool_wal_t* wal, uint64_t seg_id, char* out_path) {
    snprintf(out_path, 1024, "%s/%010llu.wal", wal->base_dir, (unsigned long long)seg_id);
}

int pool_wal_init(pool_wal_t* wal, const char* dir, uint64_t segment_size_mb, uint32_t max_standby) {
    memset(wal, 0, sizeof(pool_wal_t));
    strncpy(wal->base_dir, dir, sizeof(wal->base_dir) - 1);

    wal->segment_size_bytes = segment_size_mb * 1024 * 1024;
    wal->max_standby_files = max_standby;
    wal->standby_paths = aml_malloc(sizeof(char*) * max_standby);

    pthread_mutex_init(&wal->mu, NULL);

    mkdir(dir, 0755);

    DIR *dp = opendir(dir);
    struct dirent *ep;

    uint64_t min_seg = UINT64_MAX;
    uint64_t max_seg = 0;
    bool found_segments = false;

    if (dp != NULL) {
        while ((ep = readdir(dp))) {
            if (strncmp(ep->d_name, "standby_", 8) == 0) {
                if (wal->standby_count < wal->max_standby_files) {
                    wal->standby_paths[wal->standby_count++] = aml_strdupf("%s/%s", dir, ep->d_name);
                } else {
                    char excess[1024];
                    snprintf(excess, 1024, "%s/%s", dir, ep->d_name);
                    unlink(excess);
                }
            } else if (strstr(ep->d_name, ".wal")) {
                uint64_t seg_id;
                if (sscanf(ep->d_name, "%llu.wal", &seg_id) == 1) {
                    if (seg_id < min_seg) min_seg = seg_id;
                    if (seg_id > max_seg) max_seg = seg_id;
                    found_segments = true;
                }
            }
        }
        closedir(dp);
    }

    if (!found_segments) {
        wal->oldest_seg_id = 1;
        wal->current_seg_id = 1;
        wal->file_offset = WAL_HEADER_SIZE;
        wal->next_lsn = 1;

        char path[1024];
        get_segment_path(wal, 1, path);
        wal->active_fd = open(path, O_RDWR | O_CREAT, 0644);

        preallocate_file(wal->active_fd, wal->segment_size_bytes);
        pool_file_header_t hdr = { WAL_MAGIC_NUMBER, 1, wal->next_lsn, 0 };

        // [Phase 7] Catch partial initialization write
        if (pwrite(wal->active_fd, &hdr, sizeof(hdr), 0) != sizeof(hdr)) {
            close(wal->active_fd);
            return -1;
        }

    } else {
        wal->oldest_seg_id = min_seg;
        wal->current_seg_id = max_seg;

        char path[1024];
        get_segment_path(wal, max_seg, path);
        wal->active_fd = open(path, O_RDWR, 0644);

        wal->next_lsn = 1;
        pool_file_header_t fhdr;
        if (pread(wal->active_fd, &fhdr, sizeof(fhdr), 0) == sizeof(fhdr)) {
            wal->next_lsn = fhdr.start_lsn;
            uint64_t off = WAL_HEADER_SIZE;
            while (true) {
                pool_frame_header_t frame;
                if (pread(wal->active_fd, &frame, sizeof(frame), off) != sizeof(frame)) break;
                if (frame.payload_len == 0 && frame.seq == 0) break;
                if (frame.lsn >= wal->next_lsn) wal->next_lsn = frame.lsn + 1;
                off += sizeof(frame) + frame.payload_len;
            }
            wal->file_offset = off;
        } else {
            wal->file_offset = WAL_HEADER_SIZE;
        }
    }

    return 0;
}

pool_wal_iter_t* pool_wal_iter_init(pool_wal_t* wal) {
    pool_wal_iter_t* iter = aml_zalloc(sizeof(pool_wal_iter_t));
    iter->wal = wal;
    iter->current_seg = wal->oldest_seg_id;
    iter->fd = -1;
    iter->offset = WAL_HEADER_SIZE;
    return iter;
}

bool pool_wal_iter_next(pool_wal_iter_t* iter, uint64_t* seq, uint64_t* lsn, uint8_t* type, const uint8_t** payload, uint32_t* len) {
    if (iter->current_payload) {
        aml_free(iter->current_payload);
        iter->current_payload = NULL;
    }

    while (iter->current_seg <= iter->wal->current_seg_id) {
        if (iter->fd < 0) {
            char path[1024];
            get_segment_path(iter->wal, iter->current_seg, path);
            iter->fd = open(path, O_RDONLY);
            if (iter->fd < 0) {
                iter->current_seg++;
                continue;
            }
            iter->offset = WAL_HEADER_SIZE;
        }

        pool_frame_header_t hdr;
        if (pread(iter->fd, &hdr, sizeof(hdr), iter->offset) != sizeof(hdr)) {
            close(iter->fd); iter->fd = -1;
            iter->current_seg++;
            continue;
        }

        if (hdr.payload_len == 0 && hdr.seq == 0) {
            close(iter->fd); iter->fd = -1;
            iter->current_seg++;
            continue;
        }

        uint8_t* p = aml_malloc(hdr.payload_len);
        pread(iter->fd, p, hdr.payload_len, iter->offset + sizeof(hdr));

        if (hdr.frame_crc != crc32(p, hdr.payload_len)) {
            aml_free(p);
            if (iter->current_seg == iter->wal->current_seg_id) {
                iter->wal->file_offset = iter->offset;
            }
            close(iter->fd); iter->fd = -1;
            return false;
        }

        iter->offset += sizeof(hdr) + hdr.payload_len;
        if (iter->current_seg == iter->wal->current_seg_id) {
            iter->wal->file_offset = iter->offset;
        }

        iter->current_payload = p;
        *seq = hdr.seq;
        *lsn = hdr.lsn;
        *type = hdr.type;
        *len = hdr.payload_len;
        *payload = p;
        return true;
    }
    return false;
}

void pool_wal_iter_destroy(pool_wal_iter_t* iter) {
    if (!iter) return;
    if (iter->fd >= 0) close(iter->fd);
    if (iter->current_payload) aml_free(iter->current_payload);
    aml_free(iter);
}

// Assumes wal->mu is already locked
static bool pool_wal_rotate_locked(pool_wal_t* wal, uint64_t next_lsn) {
    close(wal->active_fd);

    wal->current_seg_id++;
    char new_path[1024];
    get_segment_path(wal, wal->current_seg_id, new_path);

    if (wal->standby_count > 0) {
        wal->standby_count--;
        char* standby_path = wal->standby_paths[wal->standby_count];
        rename(standby_path, new_path);
        aml_free(standby_path);
        wal->active_fd = open(new_path, O_RDWR);
    } else {
        wal->active_fd = open(new_path, O_RDWR | O_CREAT, 0644);
        preallocate_file(wal->active_fd, wal->segment_size_bytes);
    }

    if (wal->active_fd < 0) return false;

    pool_file_header_t hdr = { WAL_MAGIC_NUMBER, wal->current_seg_id, next_lsn, 0 };
    if (pwrite(wal->active_fd, &hdr, sizeof(hdr), 0) != sizeof(hdr)) return false;

    wal->file_offset = WAL_HEADER_SIZE;
    return true;
}

// [Phase 6] Strictly thread-safe exact-byte append
uint64_t pool_wal_append(pool_wal_t* wal, uint64_t seq, uint8_t type, const uint8_t* payload, uint32_t len) {
    uint32_t frame_size = sizeof(pool_frame_header_t) + len;

    pthread_mutex_lock(&wal->mu);
    uint64_t my_lsn = wal->next_lsn++;

    if (wal->file_offset + frame_size > wal->segment_size_bytes) {
        if (!pool_wal_rotate_locked(wal, my_lsn)) {
            wal->next_lsn--;
            pthread_mutex_unlock(&wal->mu);
            return 0;
        }
    }

    pool_frame_header_t frame_hdr = {
        .payload_len = len,
        .seq = seq,
        .lsn = my_lsn,
        .type = type,
        .frame_crc = crc32(payload, len)
    };

    if (pwrite(wal->active_fd, &frame_hdr, sizeof(frame_hdr), wal->file_offset) != sizeof(frame_hdr)) {
        wal->next_lsn--;
        pthread_mutex_unlock(&wal->mu);
        return 0; // Error
    }

    if (len > 0) {
        if (pwrite(wal->active_fd, payload, len, wal->file_offset + sizeof(frame_hdr)) != (ssize_t)len) {
            wal->next_lsn--;
            pthread_mutex_unlock(&wal->mu);
            return 0; // Error
        }
    }

    wal->file_offset += frame_size;
    pthread_mutex_unlock(&wal->mu);
    return my_lsn;
}

void pool_wal_purge(pool_wal_t* wal, uint64_t safe_lsn) {
    pthread_mutex_lock(&wal->mu);
    uint64_t scan_seg = wal->oldest_seg_id;

    while (scan_seg < wal->current_seg_id) {
        char path[1024];
        get_segment_path(wal, scan_seg, path);

        int fd = open(path, O_RDONLY);
        if (fd < 0) {
            scan_seg++;
            wal->oldest_seg_id = scan_seg;
            continue;
        }

        char next_path[1024];
        get_segment_path(wal, scan_seg + 1, next_path);
        int next_fd = open(next_path, O_RDONLY);

        pool_file_header_t next_hdr;
        bool can_purge = false;

        if (next_fd >= 0) {
            if (pread(next_fd, &next_hdr, sizeof(next_hdr), 0) == sizeof(next_hdr)) {
                // [Phase 6] Purges safely use the global LSN
                if (next_hdr.start_lsn <= safe_lsn && next_hdr.start_lsn > 0) {
                    can_purge = true;
                }
            }
            close(next_fd);
        }
        close(fd);

        if (can_purge) {
            if (wal->standby_count < wal->max_standby_files) {
                char* standby_path = aml_strdupf("%s/standby_%llu_%u.wal", wal->base_dir, (unsigned long long)time(NULL), wal->standby_count);
                rename(path, standby_path);
                wal->standby_paths[wal->standby_count++] = standby_path;
            } else {
                unlink(path);
            }
            scan_seg++;
            wal->oldest_seg_id = scan_seg;
        } else {
            break;
        }
    }
    pthread_mutex_unlock(&wal->mu);
}

void pool_wal_sync(pool_wal_t* wal) {
    pthread_mutex_lock(&wal->mu);
    int fd = wal->active_fd;
    pthread_mutex_unlock(&wal->mu); // Release lock before blocking I/O!
    if (fd >= 0) {
#ifdef __APPLE__
        fcntl(fd, F_FULLFSYNC, 0);
#else
        fdatasync(fd);
#endif
    }
}

void pool_wal_close(pool_wal_t* wal) {
    pthread_mutex_lock(&wal->mu);
    if (wal->active_fd > 0) close(wal->active_fd);

    for (uint32_t i = 0; i < wal->standby_count; i++) {
        aml_free(wal->standby_paths[i]);
    }
    aml_free(wal->standby_paths);
    if (wal->ckpts) aml_free(wal->ckpts);
    pthread_mutex_unlock(&wal->mu);

    pthread_mutex_destroy(&wal->mu);
}

// -----------------------------------------------------------------------------
// NATIVE LSM-TREE TRANSLATION LAYER
// -----------------------------------------------------------------------------

typedef struct {
    lsm_wal_iter_t base;
    pool_wal_iter_t *pool_iter;
} lsm_api_iter_t;

static bool lsm_api_append(lsm_wal_t *lsm, uint32_t table_id, uint64_t seq_num, uint8_t op, const void *key, uint32_t klen, const void *val, uint32_t vlen, uint64_t *out_lsn) {
    pool_wal_t *wal = (pool_wal_t *)lsm->ctx;

    uint32_t payload_len = 4 + 4 + 4 + klen + vlen;
    uint8_t *buf = aml_malloc(payload_len);

    uint32_t ptr = 0;
    encode_u32_le(buf + ptr, table_id); ptr += 4;
    encode_u32_le(buf + ptr, klen); ptr += 4;
    encode_u32_le(buf + ptr, vlen); ptr += 4;

    if (klen > 0 && key) { memcpy(buf + ptr, key, klen); ptr += klen; }
    if (vlen > 0 && val) { memcpy(buf + ptr, val, vlen); ptr += vlen; }

    uint64_t lsn = pool_wal_append(wal, seq_num, op, buf, payload_len);
    aml_free(buf);

    if (lsn == 0) return false;
    if (out_lsn) *out_lsn = lsn;
    return true;
}

static void lsm_api_sync(lsm_wal_t *lsm) {
    pool_wal_t *wal = (pool_wal_t *)lsm->ctx;
    pool_wal_sync(wal);
}

static void lsm_api_checkpoint(lsm_wal_t *lsm, uint32_t table_id, uint64_t lsn) {
    pool_wal_t *wal = (pool_wal_t *)lsm->ctx;

    pthread_mutex_lock(&wal->mu);
    bool found = false;
    for (size_t i = 0; i < wal->num_ckpts; i++) {
        if (wal->ckpts[i].table_id == table_id) {
            wal->ckpts[i].safe_lsn = lsn;
            found = true;
            break;
        }
    }

    if (!found) {
        if (wal->num_ckpts == wal->cap_ckpts) {
            wal->cap_ckpts = wal->cap_ckpts == 0 ? 8 : wal->cap_ckpts * 2;
            wal->ckpts = aml_realloc(wal->ckpts, wal->cap_ckpts * sizeof(pool_wal_ckpt_t));
        }
        wal->ckpts[wal->num_ckpts].table_id = table_id;
        wal->ckpts[wal->num_ckpts].safe_lsn = lsn;
        wal->num_ckpts++;
    }

    uint64_t min_lsn = UINT64_MAX;
    for (size_t i = 0; i < wal->num_ckpts; i++) {
        if (wal->ckpts[i].safe_lsn < min_lsn) {
            min_lsn = wal->ckpts[i].safe_lsn;
        }
    }
    pthread_mutex_unlock(&wal->mu);

    if (min_lsn != UINT64_MAX && wal->num_ckpts > 0) {
        pool_wal_purge(wal, min_lsn);
    }
}

static void lsm_api_unregister_table(lsm_wal_t *lsm, uint32_t table_id) {
    pool_wal_t *wal = (pool_wal_t *)lsm->ctx;
    pthread_mutex_lock(&wal->mu);
    for (size_t i = 0; i < wal->num_ckpts; i++) {
        if (wal->ckpts[i].table_id == table_id) {
            wal->ckpts[i] = wal->ckpts[--wal->num_ckpts];
            break;
        }
    }

    // Recalculate minimum LSN for GC
    uint64_t min_lsn = UINT64_MAX;
    for (size_t i = 0; i < wal->num_ckpts; i++) {
        if (wal->ckpts[i].safe_lsn < min_lsn) {
            min_lsn = wal->ckpts[i].safe_lsn;
        }
    }
    pthread_mutex_unlock(&wal->mu);

    if (min_lsn != UINT64_MAX && wal->num_ckpts > 0) {
        pool_wal_purge(wal, min_lsn);
    }
}

static void lsm_api_close(lsm_wal_t *lsm) {
    if (!lsm) return;
    pool_wal_t *wal = (pool_wal_t *)lsm->ctx;
    if (wal) {
        pool_wal_close(wal);
        aml_free(wal);
    }
    aml_free(lsm);
}

static bool lsm_api_iter_next(lsm_wal_iter_t *iter, uint32_t *out_table_id, uint64_t *out_lsn, uint8_t *out_op,
                              const void **out_key, uint32_t *out_klen,
                              const void **out_val, uint32_t *out_vlen) {
    lsm_api_iter_t *w_iter = (lsm_api_iter_t *)iter;

    uint64_t seq, lsn;
    uint8_t op;
    const uint8_t *payload;
    uint32_t len;

    if (!pool_wal_iter_next(w_iter->pool_iter, &seq, &lsn, &op, &payload, &len)) return false;

    uint32_t ptr = 0;
    if (len < 12) return false;

    *out_table_id = decode_u32_le(payload + ptr); ptr += 4;
    *out_klen = decode_u32_le(payload + ptr); ptr += 4;
    *out_vlen = decode_u32_le(payload + ptr); ptr += 4;

    if (ptr + *out_klen + *out_vlen > len) return false;

    *out_key = (*out_klen > 0) ? (const void*)(payload + ptr) : NULL;
    ptr += *out_klen;

    *out_val = (*out_vlen > 0) ? (const void*)(payload + ptr) : NULL;
    *out_op = op;
    *out_lsn = lsn;

    return true;
}

static void lsm_api_iter_destroy(lsm_wal_iter_t *iter) {
    if (!iter) return;
    lsm_api_iter_t *w_iter = (lsm_api_iter_t *)iter;
    pool_wal_iter_destroy(w_iter->pool_iter);
    aml_free(w_iter);
}

static lsm_wal_iter_t* lsm_api_iter_init(lsm_wal_t *lsm) {
    pool_wal_t *wal = (pool_wal_t *)lsm->ctx;
    lsm_api_iter_t *w_iter = aml_zalloc(sizeof(lsm_api_iter_t));

    w_iter->base.ctx = w_iter;
    w_iter->base.next = lsm_api_iter_next;
    w_iter->base.destroy = lsm_api_iter_destroy;

    w_iter->pool_iter = pool_wal_iter_init(wal);
    return (lsm_wal_iter_t*)w_iter;
}

lsm_wal_t *pool_to_lsm_wal(const char *dir, uint64_t segment_size_mb, uint32_t max_standby) {
    pool_wal_t *pool = aml_zalloc(sizeof(pool_wal_t));

    if (pool_wal_init(pool, dir, segment_size_mb, max_standby) != 0) {
        aml_free(pool);
        return NULL;
    }

    lsm_wal_t *lsm = aml_zalloc(sizeof(lsm_wal_t));
    lsm->ctx = pool;
    lsm->append = lsm_api_append;
    lsm->sync = lsm_api_sync;
    lsm->checkpoint = lsm_api_checkpoint;
    lsm->unregister_table = lsm_api_unregister_table;
    lsm->close = lsm_api_close;
    lsm->iter_init = lsm_api_iter_init;

    return lsm;
}
