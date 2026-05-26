// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-table-store-library/sstable_reader.h"
#include "a-table-store-library/lsm_cache.h"
#include "the-lz4-library/lz4/lz4.h"
#include "the-lz4-library/xxhash.h"
#include "a-memory-library/aml_alloc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// [Phase 11] Support legal 1024-byte user keys globally
#define MAX_KEY_SIZE 1024
#define INTERNAL_KEY_TRAILER_SIZE 8
#define MAX_INTERNAL_KEY_SIZE (MAX_KEY_SIZE + INTERNAL_KEY_TRAILER_SIZE)

static inline uint32_t decode_u32_le(const uint8_t *src) {
    return (uint32_t)src[0] | ((uint32_t)src[1] << 8) | ((uint32_t)src[2] << 16) | ((uint32_t)src[3] << 24);
}
static inline uint64_t decode_u64_le(const uint8_t *src) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= ((uint64_t)src[i]) << (i * 8);
    return v;
}

static int decode_varint32(const uint8_t *src, uint32_t limit, uint32_t *value) {
    uint32_t result = 0;
    int shift = 0;
    uint32_t bytes = 0;
    while (bytes < limit && bytes < 5) {
        uint8_t b = src[bytes++];
        result |= (uint32_t)(b & 127) << shift;
        if (!(b & 128)) {
            *value = result;
            return bytes;
        }
        shift += 7;
    }
    return 0;
}

struct sstable_reader_s {
    char base_path[512];
    lsm_storage_backend_t *backend;
    lsm_env_t *env;
    uint32_t table_id;
    uint64_t file_id;

    void *data_reader;

    int filter_type;
    uint8_t *bitmap;
    size_t bitmap_cap;
    uint64_t min_bitmap_id;

    uint8_t *index_buf;
    uint32_t index_size;
};

sstable_reader_t *sstable_reader_init(const char *base_path, lsm_storage_backend_t *backend, lsm_env_t *env, uint32_t table_id, uint64_t file_id) {
    sstable_reader_t *r = aml_zalloc(sizeof(sstable_reader_t));
    strncpy(r->base_path, base_path, 511);
    r->backend = backend;
    r->env = env;
    r->table_id = table_id;
    r->file_id = file_id;

    char data_path[520];
    snprintf(data_path, sizeof(data_path), "%s.data", base_path);
    r->data_reader = backend->open_reader(data_path);
    if (!r->data_reader) { aml_free(r); return NULL; }

    char meta_path[520];
    snprintf(meta_path, sizeof(meta_path), "%s.meta", base_path);
    void *meta = backend->open_reader(meta_path);
    if (!meta) { backend->close_reader(r->data_reader); aml_free(r); return NULL; }

    uint8_t header[9];
    if (backend->pread(meta, header, 9, 0) != 9) goto fail;
    if (memcmp(header, "META", 4) != 0) goto fail;

    r->filter_type = header[4];
    uint32_t filter_len = decode_u32_le(header + 5);

    if (filter_len > 100 * 1024 * 1024) goto fail;

    uint64_t offset = 9;
    if (filter_len > 0) {
        uint8_t *fbuf = aml_malloc(filter_len);
        if (backend->pread(meta, fbuf, filter_len, offset) != (ssize_t)filter_len) {
            aml_free(fbuf); goto fail;
        }
        if (r->filter_type == 2 /* FILTER_BITMAP */) {
            if (filter_len >= 8) {
                r->min_bitmap_id = decode_u64_le(fbuf);
                r->bitmap_cap = filter_len - 8;
                r->bitmap = aml_malloc(r->bitmap_cap);
                memcpy(r->bitmap, fbuf + 8, r->bitmap_cap);
            }
        }
        aml_free(fbuf);
        offset += filter_len;
    }

    uint8_t idx_sz_buf[4];
    if (backend->pread(meta, idx_sz_buf, 4, offset) != 4) goto fail;
    r->index_size = decode_u32_le(idx_sz_buf);
    offset += 4;

    if (r->index_size > 256 * 1024 * 1024) goto fail;

    if (r->index_size > 0) {
        r->index_buf = aml_malloc(r->index_size);
        if (backend->pread(meta, r->index_buf, r->index_size, offset) != (ssize_t)r->index_size) {
            goto fail;
        }
    }

    backend->close_reader(meta);
    return r;

fail:
    if (meta) backend->close_reader(meta);
    if (r->data_reader) backend->close_reader(r->data_reader);
    if (r->bitmap) aml_free(r->bitmap);
    if (r->index_buf) aml_free(r->index_buf);
    aml_free(r);
    return NULL;
}

void sstable_reader_destroy(sstable_reader_t *r) {
    if (!r) return;
    if (r->data_reader) r->backend->close_reader(r->data_reader);
    if (r->bitmap) aml_free(r->bitmap);
    if (r->index_buf) aml_free(r->index_buf);
    aml_free(r);
}

static lsm_cache_handle_t *sstable_reader_load_block(sstable_reader_t *r, uint64_t offset, uint64_t disk_size) {
    lsm_cache_handle_t *h = lsm_cache_get(r->env->block_cache, r->table_id, r->file_id, offset);
    if (h) return h;

    if (disk_size < 9 || disk_size > 128 * 1024 * 1024) return NULL;

    uint8_t *disk_buf = aml_malloc(disk_size);
    if (r->backend->pread(r->data_reader, disk_buf, disk_size, offset) != (ssize_t)disk_size) {
        aml_free(disk_buf); return NULL;
    }

    uint32_t uncomp_size = decode_u32_le(disk_buf + disk_size - 9);
    uint8_t comp_flag = disk_buf[disk_size - 5];
    uint32_t checksum = decode_u32_le(disk_buf + disk_size - 4);

    if (XXH32(disk_buf, disk_size - 9, 0) != checksum) {
        aml_free(disk_buf); return NULL;
    }

    uint8_t *uncomp_buf = NULL;

    if (comp_flag == 0 /* COMPRESS_NONE */) {
        if (uncomp_size != disk_size - 9) {
            aml_free(disk_buf); return NULL;
        }
        uncomp_buf = aml_malloc(uncomp_size);
        memcpy(uncomp_buf, disk_buf, uncomp_size);
    } else if (comp_flag == 1 /* COMPRESS_LZ4 */) {
        if (uncomp_size > 128 * 1024 * 1024) { aml_free(disk_buf); return NULL; }
        uncomp_buf = aml_malloc(uncomp_size);

        int dec_bytes = LZ4_decompress_safe((const char*)disk_buf, (char*)uncomp_buf, disk_size - 9, uncomp_size);

        if (dec_bytes < 0 || (uint32_t)dec_bytes != uncomp_size) {
            aml_free(disk_buf);
            aml_free(uncomp_buf);
            return NULL;
        }
    } else {
        aml_free(disk_buf); return NULL;
    }

    aml_free(disk_buf);
    h = lsm_cache_put_or_get(r->env->block_cache, r->table_id, r->file_id, offset, uncomp_buf, uncomp_size);
    return h;
}

int sstable_reader_get(sstable_reader_t *r, const void *key, uint32_t key_len, uint64_t read_seq_num, void **out_val, uint32_t *out_vlen) {
    if (r->filter_type == 2 /* FILTER_BITMAP */ && key_len >= 8 && r->bitmap) {
        uint64_t current_id;
        memcpy(&current_id, key, 8);
        if (current_id < r->min_bitmap_id) return 0;

        uint64_t diff = current_id - r->min_bitmap_id;
        size_t byte_idx = diff / 8;
        if (byte_idx < r->bitmap_cap) {
            if (!(r->bitmap[byte_idx] & (1 << (diff % 8)))) {
                return 0; // Filter mathematical rejection
            }
        }
    }

    uint32_t ptr = 0;
    uint32_t next_block_ptr = 0;
    uint64_t target_block_offset = UINT64_MAX;
    uint64_t target_block_size = 0;

    // Search the index blocks for the first candidate block
    while (ptr < r->index_size) {
        if (ptr + 4 > r->index_size) break;
        uint32_t iklen = decode_u32_le(r->index_buf + ptr); ptr += 4;

        if (ptr + iklen + 16 > r->index_size) break;

        uint32_t ulen = iklen > 8 ? iklen - 8 : 0;
        uint32_t min_len = key_len < ulen ? key_len : ulen;

        int cmp = memcmp(key, r->index_buf + ptr, min_len);
        if (cmp == 0) cmp = key_len < ulen ? -1 : (key_len > ulen ? 1 : 0);

        uint64_t offset = decode_u64_le(r->index_buf + ptr + iklen);
        uint64_t disk_sz = decode_u64_le(r->index_buf + ptr + iklen + 8);

        ptr += iklen + 16;

        if (cmp <= 0) {
            target_block_offset = offset;
            target_block_size = disk_sz;
            next_block_ptr = ptr;
            break;
        }
    }

    if (target_block_offset == UINT64_MAX) return 0;

    char last_key[MAX_INTERNAL_KEY_SIZE];
    uint32_t last_key_len = 0;

    // [Phase 11] Cross-block traversal for multi-version snapshot keys
    while (target_block_offset != UINT64_MAX) {
        lsm_cache_handle_t *h = sstable_reader_load_block(r, target_block_offset, target_block_size);
        if (!h) return 0;

        size_t block_size_st;
        uint8_t *block_buf = (uint8_t *)lsm_cache_handle_data(h, &block_size_st);
        uint32_t block_size = (uint32_t)block_size_st;

        if (block_size < 4) { lsm_cache_handle_release(r->env->block_cache, h); return 0; }
        uint32_t num_restarts = decode_u32_le(block_buf + block_size - 4);

        if (num_restarts == 0 || ((uint64_t)num_restarts * 4) > (block_size - 4)) {
            lsm_cache_handle_release(r->env->block_cache, h);
            return 0;
        }

        uint32_t restarts_offset = block_size - 4 - (num_restarts * 4);
        uint32_t bptr = 0;
        last_key_len = 0;

        bool key_match_found = false;

        while (bptr < restarts_offset) {
            uint32_t shared, unshared, vlen;
            int s_b = decode_varint32(block_buf + bptr, restarts_offset - bptr, &shared); bptr += s_b;
            int u_b = decode_varint32(block_buf + bptr, restarts_offset - bptr, &unshared); bptr += u_b;
            int v_b = decode_varint32(block_buf + bptr, restarts_offset - bptr, &vlen); bptr += v_b;

            if (s_b == 0 || u_b == 0 || v_b == 0 || bptr + unshared + vlen > restarts_offset) break;

            // [Phase 11] Strict prefix bounding validation
            if (shared > last_key_len || shared + unshared > MAX_INTERNAL_KEY_SIZE) break;

            memcpy(last_key + shared, block_buf + bptr, unshared);
            bptr += unshared;
            last_key_len = shared + unshared;

            const uint8_t *val_ptr = block_buf + bptr;
            bptr += vlen;

            uint32_t ulen = last_key_len > 8 ? last_key_len - 8 : 0;
            uint32_t min_len = key_len < ulen ? key_len : ulen;
            int cmp = memcmp(key, last_key, min_len);
            if (cmp == 0) cmp = key_len < ulen ? -1 : (key_len > ulen ? 1 : 0);

            if (cmp == 0) {
                key_match_found = true;
                uint64_t seq = decode_u64_le((const uint8_t*)last_key + ulen) >> 8;
                uint8_t op = last_key[last_key_len - 8];
                if (seq <= read_seq_num) {
                    if (op == 1 /* OP_DELETE */) {
                        lsm_cache_handle_release(r->env->block_cache, h);
                        return -1;
                    }
                    if (out_val) {
                        *out_val = aml_malloc(vlen);
                        memcpy(*out_val, val_ptr, vlen);
                        *out_vlen = vlen;
                    }
                    lsm_cache_handle_release(r->env->block_cache, h);
                    return 1;
                }
            } else if (cmp < 0) {
                lsm_cache_handle_release(r->env->block_cache, h);
                return 0; // Overshot the key, logical termination
            }
        }

        lsm_cache_handle_release(r->env->block_cache, h);

        // If the key was found but no MVCC versions matched, it may cross the block boundary
        if (key_match_found && next_block_ptr < r->index_size) {
            uint32_t ptr2 = next_block_ptr;
            if (ptr2 + 4 <= r->index_size) {
                uint32_t iklen = decode_u32_le(r->index_buf + ptr2); ptr2 += 4;
                if (ptr2 + iklen + 16 <= r->index_size) {
                    target_block_offset = decode_u64_le(r->index_buf + ptr2 + iklen);
                    target_block_size = decode_u64_le(r->index_buf + ptr2 + iklen + 8);
                    next_block_ptr = ptr2 + iklen + 16;
                    continue; // Load and scan next block
                }
            }
        }

        break; // Key exhausted
    }

    return 0;
}

struct sstable_iter_s {
    sstable_reader_t *r;
    uint32_t index_ptr;

    lsm_cache_handle_t *handle;
    uint8_t *block_buf;
    uint32_t block_size;
    uint32_t restarts_offset;

    uint32_t block_ptr;
    // [Phase 11] Increased to support MAX bounds
    char key[MAX_INTERNAL_KEY_SIZE];
    uint32_t key_len;
    const char *val;
    uint32_t val_len;
};

sstable_iter_t *sstable_iter_init(sstable_reader_t *r) {
    sstable_iter_t *it = aml_zalloc(sizeof(sstable_iter_t));
    it->r = r;
    return it;
}

bool sstable_iter_next(sstable_iter_t *it) {
    while (true) {
        if (it->handle && it->block_ptr < it->restarts_offset) {
            uint32_t shared, unshared, vlen;
            int s_b = decode_varint32(it->block_buf + it->block_ptr, it->restarts_offset - it->block_ptr, &shared); it->block_ptr += s_b;
            int u_b = decode_varint32(it->block_buf + it->block_ptr, it->restarts_offset - it->block_ptr, &unshared); it->block_ptr += u_b;
            int v_b = decode_varint32(it->block_buf + it->block_ptr, it->restarts_offset - it->block_ptr, &vlen); it->block_ptr += v_b;

            if (s_b == 0 || u_b == 0 || v_b == 0 || it->block_ptr + unshared + vlen > it->restarts_offset) break;

            // [Phase 11] Discard invalid prefix instructions to prevent parsing crashes
            if (shared > it->key_len || shared + unshared > MAX_INTERNAL_KEY_SIZE) break;

            memcpy(it->key + shared, it->block_buf + it->block_ptr, unshared);
            it->block_ptr += unshared;
            it->key_len = shared + unshared;

            it->val = (const char*)(it->block_buf + it->block_ptr);
            it->val_len = vlen;
            it->block_ptr += vlen;

            return true;
        }

        if (it->index_ptr >= it->r->index_size) return false;

        if (it->index_ptr + 4 > it->r->index_size) return false;
        uint32_t iklen = decode_u32_le(it->r->index_buf + it->index_ptr);
        it->index_ptr += 4;

        if (it->index_ptr + iklen + 16 > it->r->index_size) return false;
        it->index_ptr += iklen;

        uint64_t offset = decode_u64_le(it->r->index_buf + it->index_ptr);
        uint64_t disk_sz = decode_u64_le(it->r->index_buf + it->index_ptr + 8);
        it->index_ptr += 16;

        if (it->handle) {
            lsm_cache_handle_release(it->r->env->block_cache, it->handle);
            it->handle = NULL;
        }

        it->handle = sstable_reader_load_block(it->r, offset, disk_sz);
        if (!it->handle) return false;

        size_t iter_block_sz;
        it->block_buf = (uint8_t*)lsm_cache_handle_data(it->handle, &iter_block_sz);
        it->block_size = (uint32_t)iter_block_sz;

        if (it->block_size < 4) continue;
        uint32_t num_restarts = decode_u32_le(it->block_buf + it->block_size - 4);

        if (num_restarts == 0 || ((uint64_t)num_restarts * 4) > (it->block_size - 4)) {
            continue;
        }

        it->restarts_offset = it->block_size - 4 - (num_restarts * 4);
        it->block_ptr = 0;
    }
    return false;
}

void sstable_iter_get_kv(sstable_iter_t *it, const char **key, uint32_t *klen, const char **val, uint32_t *vlen) {
    *key = it->key;
    *klen = it->key_len;
    *val = it->val;
    *vlen = it->val_len;
}

void sstable_iter_get_meta(sstable_iter_t *it, uint64_t *seq, uint8_t *op) {
    if (it->key_len < 8) { *seq=0; *op=0; return; }
    uint64_t trailer = decode_u64_le((const uint8_t*)it->key + it->key_len - 8);
    *seq = trailer >> 8;
    *op = trailer & 0xFF;
}

void sstable_iter_destroy(sstable_iter_t *it) {
    if (!it) return;
    if (it->handle) {
        lsm_cache_handle_release(it->r->env->block_cache, it->handle);
    }
    aml_free(it);
}
