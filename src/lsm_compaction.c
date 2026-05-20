// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-table-store-library/lsm_compaction.h"
#include "a-memory-library/aml_alloc.h"
#include "the-lz4-library/xxhash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// --- Endianness Helpers ---
static inline void encode_u32_le(uint8_t *dst, uint32_t v) {
    dst[0] = v & 0xFF; dst[1] = (v >> 8) & 0xFF; dst[2] = (v >> 16) & 0xFF; dst[3] = (v >> 24) & 0xFF;
}
static inline uint32_t decode_u32_le(const uint8_t *src) {
    return (uint32_t)src[0] | ((uint32_t)src[1] << 8) | ((uint32_t)src[2] << 16) | ((uint32_t)src[3] << 24);
}
static inline void encode_u64_le(uint8_t *dst, uint64_t v) {
    for (int i = 0; i < 8; i++) dst[i] = (v >> (i * 8)) & 0xFF;
}
static inline uint64_t decode_u64_le(const uint8_t *src) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= ((uint64_t)src[i]) << (i * 8);
    return v;
}

static inline uint32_t get_user_key_len(uint32_t internal_key_len) {
    return internal_key_len > INTERNAL_KEY_TRAILER_SIZE ? internal_key_len - INTERNAL_KEY_TRAILER_SIZE : 0;
}

static void lsmc_generate_base_path(char *buf, const char *dir, uint64_t file_id) {
    snprintf(buf, 512, "%s/%06llu", dir, (unsigned long long)file_id);
}

static int lsmc_meta_cmp(const void *a, const void *b) {
    sstable_meta_t *m1 = *(sstable_meta_t **)a;
    sstable_meta_t *m2 = *(sstable_meta_t **)b;
    uint32_t ulen1 = get_user_key_len(m1->min_key_len);
    uint32_t ulen2 = get_user_key_len(m2->min_key_len);
    uint32_t min_len = ulen1 < ulen2 ? ulen1 : ulen2;
    int c = memcmp(m1->min_key, m2->min_key, min_len);
    if (c == 0) c = ulen1 < ulen2 ? -1 : (ulen1 > ulen2 ? 1 : 0);
    return c;
}

static bool lsmc_check_overlap(sstable_meta_t *a, sstable_meta_t *b) {
    uint32_t umaxA = get_user_key_len(a->max_key_len);
    uint32_t uminB = get_user_key_len(b->min_key_len);
    uint32_t min1 = umaxA < uminB ? umaxA : uminB;
    int c1 = memcmp(a->max_key, b->min_key, min1);
    if (c1 == 0) c1 = umaxA < uminB ? -1 : (umaxA > uminB ? 1 : 0);
    if (c1 < 0) return false;

    uint32_t uminA = get_user_key_len(a->min_key_len);
    uint32_t umaxB = get_user_key_len(b->max_key_len);
    uint32_t min2 = uminA < umaxB ? uminA : umaxB;
    int c2 = memcmp(a->min_key, b->max_key, min2);
    if (c2 == 0) c2 = uminA < umaxB ? -1 : (uminA > umaxB ? 1 : 0);
    if (c2 > 0) return false;

    return true;
}

typedef struct {
    const char *key;  uint32_t key_len;
    const char *val;  uint32_t val_len;
    uint64_t seq;     uint8_t op_type;
    int iterator_idx;
} heap_node_t;

typedef struct {
    heap_node_t *data;
    size_t size;
    size_t capacity;
} min_heap_t;

static int lsmc_heap_cmp(const heap_node_t *a, const heap_node_t *b) {
    uint32_t a_user_len = get_user_key_len(a->key_len);
    uint32_t b_user_len = get_user_key_len(b->key_len);
    uint32_t min_len = a_user_len < b_user_len ? a_user_len : b_user_len;
    int c = memcmp(a->key, b->key, min_len);
    if (c != 0) return c;
    if (a_user_len != b_user_len) return a_user_len < b_user_len ? -1 : 1;
    if (a->seq > b->seq) return -1;
    if (a->seq < b->seq) return 1;
    return 0;
}

static void lsmc_heap_push(min_heap_t *h, heap_node_t node) {
    h->data[h->size] = node;
    size_t i = h->size++;
    while (i > 0) {
        size_t parent = (i - 1) / 2;
        if (lsmc_heap_cmp(&h->data[i], &h->data[parent]) >= 0) break;
        heap_node_t tmp = h->data[i]; h->data[i] = h->data[parent]; h->data[parent] = tmp;
        i = parent;
    }
}

static heap_node_t lsmc_heap_pop(min_heap_t *h) {
    heap_node_t root = h->data[0];
    h->data[0] = h->data[--h->size];
    size_t i = 0;
    while (true) {
        size_t left = 2 * i + 1, right = 2 * i + 2, smallest = i;
        if (left < h->size && lsmc_heap_cmp(&h->data[left], &h->data[smallest]) < 0) smallest = left;
        if (right < h->size && lsmc_heap_cmp(&h->data[right], &h->data[smallest]) < 0) smallest = right;
        if (smallest == i) break;
        heap_node_t tmp = h->data[i]; h->data[i] = h->data[smallest]; h->data[smallest] = tmp;
        i = smallest;
    }
    return root;
}

static void replay_manifest(lsm_manifest_t *m, const char *path) {
    void *reader = m->env->router.hot_vfs->open_reader(path);
    if (!reader) return;

    uint64_t offset = 0;

    while (true) {
        uint8_t len_buf[4];
        if (m->env->router.hot_vfs->pread(reader, len_buf, 4, offset) != 4) break;
        uint32_t record_len = decode_u32_le(len_buf);
        offset += 4;

        // [Phase 3] Manifest size bounds checking (prevent memory attacks)
        if (record_len < 16 || record_len > 16 * 1024 * 1024) break;

        uint8_t *buf = aml_malloc(record_len);
        if (m->env->router.hot_vfs->pread(reader, buf, record_len, offset) != record_len) {
            aml_free(buf); break;
        }
        offset += record_len;

        uint8_t crc_buf[4];
        if (m->env->router.hot_vfs->pread(reader, crc_buf, 4, offset) != 4) {
            aml_free(buf); break;
        }
        offset += 4;

        if (XXH32(buf, record_len, 0) != decode_u32_le(crc_buf)) {
            aml_free(buf); break;
        }

        uint32_t src_lvl = decode_u32_le(buf);
        uint32_t tgt_lvl = decode_u32_le(buf + 4);
        uint32_t num_del = decode_u32_le(buf + 8);
        uint32_t num_add = decode_u32_le(buf + 12);
        uint64_t ptr = 16;

        // [Phase 3] Guard against impossible file counts
        if (num_del > 100000 || num_add > 100000) { aml_free(buf); break; }
        if (ptr + (num_del * 8) > record_len) { aml_free(buf); break; }

        sstable_meta_t **d_files = NULL;
        if (num_del > 0) {
            d_files = aml_malloc(num_del * sizeof(sstable_meta_t*));
            uint64_t *d_ids = aml_malloc(num_del * 8);
            for (uint32_t d = 0; d < num_del; d++) {
                d_ids[d] = decode_u64_le(buf + ptr); ptr += 8;
            }

            lsm_version_t *curr = m->current_version;
            for (uint32_t d = 0; d < num_del; d++) {
                d_files[d] = NULL;
                for (int lvl=0; lvl<MAX_LEVELS; lvl++) {
                    for (size_t f=0; f<curr->levels[lvl].num_files; f++) {
                        if (curr->levels[lvl].files[f]->file_id == d_ids[d]) {
                            d_files[d] = curr->levels[lvl].files[f];
                            break;
                        }
                    }
                }
            }
            aml_free(d_ids);
        }

        sstable_meta_t **a_files = NULL;
        bool parse_failed = false;

        if (num_add > 0) {
            a_files = aml_malloc(num_add * sizeof(sstable_meta_t*));
            for (uint32_t a = 0; a < num_add; a++) {
                // Minimum bytes required for a single file addition metadata structure (8+8+4+8 + 4 + 4 = 36)
                if (ptr + 36 > record_len) { parse_failed = true; break; }

                sstable_meta_t *meta = aml_zalloc(sizeof(sstable_meta_t));
                a_files[a] = meta;
                meta->ref_count = 0;
                meta->is_obsolete = false;

                pthread_mutex_init(&meta->reader_mutex, NULL);
                meta->cached_reader = NULL;

                meta->file_id = decode_u64_le(buf + ptr); ptr += 8;
                meta->file_size = decode_u64_le(buf + ptr); ptr += 8;
                meta->num_entries = decode_u32_le(buf + ptr); ptr += 4;
                meta->max_seq = decode_u64_le(buf + ptr); ptr += 8;

                meta->min_key_len = decode_u32_le(buf + ptr); ptr += 4;
                if (ptr + meta->min_key_len + 4 > record_len) { parse_failed = true; break; }

                meta->min_key = aml_malloc(meta->min_key_len);
                memcpy(meta->min_key, buf + ptr, meta->min_key_len); ptr += meta->min_key_len;

                meta->max_key_len = decode_u32_le(buf + ptr); ptr += 4;
                if (ptr + meta->max_key_len > record_len) { parse_failed = true; break; }

                meta->max_key = aml_malloc(meta->max_key_len);
                memcpy(meta->max_key, buf + ptr, meta->max_key_len); ptr += meta->max_key_len;

                if (meta->file_id >= m->next_file_id) {
                    m->next_file_id = meta->file_id + 1;
                }
            }
        }

        if (parse_failed) {
            // Memory bounds hit: safely cleanup and abort parsing
            if (d_files) aml_free(d_files);
            if (a_files) {
                for(uint32_t a=0; a<num_add; a++) {
                    if(a_files[a]) {
                        if(a_files[a]->min_key) aml_free(a_files[a]->min_key);
                        if(a_files[a]->max_key) aml_free(a_files[a]->max_key);
                        pthread_mutex_destroy(&a_files[a]->reader_mutex);
                        aml_free(a_files[a]);
                    }
                }
                aml_free(a_files);
            }
            aml_free(buf);
            break;
        }

        lsmc_version_edit(m, src_lvl, tgt_lvl, d_files, num_del, a_files, num_add);
        if (d_files) aml_free(d_files);
        if (a_files) aml_free(a_files);
        aml_free(buf);
    }
    m->env->router.hot_vfs->close_reader(reader);
}

lsm_manifest_t *lsmc_manifest_init(lsm_env_t *env, uint32_t table_id, const char *db_directory) {
    lsm_manifest_t *m = aml_zalloc(sizeof(lsm_manifest_t));
    m->db_directory = aml_strdup(db_directory);
    m->env = env;
    m->table_id = table_id;
    m->next_file_id = 1;
    pthread_mutex_init(&m->version_mutex, NULL);

    lsm_version_t *v0 = aml_zalloc(sizeof(lsm_version_t));
    v0->ref_count = 1;
    for (int i = 0; i < MAX_LEVELS; i++) {
        v0->levels[i].level_num = i;
        v0->levels[i].files_capacity = 16;
        v0->levels[i].files = aml_malloc(16 * sizeof(sstable_meta_t*));
    }
    m->current_version = v0;

    char m_path[520];
    snprintf(m_path, sizeof(m_path), "%s/CURRENT.manifest", db_directory);

    replay_manifest(m, m_path);

    m->manifest_writer = m->env->router.hot_vfs->open_appender(m_path);

    return m;
}

uint64_t lsmc_get_max_sequence(lsm_manifest_t *manifest) {
    uint64_t max_seq = 0;
    pthread_mutex_lock(&manifest->version_mutex);
    lsm_version_t *v = manifest->current_version;
    for (int lvl=0; lvl<MAX_LEVELS; lvl++) {
        for (size_t f=0; f<v->levels[lvl].num_files; f++) {
            if (v->levels[lvl].files[f]->max_seq > max_seq) {
                max_seq = v->levels[lvl].files[f]->max_seq;
            }
        }
    }
    pthread_mutex_unlock(&manifest->version_mutex);
    return max_seq;
}

lsm_version_t *lsmc_version_retain(lsm_manifest_t *manifest) {
    pthread_mutex_lock(&manifest->version_mutex);
    lsm_version_t *v = manifest->current_version;
    v->ref_count++;
    pthread_mutex_unlock(&manifest->version_mutex);
    return v;
}

void lsmc_version_release(lsm_manifest_t *manifest, lsm_version_t *v) {
    pthread_mutex_lock(&manifest->version_mutex);
    v->ref_count--;

    if (v->ref_count == 0) {
        char **paths_to_delete = aml_malloc(MAX_LEVELS * 16 * sizeof(char*));
        size_t num_paths = 0;
        size_t paths_cap = MAX_LEVELS * 16;

        lsm_storage_backend_t **vfs_for_path = aml_malloc(MAX_LEVELS * 16 * sizeof(void*));

        for (int i = 0; i < MAX_LEVELS; i++) {
            lsm_storage_backend_t *vfs = manifest->env->router.hot_vfs;
            if (i >= manifest->env->router.cold_storage_start_level) {
                vfs = manifest->env->router.cold_vfs;
            }

            for (size_t f = 0; f < v->levels[i].num_files; f++) {
                sstable_meta_t *meta = v->levels[i].files[f];
                meta->ref_count--;

                if (meta->ref_count == 0) {
                    // [Phase 5B Fix] Destroy the cached reader upon metadata death
                    if (meta->cached_reader) {
                        sstable_reader_destroy(meta->cached_reader);
                    }
                    pthread_mutex_destroy(&meta->reader_mutex);

                    if (meta->is_obsolete) {
                        char base_path[512];
                        lsmc_generate_base_path(base_path, manifest->db_directory, meta->file_id);

                        if (num_paths + 2 > paths_cap) {
                            paths_cap *= 2;
                            paths_to_delete = aml_realloc(paths_to_delete, paths_cap * sizeof(char*));
                            vfs_for_path = aml_realloc(vfs_for_path, paths_cap * sizeof(void*));
                        }

                        char *data_path = aml_malloc(520);
                        snprintf(data_path, 520, "%s.data", base_path);
                        paths_to_delete[num_paths] = data_path;
                        vfs_for_path[num_paths] = vfs;
                        num_paths++;

                        char *meta_path = aml_malloc(520);
                        snprintf(meta_path, 520, "%s.meta", base_path);
                        paths_to_delete[num_paths] = meta_path;
                        vfs_for_path[num_paths] = vfs;
                        num_paths++;
                    }
                    if (meta->min_key) aml_free(meta->min_key);
                    if (meta->max_key) aml_free(meta->max_key);
                    aml_free(meta);
                }
            }
            aml_free(v->levels[i].files);
        }
        aml_free(v);

        pthread_mutex_unlock(&manifest->version_mutex);

        for (size_t p = 0; p < num_paths; p++) {
            vfs_for_path[p]->delete_file(paths_to_delete[p]);
            aml_free(paths_to_delete[p]);
        }
        aml_free(paths_to_delete);
        aml_free(vfs_for_path);

    } else {
        pthread_mutex_unlock(&manifest->version_mutex);
    }
}

bool lsmc_version_edit(lsm_manifest_t *manifest, int source_level, int target_level,
                       sstable_meta_t **deleted_files, size_t num_deleted,
                       sstable_meta_t **added_files, size_t num_added) {
    pthread_mutex_lock(&manifest->version_mutex);

    if (manifest->manifest_writer) {
        uint32_t record_len = 16 + (num_deleted * 8);
        for (size_t a = 0; a < num_added; a++) {
            record_len += 8 + 8 + 4 + 8 + 4 + added_files[a]->min_key_len + 4 + added_files[a]->max_key_len;
        }

        uint8_t *buf = aml_malloc(record_len);
        size_t ptr = 0;
        encode_u32_le(buf + ptr, source_level); ptr += 4;
        encode_u32_le(buf + ptr, target_level); ptr += 4;
        encode_u32_le(buf + ptr, num_deleted); ptr += 4;
        encode_u32_le(buf + ptr, num_added); ptr += 4;

        for (size_t d = 0; d < num_deleted; d++) {
            encode_u64_le(buf + ptr, deleted_files[d]->file_id); ptr += 8;
        }

        for (size_t a = 0; a < num_added; a++) {
            encode_u64_le(buf + ptr, added_files[a]->file_id); ptr += 8;
            encode_u64_le(buf + ptr, added_files[a]->file_size); ptr += 8;
            encode_u32_le(buf + ptr, added_files[a]->num_entries); ptr += 4;
            encode_u64_le(buf + ptr, added_files[a]->max_seq); ptr += 8;

            encode_u32_le(buf + ptr, added_files[a]->min_key_len); ptr += 4;
            memcpy(buf + ptr, added_files[a]->min_key, added_files[a]->min_key_len); ptr += added_files[a]->min_key_len;

            encode_u32_le(buf + ptr, added_files[a]->max_key_len); ptr += 4;
            memcpy(buf + ptr, added_files[a]->max_key, added_files[a]->max_key_len); ptr += added_files[a]->max_key_len;
        }

        uint32_t crc = XXH32(buf, record_len, 0);
        uint8_t wrap[4];

        // [Phase 7] Transactional Exact-Byte Checking for Manifest IO
        bool ok = true;
        encode_u32_le(wrap, record_len);
        if (manifest->env->router.hot_vfs->append(manifest->manifest_writer, wrap, 4) != 4) ok = false;
        if (ok && manifest->env->router.hot_vfs->append(manifest->manifest_writer, buf, record_len) != (ssize_t)record_len) ok = false;
        encode_u32_le(wrap, crc);
        if (ok && manifest->env->router.hot_vfs->append(manifest->manifest_writer, wrap, 4) != 4) ok = false;
        if (ok && manifest->env->router.hot_vfs->fsync_file(manifest->manifest_writer) < 0) ok = false;

        aml_free(buf);

        if (!ok) {
            // Memory state remains untainted, safely return false
            pthread_mutex_unlock(&manifest->version_mutex);
            return false;
        }
    }

    lsm_version_t *v = aml_zalloc(sizeof(lsm_version_t));
    v->ref_count = 1;
    lsm_version_t *curr = manifest->current_version;

    for (int i = 0; i < MAX_LEVELS; i++) {
        v->levels[i].level_num = i;
        size_t cap = curr->levels[i].num_files + num_added;
        v->levels[i].files_capacity = cap < 16 ? 16 : cap;
        v->levels[i].files = aml_malloc(v->levels[i].files_capacity * sizeof(sstable_meta_t*));

        for (size_t f = 0; f < curr->levels[i].num_files; f++) {
            sstable_meta_t *meta = curr->levels[i].files[f];
            bool is_deleted = false;
            if (i == source_level || i == target_level) {
                for (size_t d = 0; d < num_deleted; d++) {
                    if (deleted_files[d] && deleted_files[d]->file_id == meta->file_id) {
                        is_deleted = true;
                        deleted_files[d]->is_obsolete = true;
                        break;
                    }
                }
            }
            if (!is_deleted) {
                v->levels[i].files[v->levels[i].num_files++] = meta;
                meta->ref_count++;
            }
        }

        if (i == target_level) {
            for (size_t a = 0; a < num_added; a++) {
                sstable_meta_t *meta = added_files[a];
                meta->ref_count = 1;
                meta->is_obsolete = false;
                v->levels[i].files[v->levels[i].num_files++] = meta;
            }
            if (i > 0) {
                qsort(v->levels[i].files, v->levels[i].num_files, sizeof(sstable_meta_t*), lsmc_meta_cmp);
            }
        }
    }

    double best_score = -1.0;
    int best_level = -1;

    for (int i = 0; i < MAX_LEVELS - 1; i++) {
        double score = 0.0;
        if (i == 0) {
            score = (double)v->levels[0].num_files / 4.0;
        } else {
            uint64_t level_bytes = 0;
            for (size_t f = 0; f < v->levels[i].num_files; f++) {
                level_bytes += v->levels[i].files[f]->file_size;
            }
            uint64_t max_bytes = 10 * 1048576ULL;
            for (int j = 1; j < i; j++) {
                max_bytes *= 10;
            }
            score = (double)level_bytes / (double)max_bytes;
        }

        if (score > best_score) {
            best_score = score;
            best_level = i;
        }
    }

    v->compaction_score = best_score;
    v->compaction_level = best_level;

    lsm_version_t *old_version = manifest->current_version;
    manifest->current_version = v;
    pthread_mutex_unlock(&manifest->version_mutex);

    lsmc_version_release(manifest, old_version);
    return true;
}

bool lsmc_compact_level(lsm_manifest_t *manifest, int source_level, uint64_t oldest_snapshot) {
    if (source_level >= MAX_LEVELS - 1) return false;

    lsm_version_t *v = lsmc_version_retain(manifest);

    lsm_level_t *L_source = &v->levels[source_level];
    lsm_level_t *L_target = &v->levels[source_level + 1];

    if (L_source->num_files == 0) {
        lsmc_version_release(manifest, v);
        return false;
    }

    sstable_meta_t **source_inputs = aml_malloc(L_source->num_files * sizeof(sstable_meta_t*));
    size_t num_source_inputs = 0;

    int start_idx = 0;

    if (source_level > 0 && manifest->compaction_pointer_lens[source_level] > 0) {
        for (size_t i = 0; i < L_source->num_files; i++) {
            sstable_meta_t *m = L_source->files[i];
            uint32_t umax = get_user_key_len(m->max_key_len);
            uint32_t min_len = umax < manifest->compaction_pointer_lens[source_level] ? umax : manifest->compaction_pointer_lens[source_level];
            int cmp = memcmp(m->max_key, manifest->compaction_pointers[source_level], min_len);
            if (cmp == 0) cmp = umax < manifest->compaction_pointer_lens[source_level] ? -1 : (umax > manifest->compaction_pointer_lens[source_level] ? 1 : 0);

            if (cmp > 0) {
                start_idx = i;
                break;
            }
        }
    }

    source_inputs[num_source_inputs++] = L_source->files[start_idx];

    if (source_level == 0) {
        bool expanded;
        do {
            expanded = false;
            for (size_t i = 0; i < L_source->num_files; i++) {
                sstable_meta_t *cand = L_source->files[i];

                bool already_in = false;
                for(size_t j=0; j<num_source_inputs; j++) {
                    if(source_inputs[j] == cand) { already_in = true; break; }
                }
                if (already_in) continue;

                bool overlaps = false;
                for(size_t j=0; j<num_source_inputs; j++) {
                    if (lsmc_check_overlap(cand, source_inputs[j])) {
                        overlaps = true;
                        break;
                    }
                }
                if (overlaps) {
                    source_inputs[num_source_inputs++] = cand;
                    expanded = true;
                }
            }
        } while (expanded);
    }

    // [Phase 4] Accurate Pointers: Identify the true max_key AFTER the expansion loop completes
    sstable_meta_t *max_cand = source_inputs[0];
    for (size_t i = 1; i < num_source_inputs; i++) {
        uint32_t ulen1 = get_user_key_len(max_cand->max_key_len);
        uint32_t ulen2 = get_user_key_len(source_inputs[i]->max_key_len);
        uint32_t min_len = ulen1 < ulen2 ? ulen1 : ulen2;
        int c = memcmp(max_cand->max_key, source_inputs[i]->max_key, min_len);
        if (c == 0) c = ulen1 < ulen2 ? -1 : (ulen1 > ulen2 ? 1 : 0);
        if (c < 0) max_cand = source_inputs[i];
    }
    uint32_t ptr_len = get_user_key_len(max_cand->max_key_len);
    char *next_ptr = aml_malloc(ptr_len);
    memcpy(next_ptr, max_cand->max_key, ptr_len);

    sstable_meta_t **merge_inputs = aml_malloc((num_source_inputs + L_target->num_files) * sizeof(sstable_meta_t*));
    size_t num_inputs = 0;

    for (size_t i = 0; i < num_source_inputs; i++) {
        merge_inputs[num_inputs++] = source_inputs[i];
    }

    for (size_t i = 0; i < L_target->num_files; i++) {
        bool overlaps = false;
        for (size_t j = 0; j < num_source_inputs; j++) {
            if (lsmc_check_overlap(source_inputs[j], L_target->files[i])) {
                overlaps = true;
                break;
            }
        }
        if (overlaps) {
            merge_inputs[num_inputs++] = L_target->files[i];
        }
    }

    aml_free(source_inputs);

    sstable_iter_t **iters = aml_malloc(num_inputs * sizeof(sstable_iter_t*));
    char filepath[512];

    min_heap_t heap = { .data = aml_malloc(num_inputs * sizeof(heap_node_t)), .size = 0, .capacity = num_inputs };

    for (size_t i = 0; i < num_inputs; i++) {
        sstable_meta_t *meta = merge_inputs[i];
        int file_level = (i < num_source_inputs) ? source_level : (source_level + 1);
        lsm_storage_backend_t *vfs = manifest->env->router.hot_vfs;
        if (file_level >= manifest->env->router.cold_storage_start_level) {
            vfs = manifest->env->router.cold_vfs;
        }

        sstable_reader_t *r = __atomic_load_n(&meta->cached_reader, __ATOMIC_ACQUIRE);
        if (!r) {
            pthread_mutex_lock(&meta->reader_mutex);
            r = __atomic_load_n(&meta->cached_reader, __ATOMIC_ACQUIRE);
            if (!r) {
                lsmc_generate_base_path(filepath, manifest->db_directory, meta->file_id);
                r = sstable_reader_init(filepath, vfs, manifest->env, manifest->table_id, meta->file_id);
                __atomic_store_n(&meta->cached_reader, r, __ATOMIC_RELEASE);
            }
            pthread_mutex_unlock(&meta->reader_mutex);
        }

        if (!r) {
            for (size_t j = 0; j < i; j++) { sstable_iter_destroy(iters[j]); }
            aml_free(merge_inputs); aml_free(iters); aml_free(heap.data); aml_free(next_ptr);
            lsmc_version_release(manifest, v);
            return false;
        }

        iters[i] = sstable_iter_init(r);
        if (sstable_iter_next(iters[i])) {
            heap_node_t node;
            sstable_iter_get_kv(iters[i], &node.key, &node.key_len, &node.val, &node.val_len);
            sstable_iter_get_meta(iters[i], &node.seq, &node.op_type);
            node.iterator_idx = i;
            lsmc_heap_push(&heap, node);
        }
    }

    size_t new_files_cap = 1024;
    sstable_meta_t **new_files = aml_zalloc(new_files_cap * sizeof(sstable_meta_t*));
    size_t num_allocated_new_files = 0;
    size_t num_new_files = 0;
    sstable_builder_t *builder = NULL;

    lsm_storage_backend_t *target_vfs = manifest->env->router.hot_vfs;
    if (source_level + 1 >= manifest->env->router.cold_storage_start_level) {
        target_vfs = manifest->env->router.cold_vfs;
    }

    pthread_mutex_lock(&manifest->version_mutex);
    uint64_t current_out_id = manifest->next_file_id++;
    pthread_mutex_unlock(&manifest->version_mutex);

    char last_user_key[MAX_INTERNAL_KEY_SIZE];
    uint32_t last_user_key_len = 0;
    bool is_bottom_level = (source_level + 1 == MAX_LEVELS - 1);

    uint64_t last_kept_seq = UINT64_MAX;
    bool compaction_failed = false; // [Phase 4] Fail-safe tracking

    while (heap.size > 0) {
        heap_node_t top = lsmc_heap_pop(&heap);
        uint32_t top_user_len = get_user_key_len(top.key_len);
        bool same_user_key = (last_user_key_len == top_user_len && memcmp(last_user_key, top.key, top_user_len) == 0);

        bool drop = false;
        if (!same_user_key) {
            memcpy(last_user_key, top.key, top_user_len);
            last_user_key_len = top_user_len;
            last_kept_seq = top.seq;

            if (top.op_type == 1 && is_bottom_level && top.seq <= oldest_snapshot) {
                drop = true;
            }
        } else {
            if (last_kept_seq <= oldest_snapshot) {
                drop = true;
            } else {
                last_kept_seq = top.seq;
                if (top.op_type == 1 && is_bottom_level && top.seq <= oldest_snapshot) {
                    drop = true;
                }
            }
        }

        if (!drop) {
            if (builder == NULL) {
                lsmc_generate_base_path(filepath, manifest->db_directory, current_out_id);
                builder = sstable_builder_init(filepath, target_vfs, FILTER_BLOOM, 20000);

                if (num_allocated_new_files >= new_files_cap) {
                    new_files_cap *= 2;
                    new_files = aml_realloc(new_files, new_files_cap * sizeof(sstable_meta_t*));
                    // Zero the newly allocated portion
                    memset(new_files + num_allocated_new_files, 0, (new_files_cap - num_allocated_new_files) * sizeof(sstable_meta_t*));
                }

                new_files[num_allocated_new_files] = aml_zalloc(sizeof(sstable_meta_t));
                new_files[num_allocated_new_files]->file_id = current_out_id;
                pthread_mutex_init(&new_files[num_allocated_new_files]->reader_mutex, NULL);
                new_files[num_allocated_new_files]->cached_reader = NULL;

                new_files[num_allocated_new_files]->min_key = aml_malloc(top.key_len);
                memcpy(new_files[num_allocated_new_files]->min_key, top.key, top.key_len);
                new_files[num_allocated_new_files]->min_key_len = top.key_len;
                num_allocated_new_files++;
            }

            if (top.seq > new_files[num_new_files]->max_seq) {
                new_files[num_new_files]->max_seq = top.seq;
            }

            if (!sstable_builder_add(builder, top.key, top.key_len, top.val, top.val_len)) {
                compaction_failed = true;
                break;
            }

            if (new_files[num_new_files]->max_key) aml_free(new_files[num_new_files]->max_key);
            new_files[num_new_files]->max_key = aml_malloc(top.key_len);
            memcpy(new_files[num_new_files]->max_key, top.key, top.key_len);
            new_files[num_new_files]->max_key_len = top.key_len;

            new_files[num_new_files]->num_entries++;

            if (sstable_builder_current_size(builder) >= TARGET_FILE_SIZE) {
                uint64_t fsz = sstable_builder_finish(builder);
                builder = NULL;
                if (fsz == 0) {
                    compaction_failed = true;
                    break;
                }

                new_files[num_new_files]->file_size = fsz;
                num_new_files++;

                pthread_mutex_lock(&manifest->version_mutex);
                current_out_id = manifest->next_file_id++;
                pthread_mutex_unlock(&manifest->version_mutex);
            }
        }

        int idx = top.iterator_idx;
        if (sstable_iter_next(iters[idx])) {
            heap_node_t next_node;
            sstable_iter_get_kv(iters[idx], &next_node.key, &next_node.key_len, &next_node.val, &next_node.val_len);
            sstable_iter_get_meta(iters[idx], &next_node.seq, &next_node.op_type);
            next_node.iterator_idx = idx;
            lsmc_heap_push(&heap, next_node);
        }
    }

    if (!compaction_failed && builder != NULL) {
        uint64_t fsz = sstable_builder_finish(builder);
        builder = NULL;
        if (fsz == 0) {
            compaction_failed = true;
        } else {
            new_files[num_new_files]->file_size = fsz;
            num_new_files++;
        }
    }

    for (size_t i = 0; i < num_inputs; i++) {
        sstable_iter_destroy(iters[i]);
    }

    // [Phase 4] Safe Orphan Cleanup on I/O Errors
    if (compaction_failed || !lsmc_version_edit(manifest, source_level, source_level + 1, merge_inputs, num_inputs, new_files, num_new_files)) {
        if (builder) sstable_builder_abort(builder);

        for (size_t i = 0; i < num_allocated_new_files; i++) {
            if (i < num_new_files) {
                // If it succeeded previously but the later batches or manifest failed, shred the disk evidence
                char base[512], datapath[520], metapath[520];
                lsmc_generate_base_path(base, manifest->db_directory, new_files[i]->file_id);
                snprintf(datapath, 520, "%s.data", base);
                snprintf(metapath, 520, "%s.meta", base);
                target_vfs->delete_file(datapath);
                target_vfs->delete_file(metapath);
            }
            if (new_files[i]->min_key) aml_free(new_files[i]->min_key);
            if (new_files[i]->max_key) aml_free(new_files[i]->max_key);
            pthread_mutex_destroy(&new_files[i]->reader_mutex);
            aml_free(new_files[i]);
        }
        aml_free(merge_inputs); aml_free(iters); aml_free(heap.data);
        aml_free(new_files); aml_free(next_ptr);
        lsmc_version_release(manifest, v);
        return false;
    }

    pthread_mutex_lock(&manifest->version_mutex);
    if (manifest->compaction_pointers[source_level]) {
        aml_free(manifest->compaction_pointers[source_level]);
    }
    manifest->compaction_pointers[source_level] = next_ptr;
    manifest->compaction_pointer_lens[source_level] = ptr_len;
    pthread_mutex_unlock(&manifest->version_mutex);

    lsmc_version_release(manifest, v);

    aml_free(merge_inputs); aml_free(iters); aml_free(heap.data); aml_free(new_files);
    return true;
}
