// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "a-table-store-library/sstable_reader.h"
#include "a-table-store-library/sstable_builder.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "the-lz4-library/lz4/lz4.h"
#include "the-lz4-library/xxhash.h"
#include "a-bloom-filter-library/bloom.h"
#include "a-memory-library/aml_alloc.h"

#define COMPRESS_LZ4 1
#define TRAILER_SIZE 8
#define MAX_KEY_SIZE 1024
#define MAX_INTERNAL_KEY_SIZE (MAX_KEY_SIZE + TRAILER_SIZE)

// --- Endianness Helpers ---
static inline uint32_t decode_u32_le(const uint8_t *src) {
    return (uint32_t)src[0] | ((uint32_t)src[1] << 8) | ((uint32_t)src[2] << 16) | ((uint32_t)src[3] << 24);
}
static inline uint64_t decode_u64_le(const uint8_t *src) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= ((uint64_t)src[i]) << (i * 8);
    return v;
}

typedef struct {
    char *key;
    uint32_t key_len;
    uint64_t offset;
    uint64_t size;
} index_entry_t;

struct sstable_reader_s {
    lsm_storage_backend_t *backend;
    void *data_reader;
    lsm_env_t *env;
    uint32_t table_id;
    uint64_t file_id;

    int filter_type;
    bloom_t *bloom;
    uint8_t *bitmap;
    size_t bitmap_cap;
    uint64_t min_bitmap_id;

    index_entry_t *index;
    uint32_t num_index_entries;
};

static inline uint32_t decode_varint32(const uint8_t **ptr, const uint8_t *end) {
    uint32_t result = 0; int shift = 0;
    while (*ptr < end) {
        uint8_t byte = **ptr; (*ptr)++;
        result |= (byte & 127) << shift;
        if (!(byte & 128)) break;
        shift += 7;
    }
    return result;
}

sstable_reader_t *sstable_reader_init(const char *base_path, lsm_storage_backend_t *backend, lsm_env_t *env, uint32_t table_id, uint64_t file_id) {
    sstable_reader_t *r = aml_zalloc(sizeof(sstable_reader_t));
    r->backend = backend;
    r->env = env;
    r->table_id = table_id;
    r->file_id = file_id;

    char data_path[520]; snprintf(data_path, sizeof(data_path), "%s.data", base_path);
    r->data_reader = backend->open_reader(data_path);
    if (!r->data_reader) { aml_free(r); return NULL; }

    char meta_path[520]; snprintf(meta_path, sizeof(meta_path), "%s.meta", base_path);
    void *meta_reader = backend->open_reader(meta_path);
    if (!meta_reader) { backend->close_reader(r->data_reader); aml_free(r); return NULL; }

    uint64_t current_offset = 0;
    uint8_t header[9];
    backend->pread(meta_reader, header, 9, current_offset);
    current_offset += 9;

    if (memcmp(header, "META", 4) != 0) {
        backend->close_reader(meta_reader); backend->close_reader(r->data_reader); aml_free(r); return NULL;
    }

    r->filter_type = header[4];
    uint32_t filter_len = decode_u32_le(header + 5);

    if (filter_len > 0) {
        uint8_t *filter_data = aml_malloc(filter_len);
        backend->pread(meta_reader, filter_data, filter_len, current_offset);
        current_offset += filter_len;

        if (r->filter_type == FILTER_BLOOM) {
            r->bloom = aml_zalloc(sizeof(bloom_t));
            r->bloom->num_bits = decode_u64_le(filter_data);
            r->bloom->num_hashes = decode_u64_le(filter_data + 8);
            size_t bits_bytes = (r->bloom->num_bits + 7) / 8;
            r->bloom->bits = aml_malloc(bits_bytes);
            memcpy(r->bloom->bits, filter_data + 16, bits_bytes);
        } else if (r->filter_type == FILTER_BITMAP) {
            r->min_bitmap_id = decode_u64_le(filter_data);
            r->bitmap_cap = filter_len - 8;
            r->bitmap = aml_malloc(r->bitmap_cap);
            memcpy(r->bitmap, filter_data + 8, r->bitmap_cap);
        }
        aml_free(filter_data);
    }

    uint8_t idx_sz_buf[4];
    backend->pread(meta_reader, idx_sz_buf, 4, current_offset);
    current_offset += 4;
    uint32_t idx_len = decode_u32_le(idx_sz_buf);

    uint8_t *idx_data = aml_malloc(idx_len);
    backend->pread(meta_reader, idx_data, idx_len, current_offset);
    backend->close_reader(meta_reader);

    size_t ptr = 0;
    uint32_t cap = 1024;
    r->index = aml_malloc(cap * sizeof(index_entry_t));
    r->num_index_entries = 0;

    while (ptr < idx_len) {
        if (r->num_index_entries >= cap) {
            cap *= 2;
            r->index = aml_realloc(r->index, cap * sizeof(index_entry_t));
        }
        index_entry_t *ie = &r->index[r->num_index_entries++];
        ie->key_len = decode_u32_le(idx_data + ptr); ptr += 4;
        ie->key = aml_malloc(ie->key_len);
        memcpy(ie->key, idx_data + ptr, ie->key_len); ptr += ie->key_len;
        ie->offset = decode_u64_le(idx_data + ptr); ptr += 8;
        ie->size = decode_u64_le(idx_data + ptr); ptr += 8;
    }

    aml_free(idx_data);
    return r;
}

int sstable_reader_get(sstable_reader_t *r, const void *key, uint32_t key_len, uint64_t read_seq_num, void **out_val, uint32_t *out_val_len) {
    uint32_t u_len = key_len;
    if (u_len > MAX_KEY_SIZE) return 0;

    if (r->filter_type == 1 && !bloom_check(r->bloom, key, u_len)) return 0;

    if (r->filter_type == 2 && u_len >= 8) {
        uint64_t current_id;
        memcpy(&current_id, key, 8);
        if (current_id < r->min_bitmap_id) return 0;
        uint64_t diff = current_id - r->min_bitmap_id;
        size_t byte_idx = diff / 8;
        if (byte_idx >= r->bitmap_cap || !(r->bitmap[byte_idx] & (1 << (diff % 8)))) return 0;
    }

    int left = 0, right = r->num_index_entries - 1, target_idx = -1;
    while (left <= right) {
        int mid = left + (right - left) / 2;
        uint32_t indexed_user_len = r->index[mid].key_len > TRAILER_SIZE ? r->index[mid].key_len - TRAILER_SIZE : 0;
        uint32_t min_len = key_len < indexed_user_len ? key_len : indexed_user_len;

        int cmp = memcmp(key, r->index[mid].key, min_len);
        if (cmp == 0) cmp = (key_len < indexed_user_len) ? -1 : (key_len > indexed_user_len ? 1 : 0);

        if (cmp <= 0) {
            target_idx = mid;
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }

    if (target_idx == -1) return 0;
    index_entry_t *idx = &r->index[target_idx];

    void *cached_block = lsm_cache_get(r->env->block_cache, r->table_id, r->file_id, idx->offset, NULL);
    if (!cached_block) {
        uint8_t *disk_buf = aml_malloc(idx->size);
        r->backend->pread(r->data_reader, disk_buf, idx->size, idx->offset);
        cached_block = lsm_cache_put_or_get(r->env->block_cache, r->table_id, r->file_id, idx->offset, disk_buf, idx->size);
    }

    uint8_t *disk_buf = (uint8_t *)cached_block;

    uint32_t uncomp_size = decode_u32_le(&disk_buf[idx->size - 9]);
    uint8_t flag = disk_buf[idx->size - 5];
    uint32_t file_crc = decode_u32_le(&disk_buf[idx->size - 4]);

    if (XXH32(disk_buf, idx->size - 9, 0) != file_crc) {
        lsm_cache_release(r->env->block_cache, r->table_id, r->file_id, idx->offset);
        return 0;
    }

    uint8_t *block = disk_buf;
    size_t block_size = uncomp_size;
    uint8_t *decomp_buf = NULL;

    if (flag == COMPRESS_LZ4) {
        decomp_buf = aml_malloc(uncomp_size);
        int decomp_size = LZ4_decompress_safe((const char*)disk_buf, (char*)decomp_buf, idx->size - 9, uncomp_size);
        if (decomp_size < 0 || (uint32_t)decomp_size != uncomp_size) {
            aml_free(decomp_buf);
            lsm_cache_release(r->env->block_cache, r->table_id, r->file_id, idx->offset);
            return 0;
        }
        block = decomp_buf;
    }

    uint32_t num_restarts = decode_u32_le(&block[block_size - 4]);
    uint32_t restarts_offset = block_size - 4 - (num_restarts * 4);
    const uint8_t *end = block + restarts_offset;

    uint32_t target_restart = 0;
    int r_left = 0, r_right = (int)num_restarts - 1;
    while (r_left <= r_right && num_restarts > 0) {
        int r_mid = r_left + (r_right - r_left) / 2;
        uint32_t offset = decode_u32_le(&block[restarts_offset + r_mid * 4]);
        const uint8_t *p = block + offset;

        uint32_t shared = decode_varint32(&p, end);
        uint32_t unshared = decode_varint32(&p, end);
        uint32_t vlen = decode_varint32(&p, end);

        (void)shared;
        (void)vlen;

        uint32_t user_len = unshared > TRAILER_SIZE ? unshared - TRAILER_SIZE : 0;
        uint32_t min_len = key_len < user_len ? key_len : user_len;

        int cmp = memcmp(key, p, min_len);
        if (cmp == 0) cmp = (key_len < user_len) ? -1 : (key_len > user_len ? 1 : 0);

        if (cmp < 0) {
            r_right = r_mid - 1;
        } else {
            target_restart = r_mid;
            r_left = r_mid + 1;
        }
    }

    uint32_t offset = decode_u32_le(&block[restarts_offset + target_restart * 4]);
    const uint8_t *ptr = block + offset;

    char current_key[MAX_INTERNAL_KEY_SIZE];
    uint32_t current_key_len = 0;
    int status = 0;

    while (ptr < end) {
        uint32_t shared = decode_varint32(&ptr, end);
        uint32_t unshared = decode_varint32(&ptr, end);
        uint32_t vlen = decode_varint32(&ptr, end);

        // [Phase 1 Fix] Protect the scan logic from out of bound reads
        if (shared + unshared > MAX_INTERNAL_KEY_SIZE || ptr + unshared + vlen > end) {
            break;
        }

        memcpy(current_key + shared, ptr, unshared); ptr += unshared;
        current_key_len = shared + unshared;

        uint32_t current_user_len = current_key_len > TRAILER_SIZE ? current_key_len - TRAILER_SIZE : 0;
        uint32_t min_len = key_len < current_user_len ? key_len : current_user_len;

        int cmp = memcmp(key, current_key, min_len);
        if (cmp == 0) cmp = (key_len < current_user_len) ? -1 : (key_len > current_user_len ? 1 : 0);

        if (cmp == 0) {
            const uint8_t *t = (const uint8_t *)(current_key + current_user_len);
            uint64_t packed = decode_u64_le(t);
            uint64_t seq = packed >> 8;
            uint8_t op = (uint8_t)(packed & 0xFF);

            if (seq <= read_seq_num) {
                if (op == 1) {
                    status = -1;
                } else {
                    *out_val = aml_malloc(vlen);
                    memcpy(*out_val, ptr, vlen);
                    if (out_val_len) *out_val_len = vlen;
                    status = 1;
                }
                break;
            }
        } else if (cmp < 0) break;
        ptr += vlen;
    }

    if (decomp_buf) aml_free(decomp_buf);
    lsm_cache_release(r->env->block_cache, r->table_id, r->file_id, idx->offset);
    return status;
}

void sstable_reader_destroy(sstable_reader_t *r) {
    if (!r) return;
    r->backend->close_reader(r->data_reader);
    if (r->bloom) bloom_destroy(r->bloom);
    if (r->bitmap) aml_free(r->bitmap);
    for (uint32_t i = 0; i < r->num_index_entries; i++) aml_free(r->index[i].key);
    aml_free(r->index);
    aml_free(r);
}

struct sstable_iter_s {
    sstable_reader_t *reader;
    uint32_t current_block_idx;
    uint64_t cached_offset;

    uint8_t *current_block_buf;
    size_t current_block_size;
    bool owns_block_buf;

    const uint8_t *ptr;
    const uint8_t *end;
    char current_key[MAX_INTERNAL_KEY_SIZE];
    uint32_t current_key_len;
    const char *current_val;
    uint32_t current_val_len;
};

static bool iter_load_next_block(sstable_iter_t *iter) {
    if (iter->current_block_idx >= iter->reader->num_index_entries) return false;

    if (iter->cached_offset != UINT64_MAX) {
        lsm_cache_release(iter->reader->env->block_cache, iter->reader->table_id, iter->reader->file_id, iter->cached_offset);
        iter->cached_offset = UINT64_MAX;
    }

    index_entry_t *idx = &iter->reader->index[iter->current_block_idx++];

    void *cached_block = lsm_cache_get(iter->reader->env->block_cache, iter->reader->table_id, iter->reader->file_id, idx->offset, NULL);
    if (!cached_block) {
        uint8_t *disk_buf = aml_malloc(idx->size);
        iter->reader->backend->pread(iter->reader->data_reader, disk_buf, idx->size, idx->offset);
        cached_block = lsm_cache_put_or_get(iter->reader->env->block_cache, iter->reader->table_id, iter->reader->file_id, idx->offset, disk_buf, idx->size);
    }

    iter->cached_offset = idx->offset;
    uint8_t *disk_buf = (uint8_t *)cached_block;

    uint32_t uncomp_size = decode_u32_le(&disk_buf[idx->size - 9]);
    uint8_t flag = disk_buf[idx->size - 5];
    uint32_t file_crc = decode_u32_le(&disk_buf[idx->size - 4]);

    if (XXH32(disk_buf, idx->size - 9, 0) != file_crc) {
        return false;
    }

    if (iter->owns_block_buf && iter->current_block_buf) {
        aml_free(iter->current_block_buf);
    }
    iter->current_block_buf = NULL;

    if (flag == COMPRESS_LZ4) {
        iter->current_block_buf = aml_malloc(uncomp_size);
        iter->current_block_size = LZ4_decompress_safe((const char*)disk_buf, (char*)iter->current_block_buf, idx->size - 9, uncomp_size);
        iter->owns_block_buf = true;
    } else {
        iter->current_block_buf = disk_buf;
        iter->current_block_size = uncomp_size;
        iter->owns_block_buf = false;
    }

    uint32_t num_restarts = decode_u32_le(&iter->current_block_buf[iter->current_block_size - 4]);
    uint32_t restarts_offset = iter->current_block_size - 4 - (num_restarts * 4);

    iter->ptr = iter->current_block_buf;
    iter->end = iter->current_block_buf + restarts_offset;
    return true;
}

sstable_iter_t *sstable_iter_init(sstable_reader_t *reader) {
    sstable_iter_t *iter = aml_zalloc(sizeof(sstable_iter_t));
    iter->reader = reader;
    iter->cached_offset = UINT64_MAX;
    iter->owns_block_buf = false;
    return iter;
}

bool sstable_iter_next(sstable_iter_t *iter) {
    while (!iter->ptr || iter->ptr >= iter->end) {
        if (!iter_load_next_block(iter)) return false;
    }

    uint32_t shared = decode_varint32(&iter->ptr, iter->end);
    uint32_t unshared = decode_varint32(&iter->ptr, iter->end);
    uint32_t vlen = decode_varint32(&iter->ptr, iter->end);

    // [Phase 1 Fix] Protect against buffer overruns from torn / corrupt SSTables
    if (shared + unshared > MAX_INTERNAL_KEY_SIZE || iter->ptr + unshared + vlen > iter->end) {
        return false;
    }

    memcpy(iter->current_key + shared, iter->ptr, unshared);
    iter->ptr += unshared;
    iter->current_key_len = shared + unshared;

    iter->current_val = (const char *)iter->ptr;
    iter->current_val_len = vlen;
    iter->ptr += vlen;
    return true;
}

void sstable_iter_get_kv(sstable_iter_t *iter, const char **key, uint32_t *klen, const char **val, uint32_t *vlen) {
    *key = iter->current_key; *klen = iter->current_key_len;
    *val = iter->current_val; *vlen = iter->current_val_len;
}

void sstable_iter_get_meta(sstable_iter_t *iter, uint64_t *seq, uint8_t *op) {
    if (iter->current_key_len >= 8) {
        const uint8_t *t = (const uint8_t*)(iter->current_key + iter->current_key_len - 8);
        uint64_t packed = decode_u64_le(t);
        *seq = packed >> 8;
        *op = (uint8_t)(packed & 0xFF);
    } else {
        *seq = 0; *op = 0;
    }
}

void sstable_iter_destroy(sstable_iter_t *iter) {
    if (!iter) return;
    if (iter->cached_offset != UINT64_MAX) {
        lsm_cache_release(iter->reader->env->block_cache, iter->reader->table_id, iter->reader->file_id, iter->cached_offset);
    }
    if (iter->owns_block_buf && iter->current_block_buf) {
        aml_free(iter->current_block_buf);
    }
    aml_free(iter);
}
