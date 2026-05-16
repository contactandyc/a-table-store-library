// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-table-store-library/sstable_builder.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "the-lz4-library/lz4/lz4.h"
#include "the-lz4-library/xxhash.h"
#include "a-bloom-filter-library/bloom.h"
#include "a-memory-library/aml_alloc.h"

#define TARGET_BLOCK_SIZE 16384
#define RESTART_INTERVAL  16
#define COMPRESS_NONE     0
#define COMPRESS_LZ4      1

#define MAX_KEY_SIZE 1024
#define INTERNAL_KEY_TRAILER_SIZE 8
#define MAX_INTERNAL_KEY_SIZE (MAX_KEY_SIZE + INTERNAL_KEY_TRAILER_SIZE)

// --- Endianness Helpers ---
static inline void encode_u32_le(uint8_t *dst, uint32_t v) {
    dst[0] = v & 0xFF; dst[1] = (v >> 8) & 0xFF; dst[2] = (v >> 16) & 0xFF; dst[3] = (v >> 24) & 0xFF;
}
static inline void encode_u64_le(uint8_t *dst, uint64_t v) {
    for (int i = 0; i < 8; i++) dst[i] = (v >> (i * 8)) & 0xFF;
}

struct sstable_builder_s {
    lsm_storage_backend_t *backend;
    void *data_writer;
    char base_path[512];

    uint64_t current_file_offset;

    uint8_t *block_buf;
    size_t block_pos;
    size_t block_capacity;

    uint8_t *lz4_scratch;
    size_t lz4_scratch_cap;

    char last_key[MAX_INTERNAL_KEY_SIZE];
    uint32_t last_key_len;
    uint32_t entry_count;

    uint32_t *restarts;
    size_t num_restarts;
    size_t restarts_cap;

    int filter_type;
    bloom_t *bloom;
    uint8_t *bitmap;
    size_t bitmap_cap;
    uint64_t min_bitmap_id;

    uint8_t *index_buf;
    size_t index_pos;
    size_t index_cap;
};

static inline int encode_varint32(uint8_t *dst, uint32_t value) {
    int bytes = 0;
    while (value >= 128) {
        dst[bytes++] = (value & 127) | 128;
        value >>= 7;
    }
    dst[bytes++] = value & 127;
    return bytes;
}

sstable_builder_t *sstable_builder_init(const char *base_path, lsm_storage_backend_t *backend, int filter_type, size_t expected_elements) {
    sstable_builder_t *b = aml_zalloc(sizeof(sstable_builder_t));
    b->backend = backend;
    b->filter_type = filter_type;
    strncpy(b->base_path, base_path, 511);

    char data_path[520];
    snprintf(data_path, sizeof(data_path), "%s.data", base_path);
    b->data_writer = backend->open_writer(data_path);
    if (!b->data_writer) { aml_free(b); return NULL; }

    b->block_capacity = TARGET_BLOCK_SIZE + 4096;
    b->block_buf = aml_malloc(b->block_capacity);
    b->lz4_scratch_cap = LZ4_compressBound(b->block_capacity);
    b->lz4_scratch = aml_malloc(b->lz4_scratch_cap);

    b->restarts_cap = 256;
    b->restarts = aml_malloc(b->restarts_cap * sizeof(uint32_t));
    b->restarts[0] = 0;
    b->num_restarts = 1;

    b->index_cap = 65536;
    b->index_buf = aml_malloc(b->index_cap);

    if (filter_type == FILTER_BLOOM) {
        b->bloom = bloom_init(expected_elements, 0.01);
    } else if (filter_type == FILTER_BITMAP) {
        // [Phase 3 Fix] Start with a baseline capacity to prevent thrashing
        size_t est_bytes = (expected_elements / 8) + 1;
        b->bitmap_cap = est_bytes < 1024 ? 1024 : est_bytes;
        b->bitmap = aml_zalloc(b->bitmap_cap);
    }

    return b;
}

static void flush_data_block(sstable_builder_t *b) {
    if (b->block_pos == 0) return;

    for (size_t i = 0; i < b->num_restarts; i++) {
        encode_u32_le(&b->block_buf[b->block_pos], b->restarts[i]);
        b->block_pos += 4;
    }
    encode_u32_le(&b->block_buf[b->block_pos], b->num_restarts);
    b->block_pos += 4;

    uint32_t uncompressed_size = b->block_pos;
    uint8_t compression_flag = COMPRESS_NONE;
    uint8_t *final_buf = b->block_buf;
    size_t final_size = b->block_pos;

    int comp_size = LZ4_compress_default((const char *)b->block_buf, (char *)b->lz4_scratch, b->block_pos, b->lz4_scratch_cap);
    if (comp_size > 0 && comp_size < (int)(b->block_pos * 0.88)) {
        final_buf = b->lz4_scratch;
        final_size = comp_size;
        compression_flag = COMPRESS_LZ4;
    }

    uint32_t checksum = XXH32(final_buf, final_size, 0);
    uint8_t trailer[9];
    encode_u32_le(trailer, uncompressed_size);
    trailer[4] = compression_flag;
    encode_u32_le(trailer + 5, checksum);

    b->backend->append(b->data_writer, final_buf, final_size);
    b->backend->append(b->data_writer, trailer, 9);

    size_t required_idx = 4 + b->last_key_len + 8 + 8;
    if (b->index_pos + required_idx > b->index_cap) {
        b->index_cap *= 2;
        b->index_buf = aml_realloc(b->index_buf, b->index_cap);
    }
    encode_u32_le(&b->index_buf[b->index_pos], b->last_key_len); b->index_pos += 4;
    memcpy(&b->index_buf[b->index_pos], b->last_key, b->last_key_len); b->index_pos += b->last_key_len;
    encode_u64_le(&b->index_buf[b->index_pos], b->current_file_offset); b->index_pos += 8;

    uint64_t disk_size = final_size + 9;
    encode_u64_le(&b->index_buf[b->index_pos], disk_size); b->index_pos += 8;

    b->current_file_offset += disk_size;
    b->block_pos = 0;
    b->num_restarts = 1;
    b->restarts[0] = 0;
    b->entry_count = 0;
    b->last_key_len = 0;
}

bool sstable_builder_add(sstable_builder_t *b, const void *key, uint32_t key_len, const void *val, uint32_t val_len) {
    if (b->block_pos > TARGET_BLOCK_SIZE) flush_data_block(b);

    size_t required_space = 15 + key_len + val_len;

    size_t slack = (b->restarts_cap * 4) + 4 + 9 + 4096;

    if (b->block_pos + required_space + slack > b->block_capacity) {
        b->block_capacity = b->block_pos + required_space + slack;
        b->block_buf = aml_realloc(b->block_buf, b->block_capacity);

        b->lz4_scratch_cap = LZ4_compressBound(b->block_capacity);
        b->lz4_scratch = aml_realloc(b->lz4_scratch, b->lz4_scratch_cap);
    }

    uint32_t user_key_len = key_len >= 8 ? key_len - 8 : key_len;

    if (b->filter_type == FILTER_BLOOM) {
        bloom_add(b->bloom, key, user_key_len);
    } else if (b->filter_type == FILTER_BITMAP && user_key_len >= 8) {
        uint64_t current_id;
        memcpy(&current_id, key, 8);

        if (b->current_file_offset == 0 && b->entry_count == 0 && b->index_pos == 0) {
            b->min_bitmap_id = current_id;
        }

        // [Phase 3 Fix] Protect against non-monotonic ID underflows
        if (current_id >= b->min_bitmap_id) {
            uint64_t diff = current_id - b->min_bitmap_id;
            size_t byte_idx = diff / 8;

            // Limit bitmap allocation to ~1MB (8 million sequential IDs)
            // Beyond that, the bitmap loses efficiency compared to Bloom
            if (byte_idx < 1024 * 1024) {
                if (byte_idx >= b->bitmap_cap) {
                    size_t new_cap = b->bitmap_cap;
                    while (byte_idx >= new_cap) new_cap *= 2;
                    uint8_t *new_map = aml_zalloc(new_cap);
                    memcpy(new_map, b->bitmap, b->bitmap_cap);
                    aml_free(b->bitmap);
                    b->bitmap = new_map;
                    b->bitmap_cap = new_cap;
                }
                b->bitmap[byte_idx] |= (1 << (diff % 8));
            }
        }
    }

    const char *key_str = (const char *)key;
    uint32_t shared = 0;

    if (b->entry_count % RESTART_INTERVAL == 0) {
        if (b->num_restarts >= b->restarts_cap) {
            b->restarts_cap *= 2;
            b->restarts = aml_realloc(b->restarts, b->restarts_cap * sizeof(uint32_t));
        }
        if (b->block_pos > 0) b->restarts[b->num_restarts++] = b->block_pos;
    } else {
        uint32_t min_len = (key_len < b->last_key_len) ? key_len : b->last_key_len;
        while (shared < min_len && key_str[shared] == b->last_key[shared]) shared++;
    }

    uint32_t unshared = key_len - shared;
    b->block_pos += encode_varint32(&b->block_buf[b->block_pos], shared);
    b->block_pos += encode_varint32(&b->block_buf[b->block_pos], unshared);
    b->block_pos += encode_varint32(&b->block_buf[b->block_pos], val_len);

    memcpy(&b->block_buf[b->block_pos], key_str + shared, unshared); b->block_pos += unshared;
    if (val_len > 0 && val) { memcpy(&b->block_buf[b->block_pos], val, val_len); b->block_pos += val_len; }

    memcpy(b->last_key, key_str, key_len);
    b->last_key_len = key_len;
    b->entry_count++;

    return true;
}

uint64_t sstable_builder_current_size(sstable_builder_t *b) {
    if (!b) return 0;
    return b->current_file_offset;
}

uint64_t sstable_builder_finish(sstable_builder_t *b) {
    if (!b) return 0;
    if (b->block_pos > 0) flush_data_block(b);

    b->backend->fsync_file(b->data_writer);
    b->backend->close_writer(b->data_writer);

    char meta_path[520];
    snprintf(meta_path, sizeof(meta_path), "%s.meta", b->base_path);
    void *meta_writer = b->backend->open_writer(meta_path);

    b->backend->append(meta_writer, "META", 4);
    uint8_t f_type = (uint8_t)b->filter_type;
    b->backend->append(meta_writer, &f_type, 1);

    uint32_t filter_len = 0;
    void *filter_data = NULL;
    if (f_type == FILTER_BLOOM) {
        filter_len = 16 + bloom_byte_size(b->bloom);
        uint8_t *blm_buf = aml_malloc(filter_len);
        encode_u64_le(blm_buf, b->bloom->num_bits);
        encode_u64_le(blm_buf + 8, b->bloom->num_hashes);
        memcpy(blm_buf + 16, b->bloom->bits, bloom_byte_size(b->bloom));
        filter_data = blm_buf;
    } else if (f_type == FILTER_BITMAP) {
        filter_len = 8 + b->bitmap_cap;
        uint8_t *bmp_buf = aml_malloc(filter_len);
        encode_u64_le(bmp_buf, b->min_bitmap_id);
        memcpy(bmp_buf + 8, b->bitmap, b->bitmap_cap);
        filter_data = bmp_buf;
    }

    uint8_t sz_buf[4];
    encode_u32_le(sz_buf, filter_len);
    b->backend->append(meta_writer, sz_buf, 4);

    if (filter_len > 0) {
        b->backend->append(meta_writer, filter_data, filter_len);
        aml_free(filter_data);
    }

    encode_u32_le(sz_buf, b->index_pos);
    b->backend->append(meta_writer, sz_buf, 4);
    b->backend->append(meta_writer, b->index_buf, b->index_pos);

    b->backend->fsync_file(meta_writer);
    b->backend->close_writer(meta_writer);

    uint64_t final_data_size = b->current_file_offset;

    if (b->bloom) bloom_destroy(b->bloom);
    if (b->bitmap) aml_free(b->bitmap);
    aml_free(b->block_buf); aml_free(b->lz4_scratch); aml_free(b->restarts); aml_free(b->index_buf);
    aml_free(b);

    return final_data_size;
}
