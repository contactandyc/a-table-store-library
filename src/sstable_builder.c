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

#define TARGET_BLOCK_SIZE 16384
#define IO_BUFFER_SIZE    1048576
#define RESTART_INTERVAL  16
#define COMPRESS_NONE     0
#define COMPRESS_LZ4      1
#define BLOOM_FP_RATE     0.01
#define SSTABLE_MAGIC     0x424C4F434B4C534DULL // "BLOCKLSM"

struct sstable_builder_s {
    FILE *file;
    uint64_t current_file_offset;

    uint8_t *block_buf;
    size_t block_pos;
    size_t block_capacity;

    uint8_t *io_buf;
    size_t io_pos;

    uint8_t *lz4_scratch;
    size_t lz4_scratch_cap;

    char last_key[1024];
    uint32_t last_key_len;
    uint32_t entry_count;

    uint32_t *restarts;
    size_t num_restarts;
    size_t restarts_cap;

    bloom_t *bloom;

    /* Index Block Buffer */
    uint8_t *index_buf;
    size_t index_pos;
    size_t index_cap;
    uint32_t num_index_entries;
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

sstable_builder_t *sstable_builder_init(const char *filepath, size_t expected_elements) {
    sstable_builder_t *b = (sstable_builder_t *)calloc(1, sizeof(sstable_builder_t));

    b->file = fopen(filepath, "wb");
    if (!b->file) { free(b); return NULL; }
    setvbuf(b->file, NULL, _IONBF, 0);

    b->block_capacity = TARGET_BLOCK_SIZE + 4096;
    b->block_buf = (uint8_t *)malloc(b->block_capacity);

    b->io_buf = (uint8_t *)malloc(IO_BUFFER_SIZE);

    b->lz4_scratch_cap = (size_t)LZ4_compressBound((int)b->block_capacity);
    b->lz4_scratch = (uint8_t *)malloc(b->lz4_scratch_cap);

    b->restarts_cap = 256;
    b->restarts = (uint32_t *)malloc(b->restarts_cap * sizeof(uint32_t));
    b->restarts[0] = 0;
    b->num_restarts = 1;

    b->index_cap = 65536; // 64KB initial index
    b->index_buf = (uint8_t *)malloc(b->index_cap);

    b->bloom = bloom_init(expected_elements, BLOOM_FP_RATE);
    return b;
}

static void flush_data_block(sstable_builder_t *b) {
    if (b->block_pos == 0) return;

    for (size_t i = 0; i < b->num_restarts; i++) {
        memcpy(&b->block_buf[b->block_pos], &b->restarts[i], sizeof(uint32_t));
        b->block_pos += sizeof(uint32_t);
    }
    uint32_t nr = (uint32_t)b->num_restarts;
    memcpy(&b->block_buf[b->block_pos], &nr, sizeof(uint32_t));
    b->block_pos += sizeof(uint32_t);

    uint8_t compression_flag = COMPRESS_NONE;
    uint8_t *final_buf = b->block_buf;
    size_t final_size = b->block_pos;

    int comp_size = LZ4_compress_default((const char *)b->block_buf, (char *)b->lz4_scratch,
                                         (int)b->block_pos, (int)b->lz4_scratch_cap);

    if (comp_size > 0 && comp_size < (int)(b->block_pos * 0.88)) {
        final_buf = b->lz4_scratch;
        final_size = (size_t)comp_size;
        compression_flag = COMPRESS_LZ4;
    }

    uint32_t checksum = XXH32(final_buf, final_size, 0);

    /* Record Index Entry: [KeyLen(4) | KeyBytes | Offset(8) | Size(8)] */
    size_t required_idx = 4 + b->last_key_len + 8 + 8;
    if (b->index_pos + required_idx > b->index_cap) {
        b->index_cap *= 2;
        b->index_buf = (uint8_t *)realloc(b->index_buf, b->index_cap);
    }
    memcpy(&b->index_buf[b->index_pos], &b->last_key_len, 4); b->index_pos += 4;
    memcpy(&b->index_buf[b->index_pos], b->last_key, b->last_key_len); b->index_pos += b->last_key_len;
    memcpy(&b->index_buf[b->index_pos], &b->current_file_offset, 8); b->index_pos += 8;

    uint64_t disk_size = final_size + 5; // payload + flag + crc
    memcpy(&b->index_buf[b->index_pos], &disk_size, 8); b->index_pos += 8;
    b->num_index_entries++;

    /* Write to I/O Batch Buffer */
    if (b->io_pos + disk_size > IO_BUFFER_SIZE) {
        fwrite(b->io_buf, 1, b->io_pos, b->file);
        b->io_pos = 0;
    }

    memcpy(&b->io_buf[b->io_pos], final_buf, final_size); b->io_pos += final_size;
    b->io_buf[b->io_pos++] = compression_flag;
    memcpy(&b->io_buf[b->io_pos], &checksum, 4); b->io_pos += 4;

    b->current_file_offset += disk_size;
    b->block_pos = 0;
    b->num_restarts = 1;
    b->restarts[0] = 0;
    b->entry_count = 0;
    b->last_key_len = 0;
}

bool sstable_builder_add(sstable_builder_t *b, const void *key, uint32_t key_len, const void *val, uint32_t val_len) {
    if (b->block_pos > TARGET_BLOCK_SIZE) flush_data_block(b);

    /*
     * Key injected into builder is the Internal Key.
     * We add the whole Internal Key to the Bloom filter for exact version checks,
     * though many databases only add the User Key prefix.
     */
    bloom_add(b->bloom, key, key_len);

    const char *key_str = (const char *)key;
    uint32_t shared = 0;

    if (b->entry_count % RESTART_INTERVAL == 0) {
        if (b->num_restarts >= b->restarts_cap) {
            b->restarts_cap *= 2;
            b->restarts = (uint32_t *)realloc(b->restarts, b->restarts_cap * sizeof(uint32_t));
        }
        if (b->block_pos > 0) b->restarts[b->num_restarts++] = (uint32_t)b->block_pos;
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
    return b->current_file_offset + b->io_pos;
}

uint64_t sstable_builder_finish(sstable_builder_t *b) {
    if (!b) return 0;
    if (b->block_pos > 0) flush_data_block(b);
    if (b->io_pos > 0) { fwrite(b->io_buf, 1, b->io_pos, b->file); b->io_pos = 0; }

    /* Write Bloom Filter */
    uint64_t bloom_offset = b->current_file_offset;
    uint64_t bloom_bits = (uint64_t)b->bloom->num_bits;
    uint64_t bloom_hashes = (uint64_t)b->bloom->num_hashes;
    size_t bloom_bytes = bloom_byte_size(b->bloom);

    fwrite(&bloom_bits, 8, 1, b->file);
    fwrite(&bloom_hashes, 8, 1, b->file);
    fwrite(b->bloom->bits, 1, bloom_bytes, b->file);
    b->current_file_offset += 16 + bloom_bytes;

    /* Write Index Block */
    uint64_t index_offset = b->current_file_offset;
    fwrite(&b->num_index_entries, 4, 1, b->file);
    fwrite(b->index_buf, 1, b->index_pos, b->file);
    b->current_file_offset += 4 + b->index_pos;

    /* Write 24-byte Footer */
    uint64_t magic = SSTABLE_MAGIC;
    fwrite(&bloom_offset, 8, 1, b->file);
    fwrite(&index_offset, 8, 1, b->file);
    fwrite(&magic, 8, 1, b->file);

    uint64_t final_size = b->current_file_offset + 24;

    fclose(b->file);
    bloom_destroy(b->bloom);
    free(b->block_buf); free(b->io_buf); free(b->lz4_scratch); free(b->restarts); free(b->index_buf);
    free(b);

    return final_size;
}
