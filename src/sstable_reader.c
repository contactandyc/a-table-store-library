// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-table-store-library/sstable_reader.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "the-lz4-library/lz4/lz4.h"
#include "the-lz4-library/xxhash.h"
#include "a-bloom-filter-library/bloom.h"

#define SSTABLE_MAGIC 0x424C4F434B4C534DULL

typedef struct {
    char *key;
    uint32_t key_len;
    uint64_t offset;
    uint64_t size;
} index_entry_t;

struct sstable_reader_s {
    FILE *file;

    bloom_t *bloom;

    index_entry_t *index;
    uint32_t num_index_entries;

    uint8_t *lz4_scratch;
};

/* --- Internal Utility to Decode Varints --- */
static inline uint32_t decode_varint32(const uint8_t **ptr) {
    uint32_t result = 0;
    int shift = 0;
    while (1) {
        uint8_t byte = **ptr;
        (*ptr)++;
        result |= (byte & 127) << shift;
        if (!(byte & 128)) break;
        shift += 7;
    }
    return result;
}

/* --------------------------------------------------------------------------
 * Initialization (Loading Metadata into RAM)
 * -------------------------------------------------------------------------- */

sstable_reader_t *sstable_reader_init(const char *filepath) {
    FILE *f = fopen(filepath, "rb");
    if (!f) return NULL;

    fseek(f, -24, SEEK_END);
    uint64_t bloom_offset, index_offset, magic;
    if (fread(&bloom_offset, 8, 1, f) != 1) { fclose(f); return NULL; }
    if (fread(&index_offset, 8, 1, f) != 1) { fclose(f); return NULL; }
    if (fread(&magic, 8, 1, f) != 1) { fclose(f); return NULL; }

    if (magic != SSTABLE_MAGIC) { fclose(f); return NULL; }

    sstable_reader_t *r = (sstable_reader_t *)calloc(1, sizeof(sstable_reader_t));
    r->file = f;

    fseek(f, bloom_offset, SEEK_SET);
    uint64_t bloom_bits, bloom_hashes;
    fread(&bloom_bits, 8, 1, f);
    fread(&bloom_hashes, 8, 1, f);

    size_t bits_bytes = (bloom_bits + 7) / 8;

    r->bloom = (bloom_t *)calloc(1, sizeof(bloom_t));
    r->bloom->num_bits = bloom_bits;
    r->bloom->num_hashes = bloom_hashes;
    r->bloom->bits = (uint8_t *)malloc(bits_bytes);
    fread(r->bloom->bits, 1, bits_bytes, f);

    fseek(f, index_offset, SEEK_SET);
    fread(&r->num_index_entries, 4, 1, f);

    r->index = (index_entry_t *)malloc(r->num_index_entries * sizeof(index_entry_t));
    for (uint32_t i = 0; i < r->num_index_entries; i++) {
        fread(&r->index[i].key_len, 4, 1, f);
        r->index[i].key = (char *)malloc(r->index[i].key_len);
        fread(r->index[i].key, 1, r->index[i].key_len, f);
        fread(&r->index[i].offset, 8, 1, f);
        fread(&r->index[i].size, 8, 1, f);
    }

    r->lz4_scratch = (uint8_t *)malloc(32768);

    return r;
}

void sstable_reader_destroy(sstable_reader_t *r) {
    if (!r) return;
    fclose(r->file);
    bloom_destroy(r->bloom);
    for (uint32_t i = 0; i < r->num_index_entries; i++) free(r->index[i].key);
    free(r->index);
    free(r->lz4_scratch);
    free(r);
}

/* --------------------------------------------------------------------------
 * Point Lookup / Delta Decoding
 * -------------------------------------------------------------------------- */

void *sstable_reader_get(sstable_reader_t *r, const void *key, uint32_t key_len, uint32_t *out_val_len) {
    if (!bloom_check(r->bloom, key, key_len)) {
        return NULL;
    }

    int left = 0, right = r->num_index_entries - 1;
    int target_idx = -1;

    while (left <= right) {
        int mid = left + (right - left) / 2; // FIXED division by 0

        int cmp;
        uint32_t min_len = key_len < r->index[mid].key_len ? key_len : r->index[mid].key_len;
        cmp = memcmp(key, r->index[mid].key, min_len);
        if (cmp == 0) cmp = (key_len < r->index[mid].key_len) ? -1 : (key_len > r->index[mid].key_len ? 1 : 0);

        if (cmp == 0) { target_idx = mid; break; }
        else if (cmp < 0) { target_idx = mid; right = mid - 1; }
        else { left = mid + 1; }
    }

    if (target_idx == -1) return NULL;

    index_entry_t *idx = &r->index[target_idx];
    uint8_t *disk_buf = (uint8_t *)malloc(idx->size);

    fseek(r->file, idx->offset, SEEK_SET);
    fread(disk_buf, 1, idx->size, r->file);

    uint8_t flag = disk_buf[idx->size - 5];
    uint32_t file_crc;
    memcpy(&file_crc, &disk_buf[idx->size - 4], 4);

    uint32_t actual_crc = XXH32(disk_buf, idx->size - 5, 0);
    if (actual_crc != file_crc) { free(disk_buf); return NULL; }

    uint8_t *block = disk_buf;
    size_t block_size = idx->size - 5;

    if (flag == 1) { // COMPRESS_LZ4
        int decomp_size = LZ4_decompress_safe((const char*)disk_buf, (char*)r->lz4_scratch, block_size, 32768);
        if (decomp_size < 0) { free(disk_buf); return NULL; }
        block = r->lz4_scratch;
        block_size = decomp_size;
    }

    uint32_t num_restarts;
    memcpy(&num_restarts, &block[block_size - 4], 4);
    uint32_t restarts_offset = block_size - 4 - (num_restarts * 4);

    const uint8_t *ptr = block;
    const uint8_t *end = block + restarts_offset;
    char current_key[1024];
    uint32_t current_key_len = 0;

    while (ptr < end) {
        uint32_t shared = decode_varint32(&ptr);
        uint32_t unshared = decode_varint32(&ptr);
        uint32_t vlen = decode_varint32(&ptr);

        memcpy(current_key + shared, ptr, unshared);
        ptr += unshared;
        current_key_len = shared + unshared;

        int cmp;
        uint32_t min_len = key_len < current_key_len ? key_len : current_key_len;
        cmp = memcmp(key, current_key, min_len);
        if (cmp == 0) cmp = (key_len < current_key_len) ? -1 : (key_len > current_key_len ? 1 : 0);

        if (cmp == 0) {
            void *val = malloc(vlen);
            memcpy(val, ptr, vlen);
            if (out_val_len) *out_val_len = vlen;
            free(disk_buf);
            return val;
        } else if (cmp < 0) {
            break;
        }

        ptr += vlen;
    }

    free(disk_buf);
    return NULL;
}

/* --------------------------------------------------------------------------
 * Iterators
 * -------------------------------------------------------------------------- */

struct sstable_iter_s {
    sstable_reader_t *reader;

    uint32_t current_block_idx;
    uint8_t *current_block_buf;
    size_t current_block_size;

    const uint8_t *ptr;
    const uint8_t *end;

    char current_key[1024];
    uint32_t current_key_len;

    const char *current_val;
    uint32_t current_val_len;
};

static bool iter_load_next_block(sstable_iter_t *iter) {
    if (iter->current_block_idx >= iter->reader->num_index_entries) {
        return false;
    }

    index_entry_t *idx = &iter->reader->index[iter->current_block_idx];
    iter->current_block_idx++;

    uint8_t *disk_buf = (uint8_t *)malloc(idx->size);
    fseek(iter->reader->file, idx->offset, SEEK_SET);
    fread(disk_buf, 1, idx->size, iter->reader->file);

    uint8_t flag = disk_buf[idx->size - 5];
    size_t block_size = idx->size - 5;

    if (iter->current_block_buf) free(iter->current_block_buf);

    if (flag == 1) { // COMPRESS_LZ4
        iter->current_block_buf = (uint8_t *)malloc(32768);
        int d_size = LZ4_decompress_safe((const char*)disk_buf, (char*)iter->current_block_buf, block_size, 32768);
        iter->current_block_size = d_size;
        free(disk_buf);
    } else {
        iter->current_block_buf = disk_buf;
        iter->current_block_size = block_size;
    }

    uint32_t num_restarts;
    memcpy(&num_restarts, &iter->current_block_buf[iter->current_block_size - 4], 4);
    uint32_t restarts_offset = iter->current_block_size - 4 - (num_restarts * 4);

    iter->ptr = iter->current_block_buf;
    iter->end = iter->current_block_buf + restarts_offset;

    return true;
}

sstable_iter_t *sstable_iter_init(sstable_reader_t *reader) {
    sstable_iter_t *iter = calloc(1, sizeof(sstable_iter_t));
    iter->reader = reader;
    iter->current_block_idx = 0;
    return iter;
}

bool sstable_iter_next(sstable_iter_t *iter) {
    while (!iter->ptr || iter->ptr >= iter->end) {
        if (!iter_load_next_block(iter)) {
            return false;
        }
    }

    uint32_t shared = decode_varint32(&iter->ptr);
    uint32_t unshared = decode_varint32(&iter->ptr);
    uint32_t vlen = decode_varint32(&iter->ptr);

    memcpy(iter->current_key + shared, iter->ptr, unshared);
    iter->ptr += unshared;
    iter->current_key_len = shared + unshared;

    iter->current_val = (const char *)iter->ptr;
    iter->current_val_len = vlen;
    iter->ptr += vlen;

    return true;
}

void sstable_iter_get_kv(sstable_iter_t *iter, const char **key, uint32_t *klen, const char **val, uint32_t *vlen) {
    *key = iter->current_key;
    *klen = iter->current_key_len;
    *val = iter->current_val;
    *vlen = iter->current_val_len;
}

void sstable_iter_get_meta(sstable_iter_t *iter, uint64_t *seq, uint8_t *op) {
    /*
     * Extract seq/op from the 8-byte internal key trailer.
     * Trailer layout: 56 bits for Sequence Number, 8 bits for OpType.
     */
    if (iter->current_key_len >= 8) {
        uint32_t trailer_offset = iter->current_key_len - 8;
        uint64_t packed;
        memcpy(&packed, iter->current_key + trailer_offset, 8);

        *seq = packed >> 8;
        *op = (uint8_t)(packed & 0xFF);
    } else {
        *seq = 0;
        *op = 0;
    }
}

void sstable_iter_destroy(sstable_iter_t *iter) {
    if (!iter) return;
    if (iter->current_block_buf) free(iter->current_block_buf);
    free(iter);
}
