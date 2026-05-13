// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef SSTABLE_READER_H
#define SSTABLE_READER_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/* Forward declare both structures */
typedef struct sstable_reader_s sstable_reader_t;
typedef struct sstable_iter_s sstable_iter_t;

/* Opens the SSTable, reads the footer, and caches the Bloom Filter and Index in RAM */
sstable_reader_t *sstable_reader_init(const char *filepath);

/*
 * Point Lookup.
 * Returns a malloc'd pointer to the value bytes (caller must free), or NULL if not found.
 */
void *sstable_reader_get(sstable_reader_t *reader, const void *key, uint32_t key_len, uint32_t *out_val_len);

/* Free resources */
void sstable_reader_destroy(sstable_reader_t *reader);

/* Initialize an iterator to scan the SSTable from the beginning */
sstable_iter_t *sstable_iter_init(sstable_reader_t *reader);

/* Advance the iterator. Returns true if a record exists, false on EOF */
bool sstable_iter_next(sstable_iter_t *iter);

/* Extract the current Key and Value */
void sstable_iter_get_kv(sstable_iter_t *iter, const char **key, uint32_t *klen, const char **val, uint32_t *vlen);

/* Extract internal LSM metadata (Sequence Number and Op Type) */
void sstable_iter_get_meta(sstable_iter_t *iter, uint64_t *seq, uint8_t *op);

/* Free the iterator */
void sstable_iter_destroy(sstable_iter_t *iter);

#endif /* SSTABLE_READER_H */
