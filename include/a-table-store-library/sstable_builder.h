// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef SSTABLE_BUILDER_H
#define SSTABLE_BUILDER_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

typedef struct sstable_builder_s sstable_builder_t;

/*
 * Initialize a new SSTable builder and open the file for writing.
 * expected_elements is required to optimally size the internal Bloom filter.
 */
sstable_builder_t *sstable_builder_init(const char *filepath, size_t expected_elements);

/*
 * Add a record to the SSTable.
 * MUST be called in strictly ascending sorted order!
 */
bool sstable_builder_add(sstable_builder_t *builder,
                         const void *key, uint32_t key_len,
                         const void *val, uint32_t val_len);

// Add this new function to track the 2MB split limit
uint64_t sstable_builder_current_size(sstable_builder_t *builder);

/* Returns the final file size in bytes, or 0 on failure.
  * Seal the final blocks, flush the I/O buffer to disk,
  * write the Index and Bloom filters, and close the file.
 */
uint64_t sstable_builder_finish(sstable_builder_t *builder);

#endif /* SSTABLE_BUILDER_H */
