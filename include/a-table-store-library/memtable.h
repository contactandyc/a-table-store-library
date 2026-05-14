// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef MEMTABLE_H
#define MEMTABLE_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/* Opaque structures */
typedef struct memtable_s memtable_t;
typedef struct memtable_row_s memtable_row_t;
typedef struct memtable_row_index_s memtable_row_index_t;

typedef enum {
    OP_PUT = 0,
    OP_DELETE = 1
} op_type_t;

/* --------------------------------------------------------------------------
 * Lifecycle API
 * -------------------------------------------------------------------------- */

/* Initialize a new MemTable (used for both Primary and Index tables) */
memtable_t *memtable_init(size_t pool_size, size_t mem_limit);

/* Instantly reclaim all memory used by the MemTable */
void memtable_destroy(memtable_t *mt);


/* --------------------------------------------------------------------------
 * Primary Index API
 * -------------------------------------------------------------------------- */

/* Insert a primary record (Single-Writer thread). Returns false if full. */
bool memtable_put(memtable_t *mt, uint64_t seq_num, op_type_t op,
                  const void *key, uint32_t key_len,
                  const void *val, uint32_t val_len);

/* Read a primary record (Multi-Reader lock-free). Returns NULL if deleted or not found. */
const void *memtable_get(memtable_t *mt, const void *key, uint32_t key_len,
                         uint64_t read_seq_num, uint32_t *out_len, bool *is_deleted);

/* Primary Iterators (for flushing to SSTable) */
memtable_row_t *memtable_first(memtable_t *mt);
memtable_row_t *memtable_next(memtable_row_t *row);

const void *memtable_row_get_key(const memtable_row_t *row, uint32_t *out_len);
const void *memtable_row_get_val(const memtable_row_t *row, uint32_t *out_len);
uint64_t    memtable_row_get_seq(const memtable_row_t *row);
op_type_t   memtable_row_get_op(const memtable_row_t *row);


/* --------------------------------------------------------------------------
 * Secondary Index API
 * -------------------------------------------------------------------------- */

/* Insert a secondary index record. Returns false if full. */
bool memtable_index_put(memtable_t *mt, uint64_t seq_num, op_type_t op,
                        const void *sec_key, uint32_t sec_key_len,
                        const void *pri_key, uint32_t pri_key_len);

/*
 * Seek to the first index entry matching the exact secondary key prefix.
 * The iterator must be manually advanced to filter by read_seq_num and extract primary keys.
 */
memtable_row_index_t *memtable_index_search(memtable_t *mt, const void *sec_key, uint32_t sec_key_len);

/* Secondary Index Iterators */
memtable_row_index_t *memtable_index_first(memtable_t *mt);
memtable_row_index_t *memtable_index_next(memtable_row_index_t *row);

const void *memtable_row_index_get_sec_key(const memtable_row_index_t *row, uint32_t *out_len);
const void *memtable_row_index_get_pri_key(const memtable_row_index_t *row, uint32_t *out_len);
uint64_t    memtable_row_index_get_seq(const memtable_row_index_t *row);
op_type_t   memtable_row_index_get_op(const memtable_row_index_t *row);

#endif /* MEMTABLE_H */
