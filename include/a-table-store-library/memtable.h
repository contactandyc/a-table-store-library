// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef MEMTABLE_H
#define MEMTABLE_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

typedef struct memtable_s memtable_t;
typedef struct memtable_row_s memtable_row_t;
typedef struct memtable_row_index_s memtable_row_index_t;

typedef enum {
    OP_PUT = 0,
    OP_DELETE = 1
} op_type_t;

memtable_t *memtable_init(void);

/* Atomically increment the usage counter so concurrent flushes don't destroy memory in use */
void memtable_retain(memtable_t *mt);

/* Decrement the usage counter. If 0, memory is instantly reclaimed to the OS */
void memtable_release(memtable_t *mt);

size_t memtable_get_memory_usage(memtable_t *mt);

bool memtable_put(memtable_t *mt, uint64_t seq_num, op_type_t op, const void *key, uint32_t key_len, const void *val, uint32_t val_len);
const void *memtable_get(memtable_t *mt, const void *key, uint32_t key_len, uint64_t read_seq_num, uint32_t *out_len, bool *is_deleted);

memtable_row_t *memtable_first(memtable_t *mt);
memtable_row_t *memtable_next(memtable_row_t *row);
const void *memtable_row_get_key(const memtable_row_t *row, uint32_t *out_len);
const void *memtable_row_get_val(const memtable_row_t *row, uint32_t *out_len);
uint64_t memtable_row_get_seq(const memtable_row_t *row);
op_type_t memtable_row_get_op(const memtable_row_t *row);

bool memtable_index_put(memtable_t *mt, uint64_t seq_num, op_type_t op, const void *sec_key, uint32_t sec_key_len, const void *pri_key, uint32_t pri_key_len);
memtable_row_index_t *memtable_index_search(memtable_t *mt, const void *sec_key, uint32_t sec_key_len);
memtable_row_index_t *memtable_index_first(memtable_t *mt);
memtable_row_index_t *memtable_index_next(memtable_row_index_t *row);
const void *memtable_row_index_get_sec_key(const memtable_row_index_t *row, uint32_t *out_len);
const void *memtable_row_index_get_pri_key(const memtable_row_index_t *row, uint32_t *out_len);
uint64_t memtable_row_index_get_seq(const memtable_row_index_t *row);
op_type_t memtable_row_index_get_op(const memtable_row_index_t *row);

#endif /* MEMTABLE_H */
