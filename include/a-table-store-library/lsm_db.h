// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef LSM_DB_H
#define LSM_DB_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "a-table-store-library/lsm_env.h"

typedef struct lsm_db_s lsm_db_t;
typedef struct lsm_db_iter_s lsm_db_iter_t;
typedef struct lsm_write_batch_s lsm_write_batch_t;

lsm_db_t *lsm_db_open(lsm_env_t *env, uint32_t table_id, const char *db_directory);

void lsm_db_retain(lsm_db_t *db);
void lsm_db_release(lsm_db_t *db);
void lsm_db_close(lsm_db_t *db);

// Single operations
bool lsm_db_put(lsm_db_t *db, const void *key, uint32_t key_len, const void *val, uint32_t val_len);
bool lsm_db_delete(lsm_db_t *db, const void *key, uint32_t key_len);

// Write Batches (Atomic)
lsm_write_batch_t *lsm_write_batch_init(void);
bool lsm_write_batch_put(lsm_write_batch_t *batch, const void *key, uint32_t key_len, const void *val, uint32_t val_len);
bool lsm_write_batch_delete(lsm_write_batch_t *batch, const void *key, uint32_t key_len);
void lsm_write_batch_destroy(lsm_write_batch_t *batch);
bool lsm_db_write(lsm_db_t *db, lsm_write_batch_t *batch);

// Snapshots (MVCC Isolation)
uint64_t lsm_db_take_snapshot(lsm_db_t *db);
void lsm_db_release_snapshot(lsm_db_t *db, uint64_t seq_num);

// Reads (Pass UINT64_MAX for read_seq_num to read latest)
void *lsm_db_get(lsm_db_t *db, const void *key, uint32_t key_len, uint64_t read_seq_num, uint32_t *out_val_len);

lsm_db_iter_t *lsm_db_iter_init(lsm_db_t *db, uint64_t read_seq_num);
bool lsm_db_iter_next(lsm_db_iter_t *iter, const void **key, uint32_t *klen, const void **val, uint32_t *vlen);
void lsm_db_iter_destroy(lsm_db_iter_t *iter);

uint32_t lsm_db_get_table_id(lsm_db_t *db);
size_t lsm_db_get_active_mem_usage(lsm_db_t *db);
bool lsm_db_force_flush(lsm_db_t *db);

#endif /* LSM_DB_H */
