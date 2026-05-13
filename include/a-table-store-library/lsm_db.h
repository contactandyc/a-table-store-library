// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef LSM_DB_H
#define LSM_DB_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

typedef struct lsm_db_s lsm_db_t;
typedef struct lsm_db_iter_s lsm_db_iter_t;

/* Open the database, specifying a MemTable size limit (e.g., 64MB) */
lsm_db_t *lsm_db_open(const char *db_directory, size_t mem_limit);

/* Close the database and flush pending memory to disk */
void lsm_db_close(lsm_db_t *db);

/* --- CRUD Operations --- */
bool lsm_db_put(lsm_db_t *db, const void *key, uint32_t key_len, const void *val, uint32_t val_len);
bool lsm_db_delete(lsm_db_t *db, const void *key, uint32_t key_len);
void *lsm_db_get(lsm_db_t *db, const void *key, uint32_t key_len, uint32_t *out_val_len);

/* --- Streaming Iterator (Required for SQL Table Scans) --- */
lsm_db_iter_t *lsm_db_iter_init(lsm_db_t *db);
bool lsm_db_iter_next(lsm_db_iter_t *iter, const void **key, uint32_t *klen, const void **val, uint32_t *vlen);
void lsm_db_iter_destroy(lsm_db_iter_t *iter);

#endif /* LSM_DB_H */
