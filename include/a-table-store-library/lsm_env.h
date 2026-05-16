// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef LSM_ENV_H
#define LSM_ENV_H

#include "a-table-store-library/lsm_storage.h"
#include "a-table-store-library/lsm_cache.h"
#include "a-table-store-library/lsm_pool.h"
#include "a-table-store-library/lsm_wal.h"
#include <stdatomic.h>
#include <pthread.h>

typedef struct lsm_db_s lsm_db_t; // Forward declaration

typedef struct {
    lsm_storage_backend_t *hot_vfs;
    lsm_storage_backend_t *cold_vfs;
    int cold_storage_start_level;
} lsm_storage_router_t;

typedef struct {
    lsm_storage_router_t router;
    lsm_cache_t *block_cache;
    lsm_pool_t *bg_pool;

    // --- Global Write Buffer Pool ---
    size_t global_mem_limit;
    _Atomic size_t global_mem_usage;

    // --- Global Pluggable WAL ---
    lsm_wal_t *global_wal;

    // --- Database Registry ---
    lsm_db_t **tables;
    size_t num_tables;
    size_t tables_capacity;
    pthread_mutex_t env_mutex;
} lsm_env_t;

/* Initialize the universe, now accepting the pluggable Global WAL */
lsm_env_t *lsm_env_init(size_t cache_bytes, size_t global_memtable_limit, int num_bg_threads,
                        lsm_storage_backend_t *hot_vfs, lsm_storage_backend_t *cold_vfs, int cold_level,
                        lsm_wal_t *global_wal);

void lsm_env_destroy(lsm_env_t *env);

/* Internal API for Table Management */
void lsm_env_register_db(lsm_env_t *env, lsm_db_t *db);
void lsm_env_unregister_db(lsm_env_t *env, lsm_db_t *db);
void lsm_env_enforce_memory_limit(lsm_env_t *env);

/*
 * TWO-PHASE RECOVERY:
 * Call this AFTER lsm_db_open() has been called for all active tables.
 * It will demultiplex the WAL and hydrate the MemTables.
 */
void lsm_env_recover_wal(lsm_env_t *env);

#endif /* LSM_ENV_H */
