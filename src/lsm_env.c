// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-table-store-library/lsm_env.h"
#include "a-table-store-library/lsm_db.h"
#include "a-memory-library/aml_alloc.h"

lsm_env_t *lsm_env_init(size_t cache_bytes, size_t global_memtable_limit, int num_bg_threads,
                        lsm_storage_backend_t *hot_vfs, lsm_storage_backend_t *cold_vfs, int cold_level,
                        lsm_wal_t *global_wal) {

    lsm_env_t *env = aml_zalloc(sizeof(lsm_env_t));

    env->router.hot_vfs = hot_vfs;
    env->router.cold_vfs = cold_vfs;
    env->router.cold_storage_start_level = cold_level;

    env->block_cache = lsm_cache_init(cache_bytes);
    env->bg_pool = lsm_pool_init(num_bg_threads);

    env->global_mem_limit = global_memtable_limit;
    atomic_init(&env->global_mem_usage, 0);

    env->global_wal = global_wal;

    env->tables_capacity = 256;
    env->tables = aml_malloc(env->tables_capacity * sizeof(lsm_db_t*));
    env->num_tables = 0;
    pthread_mutex_init(&env->env_mutex, NULL);

    return env;
}

void lsm_env_register_db(lsm_env_t *env, lsm_db_t *db) {
    pthread_mutex_lock(&env->env_mutex);
    if (env->num_tables >= env->tables_capacity) {
        env->tables_capacity *= 2;
        env->tables = aml_realloc(env->tables, env->tables_capacity * sizeof(lsm_db_t*));
    }
    env->tables[env->num_tables++] = db;
    pthread_mutex_unlock(&env->env_mutex);
}

void lsm_env_unregister_db(lsm_env_t *env, lsm_db_t *db) {
    pthread_mutex_lock(&env->env_mutex);
    for (size_t i = 0; i < env->num_tables; i++) {
        if (env->tables[i] == db) {
            env->tables[i] = env->tables[--env->num_tables];
            break;
        }
    }
    pthread_mutex_unlock(&env->env_mutex);
}

void lsm_env_enforce_memory_limit(lsm_env_t *env) {
    if (atomic_load(&env->global_mem_usage) <= env->global_mem_limit) return;

    if (pthread_mutex_trylock(&env->env_mutex) != 0) return;

    lsm_db_t *largest_db = NULL;
    size_t max_size = 0;

    for (size_t i = 0; i < env->num_tables; i++) {
        size_t sz = lsm_db_get_active_mem_usage(env->tables[i]);
        if (sz > max_size) {
            max_size = sz;
            largest_db = env->tables[i];
        }
    }

    if (largest_db) {
        lsm_db_retain(largest_db);
    }
    pthread_mutex_unlock(&env->env_mutex);

    if (largest_db) {
        if (max_size > 1024 * 1024) {
            lsm_db_force_flush(largest_db);
        }
        lsm_db_release(largest_db);
    }
}

extern void lsm_db_inject_recovery(lsm_db_t *db, uint8_t op, const void *key, uint32_t klen, const void *val, uint32_t vlen);

void lsm_env_recover_wal(lsm_env_t *env) {
    if (!env->global_wal || !env->global_wal->iter_init) return;

    lsm_wal_iter_t *iter = env->global_wal->iter_init(env->global_wal);
    if (!iter) return;

    uint32_t table_id;
    uint8_t op;
    const void *k, *v;
    uint32_t klen, vlen;

    while (iter->next(iter, &table_id, &op, &k, &klen, &v, &vlen)) {
        lsm_db_t *target_db = NULL;
        pthread_mutex_lock(&env->env_mutex);
        for (size_t i = 0; i < env->num_tables; i++) {
            if (lsm_db_get_table_id(env->tables[i]) == table_id) {
                target_db = env->tables[i];
                lsm_db_retain(target_db);
                break;
            }
        }
        pthread_mutex_unlock(&env->env_mutex);

        if (target_db) {
            lsm_db_inject_recovery(target_db, op, k, klen, v, vlen);
            lsm_db_release(target_db);
        }
    }

    if (iter->destroy) iter->destroy(iter);
}

void lsm_env_destroy(lsm_env_t *env) {
    if (!env) return;

    // FIX B16: Safely close any remaining orphaned DBs to guarantee graceful thread shutdown
    while (env->num_tables > 0) {
        lsm_db_close(env->tables[env->num_tables - 1]);
    }

    lsm_pool_destroy(env->bg_pool);
    lsm_cache_destroy(env->block_cache);
    pthread_mutex_destroy(&env->env_mutex);
    aml_free(env->tables);
    aml_free(env);
}
