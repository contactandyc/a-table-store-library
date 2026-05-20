// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef LSM_POOL_H
#define LSM_POOL_H

#include <pthread.h>
#include <stdbool.h>

typedef struct lsm_pool_s lsm_pool_t;

typedef enum {
    JOB_PRIORITY_LOW = 0,    // L1+ Compactions (Cold data merging)
    JOB_PRIORITY_HIGH = 1,   // L0 Compactions (To clear the hot local disk)
    JOB_PRIORITY_URGENT = 2  // MemTable Flushes (Must execute immediately to prevent Write Stalls)
} lsm_job_priority_t;

typedef void (*lsm_job_func_t)(void *arg);

lsm_pool_t *lsm_pool_init(int num_threads);

/* Returns false if the pool is shutting down and the job was rejected */
bool lsm_pool_submit(lsm_pool_t *pool, lsm_job_func_t func, void *arg, lsm_job_priority_t priority);

void lsm_pool_destroy(lsm_pool_t *pool);

#endif /* LSM_POOL_H */
