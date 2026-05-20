// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-table-store-library/lsm_pool.h"
#include "a-memory-library/aml_alloc.h"
#include <stdbool.h>

typedef struct lsm_job_node_s {
    lsm_job_func_t func;
    void *arg;
    struct lsm_job_node_s *next;
} lsm_job_node_t;

struct lsm_pool_s {
    pthread_t *threads;
    int num_threads;

    pthread_mutex_t queue_mutex;
    pthread_cond_t queue_cond;

    // 3 Distinct Priority Queues
    lsm_job_node_t *queues[3];
    lsm_job_node_t *tails[3];

    int queued_jobs;
    int executing_jobs;
    bool shutdown;
};

static void *worker_loop(void *arg) {
    lsm_pool_t *pool = (lsm_pool_t *)arg;

    while (true) {
        pthread_mutex_lock(&pool->queue_mutex);

        while (pool->queued_jobs == 0 && !pool->shutdown) {
            pthread_cond_wait(&pool->queue_cond, &pool->queue_mutex);
        }

        if (pool->shutdown && pool->queued_jobs == 0) {
            pthread_mutex_unlock(&pool->queue_mutex);
            break;
        }

        // Pop highest priority job (2 -> 1 -> 0)
        lsm_job_node_t *job_node = NULL;
        for (int p = 2; p >= 0; p--) {
            if (pool->queues[p] != NULL) {
                job_node = pool->queues[p];
                pool->queues[p] = job_node->next;
                if (pool->queues[p] == NULL) {
                    pool->tails[p] = NULL;
                }
                pool->queued_jobs--;
                pool->executing_jobs++; // Mark as executing while we drop the lock
                break;
            }
        }

        pthread_mutex_unlock(&pool->queue_mutex);

        // Execute outside the lock so other threads can pick up jobs!
        if (job_node) {
            job_node->func(job_node->arg);
            aml_free(job_node);

            pthread_mutex_lock(&pool->queue_mutex);
            pool->executing_jobs--;
            pthread_mutex_unlock(&pool->queue_mutex);
        }
    }
    return NULL;
}

lsm_pool_t *lsm_pool_init(int num_threads) {
    lsm_pool_t *pool = aml_zalloc(sizeof(lsm_pool_t));
    pool->num_threads = num_threads;

    pthread_mutex_init(&pool->queue_mutex, NULL);
    pthread_cond_init(&pool->queue_cond, NULL);

    pool->threads = aml_malloc(num_threads * sizeof(pthread_t));
    for (int i = 0; i < num_threads; i++) {
        pthread_create(&pool->threads[i], NULL, worker_loop, pool);
    }

    return pool;
}

bool lsm_pool_submit(lsm_pool_t *pool, lsm_job_func_t func, void *arg, lsm_job_priority_t priority) {
    if (priority < 0 || priority > 2) priority = 0;

    lsm_job_node_t *node = aml_zalloc(sizeof(lsm_job_node_t));
    node->func = func;
    node->arg = arg;

    pthread_mutex_lock(&pool->queue_mutex);

    if (pool->shutdown) {
        pthread_mutex_unlock(&pool->queue_mutex);
        aml_free(node);
        return false;
    }

    if (pool->tails[priority] == NULL) {
        pool->queues[priority] = node;
        pool->tails[priority] = node;
    } else {
        pool->tails[priority]->next = node;
        pool->tails[priority] = node;
    }

    pool->queued_jobs++;
    pthread_cond_signal(&pool->queue_cond);

    pthread_mutex_unlock(&pool->queue_mutex);
    return true;
}

void lsm_pool_destroy(lsm_pool_t *pool) {
    if (!pool) return;

    pthread_mutex_lock(&pool->queue_mutex);
    pool->shutdown = true;
    pthread_cond_broadcast(&pool->queue_cond);
    pthread_mutex_unlock(&pool->queue_mutex);

    for (int i = 0; i < pool->num_threads; i++) {
        pthread_join(pool->threads[i], NULL);
    }

    pthread_mutex_destroy(&pool->queue_mutex);
    pthread_cond_destroy(&pool->queue_cond);

    aml_free(pool->threads);
    aml_free(pool);
}
