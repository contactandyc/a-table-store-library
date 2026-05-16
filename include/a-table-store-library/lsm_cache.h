// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef LSM_CACHE_H
#define LSM_CACHE_H

#include <stdint.h>
#include <stddef.h>

typedef struct lsm_cache_s lsm_cache_t;
typedef struct cache_node_s lsm_cache_handle_t;

lsm_cache_t *lsm_cache_init(size_t total_capacity_bytes);

/*
 * DEDUPLICATING INSERT:
 * Returns a PINNED handle. Caller MUST call lsm_cache_handle_release.
 */
lsm_cache_handle_t *lsm_cache_put_or_get(lsm_cache_t *cache, uint32_t table_id, uint64_t file_id, uint64_t offset, void *block, size_t block_size);

/* Returns a PINNED handle or NULL. Caller MUST release if not NULL. */
lsm_cache_handle_t *lsm_cache_get(lsm_cache_t *cache, uint32_t table_id, uint64_t file_id, uint64_t offset);

/* Extract the raw payload from the pinned handle */
const void *lsm_cache_handle_data(lsm_cache_handle_t *handle, size_t *out_size);

/* Drop the pin. If ref_count reaches 0, the LRU can evict it. */
void lsm_cache_handle_release(lsm_cache_t *cache, lsm_cache_handle_t *handle);

void lsm_cache_destroy(lsm_cache_t *cache);

#endif /* LSM_CACHE_H */
