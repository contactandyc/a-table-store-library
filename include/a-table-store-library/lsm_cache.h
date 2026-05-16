// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef LSM_CACHE_H
#define LSM_CACHE_H

#include <stdint.h>
#include <stddef.h>

typedef struct lsm_cache_s lsm_cache_t;

lsm_cache_t *lsm_cache_init(size_t total_capacity_bytes);

/*
 * DEDUPLICATING INSERT:
 * If another thread inserted this block first, the cache will free() your block
 * and return the existing one. Always sets ref_count = 1 for the returned block.
 */
void *lsm_cache_put_or_get(lsm_cache_t *cache, uint32_t table_id, uint64_t file_id, uint64_t offset, void *block, size_t block_size);

void *lsm_cache_get(lsm_cache_t *cache, uint32_t table_id, uint64_t file_id, uint64_t offset, size_t *out_size);

/* O(1) Release using the unique composite key */
void lsm_cache_release(lsm_cache_t *cache, uint32_t table_id, uint64_t file_id, uint64_t offset);

void lsm_cache_destroy(lsm_cache_t *cache);

#endif /* LSM_CACHE_H */
