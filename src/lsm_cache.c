// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-table-store-library/lsm_cache.h"
#include "a-memory-library/aml_alloc.h"
#include <pthread.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

#define NUM_SHARDS 64
#define BUCKETS_PER_SHARD 2048

typedef struct cache_node_s {
    uint32_t table_id;
    uint64_t file_id;
    uint64_t offset;
    void *block;
    size_t size;
    int ref_count;
    struct cache_node_s *prev;
    struct cache_node_s *next;
    struct cache_node_s *hash_next;
} cache_node_t;

typedef struct {
    pthread_mutex_t mutex;
    size_t capacity;
    size_t usage;
    cache_node_t *hash_table[BUCKETS_PER_SHARD];
    cache_node_t *lru_head;
    cache_node_t *lru_tail;
} cache_shard_t;

struct lsm_cache_s {
    cache_shard_t shards[NUM_SHARDS];
};

static inline uint64_t hash_key(uint32_t tid, uint64_t fid, uint64_t off) {
    uint64_t h = tid;
    h ^= (fid << 16) | (fid >> 48);
    h ^= (off << 32) | (off >> 32);
    h *= 0x9E3779B97F4A7C15ULL;
    h ^= h >> 30;
    return h;
}

static inline void get_shard_and_bucket(uint64_t h, uint32_t *shard_idx, uint32_t *bucket) {
    *shard_idx = h & (NUM_SHARDS - 1);
    *bucket    = (h >> 6) & (BUCKETS_PER_SHARD - 1);
}

static void lru_remove(cache_shard_t *shard, cache_node_t *node) {
    if (node->prev) node->prev->next = node->next;
    else shard->lru_head = node->next;
    if (node->next) node->next->prev = node->prev;
    else shard->lru_tail = node->prev;
}

static void lru_push_front(cache_shard_t *shard, cache_node_t *node) {
    node->next = shard->lru_head;
    node->prev = NULL;
    if (shard->lru_head) shard->lru_head->prev = node;
    shard->lru_head = node;
    if (!shard->lru_tail) shard->lru_tail = node;
}

static bool evict_from_shard(cache_shard_t *shard) {
    cache_node_t *curr = shard->lru_tail;
    while (curr != NULL && curr->ref_count > 0) {
        curr = curr->prev;
    }
    if (!curr) return false;

    lru_remove(shard, curr);

    uint64_t h = hash_key(curr->table_id, curr->file_id, curr->offset);
    uint32_t shard_idx, bucket;
    get_shard_and_bucket(h, &shard_idx, &bucket);

    cache_node_t **ptr = &shard->hash_table[bucket];
    while (*ptr) {
        if (*ptr == curr) {
            *ptr = curr->hash_next;
            break;
        }
        ptr = &(*ptr)->hash_next;
    }

    shard->usage -= curr->size;
    aml_free(curr->block);
    aml_free(curr);
    return true;
}

lsm_cache_t *lsm_cache_init(size_t total_capacity_bytes) {
    lsm_cache_t *cache = aml_zalloc(sizeof(lsm_cache_t));
    size_t shard_cap = total_capacity_bytes / NUM_SHARDS;
    for (int i = 0; i < NUM_SHARDS; i++) {
        pthread_mutex_init(&cache->shards[i].mutex, NULL);
        cache->shards[i].capacity = shard_cap;
        cache->shards[i].usage = 0;
    }
    return cache;
}

void *lsm_cache_put_or_get(lsm_cache_t *cache, uint32_t table_id, uint64_t file_id, uint64_t offset, void *block, size_t block_size) {
    uint64_t h = hash_key(table_id, file_id, offset);
    uint32_t shard_idx, bucket;
    get_shard_and_bucket(h, &shard_idx, &bucket);

    cache_shard_t *shard = &cache->shards[shard_idx];

    pthread_mutex_lock(&shard->mutex);

    cache_node_t *curr = shard->hash_table[bucket];
    while (curr) {
        if (curr->table_id == table_id && curr->file_id == file_id && curr->offset == offset) {
            curr->ref_count++;
            lru_remove(shard, curr);
            lru_push_front(shard, curr);
            pthread_mutex_unlock(&shard->mutex);

            aml_free(block);
            return curr->block;
        }
        curr = curr->hash_next;
    }

    while (shard->usage + block_size > shard->capacity && shard->usage > 0) {
        if (!evict_from_shard(shard)) break;
    }

    cache_node_t *node = aml_zalloc(sizeof(cache_node_t));
    node->table_id = table_id;
    node->file_id = file_id;
    node->offset = offset;
    node->block = block;
    node->size = block_size;
    node->ref_count = 1;

    node->hash_next = shard->hash_table[bucket];
    shard->hash_table[bucket] = node;

    lru_push_front(shard, node);
    shard->usage += block_size;

    pthread_mutex_unlock(&shard->mutex);
    return block;
}

void *lsm_cache_get(lsm_cache_t *cache, uint32_t table_id, uint64_t file_id, uint64_t offset, size_t *out_size) {
    uint64_t h = hash_key(table_id, file_id, offset);
    uint32_t shard_idx, bucket;
    get_shard_and_bucket(h, &shard_idx, &bucket);

    cache_shard_t *shard = &cache->shards[shard_idx];

    pthread_mutex_lock(&shard->mutex);
    cache_node_t *curr = shard->hash_table[bucket];

    while (curr) {
        if (curr->table_id == table_id && curr->file_id == file_id && curr->offset == offset) {
            curr->ref_count++;
            lru_remove(shard, curr);
            lru_push_front(shard, curr);

            if (out_size) *out_size = curr->size;
            void *ret_block = curr->block;

            pthread_mutex_unlock(&shard->mutex);
            return ret_block;
        }
        curr = curr->hash_next;
    }

    pthread_mutex_unlock(&shard->mutex);
    return NULL;
}

void lsm_cache_release(lsm_cache_t *cache, uint32_t table_id, uint64_t file_id, uint64_t offset) {
    uint64_t h = hash_key(table_id, file_id, offset);
    uint32_t shard_idx, bucket;
    get_shard_and_bucket(h, &shard_idx, &bucket);

    cache_shard_t *shard = &cache->shards[shard_idx];

    pthread_mutex_lock(&shard->mutex);
    cache_node_t *curr = shard->hash_table[bucket];
    while (curr) {
        if (curr->table_id == table_id && curr->file_id == file_id && curr->offset == offset) {
            curr->ref_count--;
            break;
        }
        curr = curr->hash_next;
    }
    pthread_mutex_unlock(&shard->mutex);
}

void lsm_cache_destroy(lsm_cache_t *cache) {
    if (!cache) return;
    for (int i = 0; i < NUM_SHARDS; i++) {
        pthread_mutex_lock(&cache->shards[i].mutex);
        cache_node_t *curr = cache->shards[i].lru_head;
        while (curr) {
            cache_node_t *next = curr->next;
            aml_free(curr->block);
            aml_free(curr);
            curr = next;
        }
        pthread_mutex_unlock(&cache->shards[i].mutex);
        pthread_mutex_destroy(&cache->shards[i].mutex);
    }
    aml_free(cache);
}
