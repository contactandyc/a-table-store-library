// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "a-table-store-library/lsm_cache.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(cache_basic_put_and_get) {
    lsm_cache_t *cache = lsm_cache_init(1024 * 1024); // 1MB

    void *block = malloc(4096);
    memset(block, 0xAA, 4096);

    // Insert block (Cache takes ownership, dedups if necessary)
    void *cached = lsm_cache_put_or_get(cache, 1, 100, 0, block, 4096);
    MACRO_ASSERT_TRUE(cached == block); // Because it was new, it should return same pointer

    // Retrieve it
    size_t out_size;
    void *retrieved = lsm_cache_get(cache, 1, 100, 0, &out_size);
    MACRO_ASSERT_TRUE(retrieved != NULL);
    MACRO_ASSERT_EQ_INT(out_size, 4096);

    // Must release both the original put's implicit ref, and the get's ref
    lsm_cache_release(cache, 1, 100, 0);
    lsm_cache_release(cache, 1, 100, 0);

    lsm_cache_destroy(cache);
}

MACRO_TEST(cache_deduplicates_racing_inserts) {
    lsm_cache_t *cache = lsm_cache_init(1024 * 1024);

    void *block1 = malloc(4096);
    void *block2 = malloc(4096); // Represents a duplicate disk read

    void *c1 = lsm_cache_put_or_get(cache, 1, 50, 0, block1, 4096);
    void *c2 = lsm_cache_put_or_get(cache, 1, 50, 0, block2, 4096);

    // Cache MUST drop block2, free it immediately, and return the block1 pointer
    MACRO_ASSERT_TRUE(c1 == block1);
    MACRO_ASSERT_TRUE(c2 == block1);

    lsm_cache_release(cache, 1, 50, 0);
    lsm_cache_release(cache, 1, 50, 0);

    lsm_cache_destroy(cache);
}

MACRO_TEST(cache_lru_eviction_respects_capacity) {
    lsm_cache_t *cache = lsm_cache_init(1024 * 1024); // 1 MB Capacity

    void *b1 = malloc(4096);
    lsm_cache_put_or_get(cache, 1, 1, 0, b1, 4096);
    lsm_cache_release(cache, 1, 1, 0);

    // Push 500 blocks (~2 MB total) to mathematically guarantee
    // that all 64 shards are completely overrun and force evictions.
    for (uint64_t i = 2; i <= 500; i++) {
        void *b = malloc(4096);
        lsm_cache_put_or_get(cache, 1, i, 0, b, 4096);
        lsm_cache_release(cache, 1, i, 0);
    }

    size_t sz;
    // Block 1 was pushed far out of the LRU and should be evicted
    MACRO_ASSERT_TRUE(lsm_cache_get(cache, 1, 1, 0, &sz) == NULL);

    // Block 500 was just inserted and should have survived
    MACRO_ASSERT_TRUE(lsm_cache_get(cache, 1, 500, 0, &sz) != NULL);
    lsm_cache_release(cache, 1, 500, 0);

    lsm_cache_destroy(cache);
}

MACRO_TEST(cache_pinned_blocks_prevent_eviction_without_hanging) {
    lsm_cache_t *cache = lsm_cache_init(1024 * 1024); // 1 MB Capacity

    // Push 500 blocks (~2 MB total) and PIN them all by NOT releasing them!
    for (uint64_t i = 1; i <= 500; i++) {
        void *b = malloc(4096);
        lsm_cache_put_or_get(cache, 1, i, 0, b, 4096);
    }

    size_t sz;
    // Because they are pinned, the cache must bypass the hard limit to prevent
    // an infinite hang, allowing memory to float above capacity temporarily.
    // Block 1 should STILL be there!
    MACRO_ASSERT_TRUE(lsm_cache_get(cache, 1, 1, 0, &sz) != NULL);

    lsm_cache_release(cache, 1, 1, 0); // Release the manual get

    // Cleanup the original put pins
    for (uint64_t i = 1; i <= 500; i++) {
        lsm_cache_release(cache, 1, i, 0);
    }

    lsm_cache_destroy(cache);
}

MACRO_TEST(cache_resists_refcount_underflow) {
    lsm_cache_t *cache = lsm_cache_init(1024 * 1024);

    void *b1 = malloc(4096);
    lsm_cache_put_or_get(cache, 1, 99, 0, b1, 4096);

    // Intentionally over-release the block
    lsm_cache_release(cache, 1, 99, 0);
    lsm_cache_release(cache, 1, 99, 0);
    lsm_cache_release(cache, 1, 99, 0);

    // Force LRU eviction cycle
    for (uint64_t i = 100; i <= 600; i++) {
        void *b = malloc(4096);
        lsm_cache_put_or_get(cache, 1, i, 0, b, 4096);
        lsm_cache_release(cache, 1, i, 0);
    }

    size_t sz;
    // If the saturated decrement worked, the refcount hit 0 (not -2) and allowed
    // the block to be naturally evicted by the LRU sweep.
    MACRO_ASSERT_TRUE(lsm_cache_get(cache, 1, 99, 0, &sz) == NULL);

    lsm_cache_destroy(cache);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, cache_basic_put_and_get);
    MACRO_ADD(tests, cache_deduplicates_racing_inserts);
    MACRO_ADD(tests, cache_lru_eviction_respects_capacity);
    MACRO_ADD(tests, cache_pinned_blocks_prevent_eviction_without_hanging);
    MACRO_ADD(tests, cache_resists_refcount_underflow);

    macro_run_all("lsm_cache", tests, test_count);
    return 0;
}
