// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "a-table-store-library/lsm_cache.h"
#include "a-memory-library/aml_alloc.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(cache_basic_put_and_get) {
    lsm_cache_t *cache = lsm_cache_init(1024 * 1024); // 1MB

    void *block = aml_malloc(4096);
    memset(block, 0xAA, 4096);

    lsm_cache_handle_t *h1 = lsm_cache_put_or_get(cache, 1, 100, 0, block, 4096);
    MACRO_ASSERT_TRUE(lsm_cache_handle_data(h1, NULL) == block);

    lsm_cache_handle_t *h2 = lsm_cache_get(cache, 1, 100, 0);
    MACRO_ASSERT_TRUE(h2 != NULL);

    size_t out_size;
    MACRO_ASSERT_TRUE(lsm_cache_handle_data(h2, &out_size) == block);
    MACRO_ASSERT_EQ_INT(out_size, 4096);

    lsm_cache_handle_release(cache, h1);
    lsm_cache_handle_release(cache, h2);

    lsm_cache_destroy(cache);
}

MACRO_TEST(cache_deduplicates_racing_inserts) {
    lsm_cache_t *cache = lsm_cache_init(1024 * 1024);

    void *block1 = aml_malloc(4096);
    void *block2 = aml_malloc(4096);

    lsm_cache_handle_t *c1 = lsm_cache_put_or_get(cache, 1, 50, 0, block1, 4096);
    lsm_cache_handle_t *c2 = lsm_cache_put_or_get(cache, 1, 50, 0, block2, 4096);

    MACRO_ASSERT_TRUE(lsm_cache_handle_data(c1, NULL) == block1);
    MACRO_ASSERT_TRUE(lsm_cache_handle_data(c2, NULL) == block1);

    lsm_cache_handle_release(cache, c1);
    lsm_cache_handle_release(cache, c2);

    lsm_cache_destroy(cache);
}

MACRO_TEST(cache_lru_eviction_respects_capacity) {
    lsm_cache_t *cache = lsm_cache_init(1024 * 1024);

    void *b1 = aml_malloc(4096);
    lsm_cache_handle_t *h_first = lsm_cache_put_or_get(cache, 1, 1, 0, b1, 4096);
    lsm_cache_handle_release(cache, h_first);

    for (uint64_t i = 2; i <= 500; i++) {
        void *b = aml_malloc(4096);
        lsm_cache_handle_t *h = lsm_cache_put_or_get(cache, 1, i, 0, b, 4096);
        lsm_cache_handle_release(cache, h);
    }

    MACRO_ASSERT_TRUE(lsm_cache_get(cache, 1, 1, 0) == NULL);

    lsm_cache_handle_t *h_last = lsm_cache_get(cache, 1, 500, 0);
    MACRO_ASSERT_TRUE(h_last != NULL);
    lsm_cache_handle_release(cache, h_last);

    lsm_cache_destroy(cache);
}

MACRO_TEST(cache_pinned_blocks_prevent_eviction_without_hanging) {
    lsm_cache_t *cache = lsm_cache_init(1024 * 1024);

    lsm_cache_handle_t *pins[501];
    for (uint64_t i = 1; i <= 500; i++) {
        void *b = aml_malloc(4096);
        pins[i] = lsm_cache_put_or_get(cache, 1, i, 0, b, 4096);
    }

    lsm_cache_handle_t *check = lsm_cache_get(cache, 1, 1, 0);
    MACRO_ASSERT_TRUE(check != NULL);
    lsm_cache_handle_release(cache, check);

    for (uint64_t i = 1; i <= 500; i++) {
        lsm_cache_handle_release(cache, pins[i]);
    }

    lsm_cache_destroy(cache);
}

MACRO_TEST(cache_aborts_on_double_release) {
    // We must fork because the expected behavior is a fatal process abort!
    pid_t pid = fork();
    if (pid == 0) {
        lsm_cache_t *cache = lsm_cache_init(1024 * 1024);
        void *b = aml_malloc(4096);
        lsm_cache_handle_t *h = lsm_cache_put_or_get(cache, 1, 1, 0, b, 4096);

        lsm_cache_handle_release(cache, h); // Legal release
        lsm_cache_handle_release(cache, h); // FATAL: Double release! Should abort.

        exit(0); // Should never reach this
    }

    int status;
    waitpid(pid, &status, 0);

    // ASSERTION: The child process was killed by a signal (SIGABRT)
    MACRO_ASSERT_TRUE(WIFSIGNALED(status));
    MACRO_ASSERT_EQ_INT(WTERMSIG(status), SIGABRT);
}

MACRO_TEST(cache_aborts_if_destroyed_with_pinned_blocks) {
    pid_t pid = fork();
    if (pid == 0) {
        lsm_cache_t *cache = lsm_cache_init(1024 * 1024);
        void *b = aml_malloc(4096);

        // Pin a block and deliberately "forget" to release it
        lsm_cache_put_or_get(cache, 1, 1, 0, b, 4096);

        // FATAL: Destroying the cache while a block is pinned means an active
        // reader thread is about to get a Use-After-Free. Should abort!
        lsm_cache_destroy(cache);

        exit(0);
    }

    int status;
    waitpid(pid, &status, 0);

    MACRO_ASSERT_TRUE(WIFSIGNALED(status));
    MACRO_ASSERT_EQ_INT(WTERMSIG(status), SIGABRT);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, cache_basic_put_and_get);
    MACRO_ADD(tests, cache_deduplicates_racing_inserts);
    MACRO_ADD(tests, cache_lru_eviction_respects_capacity);
    MACRO_ADD(tests, cache_pinned_blocks_prevent_eviction_without_hanging);
    MACRO_ADD(tests, cache_aborts_on_double_release);
    MACRO_ADD(tests, cache_aborts_if_destroyed_with_pinned_blocks);

    macro_run_all("lsm_cache", tests, test_count);
    return 0;
}
