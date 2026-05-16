// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "a-table-store-library/sstable_builder.h"
#include "a-table-store-library/sstable_reader.h"
#include "a-table-store-library/lsm_env.h"
#include "a-table-store-library/memtable.h" // FIX: Added to expose OP_PUT / OP_DELETE
#include "the-macro-library/macro_test.h"

// Helper to construct a packed internal key
static void pack_ikey(char *dst, const char *user_key, uint64_t seq, uint8_t op) {
    size_t ulen = strlen(user_key);
    memcpy(dst, user_key, ulen);
    uint64_t trailer = (seq << 8) | op;
    memcpy(dst + ulen, &trailer, 8);
}

static void cleanup_files() {
    system("rm -rf /tmp/sstable_test*");
}

MACRO_TEST(sstable_build_and_read_roundtrip) {
    cleanup_files();

    lsm_env_t *env = lsm_env_init(1024 * 1024, 1024 * 1024 * 10, 1,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);

    const char *base = "/tmp/sstable_test_01";
    sstable_builder_t *b = sstable_builder_init(base, &local_posix_backend, FILTER_BLOOM, 100);

    char ikey1[1024], ikey2[1024];
    pack_ikey(ikey1, "apple", 10, OP_PUT);
    pack_ikey(ikey2, "zebra", 11, OP_PUT);

    sstable_builder_add(b, ikey1, 5 + 8, "red", 3);
    sstable_builder_add(b, ikey2, 5 + 8, "stripes", 6);
    sstable_builder_finish(b);

    sstable_reader_t *r = sstable_reader_init(base, &local_posix_backend, env, 1, 1);
    MACRO_ASSERT_TRUE(r != NULL);

    uint32_t vlen;
    void *val;
    int status;

    // Test hit
    status = sstable_reader_get(r, "apple", 5, &val, &vlen);
    MACRO_ASSERT_EQ_INT(status, 1);
    MACRO_ASSERT_EQ_INT(vlen, 3);
    MACRO_ASSERT_TRUE(memcmp(val, "red", 3) == 0);
    free(val);

    // Test miss
    status = sstable_reader_get(r, "banana", 6, &val, &vlen);
    MACRO_ASSERT_EQ_INT(status, 0); // Bloom filter should outright reject it

    sstable_reader_destroy(r);
    lsm_env_destroy(env);
}

MACRO_TEST(sstable_tombstone_parsing_returns_negative_one) {
    cleanup_files();
    lsm_env_t *env = lsm_env_init(1024 * 1024, 1024 * 1024 * 10, 1,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);

    const char *base = "/tmp/sstable_test_02";
    sstable_builder_t *b = sstable_builder_init(base, &local_posix_backend, FILTER_BLOOM, 100);

    char ikey[1024];
    pack_ikey(ikey, "ghost", 15, OP_DELETE); // Write a tombstone explicitly

    sstable_builder_add(b, ikey, 5 + 8, NULL, 0);
    sstable_builder_finish(b);

    sstable_reader_t *r = sstable_reader_init(base, &local_posix_backend, env, 1, 1);

    uint32_t vlen;
    void *val = NULL;
    int status = sstable_reader_get(r, "ghost", 5, &val, &vlen);

    // Must return -1 to signal to the DB that older levels should be masked
    MACRO_ASSERT_EQ_INT(status, -1);

    sstable_reader_destroy(r);
    lsm_env_destroy(env);
}

MACRO_TEST(sstable_iterator_decodes_trailers_correctly) {
    cleanup_files();
    lsm_env_t *env = lsm_env_init(1024 * 1024, 1024 * 1024 * 10, 1,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);

    const char *base = "/tmp/sstable_test_03";
    sstable_builder_t *b = sstable_builder_init(base, &local_posix_backend, FILTER_NONE, 100);

    char ikey[1024];
    pack_ikey(ikey, "keyA", 50, OP_PUT);
    sstable_builder_add(b, ikey, 4 + 8, "valA", 4);
    sstable_builder_finish(b);

    sstable_reader_t *r = sstable_reader_init(base, &local_posix_backend, env, 1, 1);
    sstable_iter_t *it = sstable_iter_init(r);

    MACRO_ASSERT_TRUE(sstable_iter_next(it));

    const char *k, *v;
    uint32_t klen, vlen;
    sstable_iter_get_kv(it, &k, &klen, &v, &vlen);

    // Iterators return Internal Keys (so length is 4 + 8 = 12)
    MACRO_ASSERT_EQ_INT(klen, 12);
    MACRO_ASSERT_TRUE(memcmp(k, "keyA", 4) == 0);

    uint64_t seq;
    uint8_t op;
    sstable_iter_get_meta(it, &seq, &op);

    MACRO_ASSERT_EQ_INT(seq, 50);
    MACRO_ASSERT_EQ_INT(op, OP_PUT);

    sstable_iter_destroy(it);
    sstable_reader_destroy(r);
    lsm_env_destroy(env);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, sstable_build_and_read_roundtrip);
    MACRO_ADD(tests, sstable_tombstone_parsing_returns_negative_one);
    MACRO_ADD(tests, sstable_iterator_decodes_trailers_correctly);

    macro_run_all("lsm_sstable_disk_format", tests, test_count);
    return 0;
}
