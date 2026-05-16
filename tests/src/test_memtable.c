// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "a-table-store-library/memtable.h"
#include "the-macro-library/macro_test.h"

// --- 1. BASIC CRUD & TOMBSTONES ---

MACRO_TEST(memtable_put_and_get) {
    memtable_t *mt = memtable_init();

    // Put "key1" = "val1" at Seq 1
    MACRO_ASSERT_TRUE(memtable_put(mt, 1, OP_PUT, "key1", 4, "val1", 4));

    uint32_t vlen = 0;
    bool is_deleted = false;
    const char *val = (const char *)memtable_get(mt, "key1", 4, UINT64_MAX, &vlen, &is_deleted);

    MACRO_ASSERT_TRUE(val != NULL);
    MACRO_ASSERT_EQ_INT(vlen, 4);
    MACRO_ASSERT_FALSE(is_deleted);
    MACRO_ASSERT_TRUE(memcmp(val, "val1", 4) == 0);

    memtable_release(mt);
}

MACRO_TEST(memtable_update_shadows_older_version) {
    memtable_t *mt = memtable_init();

    memtable_put(mt, 1, OP_PUT, "key1", 4, "val1", 4);
    memtable_put(mt, 2, OP_PUT, "key1", 4, "val2", 4); // Higher sequence

    uint32_t vlen = 0;
    bool is_deleted = false;
    const char *val = (const char *)memtable_get(mt, "key1", 4, UINT64_MAX, &vlen, &is_deleted);

    MACRO_ASSERT_TRUE(val != NULL);
    MACRO_ASSERT_TRUE(memcmp(val, "val2", 4) == 0);

    memtable_release(mt);
}

MACRO_TEST(memtable_delete_masks_previous_value) {
    memtable_t *mt = memtable_init();

    memtable_put(mt, 1, OP_PUT, "key1", 4, "val1", 4);
    memtable_put(mt, 2, OP_DELETE, "key1", 4, NULL, 0); // Tombstone

    uint32_t vlen = 0;
    bool is_deleted = false;
    const char *val = (const char *)memtable_get(mt, "key1", 4, UINT64_MAX, &vlen, &is_deleted);

    // Must return NULL but report it explicitly as a tombstone
    MACRO_ASSERT_TRUE(val == NULL);
    MACRO_ASSERT_TRUE(is_deleted);

    memtable_release(mt);
}

// --- 2. SNAPSHOT ISOLATION (MVCC) ---

MACRO_TEST(memtable_snapshot_read_ignores_future_writes) {
    memtable_t *mt = memtable_init();

    memtable_put(mt, 1, OP_PUT, "key1", 4, "val1", 4);
    memtable_put(mt, 3, OP_PUT, "key1", 4, "val3", 4);

    uint32_t vlen = 0;
    bool is_deleted = false;

    // Read at sequence 2 (Should see "val1", ignoring "val3")
    const char *val = (const char *)memtable_get(mt, "key1", 4, 2, &vlen, &is_deleted);

    MACRO_ASSERT_TRUE(val != NULL);
    MACRO_ASSERT_TRUE(memcmp(val, "val1", 4) == 0);

    memtable_release(mt);
}

MACRO_TEST(memtable_snapshot_read_ignores_future_tombstones) {
    memtable_t *mt = memtable_init();

    memtable_put(mt, 1, OP_PUT, "key1", 4, "val1", 4);
    memtable_put(mt, 2, OP_DELETE, "key1", 4, NULL, 0);

    uint32_t vlen = 0;
    bool is_deleted = false;

    // Read at seq 1. The key was deleted at 2, but the snapshot should still see it.
    const char *val = (const char *)memtable_get(mt, "key1", 4, 1, &vlen, &is_deleted);

    MACRO_ASSERT_TRUE(val != NULL);
    MACRO_ASSERT_FALSE(is_deleted);

    memtable_release(mt);
}

// --- 3. ITERATION & ORDERING ---

MACRO_TEST(memtable_iteration_orders_keys_and_seqs_descending) {
    memtable_t *mt = memtable_init();

    memtable_put(mt, 1, OP_PUT, "B", 1, "1", 1);
    memtable_put(mt, 2, OP_PUT, "A", 1, "2", 1);
    memtable_put(mt, 3, OP_PUT, "A", 1, "3", 1); // A@3 shadows A@2

    memtable_row_t *row = memtable_first(mt);
    MACRO_ASSERT_TRUE(row != NULL);

    uint32_t klen;

    // 1st: A @ 3
    const char *k = (const char *)memtable_row_get_key(row, &klen);
    MACRO_ASSERT_TRUE(memcmp(k, "A", 1) == 0);
    MACRO_ASSERT_EQ_INT(memtable_row_get_seq(row), 3);

    // 2nd: A @ 2
    row = memtable_next(row);
    k = (const char *)memtable_row_get_key(row, &klen);
    MACRO_ASSERT_TRUE(memcmp(k, "A", 1) == 0);
    MACRO_ASSERT_EQ_INT(memtable_row_get_seq(row), 2);

    // 3rd: B @ 1
    row = memtable_next(row);
    k = (const char *)memtable_row_get_key(row, &klen);
    MACRO_ASSERT_TRUE(memcmp(k, "B", 1) == 0);
    MACRO_ASSERT_EQ_INT(memtable_row_get_seq(row), 1);

    row = memtable_next(row);
    MACRO_ASSERT_TRUE(row == NULL);

    memtable_release(mt);
}

MACRO_TEST(memtable_isolates_primary_and_index_records) {
    memtable_t *mt = memtable_init();

    // 1. Insert into primary
    MACRO_ASSERT_TRUE(memtable_put(mt, 1, OP_PUT, "user_alice", 10, "data_1", 6));

    // 2. Insert into index
    MACRO_ASSERT_TRUE(memtable_index_put(mt, 2, OP_PUT, "email_a@x.com", 13, "user_alice", 10));

    // 3. Verify primary skiplist only has 1 item
    memtable_row_t *p_row = memtable_first(mt);
    MACRO_ASSERT_TRUE(p_row != NULL);
    MACRO_ASSERT_TRUE(memtable_next(p_row) == NULL);

    // 4. Verify index skiplist only has 1 item
    memtable_row_index_t *i_row = memtable_index_first(mt);
    MACRO_ASSERT_TRUE(i_row != NULL);
    MACRO_ASSERT_TRUE(memtable_index_next(i_row) == NULL);

    memtable_release(mt);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, memtable_put_and_get);
    MACRO_ADD(tests, memtable_update_shadows_older_version);
    MACRO_ADD(tests, memtable_delete_masks_previous_value);
    MACRO_ADD(tests, memtable_snapshot_read_ignores_future_writes);
    MACRO_ADD(tests, memtable_snapshot_read_ignores_future_tombstones);
    MACRO_ADD(tests, memtable_iteration_orders_keys_and_seqs_descending);
    MACRO_ADD(tests, memtable_isolates_primary_and_index_records);

    macro_run_all("lsm_memtable", tests, test_count);
    return 0;
}
