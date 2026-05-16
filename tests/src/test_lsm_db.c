// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "a-table-store-library/lsm_env.h"
#include "a-table-store-library/lsm_db.h"
#include "the-macro-library/macro_test.h"

static void cleanup_db() {
    system("rm -rf /tmp/lsm_db_test");
    system("mkdir -p /tmp/lsm_db_test");
}

// --- MOCK WAL FOR CHECKPOINT TESTING ---
static uint64_t last_checkpoint_seq = 0;
static uint32_t last_checkpoint_table = 0;
static bool wal_checkpoint_called = false;

static void mock_wal_checkpoint(lsm_wal_t *wal, uint32_t table_id, uint64_t seq_num) {
    (void)wal;
    last_checkpoint_table = table_id;
    last_checkpoint_seq = seq_num;
    wal_checkpoint_called = true;
}
static bool mock_wal_append(lsm_wal_t *w, uint32_t t, uint8_t o, const void *k, uint32_t kl, const void *v, uint32_t vl) {
    (void)w; (void)t; (void)o; (void)k; (void)kl; (void)v; (void)vl;
    return true;
}
static void mock_wal_sync(lsm_wal_t *w) { (void)w; }
static void mock_wal_close(lsm_wal_t *w) { (void)w; }
static lsm_wal_iter_t* mock_wal_iter_init(lsm_wal_t *w) { (void)w; return NULL; }

static lsm_wal_t mock_wal = {
    .ctx = NULL,
    .append = mock_wal_append,
    .sync = mock_wal_sync,
    .checkpoint = mock_wal_checkpoint,
    .close = mock_wal_close,
    .iter_init = mock_wal_iter_init
};

MACRO_TEST(db_basic_put_get_delete) {
    cleanup_db();
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");

    MACRO_ASSERT_TRUE(lsm_db_put(db, "usr_1", 5, "data_A", 6));
    MACRO_ASSERT_TRUE(lsm_db_put(db, "usr_2", 5, "data_B", 6));

    uint32_t vlen;
    char *v1 = lsm_db_get(db, "usr_1", 5, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v1 != NULL);
    MACRO_ASSERT_TRUE(memcmp(v1, "data_A", 6) == 0);
    free(v1);

    // Delete
    MACRO_ASSERT_TRUE(lsm_db_delete(db, "usr_1", 5));
    char *v1_del = lsm_db_get(db, "usr_1", 5, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v1_del == NULL);

    lsm_db_close(db);
    lsm_env_destroy(env);
}

MACRO_TEST(db_force_flush_and_read_from_sstable) {
    cleanup_db();
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");

    lsm_db_put(db, "disk_key", 8, "disk_val", 8);

    MACRO_ASSERT_TRUE(lsm_db_force_flush(db));
    usleep(100 * 1000);

    uint32_t vlen;
    char *v = lsm_db_get(db, "disk_key", 8, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v != NULL);
    MACRO_ASSERT_TRUE(memcmp(v, "disk_val", 8) == 0);
    free(v);

    lsm_db_close(db);
    lsm_env_destroy(env);
}

MACRO_TEST(db_iterator_merges_memory_and_disk_safely) {
    cleanup_db();
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");

    lsm_db_put(db, "A", 1, "disk_A", 6);
    lsm_db_put(db, "C", 1, "disk_C", 6);
    lsm_db_force_flush(db);
    usleep(100 * 1000);

    lsm_db_put(db, "B", 1, "ram_B", 5);
    lsm_db_put(db, "C", 1, "ram_C", 5);

    lsm_db_iter_t *it = lsm_db_iter_init(db, UINT64_MAX);

    const void *k, *v;
    uint32_t klen, vlen;

    MACRO_ASSERT_TRUE(lsm_db_iter_next(it, &k, &klen, &v, &vlen));
    MACRO_ASSERT_TRUE(memcmp(k, "A", 1) == 0);
    MACRO_ASSERT_TRUE(memcmp(v, "disk_A", 6) == 0);

    MACRO_ASSERT_TRUE(lsm_db_iter_next(it, &k, &klen, &v, &vlen));
    MACRO_ASSERT_TRUE(memcmp(k, "B", 1) == 0);
    MACRO_ASSERT_TRUE(memcmp(v, "ram_B", 5) == 0);

    MACRO_ASSERT_TRUE(lsm_db_iter_next(it, &k, &klen, &v, &vlen));
    MACRO_ASSERT_TRUE(memcmp(k, "C", 1) == 0);
    MACRO_ASSERT_TRUE(memcmp(v, "ram_C", 5) == 0);

    MACRO_ASSERT_FALSE(lsm_db_iter_next(it, &k, &klen, &v, &vlen));

    lsm_db_iter_destroy(it);
    lsm_db_close(db);
    lsm_env_destroy(env);
}

MACRO_TEST(db_wal_checkpoint_triggered_on_flush) {
    cleanup_db();
    wal_checkpoint_called = false;
    last_checkpoint_seq = 0;
    last_checkpoint_table = 0;

    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &local_posix_backend, &local_posix_backend, 2, &mock_wal);
    lsm_db_t *db = lsm_db_open(env, 42, "/tmp/lsm_db_test");

    lsm_db_put(db, "chk_key", 7, "chk_val", 7);
    lsm_db_put(db, "chk_key2", 8, "chk_val2", 8);

    MACRO_ASSERT_TRUE(lsm_db_force_flush(db));
    usleep(150 * 1000);

    MACRO_ASSERT_TRUE(wal_checkpoint_called);
    MACRO_ASSERT_EQ_INT(last_checkpoint_table, 42);
    MACRO_ASSERT_TRUE(last_checkpoint_seq >= 2);

    lsm_db_close(db);
    lsm_env_destroy(env);
}

MACRO_TEST(db_memory_limit_triggers_stall_and_flush) {
    cleanup_db();

    size_t limit = 200 * 1024;
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, limit, 2,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");

    char val[1024];
    memset(val, 'A', sizeof(val));

    for (int i = 0; i < 200; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%d", i);
        MACRO_ASSERT_TRUE(lsm_db_put(db, key, strlen(key), val, sizeof(val)));
    }

    lsm_db_close(db);
    lsm_env_destroy(env);
}

MACRO_TEST(db_l0_cascading_compaction_consolidates_correctly) {
    cleanup_db();
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");

    lsm_db_put(db, "item_2", 6, "old_two", 7);
    lsm_db_put(db, "item_4", 6, "four", 4);
    lsm_db_force_flush(db);
    usleep(100 * 1000);

    lsm_db_put(db, "item_3", 6, "three", 5);
    lsm_db_put(db, "item_5", 6, "five", 4);
    lsm_db_force_flush(db);
    usleep(100 * 1000);

    lsm_db_put(db, "item_1", 6, "one", 3);
    lsm_db_put(db, "item_2", 6, "new_two", 7);
    lsm_db_force_flush(db);
    usleep(100 * 1000);

    lsm_db_put(db, "item_9", 6, "nine", 4);
    lsm_db_force_flush(db);
    usleep(200 * 1000);

    uint32_t vlen;
    char *v = lsm_db_get(db, "item_2", 6, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v != NULL);
    MACRO_ASSERT_EQ_INT(vlen, 7);
    MACRO_ASSERT_TRUE(memcmp(v, "new_two", 7) == 0);
    free(v);

    v = lsm_db_get(db, "item_3", 6, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v != NULL);
    MACRO_ASSERT_TRUE(memcmp(v, "three", 5) == 0);
    free(v);

    lsm_db_close(db);
    lsm_env_destroy(env);
}

MACRO_TEST(db_snapshots_provide_repeatable_reads) {
    cleanup_db();
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");

    // Time T1
    lsm_db_put(db, "bank_balance", 12, "100", 3);

    // Freeze Time T1
    uint64_t snap = lsm_db_take_snapshot(db);

    // Time T2
    lsm_db_put(db, "bank_balance", 12, "200", 3);
    lsm_db_put(db, "new_account", 11, "50", 2);

    uint32_t vlen;

    // Validate Snapshot ignores the future
    char *v_snap = lsm_db_get(db, "bank_balance", 12, snap, &vlen);
    MACRO_ASSERT_TRUE(v_snap != NULL);
    MACRO_ASSERT_TRUE(memcmp(v_snap, "100", 3) == 0);
    free(v_snap);

    char *v_snap2 = lsm_db_get(db, "new_account", 11, snap, &vlen);
    MACRO_ASSERT_TRUE(v_snap2 == NULL);

    // Validate normal read sees latest
    char *v_latest = lsm_db_get(db, "bank_balance", 12, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v_latest != NULL);
    MACRO_ASSERT_TRUE(memcmp(v_latest, "200", 3) == 0);
    free(v_latest);

    lsm_db_release_snapshot(db, snap);
    lsm_db_close(db);
    lsm_env_destroy(env);
}

MACRO_TEST(db_write_batch_applies_atomically) {
    cleanup_db();
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");

    lsm_db_put(db, "K1", 2, "V1", 2);

    lsm_write_batch_t *b = lsm_write_batch_init();
    lsm_write_batch_put(b, "K2", 2, "V2", 2);
    lsm_write_batch_delete(b, "K1", 2);

    MACRO_ASSERT_TRUE(lsm_db_write(db, b));
    lsm_write_batch_destroy(b);

    uint32_t vlen;
    char *v = lsm_db_get(db, "K1", 2, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v == NULL); // Deleted by batch

    v = lsm_db_get(db, "K2", 2, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v != NULL); // Inserted by batch
    MACRO_ASSERT_TRUE(memcmp(v, "V2", 2) == 0);
    free(v);

    lsm_db_close(db);
    lsm_env_destroy(env);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, db_basic_put_get_delete);
    MACRO_ADD(tests, db_force_flush_and_read_from_sstable);
    MACRO_ADD(tests, db_iterator_merges_memory_and_disk_safely);
    MACRO_ADD(tests, db_wal_checkpoint_triggered_on_flush);
    MACRO_ADD(tests, db_memory_limit_triggers_stall_and_flush);
    MACRO_ADD(tests, db_l0_cascading_compaction_consolidates_correctly);
    MACRO_ADD(tests, db_snapshots_provide_repeatable_reads);
    MACRO_ADD(tests, db_write_batch_applies_atomically);

    macro_run_all("lsm_database_integration", tests, test_count);
    return 0;
}
