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
#include "a-table-store-library/sstable_builder.h"
#include "a-table-store-library/sstable_reader.h"
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

MACRO_TEST(db_mvcc_survives_background_compaction) {
    cleanup_db();
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");

    // 1. Write V1 and flush to disk
    lsm_db_put(db, "target_key", 10, "VERSION_1", 9);
    lsm_db_force_flush(db);
    usleep(100 * 1000); // Wait for flush

    // 2. Take a snapshot pinned to V1
    uint64_t snap = lsm_db_take_snapshot(db);

    // 3. Write V2 and V3, flushing each to create multiple L0 files
    lsm_db_put(db, "target_key", 10, "VERSION_2", 9);
    lsm_db_force_flush(db);
    usleep(100 * 1000);

    lsm_db_put(db, "target_key", 10, "VERSION_3", 9);
    lsm_db_force_flush(db);

    // 4. Wait long enough for L0 -> L1 compaction to trigger and finish
    usleep(500 * 1000);

    uint32_t vlen;

    // 5. Normal read should see V3
    char *v_latest = lsm_db_get(db, "target_key", 10, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v_latest != NULL);
    MACRO_ASSERT_TRUE(memcmp(v_latest, "VERSION_3", 9) == 0);
    free(v_latest);

    // 6. Snapshot read MUST see V1. If our compaction bug wasn't fixed,
    // V1 would have been deleted and this would return NULL or V3.
    char *v_snap = lsm_db_get(db, "target_key", 10, snap, &vlen);
    MACRO_ASSERT_TRUE(v_snap != NULL);
    MACRO_ASSERT_TRUE(memcmp(v_snap, "VERSION_1", 9) == 0);
    free(v_snap);

    lsm_db_release_snapshot(db, snap);
    lsm_db_close(db);
    lsm_env_destroy(env);
}

MACRO_TEST(db_sequence_numbers_restore_correctly_on_restart) {
    cleanup_db();

    // 1. Start DB, write data, and close it
    lsm_env_t *env1 = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                   &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db1 = lsm_db_open(env1, 1, "/tmp/lsm_db_test");

    lsm_db_put(db1, "keyA", 4, "val_1", 5);
    lsm_db_force_flush(db1);
    usleep(100 * 1000); // Wait for flush to persist max_seq to manifest
    lsm_db_close(db1);
    lsm_env_destroy(env1);

    // 2. Reopen DB from disk
    lsm_env_t *env2 = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                   &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db2 = lsm_db_open(env2, 1, "/tmp/lsm_db_test");

    // 3. Overwrite the key. If sequence numbers restarted at 0, this write
    // would have a lower sequence number than the disk version, and the disk
    // version would incorrectly shadow this new write.
    lsm_db_put(db2, "keyA", 4, "val_2", 5);

    uint32_t vlen;
    char *v = lsm_db_get(db2, "keyA", 4, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v != NULL);
    MACRO_ASSERT_TRUE(memcmp(v, "val_2", 5) == 0); // Must see the new value
    free(v);

    lsm_db_close(db2);
    lsm_env_destroy(env2);
}

MACRO_TEST(sstable_reader_defends_against_buffer_overflows) {
    cleanup_db();
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);

    // 1. Build a valid SSTable
    const char *base = "/tmp/lsm_db_test/000001";
    sstable_builder_t *b = sstable_builder_init(base, &local_posix_backend, FILTER_NONE, 10);

    char ikey[1024];
    size_t ulen = strlen("target_key");
    memcpy(ikey, "target_key", ulen);
    uint64_t trailer = (100ULL << 8) | 0; // seq 100, OP_PUT
    uint8_t *t = (uint8_t *)(ikey + ulen);
    for (int i = 0; i < 8; i++) t[i] = (trailer >> (i * 8)) & 0xFF;

    sstable_builder_add(b, ikey, ulen + 8, "valid_val", 9);
    sstable_builder_finish(b);

    // 2. Intentionally corrupt the .data file by writing junk into the middle of it
    char data_path[520];
    snprintf(data_path, sizeof(data_path), "%s.data", base);
    FILE *f = fopen(data_path, "r+b");
    MACRO_ASSERT_TRUE(f != NULL);
    fseek(f, 4, SEEK_SET); // Skip a bit into the block
    uint32_t malicious_length = 0x7FFFFFFF; // A massive fake varint
    fwrite(&malicious_length, 4, 1, f);
    fclose(f);

    // 3. Attempt to read from the corrupted file
    sstable_reader_t *r = sstable_reader_init(base, &local_posix_backend, env, 1, 1);
    MACRO_ASSERT_TRUE(r != NULL);

    uint32_t vlen;
    void *val = NULL;

    // This should NOT crash/segfault. It should hit our bounds check,
    // break out of the loop, and return 0 (Not Found).
    int status = sstable_reader_get(r, "target_key", 10, UINT64_MAX, &val, &vlen);
    MACRO_ASSERT_EQ_INT(status, 0);

    sstable_reader_destroy(r);
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
    MACRO_ADD(tests, db_mvcc_survives_background_compaction);
    MACRO_ADD(tests, db_sequence_numbers_restore_correctly_on_restart);
    MACRO_ADD(tests, sstable_reader_defends_against_buffer_overflows);

    macro_run_all("lsm_database_integration", tests, test_count);
    return 0;
}
