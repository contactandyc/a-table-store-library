// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include "a-table-store-library/pool_wal.h"
#include "a-table-store-library/lsm_env.h"
#include "a-table-store-library/lsm_db.h"
#include "a-table-store-library/sstable_builder.h"
#include "a-table-store-library/sstable_reader.h"
#include "a-memory-library/aml_alloc.h"
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
static bool mock_wal_append(lsm_wal_t *w, uint32_t t, uint64_t s, uint8_t o, const void *k, uint32_t kl, const void *v, uint32_t vl) {
    (void)w; (void)t; (void)s; (void)o; (void)k; (void)kl; (void)v; (void)vl;
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
    aml_free(v1);

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
    aml_free(v);

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
    aml_free(v);

    v = lsm_db_get(db, "item_3", 6, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v != NULL);
    MACRO_ASSERT_TRUE(memcmp(v, "three", 5) == 0);
    aml_free(v);

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

    // freeze Time T1
    uint64_t snap = lsm_db_take_snapshot(db);

    // Time T2
    lsm_db_put(db, "bank_balance", 12, "200", 3);
    lsm_db_put(db, "new_account", 11, "50", 2);

    uint32_t vlen;

    // Validate Snapshot ignores the future
    char *v_snap = lsm_db_get(db, "bank_balance", 12, snap, &vlen);
    MACRO_ASSERT_TRUE(v_snap != NULL);
    MACRO_ASSERT_TRUE(memcmp(v_snap, "100", 3) == 0);
    aml_free(v_snap);

    char *v_snap2 = lsm_db_get(db, "new_account", 11, snap, &vlen);
    MACRO_ASSERT_TRUE(v_snap2 == NULL);

    // Validate normal read sees latest
    char *v_latest = lsm_db_get(db, "bank_balance", 12, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v_latest != NULL);
    MACRO_ASSERT_TRUE(memcmp(v_latest, "200", 3) == 0);
    aml_free(v_latest);

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
    aml_free(v);

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
    aml_free(v_latest);

    // 6. Snapshot read MUST see V1. If our compaction bug wasn't fixed,
    // V1 would have been deleted and this would return NULL or V3.
    char *v_snap = lsm_db_get(db, "target_key", 10, snap, &vlen);
    MACRO_ASSERT_TRUE(v_snap != NULL);
    MACRO_ASSERT_TRUE(memcmp(v_snap, "VERSION_1", 9) == 0);
    aml_free(v_snap);

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
    aml_free(v);

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

// --- Phase 1 Lifecycle Verification Tests ---

MACRO_TEST(db_env_destroy_avoids_teardown_deadlock) {
    cleanup_db();
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");
    lsm_db_put(db, "teardown_test", 13, "data", 4);

    // In the buggy code, lsm_env_destroy() retained the db and then called lsm_db_close().
    // lsm_db_close() waited indefinitely for ref_count to drop to 1, deadlocking.
    // Our Phase 1 fix decoupling owner_refs and active_ops prevents this deadlock entirely.
    // If the test successfully completes and exits, the deadlock is fixed.
    lsm_env_destroy(env);

    MACRO_ASSERT_TRUE(true);
}

struct lease_drain_args {
    lsm_db_t *db;
    bool close_returned;
};

static void *close_thread_func(void *arg) {
    struct lease_drain_args *args = (struct lease_drain_args*)arg;
    lsm_db_close(args->db);
    args->close_returned = true;
    return NULL;
}

MACRO_TEST(db_active_lease_prevents_premature_close) {
    cleanup_db();
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");
    lsm_db_put(db, "lease_key", 9, "lease_val", 9);

    // Acquire an operation lease directly via the public Iterator API
    lsm_db_iter_t *it = lsm_db_iter_init(db, UINT64_MAX);
    MACRO_ASSERT_TRUE(it != NULL);

    struct lease_drain_args args = { .db = db, .close_returned = false };
    pthread_t tid;
    pthread_create(&tid, NULL, close_thread_func, &args);

    // Give the closing thread time to trigger and wait.
    // Because we hold an active_ops lease via the iterator, lsm_db_close MUST stall.
    usleep(100 * 1000);

    // Validate the closing thread is indeed safely blocked and hasn't destroyed our pointers
    MACRO_ASSERT_FALSE(args.close_returned);

    // Now release the lease
    lsm_db_iter_destroy(it);

    // The closing thread should now wake up, finish its job, and return
    pthread_join(tid, NULL);
    MACRO_ASSERT_TRUE(args.close_returned);

    lsm_env_destroy(env);
}


// --- Phase 2 Concurrency Tests ---

typedef struct {
    lsm_db_t *db;
    pthread_mutex_t *mut;
    pthread_cond_t *cond;
    bool *start_flag;
    bool write_result;
} concurrent_writer_args_t;

static void *async_writer_thread(void *arg) {
    concurrent_writer_args_t *args = (concurrent_writer_args_t *)arg;

    pthread_mutex_lock(args->mut);
    while (!*(args->start_flag)) {
        pthread_cond_wait(args->cond, args->mut);
    }
    pthread_mutex_unlock(args->mut);

    // Attempt a write. If the database is closing or closed, this should cleanly return false
    // without triggering a Use-After-Free.
    args->write_result = lsm_db_put(args->db, "concurrent_key", 14, "payload", 7);
    return NULL;
}

MACRO_TEST(db_close_rejects_new_traffic_instantly) {
    cleanup_db();
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");

    // Manually increment owner_refs to simulate an active long-running external hold
    lsm_db_retain(db);

    pthread_mutex_t mut = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
    bool start_flag = false;

    concurrent_writer_args_t writer_args = {
        .db = db,
        .mut = &mut,
        .cond = &cond,
        .start_flag = &start_flag,
        .write_result = true
    };

    pthread_t thread;
    pthread_create(&thread, NULL, async_writer_thread, &writer_args);

    // Trigger the closing sequence in a background thread
    pthread_t close_thread;
    struct lease_drain_args c_args = { .db = db };
    pthread_create(&close_thread, NULL, close_thread_func, &c_args);

    // Yield slightly to ensure the close sequence trips `closing = true`
    usleep(50 * 1000);

    // Unleash the concurrent writer
    pthread_mutex_lock(&mut);
    start_flag = true;
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mut);

    pthread_join(thread, NULL);

    // ASSERTION: The write must have returned false because the DB entered a closing state.
    MACRO_ASSERT_FALSE(writer_args.write_result);

    // Clean up our manual pinned reference, which triggers the CV wake up inside close
    lsm_db_release(db);
    pthread_join(close_thread, NULL);

    lsm_env_destroy(env);
}

MACRO_TEST(db_background_jobs_survives_db_handle_close) {
    cleanup_db();
    // Initialize an environment with an artificially tiny memory threshold to force an immediate flush
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 1024, 1,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");

    // Pump entries to load up the active memtable explicitly
    char large_val[2048];
    memset(large_val, 'z', sizeof(large_val));
    lsm_db_put(db, "flush_me_key", 12, large_val, sizeof(large_val));

    // Force a background flush sequence asynchronously
    lsm_db_force_flush(db);

    // Immediately trigger close. Thanks to our Phase 1 active_jobs tracking,
    // lsm_db_close will block cleanly on the condition variable until the async job completes.
    lsm_db_close(db);

    // If Phase 1 job retention fails, this test will crash / throw memory faults
    // before reaching this point due to the background worker accessing freed pointers.
    MACRO_ASSERT_TRUE(true);

    lsm_env_destroy(env);
}

MACRO_TEST(db_compaction_overlap_and_starvation_prevention) {
    cleanup_db();
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");

    // Write overlapping sets of data to force the new L0 overlapping logic and L1+ pointers
    lsm_db_put(db, "A", 1, "val_A", 5);
    lsm_db_put(db, "M", 1, "val_M", 5);
    lsm_db_force_flush(db);
    usleep(50 * 1000);

    lsm_db_put(db, "F", 1, "val_F", 5);
    lsm_db_put(db, "Z", 1, "val_Z", 5);
    lsm_db_force_flush(db);
    usleep(50 * 1000);

    lsm_db_put(db, "C", 1, "val_C", 5);
    lsm_db_put(db, "Y", 1, "val_Y", 5);
    lsm_db_force_flush(db);

    // Give background compaction time to process the cascading overlaps
    usleep(500 * 1000);

    uint32_t vlen;
    char *v = lsm_db_get(db, "A", 1, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v != NULL); aml_free(v);

    v = lsm_db_get(db, "M", 1, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v != NULL); aml_free(v);

    v = lsm_db_get(db, "Z", 1, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v != NULL); aml_free(v);

    lsm_db_close(db);
    lsm_env_destroy(env);
}

// --- Phase 5B SSTable Caching Tests ---

typedef struct {
    lsm_db_t *db;
    pthread_mutex_t *mut;
    pthread_cond_t *cond;
    bool *start_flag;
    int successes;
} cache_reader_args_t;

static void *async_cache_reader(void *arg) {
    cache_reader_args_t *args = (cache_reader_args_t *)arg;

    pthread_mutex_lock(args->mut);
    while (!*(args->start_flag)) {
        pthread_cond_wait(args->cond, args->mut);
    }
    pthread_mutex_unlock(args->mut);

    uint32_t vlen;
    char *v = lsm_db_get(args->db, "hot_key", 7, UINT64_MAX, &vlen);
    if (v && vlen == 7 && memcmp(v, "hot_val", 7) == 0) {
        __atomic_fetch_add(&args->successes, 1, __ATOMIC_SEQ_CST);
    }
    if (v) aml_free(v);
    return NULL;
}

MACRO_TEST(db_concurrent_reads_init_cache_safely) {
    cleanup_db();
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");

    // Write a key and force it to disk so the memtable is empty
    lsm_db_put(db, "hot_key", 7, "hot_val", 7);
    lsm_db_force_flush(db);
    usleep(50 * 1000);

    int num_threads = 10;
    pthread_t threads[10];
    pthread_mutex_t mut = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
    bool start_flag = false;

    cache_reader_args_t args = { .db = db, .mut = &mut, .cond = &cond, .start_flag = &start_flag, .successes = 0 };

    for (int i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, async_cache_reader, &args);
    }

    // Unleash the read storm on the uninitialized cached reader
    pthread_mutex_lock(&mut);
    start_flag = true;
    pthread_cond_broadcast(&cond);
    pthread_mutex_unlock(&mut);

    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    // All threads should have successfully read the value without crashing
    MACRO_ASSERT_EQ_INT(args.successes, num_threads);

    lsm_db_close(db);
    lsm_env_destroy(env);
}

MACRO_TEST(db_sstable_reader_caching_and_cleanup) {
    cleanup_db();
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");

    // Create File 1
    lsm_db_put(db, "cache_key_1", 11, "value_1", 7);
    lsm_db_force_flush(db);
    usleep(50 * 1000);

    // Read 1: Initializes the cache
    uint32_t vlen;
    char *v = lsm_db_get(db, "cache_key_1", 11, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v != NULL);
    MACRO_ASSERT_TRUE(memcmp(v, "value_1", 7) == 0);
    aml_free(v);

    // Read 2: Hits the active cache
    v = lsm_db_get(db, "cache_key_1", 11, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v != NULL);
    aml_free(v);

    // Force compaction by pushing more files past the L0 limit
    for (int i=0; i<5; i++) {
        char k[32]; snprintf(k, 32, "dummy_%d", i);
        lsm_db_put(db, k, strlen(k), "val", 3);
        lsm_db_force_flush(db);
        usleep(20 * 1000);
    }

    // Wait for the background pool to finish compacting L0 -> L1
    usleep(500 * 1000);

    // Read 3: Reads from new L1 file (initializes new cache, old cache was safely destroyed)
    v = lsm_db_get(db, "cache_key_1", 11, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v != NULL);
    aml_free(v);

    lsm_db_close(db);
    lsm_env_destroy(env);
}

MACRO_TEST(db_compaction_scoring_calculates_amplification) {
    cleanup_db();
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");

    // Push 3 files to L0 (Trigger limit is 4).
    for (int i = 0; i < 3; i++) {
        char k[32]; snprintf(k, 32, "score_key_%d", i);
        lsm_db_put(db, k, strlen(k), "val", 3);
        lsm_db_force_flush(db);

        // Safely wait for the flush to finish by checking L0 file count
        size_t expected_l0 = i + 1;
        size_t current_l0 = 0;
        while (true) {
            lsm_db_debug_get_compaction_metrics(db, NULL, NULL, &current_l0, NULL);
            if (current_l0 >= expected_l0) break;
            usleep(10 * 1000);
        }
    }

    double score = 0;
    int level = -1;
    size_t l0 = 0, l1 = 0;

    lsm_db_debug_get_compaction_metrics(db, &score, &level, &l0, &l1);

    // With 3 files in L0, the score should be exactly 0.75 (3 / 4.0)
    MACRO_ASSERT_EQ_INT(level, 0);
    MACRO_ASSERT_TRUE(score >= 0.74 && score <= 0.76);

    // Push a 4th file to push the score to 1.0 (Triggers compaction)
    lsm_db_put(db, "trigger_key", 10, "val", 3);
    lsm_db_force_flush(db);

    // Polling loop instead of hard sleep to prevent CI flakiness
    for (int wait = 0; wait < 50; wait++) {
        lsm_db_debug_get_compaction_metrics(db, &score, &level, &l0, &l1);
        // If the score dropped below 1.0, the background thread did its job
        if (score < 1.0 && l1 > 0) break;
        usleep(100 * 1000);
    }

    lsm_db_debug_get_compaction_metrics(db, &score, &level, &l0, &l1);

    // L0 should have shed at least one file to drop the score below 1.0.
    MACRO_ASSERT_TRUE(score < 1.0);
    MACRO_ASSERT_TRUE(l0 < 4);
    MACRO_ASSERT_TRUE(l1 > 0);

    lsm_db_close(db);
    lsm_env_destroy(env);
}

typedef struct {
    lsm_db_t *db;
    int thread_id;
    pthread_mutex_t *mut;
    pthread_cond_t *cond;
    bool *start_flag;
} group_commit_args_t;

static void *async_group_writer(void *arg) {
    group_commit_args_t *args = (group_commit_args_t *)arg;

    pthread_mutex_lock(args->mut);
    while (!*(args->start_flag)) {
        pthread_cond_wait(args->cond, args->mut);
    }
    pthread_mutex_unlock(args->mut);

    // Each thread writes 100 unique keys
    for (int i = 0; i < 100; i++) {
        char key[64];
        char val[64];
        snprintf(key, sizeof(key), "key_%d_%d", args->thread_id, i);
        snprintf(val, sizeof(val), "val_%d_%d", args->thread_id, i);

        lsm_db_put(args->db, key, strlen(key), val, strlen(val));
    }
    return NULL;
}

MACRO_TEST(db_group_commit_handles_concurrent_write_storms) {
    cleanup_db();
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);
    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");

    int num_threads = 20;
    pthread_t threads[20];
    group_commit_args_t args[20];

    pthread_mutex_t mut = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
    bool start_flag = false;

    for (int i = 0; i < num_threads; i++) {
        args[i].db = db;
        args[i].thread_id = i;
        args[i].mut = &mut;
        args[i].cond = &cond;
        args[i].start_flag = &start_flag;
        pthread_create(&threads[i], NULL, async_group_writer, &args[i]);
    }

    // Unleash the hounds
    pthread_mutex_lock(&mut);
    start_flag = true;
    pthread_cond_broadcast(&cond);
    pthread_mutex_unlock(&mut);

    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    // Verify absolutely every key made it into the database via the super-batches
    uint32_t vlen;
    for (int i = 0; i < num_threads; i++) {
        for (int j = 0; j < 100; j++) {
            char key[64];
            char expected_val[64];
            snprintf(key, sizeof(key), "key_%d_%d", i, j);
            snprintf(expected_val, sizeof(expected_val), "val_%d_%d", i, j);

            char *v = lsm_db_get(db, key, strlen(key), UINT64_MAX, &vlen);
            MACRO_ASSERT_TRUE(v != NULL);
            MACRO_ASSERT_TRUE(memcmp(v, expected_val, strlen(expected_val)) == 0);
            aml_free(v);
        }
    }

    lsm_db_close(db);
    lsm_env_destroy(env);
}

static inline void encode_u32_le_test(uint8_t *dst, uint32_t v) {
    dst[0] = v & 0xFF; dst[1] = (v >> 8) & 0xFF; dst[2] = (v >> 16) & 0xFF; dst[3] = (v >> 24) & 0xFF;
}

MACRO_TEST(db_wal_recovery_rejects_corrupted_batches) {
    cleanup_db();

    // Setup environment with a REAL pluggable WAL
    lsm_wal_t *wal = pool_to_lsm_wal("/tmp/lsm_db_test_wal", 1, 2);
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &local_posix_backend, &local_posix_backend, 2, wal);

    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");
    lsm_db_put(db, "valid_key", 9, "valid_val", 9);

    // Close cleanly without flushing so the record stays in the WAL
    lsm_db_close(db);
    lsm_env_destroy(env);
    // Now, intentionally forge a malicious WAL batch
    pool_wal_t pool;
    MACRO_ASSERT_EQ_INT(pool_wal_init(&pool, "/tmp/lsm_db_test_wal", 1, 2), 0);

    uint8_t mal[21] = {0}; // 8 (seq) + 4 (count) + 1 (op) + 4 (klen) + 4 (vlen)
    mal[0] = 10; // start_seq = 10
    encode_u32_le_test(mal + 8, 1); // count = 1
    mal[12] = 0; // OP_PUT
    encode_u32_le_test(mal + 13, 0x7FFFFFFF); // MALICIOUS: Claim key is 2 Gigabytes!
    encode_u32_le_test(mal + 17, 0); // vlen = 0

    // Pass it to the underlying pool wal (simulating a corrupt disk write)
    pool_wal_append(&pool, 2, 2 /* OP_BATCH */, mal, 21);
    pool_wal_sync(&pool);
    pool_wal_close(&pool);

    // Reopen DB. lsm_env_recover_wal will run automatically.
    lsm_wal_t *wal2 = pool_to_lsm_wal("/tmp/lsm_db_test_wal", 1, 2);
    lsm_env_t *env2 = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                   &local_posix_backend, &local_posix_backend, 2, wal2);

    lsm_db_t *db2 = lsm_db_open(env2, 1, "/tmp/lsm_db_test");
    lsm_env_recover_wal(env2);

    // ASSERTION: The environment did not crash!
    // And the valid key is still recoverable.
    uint32_t vlen;
    char *v = lsm_db_get(db2, "valid_key", 9, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v != NULL);
    aml_free(v);

    lsm_db_close(db2);
    lsm_env_destroy(env2);
}

// --- FAULTY VFS FOR DB ---
static bool db_fault_delete_called = false;

static int db_fault_fsync_file(void *f) {
    (void)f; // FIX: Suppress unused parameter warning
    return -1; // Permanently simulate a broken disk
}

// FIX: Changed return type from 'int' to 'bool'
static bool db_fault_delete_file(const char *path) {
    db_fault_delete_called = true;
    return local_posix_backend.delete_file(path);
}

MACRO_TEST(db_flush_job_backs_off_and_avoids_deadlock) {
    cleanup_db();

    lsm_storage_backend_t fault_vfs;
    memcpy(&fault_vfs, &local_posix_backend, sizeof(lsm_storage_backend_t));
    fault_vfs.fsync_file = db_fault_fsync_file;
    fault_vfs.delete_file = db_fault_delete_file;
    db_fault_delete_called = false;

    lsm_wal_t *wal = pool_to_lsm_wal("/tmp/lsm_db_test_wal", 1, 2);
    lsm_env_t *env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                                  &fault_vfs, &fault_vfs, 2, wal);

    lsm_db_t *db = lsm_db_open(env, 1, "/tmp/lsm_db_test");
    lsm_db_put(db, "vital_data", 10, "val1", 4);

    // Force a flush. This dispatches the background job to the broken VFS.
    lsm_db_force_flush(db);

    // Wait for the background thread to fail, clean up, and exit.
    // We poll the boolean flag because 'db' is an opaque struct to the test file.
    for (int wait = 0; wait < 50; wait++) {
        if (db_fault_delete_called) break;
        usleep(100 * 1000); // Wait 100ms
    }

    // ASSERTION 1: The flush aborted and cleaned up the disk fragments
    MACRO_ASSERT_TRUE(db_fault_delete_called);

    // Now, call close! Because we fixed the deadlock loop, the close function
    // will see the stranded imm_memtable, intentionally drop it, and shut down safely!
    lsm_db_close(db);
    lsm_env_destroy(env);

    // ASSERTION 2: WAL Recovery!
    // Boot the DB back up with a fixed disk. The WAL should completely
    // recover the dropped imm_memtable because the checkpoint was safely delayed!
    wal = pool_to_lsm_wal("/tmp/lsm_db_test_wal", 1, 2);
    env = lsm_env_init(4 * 1024 * 1024, 16 * 1024 * 1024, 2,
                       &local_posix_backend, &local_posix_backend, 2, wal);
    db = lsm_db_open(env, 1, "/tmp/lsm_db_test");
    lsm_env_recover_wal(env);

    uint32_t vlen;
    char *v = lsm_db_get(db, "vital_data", 10, UINT64_MAX, &vlen);
    MACRO_ASSERT_TRUE(v != NULL);
    MACRO_ASSERT_TRUE(memcmp(v, "val1", 4) == 0);
    if (v) aml_free(v);

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
    MACRO_ADD(tests, db_mvcc_survives_background_compaction);
    MACRO_ADD(tests, db_sequence_numbers_restore_correctly_on_restart);
    MACRO_ADD(tests, sstable_reader_defends_against_buffer_overflows);

    MACRO_ADD(tests, db_env_destroy_avoids_teardown_deadlock);
    MACRO_ADD(tests, db_active_lease_prevents_premature_close);

    MACRO_ADD(tests, db_close_rejects_new_traffic_instantly);
    MACRO_ADD(tests, db_background_jobs_survives_db_handle_close);
    MACRO_ADD(tests, db_compaction_overlap_and_starvation_prevention);
    MACRO_ADD(tests, db_concurrent_reads_init_cache_safely);
    MACRO_ADD(tests, db_sstable_reader_caching_and_cleanup);
    MACRO_ADD(tests, db_compaction_scoring_calculates_amplification);
    MACRO_ADD(tests, db_group_commit_handles_concurrent_write_storms);

    MACRO_ADD(tests, db_wal_recovery_rejects_corrupted_batches);
    MACRO_ADD(tests, db_flush_job_backs_off_and_avoids_deadlock);

    macro_run_all("lsm_database_integration", tests, test_count);
    return 0;
}
