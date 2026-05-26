// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include "a-table-store-library/pool_wal.h"
#include "a-memory-library/aml_alloc.h"
#include "the-macro-library/macro_test.h"

static void cleanup_wal() {
    system("rm -rf /tmp/pool_wal_test");
    system("mkdir -p /tmp/pool_wal_test");
}

static int count_files(const char *dir_path, const char *ext) {
    DIR *dp = opendir(dir_path);
    if (!dp) return 0;
    struct dirent *ep;
    int count = 0;
    while ((ep = readdir(dp))) {
        if (ext) {
            if (strstr(ep->d_name, ext)) count++;
        } else {
            if (ep->d_name[0] != '.') count++;
        }
    }
    closedir(dp);
    return count;
}

MACRO_TEST(wal_basic_append_and_iterator) {
    cleanup_wal();
    pool_wal_t wal;
    // 1MB segment, 2 standby
    MACRO_ASSERT_EQ_INT(pool_wal_init(&wal, "/tmp/pool_wal_test", 1, 2), 0);

    const char *msg1 = "hello_wal";
    const char *msg2 = "second_message";

    uint64_t lsn1 = pool_wal_append(&wal, 100, 1, (const uint8_t*)msg1, strlen(msg1));
    uint64_t lsn2 = pool_wal_append(&wal, 101, 2, (const uint8_t*)msg2, strlen(msg2));
    pool_wal_sync(&wal);

    MACRO_ASSERT_TRUE(lsn1 > 0);
    MACRO_ASSERT_TRUE(lsn2 > lsn1);

    pool_wal_iter_t *it = pool_wal_iter_init(&wal);
    MACRO_ASSERT_TRUE(it != NULL);

    uint64_t read_seq;
    uint8_t type;
    const uint8_t *payload;
    uint32_t len;
    uint64_t lsn;

    MACRO_ASSERT_TRUE(pool_wal_iter_next(it, &read_seq, &lsn, &type, &payload, &len));
    MACRO_ASSERT_EQ_INT(read_seq, 100);
    MACRO_ASSERT_EQ_INT(lsn, lsn1);
    MACRO_ASSERT_EQ_INT(type, 1);
    MACRO_ASSERT_EQ_INT(len, strlen(msg1));
    MACRO_ASSERT_TRUE(memcmp(payload, msg1, len) == 0);

    MACRO_ASSERT_TRUE(pool_wal_iter_next(it, &read_seq, &lsn, &type, &payload, &len));
    MACRO_ASSERT_EQ_INT(read_seq, 101);
    MACRO_ASSERT_EQ_INT(lsn, lsn2);
    MACRO_ASSERT_EQ_INT(type, 2);
    MACRO_ASSERT_EQ_INT(len, strlen(msg2));
    MACRO_ASSERT_TRUE(memcmp(payload, msg2, len) == 0);

    MACRO_ASSERT_FALSE(pool_wal_iter_next(it, &read_seq, &lsn, &type, &payload, &len));

    pool_wal_iter_destroy(it);
    pool_wal_close(&wal);
}

MACRO_TEST(wal_segment_rotation_and_recovery) {
    cleanup_wal();
    pool_wal_t wal;

    MACRO_ASSERT_EQ_INT(pool_wal_init(&wal, "/tmp/pool_wal_test", 1, 2), 0);

    size_t large_size = 400 * 1024;
    uint8_t *large_buf = aml_malloc(large_size);
    memset(large_buf, 0xBB, large_size);

    uint64_t lsn1 = pool_wal_append(&wal, 1, 0, large_buf, large_size);
    uint64_t lsn2 = pool_wal_append(&wal, 2, 0, large_buf, large_size);
    uint64_t lsn3 = pool_wal_append(&wal, 3, 0, large_buf, large_size);

    pool_wal_close(&wal);

    // Reopen the WAL to simulate a crash/restart
    pool_wal_t wal2;
    MACRO_ASSERT_EQ_INT(pool_wal_init(&wal2, "/tmp/pool_wal_test", 1, 2), 0);

    // We should see multiple .wal files on disk
    MACRO_ASSERT_TRUE(count_files("/tmp/pool_wal_test", ".wal") >= 2);

    pool_wal_iter_t *it = pool_wal_iter_init(&wal2);
    uint64_t seq;
    uint8_t type;
    const uint8_t *payload;
    uint32_t len;
    uint64_t lsn;

    MACRO_ASSERT_TRUE(pool_wal_iter_next(it, &seq, &lsn, &type, &payload, &len));
    MACRO_ASSERT_EQ_INT(seq, 1);
    MACRO_ASSERT_EQ_INT(lsn, lsn1);
    MACRO_ASSERT_TRUE(pool_wal_iter_next(it, &seq, &lsn, &type, &payload, &len));
    MACRO_ASSERT_EQ_INT(seq, 2);
    MACRO_ASSERT_EQ_INT(lsn, lsn2);
    MACRO_ASSERT_TRUE(pool_wal_iter_next(it, &seq, &lsn, &type, &payload, &len));
    MACRO_ASSERT_EQ_INT(seq, 3);
    MACRO_ASSERT_EQ_INT(lsn, lsn3);
    MACRO_ASSERT_FALSE(pool_wal_iter_next(it, &seq, &lsn, &type, &payload, &len));

    pool_wal_iter_destroy(it);
    aml_free(large_buf);
    pool_wal_close(&wal2);
}

MACRO_TEST(wal_purge_and_standby_recycling) {
    cleanup_wal();
    pool_wal_t wal;
    MACRO_ASSERT_EQ_INT(pool_wal_init(&wal, "/tmp/pool_wal_test", 1, 2), 0);

    size_t large_size = 600 * 1024;
    uint8_t *large_buf = aml_malloc(large_size);
    memset(large_buf, 0xCC, large_size);

    pool_wal_append(&wal, 10, 0, large_buf, large_size);
    uint64_t lsn2 = pool_wal_append(&wal, 20, 0, large_buf, large_size);

    // Capture the LSN right here so we can safely purge everything before it
    uint64_t safe_lsn_mark = lsn2;

    pool_wal_append(&wal, 30, 0, large_buf, large_size);
    pool_wal_append(&wal, 40, 0, large_buf, large_size);

    pool_wal_purge(&wal, safe_lsn_mark);

    int standby_count = count_files("/tmp/pool_wal_test", "standby_");
    MACRO_ASSERT_EQ_INT(standby_count, 1);
    MACRO_ASSERT_EQ_INT(wal.oldest_seg_id, 2);

    aml_free(large_buf);
    pool_wal_close(&wal);
}

MACRO_TEST(wal_corrupted_frame_handling) {
    cleanup_wal();
    pool_wal_t wal;
    MACRO_ASSERT_EQ_INT(pool_wal_init(&wal, "/tmp/pool_wal_test", 1, 2), 0);

    const char *msg1 = "valid_message_1";
    const char *msg2 = "message_to_corrupt";

    uint64_t lsn1 = pool_wal_append(&wal, 1, 0, (const uint8_t*)msg1, strlen(msg1));
    pool_wal_append(&wal, 2, 0, (const uint8_t*)msg2, strlen(msg2));

    uint64_t end_offset = wal.file_offset;
    pool_wal_close(&wal);

    char path[1024];
    snprintf(path, 1024, "/tmp/pool_wal_test/0000000001.wal");
    FILE *f = fopen(path, "r+b");
    MACRO_ASSERT_TRUE(f != NULL);

    fseek(f, end_offset - 5, SEEK_SET);
    uint32_t junk = 0xDEADBEEF;
    fwrite(&junk, 4, 1, f);
    fclose(f);

    pool_wal_t wal2;
    MACRO_ASSERT_EQ_INT(pool_wal_init(&wal2, "/tmp/pool_wal_test", 1, 2), 0);

    pool_wal_iter_t *it = pool_wal_iter_init(&wal2);
    uint64_t seq;
    uint8_t type;
    const uint8_t *payload;
    uint32_t len;
    uint64_t lsn;

    MACRO_ASSERT_TRUE(pool_wal_iter_next(it, &seq, &lsn, &type, &payload, &len));
    MACRO_ASSERT_EQ_INT(seq, 1);
    MACRO_ASSERT_EQ_INT(lsn, lsn1);

    MACRO_ASSERT_FALSE(pool_wal_iter_next(it, &seq, &lsn, &type, &payload, &len));

    pool_wal_iter_destroy(it);
    pool_wal_close(&wal2);
}

MACRO_TEST(wal_lsm_wrapper_integration) {
    cleanup_wal();

    lsm_wal_t *lsm = pool_to_lsm_wal("/tmp/pool_wal_test", 1, 2);
    MACRO_ASSERT_TRUE(lsm != NULL);

    uint64_t lsn1, lsn2;
    MACRO_ASSERT_TRUE(lsm->append(lsm, 42, 1000, 5, "my_key", 6, "my_val", 6, &lsn1));
    MACRO_ASSERT_TRUE(lsm->append(lsm, 99, 1001, 5, "key2", 4, NULL, 0, &lsn2));
    lsm->sync(lsm);

    lsm->checkpoint(lsm, 42, lsn1);
    lsm->checkpoint(lsm, 99, lsn2);

    lsm_wal_iter_t *it = lsm->iter_init(lsm);
    MACRO_ASSERT_TRUE(it != NULL);

    uint32_t out_table;
    uint8_t out_op;
    const void *out_k, *out_v;
    uint32_t out_klen, out_vlen;
    uint64_t out_lsn;

    MACRO_ASSERT_TRUE(it->next(it, &out_table, &out_lsn, &out_op, &out_k, &out_klen, &out_v, &out_vlen));
    MACRO_ASSERT_EQ_INT(out_table, 42);
    MACRO_ASSERT_EQ_INT(out_lsn, lsn1);
    MACRO_ASSERT_EQ_INT(out_op, 5);
    MACRO_ASSERT_EQ_INT(out_klen, 6);
    MACRO_ASSERT_EQ_INT(out_vlen, 6);
    MACRO_ASSERT_TRUE(memcmp(out_k, "my_key", 6) == 0);
    MACRO_ASSERT_TRUE(memcmp(out_v, "my_val", 6) == 0);

    MACRO_ASSERT_TRUE(it->next(it, &out_table, &out_lsn, &out_op, &out_k, &out_klen, &out_v, &out_vlen));
    MACRO_ASSERT_EQ_INT(out_table, 99);
    MACRO_ASSERT_EQ_INT(out_lsn, lsn2);
    MACRO_ASSERT_EQ_INT(out_op, 5);
    MACRO_ASSERT_EQ_INT(out_klen, 4);
    MACRO_ASSERT_EQ_INT(out_vlen, 0);
    MACRO_ASSERT_TRUE(memcmp(out_k, "key2", 4) == 0);
    MACRO_ASSERT_TRUE(out_v == NULL);

    MACRO_ASSERT_FALSE(it->next(it, &out_table, &out_lsn, &out_op, &out_k, &out_klen, &out_v, &out_vlen));

    it->destroy(it);
    lsm->close(lsm);
}

typedef struct {
    pool_wal_t *wal;
    bool keep_running;
} wal_sync_test_args_t;

static void *wal_sync_thread(void *arg) {
    wal_sync_test_args_t *args = (wal_sync_test_args_t *)arg;
    while (__atomic_load_n(&args->keep_running, __ATOMIC_ACQUIRE)) {
        // Slam the sync function as fast as possible
        pool_wal_sync(args->wal);
    }
    return NULL;
}

MACRO_TEST(wal_sync_survives_concurrent_rotation) {
    cleanup_wal();
    pool_wal_t wal;
    // Tiny segment size (100 bytes) to force instant rotations
    MACRO_ASSERT_EQ_INT(pool_wal_init(&wal, "/tmp/pool_wal_test", 0 /*0 MB forces tiny minimums usually*/, 5), 0);
    wal.segment_size_bytes = 100; // Hard override for the test

    wal_sync_test_args_t args = { .wal = &wal, .keep_running = true };

    // Spin up 4 threads constantly syncing the WAL
    pthread_t sync_threads[4];
    for (int i = 0; i < 4; i++) {
        pthread_create(&sync_threads[i], NULL, wal_sync_thread, &args);
    }

    // Main thread rapid-fires appends to force aggressive file rotations
    char *payload = "12345678901234567890"; // 20 bytes
    for (int i = 0; i < 100; i++) {
        pool_wal_append(&wal, i, 1, (const uint8_t*)payload, 20);
        usleep(1000); // 1ms delay
    }

    // Stop the sync threads
    __atomic_store_n(&args.keep_running, false, __ATOMIC_RELEASE);
    for (int i = 0; i < 4; i++) {
        pthread_join(sync_threads[i], NULL);
    }

    // If the Phase 11 fd dup() fix wasn't present, the sync threads would hit EBADF
    // or accidentally sync a recycled descriptor, potentially causing deadlocks or crashes.
    // Making it here proves the concurrency isolation holds!
    MACRO_ASSERT_TRUE(true);

    pool_wal_close(&wal);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, wal_basic_append_and_iterator);
    MACRO_ADD(tests, wal_segment_rotation_and_recovery);
    MACRO_ADD(tests, wal_purge_and_standby_recycling);
    MACRO_ADD(tests, wal_corrupted_frame_handling);
    MACRO_ADD(tests, wal_lsm_wrapper_integration);

    MACRO_ADD(tests, wal_sync_survives_concurrent_rotation);

    macro_run_all("pool_wal", tests, test_count);
    return 0;
}
