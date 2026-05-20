// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "a-table-store-library/sstable_builder.h"
#include "a-table-store-library/sstable_reader.h"
#include "a-table-store-library/lsm_env.h"
#include "a-table-store-library/memtable.h"
#include "a-table-store-library/lsm_compaction.h"
#include "a-memory-library/aml_alloc.h"
#include "the-macro-library/macro_test.h"
#include "the-lz4-library/xxhash.h"

// Helper to construct a packed internal key
static void pack_ikey(char *dst, const char *user_key, uint64_t seq, uint8_t op) {
    size_t ulen = strlen(user_key);
    memcpy(dst, user_key, ulen);
    uint64_t trailer = (seq << 8) | op;
    // Pack in Little Endian to match the updated implementation requirements
    uint8_t *t = (uint8_t *)(dst + ulen);
    for (int i = 0; i < 8; i++) t[i] = (trailer >> (i * 8)) & 0xFF;
}

static void cleanup_files() {
    system("rm -rf /tmp/sstable_test*");
    system("mkdir -p /tmp/sstable_test_dir");
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
    status = sstable_reader_get(r, "apple", 5, UINT64_MAX, &val, &vlen);
    MACRO_ASSERT_EQ_INT(status, 1);
    MACRO_ASSERT_EQ_INT(vlen, 3);
    MACRO_ASSERT_TRUE(memcmp(val, "red", 3) == 0);
    aml_free(val);

    // Test miss
    status = sstable_reader_get(r, "banana", 6, UINT64_MAX, &val, &vlen);
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
    int status = sstable_reader_get(r, "ghost", 5, UINT64_MAX, &val, &vlen);

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

MACRO_TEST(manifest_survives_torn_and_corrupt_writes) {
    cleanup_files();
    const char *dir = "/tmp/sstable_test_dir";

    lsm_env_t *env = lsm_env_init(1024 * 1024, 1024 * 1024 * 10, 1,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);

    // 1. Initialize manifest and write a valid version edit
    lsm_manifest_t *m1 = lsmc_manifest_init(env, 1, dir);
    sstable_meta_t *meta = aml_zalloc(sizeof(sstable_meta_t));
    meta->file_id = 500;
    meta->min_key_len = 12;
    meta->min_key = aml_malloc(12);
    pack_ikey(meta->min_key, "a", 1, OP_PUT);
    meta->max_key_len = 12;
    meta->max_key = aml_malloc(12);
    pack_ikey(meta->max_key, "z", 2, OP_PUT);

    sstable_meta_t *added[1] = { meta };
    lsmc_version_edit(m1, -1, 0, NULL, 0, added, 1);

    // Close m1 gracefully to force flush the writer fields
    env->router.hot_vfs->fsync_file(m1->manifest_writer);
    env->router.hot_vfs->close_writer(m1->manifest_writer);
    m1->manifest_writer = NULL;
    pthread_mutex_destroy(&m1->version_mutex);
    lsmc_version_release(m1, m1->current_version);
    aml_free(m1->db_directory); aml_free(m1);

    // 2. Open the manifest file manually and corrupt the end of it (append bad data bytes)
    char m_path[520];
    snprintf(m_path, sizeof(m_path), "%s/CURRENT.manifest", dir);
    FILE *f = fopen(m_path, "ab");
    MACRO_ASSERT_TRUE(f != NULL);
    uint32_t junk = 0xDEADBEEF;
    fwrite(&junk, 4, 1, f);
    fclose(f);

    lsm_manifest_t *m2 = lsmc_manifest_init(env, 1, dir);
    MACRO_ASSERT_TRUE(m2 != NULL);

    lsm_version_t *v = lsmc_version_retain(m2);
    MACRO_ASSERT_EQ_INT(v->levels[0].num_files, 1);
    MACRO_ASSERT_EQ_INT(v->levels[0].files[0]->file_id, 500);

    lsmc_version_release(m2, v);

    // Cleanup m2
    if (m2->manifest_writer) {
        env->router.hot_vfs->close_writer(m2->manifest_writer);
    }
    pthread_mutex_destroy(&m2->version_mutex);
    lsmc_version_release(m2, m2->current_version);
    aml_free(m2->db_directory); aml_free(m2);

    lsm_env_destroy(env);
}

MACRO_TEST(sstable_dynamic_lz4_buffer_handling) {
    cleanup_files();
    lsm_env_t *env = lsm_env_init(1024 * 1024, 1024 * 1024 * 10, 1,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);

    const char *base = "/tmp/sstable_test_04";
    sstable_builder_t *b = sstable_builder_init(base, &local_posix_backend, FILTER_NONE, 100);

    size_t huge_size = 200 * 1024;
    char *huge_val = aml_malloc(huge_size);
    memset(huge_val, 'X', huge_size);

    char ikey[1024];
    pack_ikey(ikey, "huge_key", 100, OP_PUT);

    sstable_builder_add(b, ikey, 8 + 8, huge_val, huge_size);
    sstable_builder_finish(b);

    sstable_reader_t *r = sstable_reader_init(base, &local_posix_backend, env, 1, 1);
    MACRO_ASSERT_TRUE(r != NULL);

    uint32_t vlen;
    void *val;
    int status = sstable_reader_get(r, "huge_key", 8, UINT64_MAX, &val, &vlen);

    MACRO_ASSERT_EQ_INT(status, 1);
    MACRO_ASSERT_EQ_INT(vlen, huge_size);
    MACRO_ASSERT_TRUE(memcmp(val, huge_val, huge_size) == 0);

    aml_free(val);
    aml_free(huge_val);
    sstable_reader_destroy(r);
    lsm_env_destroy(env);
}

MACRO_TEST(sstable_bitmap_filter_resists_non_monotonic_underflow) {
    cleanup_files();
    lsm_env_t *env = lsm_env_init(1024 * 1024, 1024 * 1024 * 10, 1,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);

    const char *base = "/tmp/sstable_test_05";
    // Initialize with FILTER_BITMAP
    sstable_builder_t *b = sstable_builder_init(base, &local_posix_backend, 2, 100);

    // Build 8-byte integer keys
    uint64_t id1 = 5000;
    uint64_t id2 = 10; // Much smaller! This caused the underflow crash.

    char ikey1[1024], ikey2[1024];

    // Pack ikey1 manually (8-byte ID + 8-byte trailer)
    memcpy(ikey1, &id1, 8);
    uint64_t trailer1 = (10ULL << 8) | 0; // seq 10, OP_PUT
    for (int i = 0; i < 8; i++) ikey1[8 + i] = (trailer1 >> (i * 8)) & 0xFF;

    // Pack ikey2 manually (8-byte ID + 8-byte trailer)
    memcpy(ikey2, &id2, 8);
    uint64_t trailer2 = (11ULL << 8) | 0; // seq 11, OP_PUT
    for (int i = 0; i < 8; i++) ikey2[8 + i] = (trailer2 >> (i * 8)) & 0xFF;

    // If Phase 3 failed, adding id2 will trigger an infinite memory allocation loop
    sstable_builder_add(b, ikey1, 8 + 8, "val1", 4);
    sstable_builder_add(b, ikey2, 8 + 8, "val2", 4);

    uint64_t size = sstable_builder_finish(b);
    MACRO_ASSERT_TRUE(size > 0);

    sstable_reader_t *r = sstable_reader_init(base, &local_posix_backend, env, 1, 1);
    MACRO_ASSERT_TRUE(r != NULL);

    sstable_reader_destroy(r);
    lsm_env_destroy(env);
}

// Binary-safe key packer
static void pack_binary_ikey(char *dst, const void *user_key, size_t ulen, uint64_t seq, uint8_t op) {
    memcpy(dst, user_key, ulen);
    uint64_t trailer = (seq << 8) | op;
    uint8_t *t = (uint8_t *)(dst + ulen);
    for (int i = 0; i < 8; i++) t[i] = (trailer >> (i * 8)) & 0xFF;
}

MACRO_TEST(manifest_rejects_malicious_allocations) {
    cleanup_files();
    const char *dir = "/tmp/sstable_test_dir";
    lsm_env_t *env = lsm_env_init(1024*1024, 1024*1024*10, 1, &local_posix_backend, &local_posix_backend, 2, NULL);

    system("mkdir -p /tmp/sstable_test_dir");
    char m_path[520];
    snprintf(m_path, sizeof(m_path), "%s/CURRENT.manifest", dir);
    FILE *f = fopen(m_path, "wb");
    MACRO_ASSERT_TRUE(f != NULL);

    uint32_t record_len = 32;
    fwrite(&record_len, 4, 1, f);

    uint8_t buf[32] = {0};
    uint32_t num_del = 150000; // Over the 100,000 hard limit
    uint32_t num_add = 150000;
    memcpy(buf + 8, &num_del, 4);
    memcpy(buf + 12, &num_add, 4);

    fwrite(buf, 32, 1, f);
    uint32_t crc = XXH32(buf, 32, 0);
    fwrite(&crc, 4, 1, f);
    fclose(f);

    // This should safely abort parsing instead of throwing an OOM/Segfault
    lsm_manifest_t *m = lsmc_manifest_init(env, 1, dir);
    MACRO_ASSERT_TRUE(m != NULL);

    lsmc_version_release(m, m->current_version);
    aml_free(m->db_directory);

    if (m->manifest_writer) {
        env->router.hot_vfs->close_writer(m->manifest_writer);
    }

    aml_free(m);
    lsm_env_destroy(env);
}

MACRO_TEST(sstable_reader_rejects_restart_array_underflow) {
    cleanup_files();
    lsm_env_t *env = lsm_env_init(1024 * 1024, 1024 * 1024 * 10, 1,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);

    const char *base = "/tmp/sstable_test_06";
    sstable_builder_t *b = sstable_builder_init(base, &local_posix_backend, FILTER_NONE, 10);

    char ikey[1024];
    pack_binary_ikey(ikey, "random_key", 10, 1, OP_PUT);

    // Use an uncompressible value so LZ4 skips compression
    uint8_t rand_val[100];
    for(int i=0; i<100; i++) rand_val[i] = rand() % 256;
    sstable_builder_add(b, ikey, 10 + 8, rand_val, 100);

    uint64_t file_size = sstable_builder_finish(b);

    // Modify the raw .data file
    char data_path[520];
    snprintf(data_path, sizeof(data_path), "%s.data", base);
    FILE *f = fopen(data_path, "r+b");
    MACRO_ASSERT_TRUE(f != NULL);

    uint8_t *file_buf = aml_malloc(file_size);
    fread(file_buf, 1, file_size, f);

    uint32_t block_size = file_size - 9;

    // Inject malicious num_restarts right before the 9-byte uncompressed block trailer
    uint32_t bad_restarts = 0x7FFFFFFF;
    memcpy(file_buf + block_size - 4, &bad_restarts, 4);

    // Recompute the block CRC so it successfully bypasses the CRC check
    // and forces the iterator to test the underflow logic!
    uint32_t new_crc = XXH32(file_buf, block_size, 0);
    memcpy(file_buf + file_size - 4, &new_crc, 4);

    fseek(f, 0, SEEK_SET);
    fwrite(file_buf, 1, file_size, f);
    fclose(f);
    aml_free(file_buf);

    sstable_reader_t *r = sstable_reader_init(base, &local_posix_backend, env, 1, 1);

    uint32_t vlen;
    void *val = NULL;

    // MUST NOT CRASH! The Phase 3 bounds check should safely reject the payload.
    int status = sstable_reader_get(r, "random_key", 10, UINT64_MAX, &val, &vlen);
    MACRO_ASSERT_EQ_INT(status, 0);

    sstable_reader_destroy(r);
    lsm_env_destroy(env);
}

MACRO_TEST(sstable_bitmap_filter_allows_out_of_range_reads) {
    cleanup_files();
    lsm_env_t *env = lsm_env_init(1024 * 1024, 1024 * 1024 * 10, 1,
                                  &local_posix_backend, &local_posix_backend, 2, NULL);

    const char *base = "/tmp/sstable_test_07";
    sstable_builder_t *b = sstable_builder_init(base, &local_posix_backend, FILTER_BITMAP, 100);

    uint64_t id_min = 10;
    uint64_t id_huge = 10000000; // 10 million. Easily exceeds the builder's 1MB bitmap cap.

    char ikey1[1024], ikey2[1024];
    pack_binary_ikey(ikey1, &id_min, 8, 1, OP_PUT);
    pack_binary_ikey(ikey2, &id_huge, 8, 2, OP_PUT);

    sstable_builder_add(b, ikey1, 8 + 8, "val1", 4);
    sstable_builder_add(b, ikey2, 8 + 8, "val_huge", 8);
    sstable_builder_finish(b);

    sstable_reader_t *r = sstable_reader_init(base, &local_posix_backend, env, 1, 1);
    MACRO_ASSERT_TRUE(r != NULL);

    uint32_t vlen;
    void *val = NULL;

    // Query the huge ID. If the Phase 3 fix is missing, this returns 0.
    // With the fix, the out-of-bounds check falls through to the index and correctly returns 1.
    int status = sstable_reader_get(r, &id_huge, 8, UINT64_MAX, &val, &vlen);

    MACRO_ASSERT_EQ_INT(status, 1);
    MACRO_ASSERT_EQ_INT(vlen, 8);
    MACRO_ASSERT_TRUE(memcmp(val, "val_huge", 8) == 0);

    aml_free(val);
    sstable_reader_destroy(r);
    lsm_env_destroy(env);
}

// --- FAULTY VFS FOR BUILDER ---
static int fault_fsync_count = 0;
static bool fault_delete_called = false;

static int fault_fsync_file(void *f) {
    if (fault_fsync_count == 0) return -1; // Simulate I/O failure!
    fault_fsync_count--;
    return local_posix_backend.fsync_file(f);
}

// FIX: Changed return type from 'int' to 'bool'
static bool fault_delete_file(const char *path) {
    fault_delete_called = true;
    return local_posix_backend.delete_file(path);
}

MACRO_TEST(sstable_builder_cleans_up_on_io_error) {
    cleanup_files();

    lsm_storage_backend_t fault_vfs;
    memcpy(&fault_vfs, &local_posix_backend, sizeof(lsm_storage_backend_t));
    fault_vfs.fsync_file = fault_fsync_file;
    fault_vfs.delete_file = fault_delete_file;

    fault_fsync_count = 0; // Fail on the very first fsync (the data file)
    fault_delete_called = false;

    const char *base = "/tmp/sstable_fault_test";
    sstable_builder_t *b = sstable_builder_init(base, &fault_vfs, FILTER_NONE, 10);
    MACRO_ASSERT_TRUE(b != NULL);

    char ikey[1024];
    pack_binary_ikey(ikey, "key1", 4, 1, OP_PUT);
    sstable_builder_add(b, ikey, 4 + 8, "val1", 4);

    // Attempt to finish. The fsync should fail, trigger an abort, delete the files, and return 0.
    uint64_t size = sstable_builder_finish(b);
    MACRO_ASSERT_EQ_INT(size, 0);

    // ASSERTION: The builder explicitly cleaned up the orphaned files via delete!
    MACRO_ASSERT_TRUE(fault_delete_called);

    FILE *f = fopen("/tmp/sstable_fault_test.data", "r");
    MACRO_ASSERT_TRUE(f == NULL); // Should not exist
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, sstable_build_and_read_roundtrip);
    MACRO_ADD(tests, sstable_tombstone_parsing_returns_negative_one);
    MACRO_ADD(tests, sstable_iterator_decodes_trailers_correctly);
    MACRO_ADD(tests, manifest_survives_torn_and_corrupt_writes);
    MACRO_ADD(tests, sstable_dynamic_lz4_buffer_handling);
    MACRO_ADD(tests, sstable_bitmap_filter_resists_non_monotonic_underflow);
    MACRO_ADD(tests, manifest_rejects_malicious_allocations);
    MACRO_ADD(tests, sstable_reader_rejects_restart_array_underflow);
    MACRO_ADD(tests, sstable_bitmap_filter_allows_out_of_range_reads);
    MACRO_ADD(tests, sstable_builder_cleans_up_on_io_error);

    macro_run_all("lsm_sstable_disk_format", tests, test_count);
    return 0;
}
