// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

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
    free(m1->db_directory); free(m1);

    // 2. Open the manifest file manually and corrupt the end of it (append bad data bytes)
    char m_path[520];
    snprintf(m_path, sizeof(m_path), "%s/CURRENT.manifest", dir);
    FILE *f = fopen(m_path, "ab");
    MACRO_ASSERT_TRUE(f != NULL);
    uint32_t junk = 0xDEADBEEF;
    fwrite(&junk, 4, 1, f); // Appending an invalid record length size
    fclose(f);

    // 3. Re-open the manifest with a fresh structure.
    // Replay loop must hit the corrupted boundary, gracefully ignore it via checksum/length limits,
    // and successfully yield the pristine older state instead of overflowing or crashing.
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
    free(m2->db_directory); free(m2);

    lsm_env_destroy(env);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, sstable_build_and_read_roundtrip);
    MACRO_ADD(tests, sstable_tombstone_parsing_returns_negative_one);
    MACRO_ADD(tests, sstable_iterator_decodes_trailers_correctly);
    MACRO_ADD(tests, manifest_survives_torn_and_corrupt_writes);

    macro_run_all("lsm_sstable_disk_format", tests, test_count);
    return 0;
}
