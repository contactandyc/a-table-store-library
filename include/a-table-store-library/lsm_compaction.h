// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef LSM_COMPACTION_H
#define LSM_COMPACTION_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "a-table-store-library/sstable_reader.h"
#include "a-table-store-library/sstable_builder.h"
#include "a-table-store-library/lsm_env.h"
#include <pthread.h>

#define MAX_LEVELS 7
#define MAX_KEY_SIZE 1024
#define INTERNAL_KEY_TRAILER_SIZE 8
#define MAX_INTERNAL_KEY_SIZE (MAX_KEY_SIZE + INTERNAL_KEY_TRAILER_SIZE)

#define TARGET_FILE_SIZE (2 * 1024 * 1024)

typedef struct {
    uint64_t file_id;
    uint64_t file_size;
    uint32_t num_entries;

    char *min_key;
    uint32_t min_key_len;
    char *max_key;
    uint32_t max_key_len;

    uint64_t max_seq;

    int ref_count;
    bool is_obsolete;
} sstable_meta_t;

typedef struct {
    int level_num;
    uint64_t total_bytes;
    sstable_meta_t **files;
    size_t num_files;
    size_t files_capacity;
} lsm_level_t;

typedef struct lsm_version_s {
    lsm_level_t levels[MAX_LEVELS];
    int ref_count;
} lsm_version_t;

typedef struct {
    uint64_t next_file_id;
    char *db_directory;
    uint32_t table_id;
    lsm_env_t *env;

    lsm_version_t *current_version;
    pthread_mutex_t version_mutex;

    void *manifest_writer;

    // [Phase 3 Fix] Compaction pointers to prevent starvation
    char *compaction_pointers[MAX_LEVELS];
    uint32_t compaction_pointer_lens[MAX_LEVELS];

} lsm_manifest_t;

lsm_manifest_t *lsmc_manifest_init(lsm_env_t *env, uint32_t table_id, const char *db_directory);
lsm_version_t *lsmc_version_retain(lsm_manifest_t *manifest);
void lsmc_version_release(lsm_manifest_t *manifest, lsm_version_t *version);
bool lsmc_version_edit(lsm_manifest_t *manifest, int source_level, int target_level,
                       sstable_meta_t **deleted_files, size_t num_deleted,
                       sstable_meta_t **added_files, size_t num_added);

bool lsmc_compact_level(lsm_manifest_t *manifest, int source_level, uint64_t oldest_snapshot);

uint64_t lsmc_get_max_sequence(lsm_manifest_t *manifest);

#endif /* LSM_COMPACTION_H */
