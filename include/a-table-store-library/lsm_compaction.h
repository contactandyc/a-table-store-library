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
#include <pthread.h> // Ensure threads are available

#define MAX_LEVELS 7
#define MAX_KEY_SIZE 1024
#define TARGET_FILE_SIZE (2 * 1024 * 1024)

/* Metadata for a single file on disk */
typedef struct {
    uint64_t file_id;
    uint64_t file_size;
    uint32_t num_entries;
    char min_key[MAX_KEY_SIZE];
    uint32_t min_key_len;
    char max_key[MAX_KEY_SIZE];
    uint32_t max_key_len;

    int ref_count; // <--- NEW: Reference Counting!
} sstable_meta_t;

/* A Level containing multiple files */
typedef struct {
    int level_num;
    uint64_t total_bytes;
    sstable_meta_t **files;
    size_t num_files;
    size_t files_capacity;
} lsm_level_t;

/* NEW: An Immutable Snapshot of the Database State */
typedef struct lsm_version_s {
    lsm_level_t levels[MAX_LEVELS];
    int ref_count;
} lsm_version_t;

/* The Database Manifest */
typedef struct {
    uint64_t next_file_id;
    char *db_directory;

    lsm_version_t *current_version;
    pthread_mutex_t version_mutex; // Protects version pointer swaps
} lsm_manifest_t;

/* Initialize the manifest */
lsm_manifest_t *lsmc_manifest_init(const char *db_directory);

/* MVCC: Pin the current state so it can't be deleted while we read it */
lsm_version_t *lsmc_version_retain(lsm_manifest_t *manifest);

/* MVCC: Release our pin. If we are the last reader, it physically deletes old files! */
void lsmc_version_release(lsm_manifest_t *manifest, lsm_version_t *version);

/* MVCC: Commit an atomic file swap */
bool lsmc_version_edit(lsm_manifest_t *manifest, int source_level, int target_level,
                       sstable_meta_t **deleted_files, size_t num_deleted,
                       sstable_meta_t **added_files, size_t num_added);

/* Trigger a compaction from L(n) to L(n+1) */
bool lsmc_compact_level(lsm_manifest_t *manifest, int source_level);

#endif /* LSM_COMPACTION_H */
