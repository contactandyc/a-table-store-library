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

#define MAX_LEVELS 7
#define MAX_KEY_SIZE 1024
#define TARGET_FILE_SIZE (2 * 1024 * 1024) // Split files at 2MB in L1+

/* Metadata for a single file on disk */
typedef struct {
    uint64_t file_id;
    uint64_t file_size;
    uint32_t num_entries;
    char min_key[MAX_KEY_SIZE];
    uint32_t min_key_len;
    char max_key[MAX_KEY_SIZE];
    uint32_t max_key_len;
} sstable_meta_t;

/* A Level containing multiple files */
typedef struct {
    int level_num;
    uint64_t total_bytes;
    sstable_meta_t **files;
    size_t num_files;
    size_t files_capacity;
} lsm_level_t;

/* The Database Manifest */
typedef struct {
    uint64_t next_file_id;
    lsm_level_t levels[MAX_LEVELS];
    const char *db_directory;
} lsm_manifest_t;

/* Initialize the manifest (in a real DB, you load this from a MANIFEST file) */
lsm_manifest_t *lsmc_manifest_init(const char *db_directory);

/* Trigger a compaction from L(n) to L(n+1) */
bool lsmc_compact_level(lsm_manifest_t *manifest, int source_level);

#endif /* LSM_COMPACTION_H */
