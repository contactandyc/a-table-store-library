// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef POOL_WAL_H
#define POOL_WAL_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include "a-table-store-library/lsm_wal.h"

#define WAL_HEADER_SIZE 4096
#define WAL_MAGIC_NUMBER 0x57414C22

typedef struct {
    uint32_t magic;
    uint64_t segment_id;
    uint64_t start_lsn;  // [Phase 6] Global LSN for safe cross-table GC
    uint32_t header_crc;
} __attribute__((packed)) pool_file_header_t;

typedef struct {
    uint32_t payload_len;
    uint64_t seq;
    uint64_t lsn;        // [Phase 6] Explicit LSN tag
    uint8_t  type;
    uint32_t frame_crc;
} __attribute__((packed)) pool_frame_header_t;

// Multi-table checkpoint tracking for garbage collection
typedef struct {
    uint32_t table_id;
    uint64_t safe_lsn;   // [Phase 6] Unified safe LSN
} pool_wal_ckpt_t;

typedef struct {
    char base_dir[512];

    // Configuration
    uint64_t segment_size_bytes;
    uint32_t max_standby_files;

    // Active State
    uint64_t current_seg_id;
    uint64_t oldest_seg_id;
    int active_fd;
    uint64_t file_offset;

    // The Standby Pool
    char** standby_paths;
    uint32_t standby_count;

    // [Phase 6] Strict Thread-Safety and Global LSN Allocator
    pthread_mutex_t mu;
    uint64_t next_lsn;

    pool_wal_ckpt_t *ckpts;
    size_t num_ckpts;
    size_t cap_ckpts;

} pool_wal_t;

// Iterator State
typedef struct {
    pool_wal_t* wal;
    uint64_t current_seg;
    int fd;
    uint64_t offset;
    uint8_t* current_payload;
} pool_wal_iter_t;

// --- Core Pool WAL API ---
int pool_wal_init(pool_wal_t* wal, const char* dir, uint64_t segment_size_mb, uint32_t max_standby_files);

// [Phase 6 Fix] Returns the assigned LSN (or 0 on failure)
uint64_t pool_wal_append(pool_wal_t* wal, uint64_t seq, uint8_t type, const uint8_t* payload, uint32_t len);

void pool_wal_purge(pool_wal_t* wal, uint64_t safe_lsn);
void pool_wal_sync(pool_wal_t* wal);
void pool_wal_close(pool_wal_t* wal);

pool_wal_iter_t* pool_wal_iter_init(pool_wal_t* wal);
bool pool_wal_iter_next(pool_wal_iter_t* iter, uint64_t* seq, uint64_t* lsn, uint8_t* type, const uint8_t** payload, uint32_t* len);
void pool_wal_iter_destroy(pool_wal_iter_t* iter);

// --- LSM-Tree Pluggable Translation API ---
lsm_wal_t *pool_to_lsm_wal(const char *dir, uint64_t segment_size_mb, uint32_t max_standby);

#endif // POOL_WAL_H
