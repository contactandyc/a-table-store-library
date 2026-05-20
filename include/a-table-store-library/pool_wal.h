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
#define WAL_MAGIC_NUMBER 0x57414C22 // Slightly different magic for this engine type

typedef struct {
    uint32_t magic;
    uint64_t segment_id; // Strictly increasing file ID (e.g. 0001, 0002)
    uint64_t start_seq;  // The lowest sequence number in this segment
    uint32_t header_crc;
} __attribute__((packed)) pool_file_header_t;

typedef struct {
    uint32_t payload_len;
    uint64_t seq;
    uint8_t  type;
    uint32_t frame_crc;
} __attribute__((packed)) pool_frame_header_t;

// Multi-table checkpoint tracking for garbage collection
typedef struct {
    uint32_t table_id;
    uint64_t safe_seq;
} pool_wal_ckpt_t;

typedef struct {
    char base_dir[512];

    // Configuration
    uint64_t segment_size_bytes;
    uint32_t max_standby_files;

    // Active State
    uint64_t current_seg_id;    // ID of the file we are appending to
    uint64_t oldest_seg_id;     // ID of the oldest file still on disk
    int active_fd;
    uint64_t file_offset;

    // The Standby Pool (Array of absolute file paths ready for recycling)
    char** standby_paths;
    uint32_t standby_count;

    // Multi-table LSM Integration State
    pthread_mutex_t mu;
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
int pool_wal_append(pool_wal_t* wal, uint64_t seq, uint8_t type, const uint8_t* payload, uint32_t len);
void pool_wal_purge(pool_wal_t* wal, uint64_t safe_checkpoint);
void pool_wal_sync(pool_wal_t* wal);
void pool_wal_close(pool_wal_t* wal);

pool_wal_iter_t* pool_wal_iter_init(pool_wal_t* wal);
bool pool_wal_iter_next(pool_wal_iter_t* iter, uint64_t* seq, uint8_t* type, const uint8_t** payload, uint32_t* len);
void pool_wal_iter_destroy(pool_wal_iter_t* iter);


// --- LSM-Tree Pluggable Translation API ---

/*
 * Allocates and initializes a fully compatible LSM Pluggable WAL interface
 * that wraps around the physical Pool WAL implementation.
 * Closing the returned lsm_wal_t will automatically free the underlying pool_wal_t.
 */
lsm_wal_t *pool_to_lsm_wal(const char *dir, uint64_t segment_size_mb, uint32_t max_standby);

#endif // POOL_WAL_H
