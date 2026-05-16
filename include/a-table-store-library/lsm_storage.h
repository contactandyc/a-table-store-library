// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef LSM_STORAGE_H
#define LSM_STORAGE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#include <sys/types.h> // For ssize_t

typedef struct lsm_storage_backend_s lsm_storage_backend_t;

struct lsm_storage_backend_s {
    void* (*open_reader)(const char *path);
    void* (*open_writer)(const char *path);   // Truncates (SSTables)
    void* (*open_appender)(const char *path); // Appends (WAL / Manifest)

    // [Phase 4A Fix] Return ssize_t to properly indicate IO errors (-1) vs EOF (0)
    ssize_t (*pread)(void *ctx, void *buf, size_t size, uint64_t offset);
    size_t (*append)(void *ctx, const void *buf, size_t size);

    // Explicit sync to disk
    int (*fsync_file)(void *ctx);

    void (*close_reader)(void *ctx);
    void (*close_writer)(void *ctx);
    bool (*delete_file)(const char *path);
};

extern lsm_storage_backend_t local_posix_backend;

#endif /* LSM_STORAGE_H */
