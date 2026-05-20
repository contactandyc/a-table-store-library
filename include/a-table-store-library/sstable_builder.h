// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef SSTABLE_BUILDER_H
#define SSTABLE_BUILDER_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "a-table-store-library/lsm_storage.h"

typedef struct sstable_builder_s sstable_builder_t;

#define FILTER_NONE   0
#define FILTER_BLOOM  1
#define FILTER_BITMAP 2

/*
 * Initialize a new SSTable builder.
 * Passes the VFS backend and base_path (e.g., "/db/00001" without extensions).
 */
sstable_builder_t *sstable_builder_init(const char *base_path, lsm_storage_backend_t *backend, int filter_type, size_t expected_elements);

/* Returns false if an internal block flush hits an I/O error */
bool sstable_builder_add(sstable_builder_t *builder, const void *key, uint32_t key_len, const void *val, uint32_t val_len);

uint64_t sstable_builder_current_size(sstable_builder_t *builder);

/* Writes the merged .meta file and fsyncs. Returns 0 on I/O failure. */
uint64_t sstable_builder_finish(sstable_builder_t *builder);

/* [Phase 4] Transactional rollback. Closes handles, deletes partial files from disk, and frees memory. */
void sstable_builder_abort(sstable_builder_t *builder);

#endif /* SSTABLE_BUILDER_H */
