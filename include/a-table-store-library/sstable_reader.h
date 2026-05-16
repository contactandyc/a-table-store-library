// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef SSTABLE_READER_H
#define SSTABLE_READER_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "a-table-store-library/lsm_env.h"

typedef struct sstable_reader_s sstable_reader_t;
typedef struct sstable_iter_s sstable_iter_t;

sstable_reader_t *sstable_reader_init(const char *base_path, lsm_storage_backend_t *backend, lsm_env_t *env, uint32_t table_id, uint64_t file_id);

/*
 * Returns:
 *  1 = Found (Value populated in out_val)
 *  0 = Not Found in this SSTable
 * -1 = Tombstone Found (Key was explicitly deleted)
 */
int sstable_reader_get(sstable_reader_t *reader, const void *key, uint32_t key_len, uint64_t read_seq_num, void **out_val, uint32_t *out_val_len);

void sstable_reader_destroy(sstable_reader_t *reader);

sstable_iter_t *sstable_iter_init(sstable_reader_t *reader);
bool sstable_iter_next(sstable_iter_t *iter);
void sstable_iter_get_kv(sstable_iter_t *iter, const char **key, uint32_t *klen, const char **val, uint32_t *vlen);
void sstable_iter_get_meta(sstable_iter_t *iter, uint64_t *seq, uint8_t *op);
void sstable_iter_destroy(sstable_iter_t *iter);

#endif /* SSTABLE_READER_H */
