// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef LSM_WAL_H
#define LSM_WAL_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

typedef struct lsm_wal_s lsm_wal_t;
typedef struct lsm_wal_iter_s lsm_wal_iter_t;

/* Iterator for Crash Recovery */
struct lsm_wal_iter_s {
    void *ctx;

    /* Returns false when the log is fully read or a torn/corrupted write is hit */
    bool (*next)(lsm_wal_iter_t *iter, uint32_t *out_table_id, uint8_t *out_op,
                 const void **out_key, uint32_t *out_klen,
                 const void **out_val, uint32_t *out_vlen);

    void (*destroy)(lsm_wal_iter_t *iter);
};

/* The Pluggable WAL Interface */
struct lsm_wal_s {
    void *ctx;

    /* Append a multiplexed record to the log */
    bool (*append)(lsm_wal_t *wal, uint32_t table_id, uint8_t op,
                   const void *key, uint32_t klen,
                   const void *val, uint32_t vlen);

    /* Force the OS to flush the log to physical media (fsync / O_DSYNC) */
    void (*sync)(lsm_wal_t *wal);

    /* Safely close the WAL */
    void (*close)(lsm_wal_t *wal);

    /* Initialize an iterator for recovery (oldest durable record to newest) */
    lsm_wal_iter_t* (*iter_init)(lsm_wal_t *wal);
};

#endif /* LSM_WAL_H */
