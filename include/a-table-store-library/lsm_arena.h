// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef LSM_ARENA_H
#define LSM_ARENA_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct lsm_arena_s lsm_arena_t;

/* Initialize a geometrically growing memory arena */
lsm_arena_t *lsm_arena_init(void);

/* Returns the total number of bytes requested from the OS */
size_t lsm_arena_used(lsm_arena_t *arena);

/* Instantly frees all chunks back to the OS */
void lsm_arena_destroy(lsm_arena_t *arena);

/* Allocate perfectly aligned memory. Fast inline bump-pointer. */
static inline void *lsm_arena_alloc(lsm_arena_t *arena, size_t size);

/* Include the inline implementations and struct definitions */
#include "a-table-store-library/impl/lsm_arena.h"

#ifdef __cplusplus
}
#endif

#endif /* LSM_ARENA_H */
