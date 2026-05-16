// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef IMPL_LSM_ARENA_H
#define IMPL_LSM_ARENA_H

/* IMPLEMENTATION FOLLOWS - API is above this line */

/* Forward declaration of the slow-path growth function */
void *_lsm_arena_grow(lsm_arena_t *arena, size_t size);

typedef struct lsm_arena_node_s {
    struct lsm_arena_node_s *next;
    char *endp;
    /* Payload starts immediately after this struct */
} lsm_arena_node_t;

struct lsm_arena_s {
    lsm_arena_node_t *head;
    char *curp;
    char *endp;

    size_t current_chunk_size;
    size_t total_memory_used;
};

static inline void *lsm_arena_alloc(lsm_arena_t *arena, size_t size) {
    /* Force 8-byte alignment for all allocations to prevent CPU bus errors */
    size = (size + 7) & ~7;

    /* Fast-path: Inline bump pointer. FIX: Verify curp is initialized! */
    if (arena->curp != NULL && arena->curp + size <= arena->endp) {
        void *result = arena->curp;
        arena->curp += size;
        return result;
    }

    /* Slow-path: Branch out to the .c file to allocate a new chunk from the OS */
    return _lsm_arena_grow(arena, size);
}

#endif /* IMPL_LSM_ARENA_H */
