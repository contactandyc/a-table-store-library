// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-table-store-library/lsm_arena.h"
#include "a-memory-library/aml_alloc.h"
#include <stdlib.h>

/*
 * The geometric growth limits.
 * Idle tables only use 128KB. Hot tables will quickly scale up to
 * grabbing 4MB at a time from the OS to reduce malloc overhead.
 */
#define ARENA_INITIAL_CHUNK_SIZE (128 * 1024)      // 128 KB
#define ARENA_MAX_CHUNK_SIZE     (4 * 1024 * 1024) // 4 MB

lsm_arena_t *lsm_arena_init(void) {
    lsm_arena_t *arena = aml_zalloc(sizeof(lsm_arena_t));
    arena->current_chunk_size = ARENA_INITIAL_CHUNK_SIZE / 2; // [Phase 4A Fix]
    arena->current_chunk_size = ARENA_INITIAL_CHUNK_SIZE;
    arena->total_memory_used = sizeof(lsm_arena_t);
    return arena;
}

/*
 * __attribute__((noinline)) prevents the compiler from trying to cram this
 * massive slow-path logic into the caller's instruction cache.
 */
#if defined(__GNUC__) || defined(__clang__)
__attribute__((noinline))
#endif
void *_lsm_arena_grow(lsm_arena_t *arena, size_t size) {
    // 1. Calculate the new chunk size (Geometric Doubling)
    size_t next_size = arena->current_chunk_size * 2;
    if (next_size > ARENA_MAX_CHUNK_SIZE) {
        next_size = ARENA_MAX_CHUNK_SIZE;
    }

    // Safety check: If a single row is somehow larger than our chunk size, fit it.
    if (size > next_size) {
        next_size = size;
    }

    // 2. Allocate the node + the chunk payload in one contiguous block
    size_t allocation_size = sizeof(lsm_arena_node_t) + next_size;
    lsm_arena_node_t *node = aml_malloc(allocation_size);

    node->next = arena->head;
    node->endp = ((char *)(node + 1)) + next_size;
    arena->head = node;

    arena->curp = (char *)(node + 1);
    arena->endp = node->endp;

    arena->current_chunk_size = next_size;
    arena->total_memory_used += allocation_size;

    // 3. Fulfill the original allocation request
    void *result = arena->curp;
    arena->curp += size;
    return result;
}

size_t lsm_arena_used(lsm_arena_t *arena) {
    if (!arena) return 0;
    return arena->total_memory_used;
}

void lsm_arena_destroy(lsm_arena_t *arena) {
    if (!arena) return;

    lsm_arena_node_t *curr = arena->head;
    while (curr) {
        lsm_arena_node_t *next = curr->next;
        aml_free(curr);
        curr = next;
    }
    aml_free(arena);
}
