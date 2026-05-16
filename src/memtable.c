// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-table-store-library/memtable.h"
#include "a-table-store-library/lsm_arena.h"
#include "a-memory-library/aml_alloc.h"
#include "the-macro-library/macro_skiplist.h"
#include <string.h>

#define MEMTABLE_MAX_HEIGHT 12

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wflexible-array-extensions"
#endif

struct memtable_row_s {
    uint64_t seq_num;
    uint32_t key_len;
    uint32_t val_len;
    op_type_t op_type;
    macro_skiplist_t link;
};

struct memtable_row_index_s {
    uint64_t seq_num;
    uint32_t sec_key_len;
    uint32_t pri_key_len;
    op_type_t op_type;
    macro_skiplist_t link;
};

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif

struct memtable_s {
    lsm_arena_t *arena;
    macro_skiplist_t *head;
    int ref_count;
};

static inline const char* internal_row_key(const memtable_row_t *row) {
    return (const char *)&row->link.forward[row->link.height];
}
static inline const char* internal_row_val(const memtable_row_t *row) {
    return internal_row_key(row) + row->key_len;
}

static inline const char* internal_index_sec_key(const memtable_row_index_t *row) {
    return (const char *)&row->link.forward[row->link.height];
}
static inline const char* internal_index_pri_key(const memtable_row_index_t *row) {
    return internal_index_sec_key(row) + row->sec_key_len;
}

typedef struct {
    const char *key;
    uint32_t key_len;
    uint64_t seq_num;
} memtable_lookup_t;

static int memtable_row_cmp(const memtable_row_t *a, const memtable_row_t *b) {
    const char *ka = internal_row_key(a), *kb = internal_row_key(b);
    size_t min_len = a->key_len < b->key_len ? a->key_len : b->key_len;

    int c = memcmp(ka, kb, min_len);
    if (c != 0) return c;
    if (a->key_len != b->key_len) return a->key_len < b->key_len ? -1 : 1;

    if (a->seq_num > b->seq_num) return -1;
    if (a->seq_num < b->seq_num) return 1;
    return 0;
}

static int memtable_kv_cmp(const memtable_lookup_t *lookup, const memtable_row_t *row) {
    const char *kr = internal_row_key(row);
    size_t min_len = lookup->key_len < row->key_len ? lookup->key_len : row->key_len;

    int c = memcmp(lookup->key, kr, min_len);
    if (c != 0) return c;
    if (lookup->key_len != row->key_len) return lookup->key_len < row->key_len ? -1 : 1;

    if (lookup->seq_num > row->seq_num) return -1;
    if (lookup->seq_num < row->seq_num) return 1;
    return 0;
}

macro_multimap_skiplist_insert_with_field(memtable_sl_insert, link, memtable_row_t, memtable_row_cmp)
macro_skiplist_lower_bound_kv_with_field(memtable_sl_lower_bound, link, memtable_lookup_t, memtable_row_t, memtable_kv_cmp)

typedef struct {
    const char *sec_key;
    uint32_t sec_key_len;
} index_lookup_t;

static int memtable_index_row_cmp(const memtable_row_index_t *a, const memtable_row_index_t *b) {
    const char *a_sec = internal_index_sec_key(a), *b_sec = internal_index_sec_key(b);
    size_t min_sec = a->sec_key_len < b->sec_key_len ? a->sec_key_len : b->sec_key_len;

    int c = memcmp(a_sec, b_sec, min_sec);
    if (c != 0) return c;
    if (a->sec_key_len != b->sec_key_len) return a->sec_key_len < b->sec_key_len ? -1 : 1;

    const char *a_pri = internal_index_pri_key(a), *b_pri = internal_index_pri_key(b);
    size_t min_pri = a->pri_key_len < b->pri_key_len ? a->pri_key_len : b->pri_key_len;

    c = memcmp(a_pri, b_pri, min_pri);
    if (c != 0) return c;
    if (a->pri_key_len != b->pri_key_len) return a->pri_key_len < b->pri_key_len ? -1 : 1;

    if (a->seq_num > b->seq_num) return -1;
    if (a->seq_num < b->seq_num) return 1;
    return 0;
}

static int memtable_index_kv_cmp(const index_lookup_t *lookup, const memtable_row_index_t *row) {
    const char *row_sec = internal_index_sec_key(row);
    size_t min_sec = lookup->sec_key_len < row->sec_key_len ? lookup->sec_key_len : row->sec_key_len;

    int c = memcmp(lookup->sec_key, row_sec, min_sec);
    if (c != 0) return c;
    if (lookup->sec_key_len != row->sec_key_len) return lookup->sec_key_len < row->sec_key_len ? -1 : 1;

    return -1;
}

macro_multimap_skiplist_insert_with_field(memtable_index_sl_insert, link, memtable_row_index_t, memtable_index_row_cmp)
macro_skiplist_lower_bound_kv_with_field(memtable_index_sl_lower_bound, link, index_lookup_t, memtable_row_index_t, memtable_index_kv_cmp)

memtable_t *memtable_init(void) {
    memtable_t *mt = (memtable_t *)aml_malloc(sizeof(memtable_t));
    if (!mt) return NULL;

    mt->arena = lsm_arena_init();
    size_t head_size = sizeof(macro_skiplist_t) + (MEMTABLE_MAX_HEIGHT * sizeof(void*));
    mt->head = (macro_skiplist_t *)lsm_arena_alloc(mt->arena, head_size);
    macro_skiplist_init_head(mt->head, MEMTABLE_MAX_HEIGHT);
    mt->ref_count = 1;

    return mt;
}

void memtable_retain(memtable_t *mt) {
    if (mt) __atomic_fetch_add(&mt->ref_count, 1, __ATOMIC_SEQ_CST);
}

void memtable_release(memtable_t *mt) {
    if (!mt) return;
    if (__atomic_sub_fetch(&mt->ref_count, 1, __ATOMIC_SEQ_CST) == 0) {
        lsm_arena_destroy(mt->arena);
        aml_free(mt);
    }
}

void memtable_destroy(memtable_t *mt) {
    memtable_release(mt);
}

size_t memtable_get_memory_usage(memtable_t *mt) {
    if (!mt) return 0;
    return lsm_arena_used(mt->arena);
}

bool memtable_put(memtable_t *mt, uint64_t seq_num, op_type_t op,
                  const void *key, uint32_t key_len,
                  const void *val, uint32_t val_len) {

    uint8_t h = macro_skiplist_random_height(MEMTABLE_MAX_HEIGHT);
    size_t row_size = sizeof(memtable_row_t) + (h * sizeof(void*)) + key_len + val_len;

    memtable_row_t *row = (memtable_row_t *)lsm_arena_alloc(mt->arena, row_size);
    row->seq_num = seq_num;
    row->key_len = key_len;
    row->val_len = val_len;
    row->op_type = op;
    row->link.height = h;

    for (int i = 0; i < h; i++) MACRO_ATOMIC_INIT(&row->link.forward[i], NULL);

    memcpy((char *)internal_row_key(row), key, key_len);
    if (val_len > 0 && val) memcpy((char *)internal_row_val(row), val, val_len);

    return memtable_sl_insert(mt->head, row);
}

const void *memtable_get(memtable_t *mt, const void *key, uint32_t key_len,
                         uint64_t read_seq_num, uint32_t *out_len, bool *is_deleted) {
    if (is_deleted) *is_deleted = false;

    memtable_lookup_t lookup = { .key = (const char *)key, .key_len = key_len, .seq_num = read_seq_num };
    memtable_row_t *row = memtable_sl_lower_bound(mt->head, &lookup);

    if (!row || row->key_len != key_len || memcmp(internal_row_key(row), key, key_len) != 0) return NULL;

    if (row->op_type == OP_DELETE) {
        if (is_deleted) *is_deleted = true;
        return NULL;
    }

    if (out_len) *out_len = row->val_len;
    return internal_row_val(row);
}

memtable_row_t *memtable_first(memtable_t *mt) {
    macro_skiplist_t *first = MACRO_ATOMIC_LOAD(&mt->head->forward[0], acquire);
    return first ? macro_parent_object(first, memtable_row_t, link) : NULL;
}

memtable_row_t *memtable_next(memtable_row_t *row) {
    macro_skiplist_t *next = MACRO_ATOMIC_LOAD(&row->link.forward[0], acquire);
    return next ? macro_parent_object(next, memtable_row_t, link) : NULL;
}

const void *memtable_row_get_key(const memtable_row_t *row, uint32_t *out_len) {
    if (out_len) *out_len = row->key_len;
    return internal_row_key(row);
}

const void *memtable_row_get_val(const memtable_row_t *row, uint32_t *out_len) {
    if (out_len) *out_len = row->val_len;
    return internal_row_val(row);
}

uint64_t memtable_row_get_seq(const memtable_row_t *row) { return row->seq_num; }
op_type_t memtable_row_get_op(const memtable_row_t *row) { return row->op_type; }

bool memtable_index_put(memtable_t *mt, uint64_t seq_num, op_type_t op,
                        const void *sec_key, uint32_t sec_key_len,
                        const void *pri_key, uint32_t pri_key_len) {

    uint8_t h = macro_skiplist_random_height(MEMTABLE_MAX_HEIGHT);
    size_t row_size = sizeof(memtable_row_index_t) + (h * sizeof(void*)) + sec_key_len + pri_key_len;

    memtable_row_index_t *row = (memtable_row_index_t *)lsm_arena_alloc(mt->arena, row_size);
    row->seq_num = seq_num;
    row->sec_key_len = sec_key_len;
    row->pri_key_len = pri_key_len;
    row->op_type = op;
    row->link.height = h;

    for (int i = 0; i < h; i++) MACRO_ATOMIC_INIT(&row->link.forward[i], NULL);

    memcpy((char *)internal_index_sec_key(row), sec_key, sec_key_len);
    memcpy((char *)internal_index_pri_key(row), pri_key, pri_key_len);

    return memtable_index_sl_insert(mt->head, row);
}

memtable_row_index_t *memtable_index_search(memtable_t *mt, const void *sec_key, uint32_t sec_key_len) {
    index_lookup_t lookup = { .sec_key = (const char *)sec_key, .sec_key_len = sec_key_len };
    return memtable_index_sl_lower_bound(mt->head, &lookup);
}

memtable_row_index_t *memtable_index_first(memtable_t *mt) {
    macro_skiplist_t *first = MACRO_ATOMIC_LOAD(&mt->head->forward[0], acquire);
    return first ? macro_parent_object(first, memtable_row_index_t, link) : NULL;
}

memtable_row_index_t *memtable_index_next(memtable_row_index_t *row) {
    macro_skiplist_t *next = MACRO_ATOMIC_LOAD(&row->link.forward[0], acquire);
    return next ? macro_parent_object(next, memtable_row_index_t, link) : NULL;
}

const void *memtable_row_index_get_sec_key(const memtable_row_index_t *row, uint32_t *out_len) {
    if (out_len) *out_len = row->sec_key_len;
    return internal_index_sec_key(row);
}

const void *memtable_row_index_get_pri_key(const memtable_row_index_t *row, uint32_t *out_len) {
    if (out_len) *out_len = row->pri_key_len;
    return internal_index_pri_key(row);
}

uint64_t memtable_row_index_get_seq(const memtable_row_index_t *row) { return row->seq_num; }
op_type_t memtable_row_index_get_op(const memtable_row_index_t *row) { return row->op_type; }
