// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "a-table-store-library/lsm_db.h"
#include "a-table-store-library/memtable.h"
#include "a-table-store-library/lsm_compaction.h"
#include "a-table-store-library/sstable_builder.h"
#include "a-table-store-library/sstable_reader.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* --- MVCC Internal Key Helpers --- */
#define TRAILER_SIZE 8

static inline void pack_internal_key(char *dst, const void *user_key, uint32_t ulen, uint64_t seq, uint8_t op) {
    memcpy(dst, user_key, ulen);
    uint64_t trailer = (seq << 8) | op;
    memcpy(dst + ulen, &trailer, 8);
}

static inline uint32_t get_user_key_len(uint32_t internal_len) {
    return internal_len > TRAILER_SIZE ? internal_len - TRAILER_SIZE : 0;
}

static inline void unpack_trailer(const char *internal_key, uint32_t internal_len, uint64_t *seq, uint8_t *op) {
    if (internal_len < TRAILER_SIZE) { *seq = 0; *op = 0; return; }
    uint64_t trailer;
    memcpy(&trailer, internal_key + (internal_len - TRAILER_SIZE), 8);
    *seq = trailer >> 8;
    *op = (uint8_t)(trailer & 0xFF);
}

struct lsm_db_s {
    char db_dir[512];
    memtable_t *memtable;
    size_t memtable_limit;
    lsm_manifest_t *manifest;
    uint64_t current_seq;
};

/* --------------------------------------------------------------------------
 * Core DB Operations
 * -------------------------------------------------------------------------- */

static void lsm_db_flush(lsm_db_t *db) {
    if (!memtable_first(db->memtable)) return;

    uint64_t file_id = db->manifest->next_file_id++;
    char path[512];
    sprintf(path, "%s/%06llu.sst", db->db_dir, (unsigned long long)file_id);

    sstable_builder_t *builder = sstable_builder_init(path, 10000);
    sstable_meta_t *meta = calloc(1, sizeof(sstable_meta_t));
    meta->file_id = file_id;
    bool is_first = true;

    memtable_row_t *row = memtable_first(db->memtable);
    char ikey[MAX_KEY_SIZE + TRAILER_SIZE];

    while (row) {
        uint32_t klen, vlen;
        const void *k = memtable_row_get_key(row, &klen);
        const void *v = memtable_row_get_val(row, &vlen);

        pack_internal_key(ikey, k, klen, memtable_row_get_seq(row), memtable_row_get_op(row));
        uint32_t iklen = klen + TRAILER_SIZE;

        sstable_builder_add(builder, ikey, iklen, v, vlen);

        if (is_first) { memcpy(meta->min_key, ikey, iklen); meta->min_key_len = iklen; is_first = false; }
        memcpy(meta->max_key, ikey, iklen); meta->max_key_len = iklen;
        meta->num_entries++;

        row = memtable_next(row);
    }

    meta->file_size = sstable_builder_finish(builder);

    lsm_level_t *L0 = &db->manifest->levels[0];
    if (L0->num_files >= L0->files_capacity) {
        L0->files_capacity *= 2;
        L0->files = realloc(L0->files, L0->files_capacity * sizeof(sstable_meta_t*));
    }
    L0->files[L0->num_files++] = meta;

    memtable_destroy(db->memtable);
    db->memtable = memtable_init(1024 * 1024, db->memtable_limit);

    if (L0->num_files >= 4) lsmc_compact_level(db->manifest, 0);
}

lsm_db_t *lsm_db_open(const char *db_directory, size_t mem_limit) {
    lsm_db_t *db = calloc(1, sizeof(lsm_db_t));
    strncpy(db->db_dir, db_directory, sizeof(db->db_dir) - 1);
    db->memtable_limit = mem_limit;
    db->memtable = memtable_init(1024 * 1024, mem_limit);
    db->manifest = lsmc_manifest_init(db_directory);
    db->current_seq = 1;
    return db;
}

void lsm_db_close(lsm_db_t *db) {
    if (!db) return;
    lsm_db_flush(db);
    memtable_destroy(db->memtable);
    free(db);
}

bool lsm_db_put(lsm_db_t *db, const void *key, uint32_t key_len, const void *val, uint32_t val_len) {
    uint64_t seq = ++db->current_seq;
    if (!memtable_put(db->memtable, seq, OP_PUT, key, key_len, val, val_len)) {
        lsm_db_flush(db);
        memtable_put(db->memtable, seq, OP_PUT, key, key_len, val, val_len);
    }
    return true;
}

bool lsm_db_delete(lsm_db_t *db, const void *key, uint32_t key_len) {
    uint64_t seq = ++db->current_seq;
    if (!memtable_put(db->memtable, seq, OP_DELETE, key, key_len, NULL, 0)) {
        lsm_db_flush(db);
        memtable_put(db->memtable, seq, OP_DELETE, key, key_len, NULL, 0);
    }
    return true;
}

void *lsm_db_get(lsm_db_t *db, const void *key, uint32_t key_len, uint32_t *out_val_len) {
    return NULL;
}

/* --------------------------------------------------------------------------
 * Global Merging Iterator (For SQL Table Scans)
 * -------------------------------------------------------------------------- */

typedef struct {
    const char *key; uint32_t klen;
    const char *val; uint32_t vlen;
    uint64_t seq; uint8_t op;
    int src_type; // 0 = memtable, 1 = sstable
    void *src_ptr;
} iter_node_t;

struct lsm_db_iter_s {
    lsm_db_t *db;
    iter_node_t *heap;
    size_t heap_size;
    sstable_reader_t **readers;
    sstable_iter_t **iters;
    size_t num_files;

    char last_user_key[MAX_KEY_SIZE];
    uint32_t last_user_key_len;
    bool is_first;
};

static int iter_cmp(const iter_node_t *a, const iter_node_t *b) {
    uint32_t ulen_a = get_user_key_len(a->klen);
    uint32_t ulen_b = get_user_key_len(b->klen);
    uint32_t min_len = ulen_a < ulen_b ? ulen_a : ulen_b;
    int c = memcmp(a->key, b->key, min_len);
    if (c != 0) return c;
    if (ulen_a != ulen_b) return ulen_a < ulen_b ? -1 : 1;
    if (a->seq > b->seq) return -1;
    if (a->seq < b->seq) return 1;
    return 0;
}

static void iter_push(lsm_db_iter_t *it, iter_node_t node) {
    it->heap[it->heap_size] = node;
    size_t i = it->heap_size++;
    while (i > 0) {
        size_t p = (i - 1) / 2;
        if (iter_cmp(&it->heap[i], &it->heap[p]) >= 0) break;
        iter_node_t tmp = it->heap[i]; it->heap[i] = it->heap[p]; it->heap[p] = tmp;
        i = p;
    }
}

static iter_node_t iter_pop(lsm_db_iter_t *it) {
    iter_node_t root = it->heap[0];
    it->heap[0] = it->heap[--it->heap_size];
    size_t i = 0;
    while (true) {
        size_t l = 2*i + 1, r = 2*i + 2, s = i;
        if (l < it->heap_size && iter_cmp(&it->heap[l], &it->heap[s]) < 0) s = l;
        if (r < it->heap_size && iter_cmp(&it->heap[r], &it->heap[s]) < 0) s = r;
        if (s == i) break;
        iter_node_t tmp = it->heap[i]; it->heap[i] = it->heap[s]; it->heap[s] = tmp;
        i = s;
    }
    return root;
}

static void advance_source(lsm_db_iter_t *it, iter_node_t *n) {
    if (n->src_type == 0) {
        memtable_row_t *nxt = memtable_next((memtable_row_t*)n->src_ptr);
        if (nxt) {
            n->src_ptr = nxt;
            n->key = memtable_row_get_key(nxt, &n->klen);
            n->val = memtable_row_get_val(nxt, &n->vlen);
            n->seq = memtable_row_get_seq(nxt);
            n->op = memtable_row_get_op(nxt);
            n->klen += TRAILER_SIZE;
            iter_push(it, *n);
        }
    } else {
        sstable_iter_t *s_it = (sstable_iter_t*)n->src_ptr;
        if (sstable_iter_next(s_it)) {
            sstable_iter_get_kv(s_it, &n->key, &n->klen, &n->val, &n->vlen);
            unpack_trailer(n->key, n->klen, &n->seq, &n->op);
            iter_push(it, *n);
        }
    }
}

lsm_db_iter_t *lsm_db_iter_init(lsm_db_t *db) {
    lsm_db_iter_t *it = calloc(1, sizeof(lsm_db_iter_t));
    it->db = db;
    it->is_first = true;

    size_t total_files = 0;
    for (int i=0; i<MAX_LEVELS; i++) total_files += db->manifest->levels[i].num_files;

    it->heap = malloc((1 + total_files) * sizeof(iter_node_t));
    it->readers = malloc(total_files * sizeof(sstable_reader_t*));
    it->iters = malloc(total_files * sizeof(sstable_iter_t*));

    /* 1. Add MemTable */
    memtable_row_t *mrow = memtable_first(db->memtable);
    if (mrow) {
        iter_node_t n = {0};
        n.src_type = 0; n.src_ptr = mrow;
        n.key = memtable_row_get_key(mrow, &n.klen);
        n.val = memtable_row_get_val(mrow, &n.vlen);
        n.seq = memtable_row_get_seq(mrow); n.op = memtable_row_get_op(mrow);
        n.klen += TRAILER_SIZE;
        iter_push(it, n);
    }

    /* 2. Add SSTables */
    char path[512];
    for (int lvl=0; lvl<MAX_LEVELS; lvl++) {
        for (size_t f=0; f<db->manifest->levels[lvl].num_files; f++) {
            sprintf(path, "%s/%06llu.sst", db->db_dir, (unsigned long long)db->manifest->levels[lvl].files[f]->file_id);
            sstable_reader_t *r = sstable_reader_init(path);
            sstable_iter_t *s_it = sstable_iter_init(r);
            it->readers[it->num_files] = r;
            it->iters[it->num_files] = s_it;
            it->num_files++;

            if (sstable_iter_next(s_it)) {
                iter_node_t n = {0};
                n.src_type = 1; n.src_ptr = s_it;
                sstable_iter_get_kv(s_it, &n.key, &n.klen, &n.val, &n.vlen);
                unpack_trailer(n.key, n.klen, &n.seq, &n.op);
                iter_push(it, n);
            }
        }
    }
    return it;
}

bool lsm_db_iter_next(lsm_db_iter_t *it, const void **key, uint32_t *klen, const void **val, uint32_t *vlen) {
    while (it->heap_size > 0) {
        /* Peek the top node so we can safely capture its values BEFORE we mutate it */
        iter_node_t top = it->heap[0];

        uint32_t ulen = get_user_key_len(top.klen);
        bool same_key = false;

        if (!it->is_first && it->last_user_key_len == ulen && memcmp(it->last_user_key, top.key, ulen) == 0) {
            same_key = true;
        }

        if (!same_key) {
            memcpy(it->last_user_key, top.key, ulen);
            it->last_user_key_len = ulen;
            it->is_first = false;
        }

        /* Capturing values now guarantees we don't accidentally return the NEXT row in the tree */
        const void *ret_key = top.key;
        const void *ret_val = top.val;
        uint32_t ret_vlen = top.vlen;
        uint8_t ret_op = top.op;

        /* Pop the heap and advance the underlying iterator */
        iter_pop(it);
        advance_source(it, &top);

        /* If it's the newest version of a valid row, yield it to the SQL Engine */
        if (!same_key && ret_op != OP_DELETE) {
            *key = ret_key;
            *klen = ulen;
            *val = ret_val;
            *vlen = ret_vlen;
            return true;
        }
    }
    return false;
}

void lsm_db_iter_destroy(lsm_db_iter_t *it) {
    if (!it) return;
    for (size_t i=0; i<it->num_files; i++) {
        sstable_iter_destroy(it->iters[i]);
        sstable_reader_destroy(it->readers[i]);
    }
    free(it->readers); free(it->iters); free(it->heap); free(it);
}