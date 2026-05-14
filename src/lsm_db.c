// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-table-store-library/lsm_db.h"
#include "a-table-store-library/memtable.h"
#include "a-table-store-library/lsm_compaction.h"
#include "a-table-store-library/sstable_builder.h"
#include "a-table-store-library/sstable_reader.h"
#include "a-memory-library/aml_alloc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#define COMPACTION_TRIGGER 4 // Safe to set low now that compaction has its own thread!

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

/* --------------------------------------------------------------------------
 * Core DB Structures with 3-Thread Concurrency
 * -------------------------------------------------------------------------- */

struct lsm_db_s {
    char db_dir[512];

    memtable_t *memtable;
    memtable_t *imm_memtable;
    size_t memtable_limit;

    lsm_manifest_t *manifest;
    uint64_t current_seq;

    // --- Tri-Thread Architecture ---
    pthread_t flusher_thread;
    pthread_t compactor_thread;

    pthread_mutex_t bg_mutex;
    pthread_cond_t flush_cond;     // Wakes up Flusher
    pthread_cond_t imm_cond;       // Wakes up Main thread
    pthread_cond_t compact_cond;   // Wakes up Compactor
    bool bg_shutdown;
};

/* --------------------------------------------------------------------------
 * Thread 2: The Flusher (Fast I/O)
 * -------------------------------------------------------------------------- */

static void *flush_worker(void *arg) {
    lsm_db_t *db = (lsm_db_t *)arg;

    while (true) {
        pthread_mutex_lock(&db->bg_mutex);
        while (!db->imm_memtable && !db->bg_shutdown) {
            pthread_cond_wait(&db->flush_cond, &db->bg_mutex);
        }

        memtable_t *imm = db->imm_memtable;
        bool shutdown = db->bg_shutdown;
        pthread_mutex_unlock(&db->bg_mutex);

        if (imm) {
            // 1. Get new File ID safely
            pthread_mutex_lock(&db->manifest->version_mutex);
            uint64_t file_id = db->manifest->next_file_id++;
            pthread_mutex_unlock(&db->manifest->version_mutex);

            char path[512];
            sprintf(path, "%s/%06llu.sst", db->db_dir, (unsigned long long)file_id);

            sstable_builder_t *builder = sstable_builder_init(path, 10000);
            sstable_meta_t *meta = aml_zalloc(sizeof(sstable_meta_t));
            meta->file_id = file_id;

            bool is_first = true;
            memtable_row_t *row = memtable_first(imm);
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

            // 2. Commit the new L0 file to the MVCC Version Set
            sstable_meta_t *added[1] = { meta };
            lsmc_version_edit(db->manifest, -1, 0, NULL, 0, added, 1);

            // 3. Clear Immutable MemTable and wake Main Thread
            pthread_mutex_lock(&db->bg_mutex);
            memtable_destroy(db->imm_memtable);
            db->imm_memtable = NULL;
            pthread_cond_signal(&db->imm_cond);

            // 4. Check if Compactor needs to wake up!
            lsm_version_t *v = lsmc_version_retain(db->manifest);
            if (v->levels[0].num_files >= COMPACTION_TRIGGER) {
                pthread_cond_signal(&db->compact_cond);
            }
            lsmc_version_release(db->manifest, v);

            pthread_mutex_unlock(&db->bg_mutex);
        }

        if (shutdown && !db->imm_memtable) break;
    }
    return NULL;
}

/* --------------------------------------------------------------------------
 * Thread 3: The Compactor (Heavy CPU/IO)
 * -------------------------------------------------------------------------- */

static void *compactor_worker(void *arg) {
    lsm_db_t *db = (lsm_db_t *)arg;

    while (true) {
        pthread_mutex_lock(&db->bg_mutex);
        while (!db->bg_shutdown) {
            lsm_version_t *v = lsmc_version_retain(db->manifest);
            bool needs_work = (v->levels[0].num_files >= COMPACTION_TRIGGER);
            lsmc_version_release(db->manifest, v);

            if (needs_work) break;
            pthread_cond_wait(&db->compact_cond, &db->bg_mutex);
        }
        bool shutdown = db->bg_shutdown;
        pthread_mutex_unlock(&db->bg_mutex);

        // Run compactions sequentially until the L0 threshold is satisfied
        while (true) {
            lsm_version_t *v = lsmc_version_retain(db->manifest);
            bool keep_going = (v->levels[0].num_files >= COMPACTION_TRIGGER);
            lsmc_version_release(db->manifest, v);

            if (!keep_going) break;
            lsmc_compact_level(db->manifest, 0); // Lock-free execution!
        }

        if (shutdown) break;
    }
    return NULL;
}

/* --------------------------------------------------------------------------
 * Core DB Operations
 * -------------------------------------------------------------------------- */

lsm_db_t *lsm_db_open(const char *db_directory, size_t mem_limit) {
    lsm_db_t *db = aml_zalloc(sizeof(lsm_db_t));
    strncpy(db->db_dir, db_directory, sizeof(db->db_dir) - 1);
    db->memtable_limit = mem_limit;
    db->memtable = memtable_init(mem_limit, mem_limit);
    db->imm_memtable = NULL;
    db->manifest = lsmc_manifest_init(db_directory);
    db->current_seq = 1;

    pthread_mutex_init(&db->bg_mutex, NULL);
    pthread_cond_init(&db->flush_cond, NULL);
    pthread_cond_init(&db->imm_cond, NULL);
    pthread_cond_init(&db->compact_cond, NULL);
    db->bg_shutdown = false;

    // Launch the specialized thread pools
    pthread_create(&db->flusher_thread, NULL, flush_worker, db);
    pthread_create(&db->compactor_thread, NULL, compactor_worker, db);

    return db;
}

void lsm_db_close(lsm_db_t *db) {
    if (!db) return;

    pthread_mutex_lock(&db->bg_mutex);

    // Drain existing work
    while (db->imm_memtable != NULL) {
        pthread_cond_wait(&db->imm_cond, &db->bg_mutex);
    }

    // Push the active MemTable into the background thread one last time
    if (memtable_first(db->memtable)) {
        db->imm_memtable = db->memtable;
        db->memtable = memtable_init(db->memtable_limit, db->memtable_limit);
        pthread_cond_signal(&db->flush_cond);

        while (db->imm_memtable != NULL) {
            pthread_cond_wait(&db->imm_cond, &db->bg_mutex);
        }
    }

    db->bg_shutdown = true;
    pthread_cond_broadcast(&db->flush_cond);
    pthread_cond_broadcast(&db->compact_cond);
    pthread_mutex_unlock(&db->bg_mutex);

    // Wait for both threads to safely exit
    pthread_join(db->flusher_thread, NULL);
    pthread_join(db->compactor_thread, NULL);

    memtable_destroy(db->memtable);

    // Force release the remaining version set to physically delete files/metadata
    if (db->manifest) {
        lsm_version_t *v = db->manifest->current_version;
        if (v) lsmc_version_release(db->manifest, v);

        pthread_mutex_destroy(&db->manifest->version_mutex);
        aml_free((void *)db->manifest->db_directory);
        aml_free(db->manifest);
    }

    pthread_mutex_destroy(&db->bg_mutex);
    pthread_cond_destroy(&db->flush_cond);
    pthread_cond_destroy(&db->imm_cond);
    pthread_cond_destroy(&db->compact_cond);

    aml_free(db);
}

bool lsm_db_put(lsm_db_t *db, const void *key, uint32_t key_len, const void *val, uint32_t val_len) {
    uint64_t seq = ++db->current_seq;

    if (!memtable_put(db->memtable, seq, OP_PUT, key, key_len, val, val_len)) {
        pthread_mutex_lock(&db->bg_mutex);
        while (db->imm_memtable != NULL) {
            pthread_cond_wait(&db->imm_cond, &db->bg_mutex);
        }

        db->imm_memtable = db->memtable;
        db->memtable = memtable_init(db->memtable_limit, db->memtable_limit);
        pthread_cond_signal(&db->flush_cond);
        pthread_mutex_unlock(&db->bg_mutex);

        memtable_put(db->memtable, seq, OP_PUT, key, key_len, val, val_len);
    }
    return true;
}

bool lsm_db_delete(lsm_db_t *db, const void *key, uint32_t key_len) {
    uint64_t seq = ++db->current_seq;

    if (!memtable_put(db->memtable, seq, OP_DELETE, key, key_len, NULL, 0)) {
        pthread_mutex_lock(&db->bg_mutex);
        while (db->imm_memtable != NULL) {
            pthread_cond_wait(&db->imm_cond, &db->bg_mutex);
        }
        db->imm_memtable = db->memtable;
        db->memtable = memtable_init(db->memtable_limit, db->memtable_limit);
        pthread_cond_signal(&db->flush_cond);
        pthread_mutex_unlock(&db->bg_mutex);

        memtable_put(db->memtable, seq, OP_DELETE, key, key_len, NULL, 0);
    }
    return true;
}

void *lsm_db_get(lsm_db_t *db, const void *key, uint32_t key_len, uint32_t *out_val_len) {
    bool is_deleted = false;

    // 1. Check Active MemTable
    const void *val = memtable_get(db->memtable, key, key_len, UINT64_MAX, out_val_len, &is_deleted);
    if (is_deleted) return NULL;
    if (val) {
        void *ret = aml_malloc(*out_val_len);
        memcpy(ret, val, *out_val_len);
        return ret;
    }

    // 2. Check Immutable MemTable
    pthread_mutex_lock(&db->bg_mutex);
    if (db->imm_memtable) {
        val = memtable_get(db->imm_memtable, key, key_len, UINT64_MAX, out_val_len, &is_deleted);
        if (is_deleted) { pthread_mutex_unlock(&db->bg_mutex); return NULL; }
        if (val) {
            void *ret = aml_malloc(*out_val_len);
            memcpy(ret, val, *out_val_len);
            pthread_mutex_unlock(&db->bg_mutex);
            return ret;
        }
    }
    pthread_mutex_unlock(&db->bg_mutex);

    // 3. Scan SSTables lock-free via Version Pinning
    lsm_version_t *v = lsmc_version_retain(db->manifest);
    char path[512];

    for (int lvl = 0; lvl < MAX_LEVELS; lvl++) {
        for (int f = v->levels[lvl].num_files - 1; f >= 0; f--) {
            sstable_meta_t *meta = v->levels[lvl].files[f];

            uint32_t umax = get_user_key_len(meta->max_key_len);
            uint32_t umin = get_user_key_len(meta->min_key_len);
            if (memcmp(key, meta->min_key, key_len < umin ? key_len : umin) < 0) continue;
            if (memcmp(key, meta->max_key, key_len < umax ? key_len : umax) > 0) continue;

            sprintf(path, "%s/%06llu.sst", db->db_dir, (unsigned long long)meta->file_id);
            sstable_reader_t *r = sstable_reader_init(path);
            if (!r) continue;

            void *disk_val = sstable_reader_get(r, key, key_len, out_val_len);
            sstable_reader_destroy(r);

            if (disk_val) {
                lsmc_version_release(db->manifest, v);
                return disk_val;
            }
        }
    }
    lsmc_version_release(db->manifest, v);
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
    lsm_version_t *version; // Pin the MVCC state!

    iter_node_t *heap;
    size_t heap_size;
    sstable_reader_t **readers;
    sstable_iter_t **iters;
    size_t num_files;

    char last_user_key[MAX_KEY_SIZE];
    uint32_t last_user_key_len;
    bool is_first;

    uint8_t *ret_val_buf;
    size_t ret_val_cap;
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
    // Await flush so the scan sees everything that was just inserted
    pthread_mutex_lock(&db->bg_mutex);
    while (db->imm_memtable != NULL) {
        pthread_cond_wait(&db->imm_cond, &db->bg_mutex);
    }
    pthread_mutex_unlock(&db->bg_mutex);

    lsm_db_iter_t *it = aml_zalloc(sizeof(lsm_db_iter_t));
    it->db = db;
    it->version = lsmc_version_retain(db->manifest); // Pin the DB state!

    it->is_first = true;
    it->ret_val_cap = 4096;
    it->ret_val_buf = aml_malloc(it->ret_val_cap);

    size_t total_files = 0;
    for (int i=0; i<MAX_LEVELS; i++) total_files += it->version->levels[i].num_files;

    it->heap = aml_malloc((1 + total_files) * sizeof(iter_node_t));
    it->readers = aml_malloc(total_files * sizeof(sstable_reader_t*));
    it->iters = aml_malloc(total_files * sizeof(sstable_iter_t*));

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

    /* 2. Add SSTables safely from our Pinned Version */
    char path[512];
    for (int lvl=0; lvl<MAX_LEVELS; lvl++) {
        for (size_t f=0; f<it->version->levels[lvl].num_files; f++) {
            sprintf(path, "%s/%06llu.sst", db->db_dir, (unsigned long long)it->version->levels[lvl].files[f]->file_id);
            sstable_reader_t *r = sstable_reader_init(path);

            if (!r) continue;

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

        uint32_t ret_vlen = top.vlen;
        uint8_t ret_op = top.op;

        if (!same_key && ret_op != OP_DELETE) {
            if (it->ret_val_cap < ret_vlen) {
                it->ret_val_cap = ret_vlen * 2 + 1024;
                it->ret_val_buf = aml_realloc(it->ret_val_buf, it->ret_val_cap);
            }
            if (ret_vlen > 0) {
                memcpy(it->ret_val_buf, top.val, ret_vlen);
            }

            iter_pop(it);
            advance_source(it, &top);

            *key = it->last_user_key;
            *klen = ulen;
            *val = it->ret_val_buf;
            *vlen = ret_vlen;
            return true;
        } else {
            iter_pop(it);
            advance_source(it, &top);
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

    // Release the pin! If we were the last reader on an old version, this drops the files!
    lsmc_version_release(it->db->manifest, it->version);

    aml_free(it->ret_val_buf);
    aml_free(it->readers); aml_free(it->iters); aml_free(it->heap); aml_free(it);
}
