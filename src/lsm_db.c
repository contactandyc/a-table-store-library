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
#include <unistd.h>
#include <time.h>

#define COMPACTION_TRIGGER 4
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
    uint32_t table_id;
    char db_dir[512];
    lsm_env_t *env;

    int ref_count;

    memtable_t *memtable;
    memtable_t *imm_memtable;

    lsm_manifest_t *manifest;
    uint64_t current_seq;
    uint64_t imm_seq; // High-watermark of the sealed memtable

    pthread_mutex_t state_mutex;
    pthread_mutex_t write_mutex;
    pthread_cond_t stall_cond;

    bool is_flushing;
    bool is_compacting;
};

static void perform_compaction_job(void *arg);

static void perform_flush_job(void *arg) {
    lsm_db_t *db = (lsm_db_t *)arg;

    pthread_mutex_lock(&db->state_mutex);
    memtable_t *imm = db->imm_memtable;
    uint64_t flushed_seq = db->imm_seq;
    pthread_mutex_unlock(&db->state_mutex);

    if (!imm) return;

    size_t freed_mem = memtable_get_memory_usage(imm);

    pthread_mutex_lock(&db->manifest->version_mutex);
    uint64_t file_id = db->manifest->next_file_id++;
    pthread_mutex_unlock(&db->manifest->version_mutex);

    char base_path[512];
    snprintf(base_path, sizeof(base_path), "%s/%06llu", db->db_dir, (unsigned long long)file_id);

    sstable_builder_t *builder = sstable_builder_init(base_path, db->env->router.hot_vfs, FILTER_BLOOM, 10000);
    sstable_meta_t *meta = aml_zalloc(sizeof(sstable_meta_t));
    meta->file_id = file_id;

    bool is_first = true;
    memtable_row_t *row = memtable_first(imm);
    char ikey[MAX_INTERNAL_KEY_SIZE];

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

    sstable_meta_t *added[1] = { meta };
    lsmc_version_edit(db->manifest, -1, 0, NULL, 0, added, 1);

    // FIX: Checkpoint WAL after durability is established
    if (db->env->global_wal && db->env->global_wal->checkpoint) {
        db->env->global_wal->checkpoint(db->env->global_wal, db->table_id, flushed_seq);
    }

    pthread_mutex_lock(&db->state_mutex);
    memtable_release(db->imm_memtable);
    db->imm_memtable = NULL;
    db->is_flushing = false;
    pthread_cond_broadcast(&db->stall_cond);
    pthread_mutex_unlock(&db->state_mutex);

    // FIX: Decrease mem usage and wake up writers stalled by limits
    atomic_fetch_sub(&db->env->global_mem_usage, freed_mem);
    pthread_mutex_lock(&db->env->global_mem_mutex);
    pthread_cond_broadcast(&db->env->global_mem_cond);
    pthread_mutex_unlock(&db->env->global_mem_mutex);

    lsm_version_t *v = lsmc_version_retain(db->manifest);
    int target_lvl = -1;
    for (int i = 0; i < MAX_LEVELS - 1; i++) {
        int limit = (i == 0) ? 4 : (10 * (1 << i));
        if (v->levels[i].num_files >= (size_t)limit) {
            target_lvl = i;
            break;
        }
    }
    lsmc_version_release(db->manifest, v);

    if (target_lvl != -1) {
        pthread_mutex_lock(&db->state_mutex);
        if (!db->is_compacting) {
            db->is_compacting = true;
            lsm_pool_submit(db->env->bg_pool, perform_compaction_job, db, 1);
        }
        pthread_mutex_unlock(&db->state_mutex);
    }
}

static void perform_compaction_job(void *arg) {
    lsm_db_t *db = (lsm_db_t *)arg;

    while (true) {
        lsm_version_t *v = lsmc_version_retain(db->manifest);
        int target_lvl = -1;
        for (int i = 0; i < MAX_LEVELS - 1; i++) {
            int limit = (i == 0) ? 4 : (10 * (1 << i));
            if (v->levels[i].num_files >= (size_t)limit) { target_lvl = i; break; }
        }
        lsmc_version_release(db->manifest, v);

        if (target_lvl == -1) break;
        lsmc_compact_level(db->manifest, target_lvl);
    }

    pthread_mutex_lock(&db->state_mutex);
    db->is_compacting = false;
    pthread_cond_broadcast(&db->stall_cond);
    pthread_mutex_unlock(&db->state_mutex);
}

lsm_db_t *lsm_db_open(lsm_env_t *env, uint32_t table_id, const char *db_directory) {
    lsm_db_t *db = aml_zalloc(sizeof(lsm_db_t));
    db->env = env;
    db->table_id = table_id;

    // Safety against overrun
    strncpy(db->db_dir, db_directory, sizeof(db->db_dir) - 1);
    db->db_dir[sizeof(db->db_dir) - 1] = '\0';

    db->ref_count = 1;

    db->manifest = lsmc_manifest_init(env, table_id, db_directory);

    uint64_t max_seq = lsmc_get_max_sequence(db->manifest);
    db->current_seq = max_seq > 0 ? max_seq : 1;
    db->imm_seq = 0;

    db->memtable = memtable_init();
    atomic_fetch_add(&env->global_mem_usage, memtable_get_memory_usage(db->memtable));
    db->imm_memtable = NULL;

    pthread_mutex_init(&db->state_mutex, NULL);
    pthread_mutex_init(&db->write_mutex, NULL);
    pthread_cond_init(&db->stall_cond, NULL);
    db->is_flushing = false;
    db->is_compacting = false;

    lsm_env_register_db(env, db);
    return db;
}

void lsm_db_retain(lsm_db_t *db) {
    if (db) __atomic_fetch_add(&db->ref_count, 1, __ATOMIC_SEQ_CST);
}

void lsm_db_release(lsm_db_t *db) {
    if (!db) return;
    if (__atomic_sub_fetch(&db->ref_count, 1, __ATOMIC_SEQ_CST) == 0) {
        aml_free(db);
    }
}

void lsm_db_close(lsm_db_t *db) {
    if (!db) return;

    // Step 1: Prevent new calls from the environment
    lsm_env_unregister_db(db->env, db);

    // Step 2: Wait for user queries (puts/gets/iterators) to drain
    while (__atomic_load_n(&db->ref_count, __ATOMIC_SEQ_CST) > 1) {
        usleep(1000);
    }

    // Step 3: Trigger final flush
    pthread_mutex_lock(&db->write_mutex);
    if (memtable_first(db->memtable)) {
        db->imm_memtable = db->memtable;
        db->imm_seq = db->current_seq;
        db->memtable = memtable_init();
        atomic_fetch_add(&db->env->global_mem_usage, memtable_get_memory_usage(db->memtable));

        if (db->env->global_wal && db->env->global_wal->sync) {
            db->env->global_wal->sync(db->env->global_wal);
        }

        pthread_mutex_lock(&db->state_mutex);
        db->is_flushing = true;
        lsm_pool_submit(db->env->bg_pool, perform_flush_job, db, 2);
        pthread_mutex_unlock(&db->state_mutex);
    }
    pthread_mutex_unlock(&db->write_mutex);

    // Step 4: Linearly wait for background jobs to finish
    pthread_mutex_lock(&db->state_mutex);
    while (db->imm_memtable != NULL || db->is_flushing || db->is_compacting) {
        pthread_cond_wait(&db->stall_cond, &db->state_mutex);
    }
    pthread_mutex_unlock(&db->state_mutex);

    // Step 5: Teardown
    atomic_fetch_sub(&db->env->global_mem_usage, memtable_get_memory_usage(db->memtable));
    memtable_release(db->memtable);

    if (db->manifest) {
        lsm_version_t *v = db->manifest->current_version;
        if (v) lsmc_version_release(db->manifest, v);

        if (db->manifest->manifest_writer) {
            db->env->router.hot_vfs->fsync_file(db->manifest->manifest_writer);
            db->env->router.hot_vfs->close_writer(db->manifest->manifest_writer);
        }

        pthread_mutex_destroy(&db->manifest->version_mutex);
        aml_free((void *)db->manifest->db_directory);
        aml_free(db->manifest);
    }

    pthread_mutex_destroy(&db->write_mutex);
    pthread_mutex_destroy(&db->state_mutex);
    pthread_cond_destroy(&db->stall_cond);

    lsm_db_release(db);
}

uint32_t lsm_db_get_table_id(lsm_db_t *db) {
    return db ? db->table_id : 0;
}

bool lsm_db_force_flush(lsm_db_t *db) {
    pthread_mutex_lock(&db->write_mutex);
    pthread_mutex_lock(&db->state_mutex);

    if (db->imm_memtable != NULL || memtable_first(db->memtable) == NULL) {
        pthread_mutex_unlock(&db->state_mutex);
        pthread_mutex_unlock(&db->write_mutex);
        return false;
    }

    db->imm_memtable = db->memtable;
    db->imm_seq = db->current_seq; // FIX: Mark high watermark
    db->memtable = memtable_init();
    atomic_fetch_add(&db->env->global_mem_usage, memtable_get_memory_usage(db->memtable));

    if (db->env->global_wal && db->env->global_wal->sync) {
        db->env->global_wal->sync(db->env->global_wal);
    }

    if (!db->is_flushing) {
        db->is_flushing = true;
        lsm_pool_submit(db->env->bg_pool, perform_flush_job, db, 2);
    }

    pthread_mutex_unlock(&db->state_mutex);
    pthread_mutex_unlock(&db->write_mutex);
    return true;
}

size_t lsm_db_get_active_mem_usage(lsm_db_t *db) {
    pthread_mutex_lock(&db->write_mutex);
    size_t sz = memtable_get_memory_usage(db->memtable);
    pthread_mutex_unlock(&db->write_mutex);
    return sz;
}

void lsm_db_inject_recovery(lsm_db_t *db, uint8_t op, const void *key, uint32_t klen, const void *val, uint32_t vlen) {
    pthread_mutex_lock(&db->write_mutex);
    uint64_t seq = ++db->current_seq;

    size_t mem_before = memtable_get_memory_usage(db->memtable);
    memtable_put(db->memtable, seq, op, key, klen, val, vlen);
    size_t diff = memtable_get_memory_usage(db->memtable) - mem_before;

    pthread_mutex_unlock(&db->write_mutex);
    atomic_fetch_add(&db->env->global_mem_usage, diff);
}

// FIX: Wait on condvar instead of cpu spinning
static bool stall_for_memory(lsm_env_t *env) {
    bool stall_failed = false;
    pthread_mutex_lock(&env->global_mem_mutex);
    while (atomic_load(&env->global_mem_usage) > (size_t)(env->global_mem_limit * 1.5)) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 5; // 5-second max write stall
        int rc = pthread_cond_timedwait(&env->global_mem_cond, &env->global_mem_mutex, &ts);
        if (rc != 0) {
            stall_failed = true;
            break;
        }
    }
    pthread_mutex_unlock(&env->global_mem_mutex);
    return !stall_failed;
}

bool lsm_db_put(lsm_db_t *db, const void *key, uint32_t key_len, const void *val, uint32_t val_len) {
    if (key_len > MAX_KEY_SIZE) return false;

    lsm_db_retain(db); // FIX: Ensure safe db shutdown

    pthread_mutex_lock(&db->write_mutex);
    uint64_t seq = ++db->current_seq;

    if (db->env->global_wal && db->env->global_wal->append) {
        db->env->global_wal->append(db->env->global_wal, db->table_id, OP_PUT, key, key_len, val, val_len);
    }

    size_t mem_before = memtable_get_memory_usage(db->memtable);
    memtable_put(db->memtable, seq, OP_PUT, key, key_len, val, val_len);
    size_t diff = memtable_get_memory_usage(db->memtable) - mem_before;

    pthread_mutex_unlock(&db->write_mutex);

    size_t current_usage = atomic_fetch_add(&db->env->global_mem_usage, diff) + diff;

    if (current_usage > db->env->global_mem_limit) {
        lsm_db_force_flush(db);
    }

    bool success = stall_for_memory(db->env);

    lsm_db_release(db);
    return success;
}

bool lsm_db_delete(lsm_db_t *db, const void *key, uint32_t key_len) {
    if (key_len > MAX_KEY_SIZE) return false;

    lsm_db_retain(db);

    pthread_mutex_lock(&db->write_mutex);
    uint64_t seq = ++db->current_seq;

    if (db->env->global_wal && db->env->global_wal->append) {
        db->env->global_wal->append(db->env->global_wal, db->table_id, OP_DELETE, key, key_len, NULL, 0);
    }

    size_t mem_before = memtable_get_memory_usage(db->memtable);
    memtable_put(db->memtable, seq, OP_DELETE, key, key_len, NULL, 0);
    size_t diff = memtable_get_memory_usage(db->memtable) - mem_before;
    pthread_mutex_unlock(&db->write_mutex);

    size_t current_usage = atomic_fetch_add(&db->env->global_mem_usage, diff) + diff;

    if (current_usage > db->env->global_mem_limit) {
        lsm_db_force_flush(db);
    }

    bool success = stall_for_memory(db->env);

    lsm_db_release(db);
    return success;
}

void *lsm_db_get(lsm_db_t *db, const void *key, uint32_t key_len, uint32_t *out_val_len) {
    lsm_db_retain(db); // FIX: Ensure db is safe during whole operation

    if (key_len > MAX_KEY_SIZE) {
        lsm_db_release(db);
        return NULL;
    }
    if (out_val_len) *out_val_len = 0;

    pthread_mutex_lock(&db->write_mutex);
    pthread_mutex_lock(&db->state_mutex);

    memtable_t *active = db->memtable;
    memtable_retain(active);
    memtable_t *imm = db->imm_memtable;
    if (imm) memtable_retain(imm);

    lsm_version_t *v = lsmc_version_retain(db->manifest);

    pthread_mutex_unlock(&db->state_mutex);
    pthread_mutex_unlock(&db->write_mutex);

    bool is_deleted = false;
    void *ret_val = NULL;

    const void *val = memtable_get(active, key, key_len, UINT64_MAX, out_val_len, &is_deleted);
    if (is_deleted) goto cleanup;
    if (val) {
        ret_val = aml_malloc(*out_val_len);
        memcpy(ret_val, val, *out_val_len);
        goto cleanup;
    }

    if (imm) {
        val = memtable_get(imm, key, key_len, UINT64_MAX, out_val_len, &is_deleted);
        if (is_deleted) goto cleanup;
        if (val) {
            ret_val = aml_malloc(*out_val_len);
            memcpy(ret_val, val, *out_val_len);
            goto cleanup;
        }
    }

    char base_path[512];
    for (int lvl = 0; lvl < MAX_LEVELS; lvl++) {
        if (v->levels[lvl].num_files == 0) continue;

        if (lvl == 0) {
            for (int f = v->levels[lvl].num_files - 1; f >= 0; f--) {
                sstable_meta_t *meta = v->levels[lvl].files[f];
                uint32_t umax = meta->max_key_len > INTERNAL_KEY_TRAILER_SIZE ? meta->max_key_len - INTERNAL_KEY_TRAILER_SIZE : 0;
                uint32_t umin = meta->min_key_len > INTERNAL_KEY_TRAILER_SIZE ? meta->min_key_len - INTERNAL_KEY_TRAILER_SIZE : 0;

                uint32_t min_cmp_len = key_len < umin ? key_len : umin;
                int cmp_min = memcmp(key, meta->min_key, min_cmp_len);
                if (cmp_min == 0) cmp_min = (key_len < umin) ? -1 : (key_len > umin ? 1 : 0);
                if (cmp_min < 0) continue;

                uint32_t max_cmp_len = key_len < umax ? key_len : umax;
                int cmp_max = memcmp(key, meta->max_key, max_cmp_len);
                if (cmp_max == 0) cmp_max = (key_len < umax) ? -1 : (key_len > umax ? 1 : 0);
                if (cmp_max > 0) continue;

                snprintf(base_path, sizeof(base_path), "%s/%06llu", db->db_dir, (unsigned long long)meta->file_id);
                lsm_storage_backend_t *vfs = db->env->router.hot_vfs;
                sstable_reader_t *r = sstable_reader_init(base_path, vfs, db->env, db->table_id, meta->file_id);
                if (!r) continue;

                void *disk_val = NULL;
                int status = sstable_reader_get(r, key, key_len, &disk_val, out_val_len);
                sstable_reader_destroy(r);

                if (status == 1) {
                    ret_val = disk_val;
                    goto cleanup;
                } else if (status == -1) {
                    goto cleanup;
                }
            }
        } else {
            int left = 0, right = v->levels[lvl].num_files - 1, target_idx = -1;
            while(left <= right) {
                int mid = left + (right - left)/2;
                sstable_meta_t *meta = v->levels[lvl].files[mid];

                uint32_t umax = meta->max_key_len > INTERNAL_KEY_TRAILER_SIZE ? meta->max_key_len - INTERNAL_KEY_TRAILER_SIZE : 0;
                uint32_t umin = meta->min_key_len > INTERNAL_KEY_TRAILER_SIZE ? meta->min_key_len - INTERNAL_KEY_TRAILER_SIZE : 0;

                uint32_t min_cmp_len = key_len < umin ? key_len : umin;
                int cmp_min = memcmp(key, meta->min_key, min_cmp_len);
                if (cmp_min == 0) cmp_min = (key_len < umin) ? -1 : (key_len > umin ? 1 : 0);

                if (cmp_min < 0) {
                    right = mid - 1;
                    continue;
                }

                uint32_t max_cmp_len = key_len < umax ? key_len : umax;
                int cmp_max = memcmp(key, meta->max_key, max_cmp_len);
                if (cmp_max == 0) cmp_max = (key_len < umax) ? -1 : (key_len > umax ? 1 : 0);

                if (cmp_max > 0) {
                    left = mid + 1;
                    continue;
                }

                target_idx = mid;
                break;
            }

            if (target_idx != -1) {
                sstable_meta_t *meta = v->levels[lvl].files[target_idx];
                snprintf(base_path, sizeof(base_path), "%s/%06llu", db->db_dir, (unsigned long long)meta->file_id);
                lsm_storage_backend_t *vfs = (lvl >= db->env->router.cold_storage_start_level) ? db->env->router.cold_vfs : db->env->router.hot_vfs;
                sstable_reader_t *r = sstable_reader_init(base_path, vfs, db->env, db->table_id, meta->file_id);
                if (r) {
                    void *disk_val = NULL;
                    int status = sstable_reader_get(r, key, key_len, &disk_val, out_val_len);
                    sstable_reader_destroy(r);
                    if (status == 1) { ret_val = disk_val; goto cleanup; }
                    else if (status == -1) goto cleanup;
                }
            }
        }
    }

cleanup:
    memtable_release(active);
    if (imm) memtable_release(imm);
    lsmc_version_release(db->manifest, v);

    lsm_db_release(db); // Release the wrap lock
    return ret_val;
}

typedef struct {
    const char *key; uint32_t klen;
    const char *val; uint32_t vlen;
    uint64_t seq; uint8_t op;
    int src_type;
    void *src_ptr;
} iter_node_t;

struct lsm_db_iter_s {
    lsm_db_t *db;
    lsm_version_t *version;
    memtable_t *active;
    memtable_t *imm;

    iter_node_t *heap;
    size_t heap_size;
    sstable_reader_t **readers;
    sstable_iter_t **iters;
    size_t num_files;

    char last_user_key[MAX_INTERNAL_KEY_SIZE];
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
            n->klen += INTERNAL_KEY_TRAILER_SIZE;
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
    lsm_db_retain(db); // FIX: Ensure DB survives iteration

    lsm_db_iter_t *it = aml_zalloc(sizeof(lsm_db_iter_t));
    it->db = db;

    pthread_mutex_lock(&db->write_mutex);
    pthread_mutex_lock(&db->state_mutex);

    it->active = db->memtable; memtable_retain(it->active);
    it->imm = db->imm_memtable; if (it->imm) memtable_retain(it->imm);
    it->version = lsmc_version_retain(db->manifest);

    pthread_mutex_unlock(&db->state_mutex);
    pthread_mutex_unlock(&db->write_mutex);

    it->is_first = true;
    it->ret_val_cap = 4096;
    it->ret_val_buf = aml_malloc(it->ret_val_cap);

    size_t total_files = 0;
    for (int i=0; i<MAX_LEVELS; i++) total_files += it->version->levels[i].num_files;

    it->heap = aml_malloc((2 + total_files) * sizeof(iter_node_t));
    it->readers = aml_malloc(total_files * sizeof(sstable_reader_t*));
    it->iters = aml_malloc(total_files * sizeof(sstable_iter_t*));

    memtable_row_t *mrow = memtable_first(it->active);
    if (mrow) {
        iter_node_t n = {0}; n.src_type = 0; n.src_ptr = mrow;
        n.key = memtable_row_get_key(mrow, &n.klen);
        n.val = memtable_row_get_val(mrow, &n.vlen);
        n.seq = memtable_row_get_seq(mrow); n.op = memtable_row_get_op(mrow);
        n.klen += INTERNAL_KEY_TRAILER_SIZE; iter_push(it, n);
    }

    if (it->imm) {
        memtable_row_t *imrow = memtable_first(it->imm);
        if (imrow) {
            iter_node_t n = {0}; n.src_type = 0; n.src_ptr = imrow;
            n.key = memtable_row_get_key(imrow, &n.klen);
            n.val = memtable_row_get_val(imrow, &n.vlen);
            n.seq = memtable_row_get_seq(imrow); n.op = memtable_row_get_op(imrow);
            n.klen += INTERNAL_KEY_TRAILER_SIZE; iter_push(it, n);
        }
    }

    char base_path[512];
    for (int lvl=0; lvl<MAX_LEVELS; lvl++) {
        for (size_t f=0; f<it->version->levels[lvl].num_files; f++) {
            snprintf(base_path, sizeof(base_path), "%s/%06llu", db->db_dir, (unsigned long long)it->version->levels[lvl].files[f]->file_id);
            lsm_storage_backend_t *vfs = (lvl >= db->env->router.cold_storage_start_level) ? db->env->router.cold_vfs : db->env->router.hot_vfs;
            sstable_reader_t *r = sstable_reader_init(base_path, vfs, db->env, db->table_id, it->version->levels[lvl].files[f]->file_id);

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
            if (ret_vlen > 0) memcpy(it->ret_val_buf, top.val, ret_vlen);

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
    memtable_release(it->active);
    if (it->imm) memtable_release(it->imm);
    lsmc_version_release(it->db->manifest, it->version);

    aml_free(it->ret_val_buf);
    aml_free(it->readers); aml_free(it->iters); aml_free(it->heap);

    lsm_db_release(it->db); // FIX: Match retain count
    aml_free(it);
}
