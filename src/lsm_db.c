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
    uint8_t *t = (uint8_t *)(dst + ulen);
    for (int i = 0; i < 8; i++) t[i] = (trailer >> (i * 8)) & 0xFF;
}

static inline uint32_t get_user_key_len(uint32_t internal_len) {
    return internal_len > TRAILER_SIZE ? internal_len - TRAILER_SIZE : 0;
}

static inline void unpack_trailer(const char *internal_key, uint32_t internal_len, uint64_t *seq, uint8_t *op) {
    if (internal_len < TRAILER_SIZE) { *seq = 0; *op = 0; return; }
    const uint8_t *t = (const uint8_t *)(internal_key + (internal_len - TRAILER_SIZE));
    uint64_t trailer = 0;
    for (int i = 0; i < 8; i++) trailer |= ((uint64_t)t[i]) << (i * 8);
    *seq = trailer >> 8;
    *op = (uint8_t)(trailer & 0xFF);
}

static inline void encode_u32_le_db(uint8_t *dst, uint32_t v) {
    dst[0] = v & 0xFF; dst[1] = (v >> 8) & 0xFF; dst[2] = (v >> 16) & 0xFF; dst[3] = (v >> 24) & 0xFF;
}

typedef struct lsm_writer_s {
    lsm_write_batch_t *batch;
    bool done;
    bool success;
    struct lsm_writer_s *next;
    pthread_cond_t cv;
} lsm_writer_t;

struct lsm_db_s {
    uint32_t table_id;
    char db_dir[512];
    lsm_env_t *env;

    int owner_refs;
    int active_ops;
    int active_jobs;
    bool closing;

    memtable_t *memtable;
    memtable_t *imm_memtable;

    lsm_manifest_t *manifest;
    uint64_t current_seq;
    uint64_t imm_seq;

    uint64_t current_wal_lsn;
    uint64_t imm_wal_lsn;

    uint64_t *snapshots;
    size_t num_snapshots;
    size_t snapshots_cap;

    pthread_mutex_t state_mutex;
    pthread_mutex_t write_mutex;
    pthread_mutex_t queue_mutex;
    pthread_cond_t stall_cond;

    uint64_t commit_ticket_head;
    uint64_t commit_ticket_tail;
    pthread_cond_t commit_cv;

    lsm_writer_t *writer_queue_head;
    lsm_writer_t *writer_queue_tail;

    bool is_flushing;
    bool is_compacting;
};

// --- Lifecycle Operation Leases ---

static bool db_try_acquire_op(lsm_db_t *db) {
    pthread_mutex_lock(&db->state_mutex);
    if (db->closing) {
        pthread_mutex_unlock(&db->state_mutex);
        return false;
    }
    db->active_ops++;
    pthread_mutex_unlock(&db->state_mutex);
    return true;
}

static void db_release_op(lsm_db_t *db) {
    pthread_mutex_lock(&db->state_mutex);
    db->active_ops--;
    if (db->closing && db->active_ops == 0 && db->active_jobs == 0) {
        pthread_cond_broadcast(&db->stall_cond);
    }
    pthread_mutex_unlock(&db->state_mutex);
}

static void db_release_job(lsm_db_t *db) {
    pthread_mutex_lock(&db->state_mutex);
    db->active_jobs--;
    if (db->closing && db->active_ops == 0 && db->active_jobs == 0) {
        pthread_cond_broadcast(&db->stall_cond);
    }
    pthread_mutex_unlock(&db->state_mutex);
}

// --- Snapshots ---
uint64_t lsm_db_take_snapshot(lsm_db_t *db) {
    if (!db_try_acquire_op(db)) return 0;
    pthread_mutex_lock(&db->write_mutex);
    uint64_t seq = db->current_seq;
    if (db->num_snapshots == db->snapshots_cap) {
        db->snapshots_cap = db->snapshots_cap == 0 ? 8 : db->snapshots_cap * 2;
        db->snapshots = aml_realloc(db->snapshots, db->snapshots_cap * sizeof(uint64_t));
    }
    db->snapshots[db->num_snapshots++] = seq;
    pthread_mutex_unlock(&db->write_mutex);
    db_release_op(db);
    return seq;
}

void lsm_db_release_snapshot(lsm_db_t *db, uint64_t seq) {
    if (!db_try_acquire_op(db)) return;
    pthread_mutex_lock(&db->write_mutex);
    for (size_t i = 0; i < db->num_snapshots; i++) {
        if (db->snapshots[i] == seq) {
            db->snapshots[i] = db->snapshots[--db->num_snapshots];
            break;
        }
    }
    pthread_mutex_unlock(&db->write_mutex);
    db_release_op(db);
}

static uint64_t get_oldest_snapshot(lsm_db_t *db) {
    pthread_mutex_lock(&db->write_mutex);
    uint64_t oldest = db->current_seq;
    for (size_t i = 0; i < db->num_snapshots; i++) {
        if (db->snapshots[i] < oldest) oldest = db->snapshots[i];
    }
    pthread_mutex_unlock(&db->write_mutex);
    return oldest;
}

// --- Background Jobs ---
static void perform_compaction_job(void *arg);

static void perform_flush_job(void *arg) {
    lsm_db_t *db = (lsm_db_t *)arg;

    pthread_mutex_lock(&db->state_mutex);
    memtable_t *imm = db->imm_memtable;
    if (imm) memtable_retain(imm);
    uint64_t flushed_lsn = db->imm_wal_lsn;
    pthread_mutex_unlock(&db->state_mutex);

    if (!imm) {
        db_release_job(db);
        lsm_db_release(db);
        return;
    }

    size_t freed_mem = memtable_get_memory_usage(imm);
    size_t est_elements = (freed_mem / 128) + 1024;

    pthread_mutex_lock(&db->manifest->version_mutex);
    uint64_t file_id = db->manifest->next_file_id++;
    pthread_mutex_unlock(&db->manifest->version_mutex);

    char base_path[512];
    snprintf(base_path, sizeof(base_path), "%s/%06llu", db->db_dir, (unsigned long long)file_id);

    sstable_builder_t *builder = sstable_builder_init(base_path, db->env->router.hot_vfs, FILTER_BLOOM, est_elements);

    // [Phase 8] Safe Initialization Failsafe
    if (!builder) {
        pthread_mutex_lock(&db->state_mutex);
        db->is_flushing = false;
        pthread_cond_broadcast(&db->stall_cond);
        pthread_mutex_unlock(&db->state_mutex);

        if (imm) memtable_release(imm);
        db_release_job(db);
        lsm_db_release(db);
        return;
    }

    sstable_meta_t *meta = aml_zalloc(sizeof(sstable_meta_t));
    meta->file_id = file_id;
    pthread_mutex_init(&meta->reader_mutex, NULL);
    meta->cached_reader = NULL;

    bool is_first = true;
    memtable_row_t *row = memtable_first(imm);
    char ikey[MAX_INTERNAL_KEY_SIZE];
    uint64_t block_max_seq = 0;
    bool io_error = false;

    while (row) {
        uint32_t klen, vlen;
        const void *k = memtable_row_get_key(row, &klen);
        const void *v = memtable_row_get_val(row, &vlen);

        uint64_t seq = memtable_row_get_seq(row);
        if (seq > block_max_seq) block_max_seq = seq;

        pack_internal_key(ikey, k, klen, seq, memtable_row_get_op(row));
        uint32_t iklen = klen + TRAILER_SIZE;

        if (!sstable_builder_add(builder, ikey, iklen, v, vlen)) {
            io_error = true;
            break;
        }

        if (is_first) {
            meta->min_key = aml_malloc(iklen);
            memcpy(meta->min_key, ikey, iklen);
            meta->min_key_len = iklen;
            is_first = false;
        }
        if (meta->max_key) aml_free(meta->max_key);
        meta->max_key = aml_malloc(iklen);
        memcpy(meta->max_key, ikey, iklen);
        meta->max_key_len = iklen;

        meta->num_entries++;
        row = memtable_next(row);
    }

    meta->max_seq = block_max_seq;

    if (!io_error) {
        meta->file_size = sstable_builder_finish(builder);
        if (meta->file_size == 0) io_error = true;
    } else {
        sstable_builder_abort(builder);
    }

    if (io_error) {
        if (meta->min_key) aml_free(meta->min_key);
        if (meta->max_key) aml_free(meta->max_key);
        pthread_mutex_destroy(&meta->reader_mutex);
        aml_free(meta);

        pthread_mutex_lock(&db->state_mutex);
        db->is_flushing = false;
        pthread_cond_broadcast(&db->stall_cond);
        pthread_mutex_unlock(&db->state_mutex);

        if (imm) memtable_release(imm);
        db_release_job(db);
        lsm_db_release(db);
        return;
    }

    sstable_meta_t *added[1] = { meta };

    if (!lsmc_version_edit(db->manifest, -1, 0, NULL, 0, added, 1)) {
        char dp[520]; snprintf(dp, 520, "%s.data", base_path); db->env->router.hot_vfs->delete_file(dp);
        char mp[520]; snprintf(mp, 520, "%s.meta", base_path); db->env->router.hot_vfs->delete_file(mp);

        if (meta->min_key) aml_free(meta->min_key);
        if (meta->max_key) aml_free(meta->max_key);
        pthread_mutex_destroy(&meta->reader_mutex);
        aml_free(meta);

        pthread_mutex_lock(&db->state_mutex);
        db->is_flushing = false;
        pthread_cond_broadcast(&db->stall_cond);
        pthread_mutex_unlock(&db->state_mutex);

        if (imm) memtable_release(imm);
        db_release_job(db);
        lsm_db_release(db);
        return;
    }

    if (db->env->global_wal && db->env->global_wal->checkpoint && flushed_lsn > 0) {
        db->env->global_wal->checkpoint(db->env->global_wal, db->table_id, flushed_lsn);
    }

    pthread_mutex_lock(&db->state_mutex);
    if (db->imm_memtable == imm) {
        memtable_release(db->imm_memtable);
        db->imm_memtable = NULL;
    }
    db->is_flushing = false;
    pthread_cond_broadcast(&db->stall_cond);
    pthread_mutex_unlock(&db->state_mutex);

    atomic_fetch_sub(&db->env->global_mem_usage, freed_mem);
    pthread_mutex_lock(&db->env->global_mem_mutex);
    pthread_cond_broadcast(&db->env->global_mem_cond);
    pthread_mutex_unlock(&db->env->global_mem_mutex);

    if (imm) memtable_release(imm);

    lsm_version_t *v = lsmc_version_retain(db->manifest);
    int target_lvl = -1;
    if (v->compaction_score >= 1.0) {
        target_lvl = v->compaction_level;
    }
    lsmc_version_release(db->manifest, v);

    if (target_lvl != -1) {
        bool need_compaction = false;
        pthread_mutex_lock(&db->state_mutex);
        if (!db->is_compacting) {
            db->is_compacting = true;
            db->active_jobs++;
            db->owner_refs++;
            need_compaction = true;
        }
        pthread_mutex_unlock(&db->state_mutex);

        // Submit completely lock-free
        if (need_compaction) {
            if (!lsm_pool_submit(db->env->bg_pool, perform_compaction_job, db, JOB_PRIORITY_HIGH)) {
                perform_compaction_job(db); // Synchronous fallback
            }
        }
    }

    db_release_job(db);
    lsm_db_release(db);
}

static void perform_compaction_job(void *arg) {
    lsm_db_t *db = (lsm_db_t *)arg;

    while (true) {
        lsm_version_t *v = lsmc_version_retain(db->manifest);
        int target_lvl = -1;

        if (v->compaction_score >= 1.0) {
            target_lvl = v->compaction_level;
        }
        lsmc_version_release(db->manifest, v);

        if (target_lvl == -1) break;

        uint64_t oldest_snap = get_oldest_snapshot(db);
        if (!lsmc_compact_level(db->manifest, target_lvl, oldest_snap)) {
            usleep(1000 * 1000);
            break;
        }
    }

    pthread_mutex_lock(&db->state_mutex);
    db->is_compacting = false;
    pthread_cond_broadcast(&db->stall_cond);
    pthread_mutex_unlock(&db->state_mutex);

    db_release_job(db);
    lsm_db_release(db);
}

// --- DB Lifecycle ---
lsm_db_t *lsm_db_open(lsm_env_t *env, uint32_t table_id, const char *db_directory) {
    lsm_db_t *db = aml_zalloc(sizeof(lsm_db_t));
    db->env = env;
    db->table_id = table_id;
    strncpy(db->db_dir, db_directory, sizeof(db->db_dir) - 1);
    db->db_dir[sizeof(db->db_dir) - 1] = '\0';

    db->owner_refs = 1;
    db->active_ops = 0;
    db->active_jobs = 0;
    db->closing = false;

    db->manifest = lsmc_manifest_init(env, table_id, db_directory);

    uint64_t max_seq = lsmc_get_max_sequence(db->manifest);
    db->current_seq = max_seq > 0 ? max_seq : 1;
    db->imm_seq = 0;

    db->commit_ticket_head = 0;
    db->commit_ticket_tail = 0;
    pthread_cond_init(&db->commit_cv, NULL);
    db->current_wal_lsn = 0;
    db->imm_wal_lsn = 0;

    db->memtable = memtable_init();
    atomic_fetch_add(&env->global_mem_usage, memtable_get_memory_usage(db->memtable));
    db->imm_memtable = NULL;

    pthread_mutex_init(&db->state_mutex, NULL);
    pthread_mutex_init(&db->write_mutex, NULL);
    pthread_mutex_init(&db->queue_mutex, NULL);
    pthread_cond_init(&db->stall_cond, NULL);

    db->writer_queue_head = NULL;
    db->writer_queue_tail = NULL;

    db->is_flushing = false;
    db->is_compacting = false;

    lsm_env_register_db(env, db);
    return db;
}

void lsm_db_retain(lsm_db_t *db) {
    if (db) {
        pthread_mutex_lock(&db->state_mutex);
        db->owner_refs++;
        pthread_mutex_unlock(&db->state_mutex);
    }
}

void lsm_db_release(lsm_db_t *db) {
    if (!db) return;

    pthread_mutex_lock(&db->state_mutex);
    int remaining = --db->owner_refs;
    pthread_mutex_unlock(&db->state_mutex);

    if (remaining == 0) {
        if (db->snapshots) aml_free(db->snapshots);
        pthread_mutex_destroy(&db->state_mutex);
        pthread_mutex_destroy(&db->write_mutex);
        pthread_mutex_destroy(&db->queue_mutex);
        pthread_cond_destroy(&db->stall_cond);
        pthread_cond_destroy(&db->commit_cv);
        aml_free(db);
    }
}

void lsm_db_close(lsm_db_t *db) {
    if (!db) return;

    pthread_mutex_lock(&db->state_mutex);
    if (db->closing) {
        pthread_mutex_unlock(&db->state_mutex);
        return;
    }
    db->closing = true;
    pthread_mutex_unlock(&db->state_mutex);

    lsm_env_unregister_db(db->env, db);

    if (db->env->global_wal && db->env->global_wal->unregister_table) {
        db->env->global_wal->unregister_table(db->env->global_wal, db->table_id);
    }

    pthread_mutex_lock(&db->state_mutex);
    while (db->active_ops > 0) {
        pthread_cond_wait(&db->stall_cond, &db->state_mutex);
    }

    while (db->is_flushing) {
        pthread_cond_wait(&db->stall_cond, &db->state_mutex);
    }
    pthread_mutex_unlock(&db->state_mutex);

    pthread_mutex_lock(&db->write_mutex);

    // [Phase 8] Contiguous Durability Fix
    bool can_flush_final = (db->imm_memtable == NULL);
    bool need_submit = false;

    if (can_flush_final && memtable_first(db->memtable)) {
        db->imm_memtable = db->memtable;
        db->imm_seq = db->current_seq;
        db->imm_wal_lsn = db->current_wal_lsn;

        db->memtable = memtable_init();
        atomic_fetch_add(&db->env->global_mem_usage, memtable_get_memory_usage(db->memtable));

        if (db->env->global_wal && db->env->global_wal->sync) {
            db->env->global_wal->sync(db->env->global_wal);
        }

        pthread_mutex_lock(&db->state_mutex);
        db->is_flushing = true;
        db->active_jobs++;
        db->owner_refs++;
        need_submit = true;
        pthread_mutex_unlock(&db->state_mutex);
    }
    pthread_mutex_unlock(&db->write_mutex);

    // Completely lock-free submission
    if (need_submit) {
        if (!lsm_pool_submit(db->env->bg_pool, perform_flush_job, db, JOB_PRIORITY_URGENT)) {
            perform_flush_job(db); // Synchronous fallback
        }
    }

    pthread_mutex_lock(&db->state_mutex);
    while (db->active_jobs > 0 || db->is_flushing || db->is_compacting) {
        pthread_cond_wait(&db->stall_cond, &db->state_mutex);
    }

    // [Phase 8] Safe memory reclamation without WAL checkpointing
    if (db->imm_memtable != NULL) {
        atomic_fetch_sub(&db->env->global_mem_usage, memtable_get_memory_usage(db->imm_memtable));
        memtable_release(db->imm_memtable);
        db->imm_memtable = NULL;
    }
    pthread_mutex_unlock(&db->state_mutex);

    atomic_fetch_sub(&db->env->global_mem_usage, memtable_get_memory_usage(db->memtable));
    memtable_release(db->memtable);

    if (db->manifest) {
        lsm_version_t *v = db->manifest->current_version;
        if (v) lsmc_version_release(db->manifest, v);

        if (db->manifest->manifest_writer) {
            db->env->router.hot_vfs->fsync_file(db->manifest->manifest_writer);
            db->env->router.hot_vfs->close_writer(db->manifest->manifest_writer);
        }

        for (int i = 0; i < MAX_LEVELS; i++) {
            if (db->manifest->compaction_pointers[i]) {
                aml_free(db->manifest->compaction_pointers[i]);
            }
        }
        pthread_mutex_destroy(&db->manifest->version_mutex);
        aml_free((void *)db->manifest->db_directory);
        aml_free(db->manifest);
    }

    lsm_db_release(db);
}

uint32_t lsm_db_get_table_id(lsm_db_t *db) { return db ? db->table_id : 0; }

bool lsm_db_force_flush(lsm_db_t *db) {
    if (!db_try_acquire_op(db)) return false;

    pthread_mutex_lock(&db->write_mutex);
    pthread_mutex_lock(&db->state_mutex);

    if (db->imm_memtable != NULL || memtable_first(db->memtable) == NULL) {
        pthread_mutex_unlock(&db->state_mutex);
        pthread_mutex_unlock(&db->write_mutex);
        db_release_op(db);
        return false;
    }

    db->imm_memtable = db->memtable;
    db->imm_seq = db->current_seq;
    db->imm_wal_lsn = db->current_wal_lsn;

    db->memtable = memtable_init();
    atomic_fetch_add(&db->env->global_mem_usage, memtable_get_memory_usage(db->memtable));

    if (db->env->global_wal && db->env->global_wal->sync) {
        db->env->global_wal->sync(db->env->global_wal);
    }

    bool need_submit = false;
    if (!db->is_flushing) {
        db->is_flushing = true;
        db->active_jobs++;
        db->owner_refs++;
        need_submit = true;
    }

    pthread_mutex_unlock(&db->state_mutex);
    pthread_mutex_unlock(&db->write_mutex);

    // Completely lock-free submission
    if (need_submit) {
        if (!lsm_pool_submit(db->env->bg_pool, perform_flush_job, db, JOB_PRIORITY_URGENT)) {
            perform_flush_job(db); // Synchronous fallback
        }
    }

    db_release_op(db);
    return true;
}

size_t lsm_db_get_active_mem_usage(lsm_db_t *db) {
    if (!db_try_acquire_op(db)) return 0;
    pthread_mutex_lock(&db->write_mutex);
    size_t sz = memtable_get_memory_usage(db->memtable);
    pthread_mutex_unlock(&db->write_mutex);
    db_release_op(db);
    return sz;
}

static void stall_for_memory(lsm_env_t *env) {
    size_t usage = atomic_load(&env->global_mem_usage);
    size_t flush_limit = env->global_mem_limit;
    size_t soft_limit = (flush_limit * 8) / 10;
    size_t hard_stall_limit = flush_limit + (flush_limit / 2);

    if (usage > soft_limit) {
        if (usage > hard_stall_limit) {
            pthread_mutex_lock(&env->global_mem_mutex);
            while (atomic_load(&env->global_mem_usage) > hard_stall_limit) {
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_sec += 5;
                if (pthread_cond_timedwait(&env->global_mem_cond, &env->global_mem_mutex, &ts) != 0) {
                    break;
                }
            }
            pthread_mutex_unlock(&env->global_mem_mutex);
        } else {
            size_t overage = usage - soft_limit;
            size_t band = hard_stall_limit - soft_limit;
            useconds_t delay = 1000 + (useconds_t)((overage * 9000) / band);
            usleep(delay);
        }
    }
}

void lsm_db_inject_recovery_seq(lsm_db_t *db, uint64_t seq, uint64_t lsn, uint8_t op, const void *key, uint32_t klen, const void *val, uint32_t vlen) {
    if (!db_try_acquire_op(db)) return;
    pthread_mutex_lock(&db->write_mutex);
    if (seq > db->current_seq) db->current_seq = seq;
    if (lsn > db->current_wal_lsn) db->current_wal_lsn = lsn;
    size_t mem_before = memtable_get_memory_usage(db->memtable);
    memtable_put(db->memtable, seq, op, key, klen, val, vlen);
    size_t diff = memtable_get_memory_usage(db->memtable) - mem_before;
    pthread_mutex_unlock(&db->write_mutex);
    atomic_fetch_add(&db->env->global_mem_usage, diff);
    db_release_op(db);
}

typedef struct {
    uint8_t type;
    void *key;
    uint32_t klen;
    void *val;
    uint32_t vlen;
} lsm_batch_entry_t;

struct lsm_write_batch_s {
    lsm_batch_entry_t *entries;
    size_t count;
    size_t capacity;
};

lsm_write_batch_t *lsm_write_batch_init(void) {
    lsm_write_batch_t *b = aml_zalloc(sizeof(lsm_write_batch_t));
    b->capacity = 16;
    b->entries = aml_malloc(b->capacity * sizeof(lsm_batch_entry_t));
    return b;
}

bool lsm_write_batch_put(lsm_write_batch_t *b, const void *key, uint32_t klen, const void *val, uint32_t vlen) {
    if (!b || !key || klen == 0 || klen > MAX_KEY_SIZE) return false;
    if (vlen > 0 && !val) return false;

    if (b->count == b->capacity) {
        b->capacity *= 2;
        b->entries = aml_realloc(b->entries, b->capacity * sizeof(lsm_batch_entry_t));
    }
    b->entries[b->count].type = OP_PUT;

    b->entries[b->count].key = aml_malloc(klen);
    memcpy(b->entries[b->count].key, key, klen);
    b->entries[b->count].klen = klen;

    if (vlen > 0) {
        b->entries[b->count].val = aml_malloc(vlen);
        memcpy(b->entries[b->count].val, val, vlen);
    } else {
        b->entries[b->count].val = NULL;
    }
    b->entries[b->count].vlen = vlen;

    b->count++;
    return true;
}

bool lsm_write_batch_delete(lsm_write_batch_t *b, const void *key, uint32_t klen) {
    if (!b || !key || klen == 0 || klen > MAX_KEY_SIZE) return false;

    if (b->count == b->capacity) {
        b->capacity *= 2;
        b->entries = aml_realloc(b->entries, b->capacity * sizeof(lsm_batch_entry_t));
    }
    b->entries[b->count].type = OP_DELETE;

    b->entries[b->count].key = aml_malloc(klen);
    memcpy(b->entries[b->count].key, key, klen);
    b->entries[b->count].klen = klen;

    b->entries[b->count].val = NULL;
    b->entries[b->count].vlen = 0;

    b->count++;
    return true;
}

void lsm_write_batch_destroy(lsm_write_batch_t *b) {
    if (!b) return;
    for(size_t i=0; i<b->count; i++) {
        aml_free(b->entries[i].key);
        if(b->entries[i].val) aml_free(b->entries[i].val);
    }
    aml_free(b->entries);
    aml_free(b);
}

bool lsm_db_write(lsm_db_t *db, lsm_write_batch_t *batch) {
    if (!batch || batch->count == 0) return true;

    if (!db_try_acquire_op(db)) return false;
    lsm_db_retain(db);

    lsm_writer_t w;
    w.batch = batch;
    w.done = false;
    w.success = false;
    w.next = NULL;
    pthread_cond_init(&w.cv, NULL);

    pthread_mutex_lock(&db->queue_mutex);

    bool is_leader = (db->writer_queue_head == NULL);
    uint64_t my_ticket = 0;

    if (is_leader) {
        db->writer_queue_head = &w;
        db->writer_queue_tail = &w;
    } else {
        db->writer_queue_tail->next = &w;
        db->writer_queue_tail = &w;
    }

    while (!w.done && db->writer_queue_head != &w) {
        pthread_cond_wait(&w.cv, &db->queue_mutex);
    }

    if (w.done) {
        pthread_mutex_unlock(&db->queue_mutex);
        pthread_cond_destroy(&w.cv);
        bool succ = w.success;
        lsm_env_t *env = db->env;
        db_release_op(db);
        lsm_db_release(db);
        if (succ) stall_for_memory(env);
        return succ;
    }

    my_ticket = db->commit_ticket_head++;

    lsm_writer_t *curr = db->writer_queue_head;
    lsm_writer_t *last_writer = curr;
    size_t total_count = 0;
    uint32_t blob_size = 12;

    while (curr != NULL) {
        uint32_t added_size = 0;
        for (size_t i = 0; i < curr->batch->count; i++) {
            added_size += 1 + 4 + 4 + curr->batch->entries[i].klen + curr->batch->entries[i].vlen;
        }
        if (total_count > 0 && blob_size + added_size > 1024 * 1024) break;

        blob_size += added_size;
        total_count += curr->batch->count;
        last_writer = curr;
        curr = curr->next;
    }

    db->writer_queue_head = last_writer->next;
    if (db->writer_queue_head == NULL) {
        db->writer_queue_tail = NULL;
    } else {
        pthread_cond_signal(&db->writer_queue_head->cv);
    }

    last_writer->next = NULL;
    pthread_mutex_unlock(&db->queue_mutex);

    pthread_mutex_lock(&db->write_mutex);

    while (db->commit_ticket_tail != my_ticket) {
        pthread_cond_wait(&db->commit_cv, &db->write_mutex);
    }

    bool write_success = true;
    uint64_t start_seq = db->current_seq;
    db->current_seq += total_count;
    uint64_t assigned_lsn = 0;

    if (db->env->global_wal && db->env->global_wal->append) {
        uint8_t *blob = aml_malloc(blob_size);
        uint32_t ptr = 0;

        uint64_t wal_seq = start_seq + 1;
        blob[0] = wal_seq & 0xFF; blob[1] = (wal_seq >> 8) & 0xFF;
        blob[2] = (wal_seq >> 16) & 0xFF; blob[3] = (wal_seq >> 24) & 0xFF;
        blob[4] = (wal_seq >> 32) & 0xFF; blob[5] = (wal_seq >> 40) & 0xFF;
        blob[6] = (wal_seq >> 48) & 0xFF; blob[7] = (wal_seq >> 56) & 0xFF;
        ptr += 8;

        encode_u32_le_db(blob + ptr, total_count); ptr += 4;

        curr = &w;
        while (curr != NULL) {
            for (size_t i = 0; i < curr->batch->count; i++) {
                blob[ptr++] = curr->batch->entries[i].type;
                encode_u32_le_db(blob + ptr, curr->batch->entries[i].klen); ptr += 4;
                encode_u32_le_db(blob + ptr, curr->batch->entries[i].vlen); ptr += 4;

                if (curr->batch->entries[i].klen > 0) {
                    memcpy(blob + ptr, curr->batch->entries[i].key, curr->batch->entries[i].klen);
                    ptr += curr->batch->entries[i].klen;
                }
                if (curr->batch->entries[i].vlen > 0) {
                    memcpy(blob + ptr, curr->batch->entries[i].val, curr->batch->entries[i].vlen);
                    ptr += curr->batch->entries[i].vlen;
                }
            }
            curr = curr->next;
        }

        if (!db->env->global_wal->append(db->env->global_wal, db->table_id, wal_seq, 2 /* OP_BATCH */, NULL, 0, blob, blob_size, &assigned_lsn)) {
            write_success = false;
        }
        aml_free(blob);
    }

    size_t diff = 0;
    if (write_success) {
        if (assigned_lsn > db->current_wal_lsn) {
            db->current_wal_lsn = assigned_lsn;
        }

        size_t mem_before = memtable_get_memory_usage(db->memtable);
        curr = &w;
        uint64_t seq = start_seq;

        while (curr != NULL) {
            for (size_t i = 0; i < curr->batch->count; i++) {
                memtable_put(db->memtable, ++seq, curr->batch->entries[i].type,
                             curr->batch->entries[i].key, curr->batch->entries[i].klen,
                             curr->batch->entries[i].val, curr->batch->entries[i].vlen);
            }
            curr = curr->next;
        }
        diff = memtable_get_memory_usage(db->memtable) - mem_before;
    } else {
        db->current_seq -= total_count;
    }

    db->commit_ticket_tail++;
    pthread_cond_broadcast(&db->commit_cv);

    pthread_mutex_unlock(&db->write_mutex);

    pthread_mutex_lock(&db->queue_mutex);
    curr = w.next;
    while (curr != NULL) {
        lsm_writer_t *nxt = curr->next;
        curr->success = write_success;
        curr->done = true;
        if (curr != &w) {
            pthread_cond_signal(&curr->cv);
        }
        curr = nxt;
    }
    pthread_mutex_unlock(&db->queue_mutex);

    if (write_success && diff > 0) {
        size_t current_usage = atomic_fetch_add(&db->env->global_mem_usage, diff) + diff;
        if (current_usage > db->env->global_mem_limit) lsm_db_force_flush(db);
    }

    pthread_cond_destroy(&w.cv);
    lsm_env_t *env = db->env;

    db_release_op(db);
    lsm_db_release(db);

    if (write_success) stall_for_memory(env);

    return write_success;
}

bool lsm_db_put(lsm_db_t *db, const void *key, uint32_t key_len, const void *val, uint32_t val_len) {
    if (!key || key_len == 0 || key_len > MAX_KEY_SIZE) return false;
    lsm_write_batch_t *b = lsm_write_batch_init();
    if (!lsm_write_batch_put(b, key, key_len, val, val_len)) {
        lsm_write_batch_destroy(b);
        return false;
    }
    bool res = lsm_db_write(db, b);
    lsm_write_batch_destroy(b);
    return res;
}

bool lsm_db_delete(lsm_db_t *db, const void *key, uint32_t key_len) {
    if (!key || key_len == 0 || key_len > MAX_KEY_SIZE) return false;
    lsm_write_batch_t *b = lsm_write_batch_init();
    if (!lsm_write_batch_delete(b, key, key_len)) {
        lsm_write_batch_destroy(b);
        return false;
    }
    bool res = lsm_db_write(db, b);
    lsm_write_batch_destroy(b);
    return res;
}

static int check_sstable_for_get(lsm_db_t *db, sstable_meta_t *meta, int lvl, const void *key, uint32_t key_len, uint64_t read_seq_num, void **disk_val, uint32_t *local_vlen) {
    uint32_t umax = meta->max_key_len > INTERNAL_KEY_TRAILER_SIZE ? meta->max_key_len - INTERNAL_KEY_TRAILER_SIZE : 0;
    uint32_t umin = meta->min_key_len > INTERNAL_KEY_TRAILER_SIZE ? meta->min_key_len - INTERNAL_KEY_TRAILER_SIZE : 0;

    uint32_t min_cmp_len = key_len < umin ? key_len : umin;
    int cmp_min = memcmp(key, meta->min_key, min_cmp_len);
    if (cmp_min == 0) cmp_min = (key_len < umin) ? -1 : (key_len > umin ? 1 : 0);
    if (cmp_min < 0) return 0;

    uint32_t max_cmp_len = key_len < umax ? key_len : umax;
    int cmp_max = memcmp(key, meta->max_key, max_cmp_len);
    if (cmp_max == 0) cmp_max = (key_len < umax) ? -1 : (key_len > umax ? 1 : 0);
    if (cmp_max > 0) return 0;

    sstable_reader_t *r = __atomic_load_n(&meta->cached_reader, __ATOMIC_ACQUIRE);
    if (!r) {
        pthread_mutex_lock(&meta->reader_mutex);
        r = __atomic_load_n(&meta->cached_reader, __ATOMIC_ACQUIRE);
        if (!r) {
            char base_path[512];
            snprintf(base_path, sizeof(base_path), "%s/%06llu", db->db_dir, (unsigned long long)meta->file_id);
            lsm_storage_backend_t *vfs = (lvl >= db->env->router.cold_storage_start_level) ? db->env->router.cold_vfs : db->env->router.hot_vfs;
            r = sstable_reader_init(base_path, vfs, db->env, db->table_id, meta->file_id);
            __atomic_store_n(&meta->cached_reader, r, __ATOMIC_RELEASE);
        }
        pthread_mutex_unlock(&meta->reader_mutex);
    }
    if (!r) return 0;

    return sstable_reader_get(r, key, key_len, read_seq_num, disk_val, local_vlen);
}

void *lsm_db_get(lsm_db_t *db, const void *key, uint32_t key_len, uint64_t read_seq_num, uint32_t *out_val_len) {
    if (!db_try_acquire_op(db)) return NULL;
    lsm_db_retain(db);

    if (!key || key_len == 0 || key_len > MAX_KEY_SIZE) {
        db_release_op(db);
        lsm_db_release(db);
        return NULL;
    }

    uint32_t local_vlen = 0;
    if (out_val_len) *out_val_len = 0;

    pthread_mutex_lock(&db->write_mutex);
    pthread_mutex_lock(&db->state_mutex);

    memtable_t *active = db->memtable; memtable_retain(active);
    memtable_t *imm = db->imm_memtable; if (imm) memtable_retain(imm);
    lsm_version_t *v = lsmc_version_retain(db->manifest);

    pthread_mutex_unlock(&db->state_mutex);
    pthread_mutex_unlock(&db->write_mutex);

    bool is_deleted = false;
    void *ret_val = NULL;

    const void *val = memtable_get(active, key, key_len, read_seq_num, &local_vlen, &is_deleted);
    if (is_deleted) goto cleanup;
    if (val) {
        ret_val = aml_malloc(local_vlen);
        memcpy(ret_val, val, local_vlen);
        if (out_val_len) *out_val_len = local_vlen;
        goto cleanup;
    }

    if (imm) {
        val = memtable_get(imm, key, key_len, read_seq_num, &local_vlen, &is_deleted);
        if (is_deleted) goto cleanup;
        if (val) {
            ret_val = aml_malloc(local_vlen);
            memcpy(ret_val, val, local_vlen);
            if (out_val_len) *out_val_len = local_vlen;
            goto cleanup;
        }
    }

    void *disk_val = NULL;

    for (int lvl = 0; lvl < MAX_LEVELS; lvl++) {
        if (v->levels[lvl].num_files == 0) continue;

        if (lvl == 0) {
            for (int f = v->levels[lvl].num_files - 1; f >= 0; f--) {
                int status = check_sstable_for_get(db, v->levels[lvl].files[f], lvl, key, key_len, read_seq_num, &disk_val, &local_vlen);
                if (status == 1) {
                    ret_val = disk_val;
                    if (out_val_len) *out_val_len = local_vlen;
                    goto cleanup;
                }
                else if (status == -1) goto cleanup;
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
                if (cmp_min < 0) { right = mid - 1; continue; }

                uint32_t max_cmp_len = key_len < umax ? key_len : umax;
                int cmp_max = memcmp(key, meta->max_key, max_cmp_len);
                if (cmp_max == 0) cmp_max = (key_len < umax) ? -1 : (key_len > umax ? 1 : 0);
                if (cmp_max > 0) { left = mid + 1; continue; }

                target_idx = mid;
                break;
            }

            if (target_idx != -1) {
                int status = check_sstable_for_get(db, v->levels[lvl].files[target_idx], lvl, key, key_len, read_seq_num, &disk_val, &local_vlen);
                if (status == 1) {
                    ret_val = disk_val;
                    if (out_val_len) *out_val_len = local_vlen;
                    goto cleanup;
                }
                else if (status == -1) goto cleanup;
            }
        }
    }

cleanup:
    memtable_release(active);
    if (imm) memtable_release(imm);
    lsmc_version_release(db->manifest, v);
    db_release_op(db);
    lsm_db_release(db);
    return ret_val;
}

// --- Iterator Utils & Setup ---
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
    uint64_t read_seq_num;

    iter_node_t *heap;
    size_t heap_size;
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
            n->key = memtable_row_get_key(nxt, &n->klen); n->val = memtable_row_get_val(nxt, &n->vlen);
            n->seq = memtable_row_get_seq(nxt); n->op = memtable_row_get_op(nxt);
            n->klen += INTERNAL_KEY_TRAILER_SIZE; iter_push(it, *n);
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

lsm_db_iter_t *lsm_db_iter_init(lsm_db_t *db, uint64_t read_seq_num) {
    if (!db_try_acquire_op(db)) return NULL;
    lsm_db_retain(db);

    lsm_db_iter_t *it = aml_zalloc(sizeof(lsm_db_iter_t));
    it->db = db;
    it->read_seq_num = read_seq_num;

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
    it->iters = aml_malloc(total_files * sizeof(sstable_iter_t*));

    memtable_row_t *mrow = memtable_first(it->active);
    if (mrow) {
        iter_node_t n = {0}; n.src_type = 0; n.src_ptr = mrow;
        n.key = memtable_row_get_key(mrow, &n.klen); n.val = memtable_row_get_val(mrow, &n.vlen);
        n.seq = memtable_row_get_seq(mrow); n.op = memtable_row_get_op(mrow);
        n.klen += INTERNAL_KEY_TRAILER_SIZE; iter_push(it, n);
    }

    if (it->imm) {
        memtable_row_t *imrow = memtable_first(it->imm);
        if (imrow) {
            iter_node_t n = {0}; n.src_type = 0; n.src_ptr = imrow;
            n.key = memtable_row_get_key(imrow, &n.klen); n.val = memtable_row_get_val(imrow, &n.vlen);
            n.seq = memtable_row_get_seq(imrow); n.op = memtable_row_get_op(imrow);
            n.klen += INTERNAL_KEY_TRAILER_SIZE; iter_push(it, n);
        }
    }

    for (int lvl=0; lvl<MAX_LEVELS; lvl++) {
        for (size_t f=0; f<it->version->levels[lvl].num_files; f++) {
            sstable_meta_t *meta = it->version->levels[lvl].files[f];

            sstable_reader_t *r = __atomic_load_n(&meta->cached_reader, __ATOMIC_ACQUIRE);
            if (!r) {
                pthread_mutex_lock(&meta->reader_mutex);
                r = __atomic_load_n(&meta->cached_reader, __ATOMIC_ACQUIRE);
                if (!r) {
                    char base_path[512];
                    snprintf(base_path, sizeof(base_path), "%s/%06llu", db->db_dir, (unsigned long long)meta->file_id);
                    lsm_storage_backend_t *vfs = (lvl >= db->env->router.cold_storage_start_level) ? db->env->router.cold_vfs : db->env->router.hot_vfs;
                    r = sstable_reader_init(base_path, vfs, db->env, db->table_id, meta->file_id);
                    __atomic_store_n(&meta->cached_reader, r, __ATOMIC_RELEASE);
                }
                pthread_mutex_unlock(&meta->reader_mutex);
            }
            if (!r) continue;

            sstable_iter_t *s_it = sstable_iter_init(r);
            it->iters[it->num_files] = s_it;
            it->num_files++;

            if (sstable_iter_next(s_it)) {
                iter_node_t n = {0}; n.src_type = 1; n.src_ptr = s_it;
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

        if (top.seq > it->read_seq_num) {
            iter_pop(it); advance_source(it, &top); continue;
        }

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

            iter_pop(it); advance_source(it, &top);
            *key = it->last_user_key; *klen = ulen;
            *val = it->ret_val_buf; *vlen = ret_vlen;
            return true;
        } else {
            iter_pop(it); advance_source(it, &top);
        }
    }
    return false;
}

void lsm_db_iter_destroy(lsm_db_iter_t *it) {
    if (!it) return;
    for (size_t i=0; i<it->num_files; i++) {
        sstable_iter_destroy(it->iters[i]);
    }
    memtable_release(it->active);
    if (it->imm) memtable_release(it->imm);
    lsmc_version_release(it->db->manifest, it->version);

    aml_free(it->ret_val_buf);
    aml_free(it->iters); aml_free(it->heap);

    db_release_op(it->db);
    lsm_db_release(it->db);
    aml_free(it);
}

void lsm_db_debug_get_compaction_metrics(lsm_db_t *db, double *score, int *level, size_t *l0_files, size_t *l1_files) {
    if (!db_try_acquire_op(db)) return;
    if (db->manifest) {
        pthread_mutex_lock(&db->manifest->version_mutex);
        lsm_version_t *v = db->manifest->current_version;
        if (score) *score = v->compaction_score;
        if (level) *level = v->compaction_level;
        if (l0_files) *l0_files = v->levels[0].num_files;
        if (l1_files) *l1_files = v->levels[1].num_files;
        pthread_mutex_unlock(&db->manifest->version_mutex);
    }
    db_release_op(db);
}
