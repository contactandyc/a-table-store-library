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

struct lsm_db_s {
    uint32_t table_id;
    char db_dir[512];
    lsm_env_t *env;

    int ref_count;

    memtable_t *memtable;
    memtable_t *imm_memtable;

    lsm_manifest_t *manifest;
    uint64_t current_seq;
    uint64_t imm_seq;

    uint64_t *snapshots;
    size_t num_snapshots;
    size_t snapshots_cap;

    pthread_mutex_t state_mutex;
    pthread_mutex_t write_mutex;
    pthread_cond_t stall_cond;

    bool is_flushing;
    bool is_compacting;
    bool is_closing; // [Phase 2 Fix] Shut off incoming traffic immediately
};

// --- Snapshots ---
uint64_t lsm_db_take_snapshot(lsm_db_t *db) {
    pthread_mutex_lock(&db->write_mutex);
    uint64_t seq = db->current_seq;
    if (db->num_snapshots == db->snapshots_cap) {
        db->snapshots_cap = db->snapshots_cap == 0 ? 8 : db->snapshots_cap * 2;
        db->snapshots = aml_realloc(db->snapshots, db->snapshots_cap * sizeof(uint64_t));
    }
    db->snapshots[db->num_snapshots++] = seq;
    pthread_mutex_unlock(&db->write_mutex);
    return seq;
}

void lsm_db_release_snapshot(lsm_db_t *db, uint64_t seq) {
    pthread_mutex_lock(&db->write_mutex);
    for (size_t i = 0; i < db->num_snapshots; i++) {
        if (db->snapshots[i] == seq) {
            db->snapshots[i] = db->snapshots[--db->num_snapshots];
            break;
        }
    }
    pthread_mutex_unlock(&db->write_mutex);
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
    if (imm) memtable_retain(imm); // [Phase 2 Fix] Retain to avoid use-after-free
    uint64_t flushed_seq = db->imm_seq;
    pthread_mutex_unlock(&db->state_mutex);

    if (!imm) {
        lsm_db_release(db); // Job is done
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
    sstable_meta_t *meta = aml_zalloc(sizeof(sstable_meta_t));
    meta->file_id = file_id;

    bool is_first = true;
    memtable_row_t *row = memtable_first(imm);
    char ikey[MAX_INTERNAL_KEY_SIZE];
    uint64_t block_max_seq = 0;

    while (row) {
        uint32_t klen, vlen;
        const void *k = memtable_row_get_key(row, &klen);
        const void *v = memtable_row_get_val(row, &vlen);

        uint64_t seq = memtable_row_get_seq(row);
        if (seq > block_max_seq) block_max_seq = seq;

        pack_internal_key(ikey, k, klen, seq, memtable_row_get_op(row));
        uint32_t iklen = klen + TRAILER_SIZE;

        sstable_builder_add(builder, ikey, iklen, v, vlen);
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
    meta->file_size = sstable_builder_finish(builder);
    meta->max_seq = block_max_seq;

    sstable_meta_t *added[1] = { meta };
    lsmc_version_edit(db->manifest, -1, 0, NULL, 0, added, 1);

    if (db->env->global_wal && db->env->global_wal->checkpoint) {
        db->env->global_wal->checkpoint(db->env->global_wal, db->table_id, flushed_seq);
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

    if (imm) memtable_release(imm); // Release local retain

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
            lsm_db_retain(db); // [Phase 2 Fix] Retain for the compaction job
            lsm_pool_submit(db->env->bg_pool, perform_compaction_job, db, JOB_PRIORITY_HIGH);
        }
        pthread_mutex_unlock(&db->state_mutex);
    }

    lsm_db_release(db); // Job is done
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

        uint64_t oldest_snap = get_oldest_snapshot(db);
        lsmc_compact_level(db->manifest, target_lvl, oldest_snap);
    }

    pthread_mutex_lock(&db->state_mutex);
    db->is_compacting = false;
    pthread_cond_broadcast(&db->stall_cond);
    pthread_mutex_unlock(&db->state_mutex);

    lsm_db_release(db); // Job is done
}

// --- DB Lifecycle ---
lsm_db_t *lsm_db_open(lsm_env_t *env, uint32_t table_id, const char *db_directory) {
    lsm_db_t *db = aml_zalloc(sizeof(lsm_db_t));
    db->env = env;
    db->table_id = table_id;
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
    db->is_closing = false;

    lsm_env_register_db(env, db);
    return db;
}

void lsm_db_retain(lsm_db_t *db) {
    if (db) __atomic_fetch_add(&db->ref_count, 1, __ATOMIC_SEQ_CST);
}

void lsm_db_release(lsm_db_t *db) {
    if (!db) return;
    int prev = __atomic_fetch_sub(&db->ref_count, 1, __ATOMIC_SEQ_CST);

    // [Phase 2 Fix] Wake up lsm_db_close if it's waiting for external refs to drain
    if (prev == 2) {
        pthread_mutex_lock(&db->state_mutex);
        pthread_cond_broadcast(&db->stall_cond);
        pthread_mutex_unlock(&db->state_mutex);
    }

    if (prev == 1) {
        if (db->snapshots) aml_free(db->snapshots);
        aml_free(db);
    }
}

void lsm_db_close(lsm_db_t *db) {
    if (!db) return;

    lsm_env_unregister_db(db->env, db);

    // [Phase 2 Fix] Zero-CPU wait for active readers & jobs to drain
    pthread_mutex_lock(&db->state_mutex);
    db->is_closing = true;
    while (__atomic_load_n(&db->ref_count, __ATOMIC_SEQ_CST) > 1) {
        pthread_cond_wait(&db->stall_cond, &db->state_mutex);
    }
    pthread_mutex_unlock(&db->state_mutex);

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
        lsm_db_retain(db); // For the flush job
        lsm_pool_submit(db->env->bg_pool, perform_flush_job, db, JOB_PRIORITY_URGENT);
        pthread_mutex_unlock(&db->state_mutex);
    }
    pthread_mutex_unlock(&db->write_mutex);

    pthread_mutex_lock(&db->state_mutex);
    while (db->imm_memtable != NULL || db->is_flushing || db->is_compacting) {
        pthread_cond_wait(&db->stall_cond, &db->state_mutex);
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

        pthread_mutex_destroy(&db->manifest->version_mutex);
        aml_free((void *)db->manifest->db_directory);
        aml_free(db->manifest);
    }

    pthread_mutex_destroy(&db->write_mutex);
    pthread_mutex_destroy(&db->state_mutex);
    pthread_cond_destroy(&db->stall_cond);

    lsm_db_release(db);
}

uint32_t lsm_db_get_table_id(lsm_db_t *db) { return db ? db->table_id : 0; }

bool lsm_db_force_flush(lsm_db_t *db) {
    pthread_mutex_lock(&db->write_mutex);
    pthread_mutex_lock(&db->state_mutex);

    if (db->imm_memtable != NULL || memtable_first(db->memtable) == NULL) {
        pthread_mutex_unlock(&db->state_mutex);
        pthread_mutex_unlock(&db->write_mutex);
        return false;
    }

    db->imm_memtable = db->memtable;
    db->imm_seq = db->current_seq;
    db->memtable = memtable_init();
    atomic_fetch_add(&db->env->global_mem_usage, memtable_get_memory_usage(db->memtable));

    if (db->env->global_wal && db->env->global_wal->sync) {
        db->env->global_wal->sync(db->env->global_wal);
    }

    if (!db->is_flushing) {
        db->is_flushing = true;
        lsm_db_retain(db); // Retain for flush job
        lsm_pool_submit(db->env->bg_pool, perform_flush_job, db, JOB_PRIORITY_URGENT);
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

static bool stall_for_memory(lsm_env_t *env) {
    size_t usage = atomic_load(&env->global_mem_usage);
    size_t flush_limit = env->global_mem_limit;

    // [Phase 5A Fix] Soft limit kicks in at 80% of the flush limit
    size_t soft_limit = (flush_limit * 8) / 10;

    // Hard stall kicks in at 150% to allow geometric arena chunks room to breathe
    size_t hard_stall_limit = flush_limit + (flush_limit / 2);

    if (usage > soft_limit) {
        if (usage > hard_stall_limit) {
            // HARD STALL: We are way out of memory. Block the thread completely.
            bool stall_failed = false;
            pthread_mutex_lock(&env->global_mem_mutex);

            // Loop in case of spurious wakeups
            while (atomic_load(&env->global_mem_usage) > hard_stall_limit) {
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_sec += 5; // 5-second safety timeout

                int rc = pthread_cond_timedwait(&env->global_mem_cond, &env->global_mem_mutex, &ts);
                if (rc != 0) {
                    stall_failed = true;
                    break;
                }
            }
            pthread_mutex_unlock(&env->global_mem_mutex);
            return !stall_failed;
        } else {
            // SOFT STALL (Backpressure): Delay the writer gracefully to let the bg_pool catch up.
            size_t overage = usage - soft_limit;
            size_t band = hard_stall_limit - soft_limit;

            useconds_t delay = 1000 + (useconds_t)((overage * 9000) / band);
            usleep(delay);
        }
    }
    return true;
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

// --- Write Batches ---
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
    if (klen > MAX_KEY_SIZE) return false;
    if (b->count == b->capacity) {
        b->capacity *= 2;
        b->entries = aml_realloc(b->entries, b->capacity * sizeof(lsm_batch_entry_t));
    }
    b->entries[b->count].type = OP_PUT;
    b->entries[b->count].key = aml_malloc(klen);
    memcpy(b->entries[b->count].key, key, klen);
    b->entries[b->count].klen = klen;
    b->entries[b->count].val = aml_malloc(vlen);
    memcpy(b->entries[b->count].val, val, vlen);
    b->entries[b->count].vlen = vlen;
    b->count++;
    return true;
}

bool lsm_write_batch_delete(lsm_write_batch_t *b, const void *key, uint32_t klen) {
    if (klen > MAX_KEY_SIZE) return false;
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

    // [Phase 2 Fix] Reject traffic if closing
    pthread_mutex_lock(&db->state_mutex);
    if (db->is_closing) {
        pthread_mutex_unlock(&db->state_mutex);
        return false;
    }
    pthread_mutex_unlock(&db->state_mutex);

    lsm_db_retain(db);
    pthread_mutex_lock(&db->write_mutex);

    uint64_t start_seq = db->current_seq + 1;
    db->current_seq += batch->count;

    if (db->env->global_wal && db->env->global_wal->append) {
        uint32_t blob_size = 4;
        for (size_t i = 0; i < batch->count; i++) {
            blob_size += 1 + 4 + 4 + batch->entries[i].klen + batch->entries[i].vlen;
        }

        uint8_t *blob = aml_malloc(blob_size);
        uint32_t ptr = 0;
        encode_u32_le_db(blob + ptr, batch->count); ptr += 4;

        for (size_t i = 0; i < batch->count; i++) {
            blob[ptr++] = batch->entries[i].type;
            encode_u32_le_db(blob + ptr, batch->entries[i].klen); ptr += 4;
            encode_u32_le_db(blob + ptr, batch->entries[i].vlen); ptr += 4;

            memcpy(blob + ptr, batch->entries[i].key, batch->entries[i].klen);
            ptr += batch->entries[i].klen;
            if (batch->entries[i].vlen > 0) {
                memcpy(blob + ptr, batch->entries[i].val, batch->entries[i].vlen);
                ptr += batch->entries[i].vlen;
            }
        }

        db->env->global_wal->append(db->env->global_wal, db->table_id, 2 /* OP_BATCH */, NULL, 0, blob, blob_size);
        aml_free(blob);
    }

    size_t mem_before = memtable_get_memory_usage(db->memtable);
    for(size_t i=0; i<batch->count; i++) {
        memtable_put(db->memtable, start_seq + i, batch->entries[i].type,
                     batch->entries[i].key, batch->entries[i].klen,
                     batch->entries[i].val, batch->entries[i].vlen);
    }
    size_t diff = memtable_get_memory_usage(db->memtable) - mem_before;
    pthread_mutex_unlock(&db->write_mutex);

    size_t current_usage = atomic_fetch_add(&db->env->global_mem_usage, diff) + diff;
    if (current_usage > db->env->global_mem_limit) lsm_db_force_flush(db);

    bool success = stall_for_memory(db->env);
    lsm_db_release(db);
    return success;
}

bool lsm_db_put(lsm_db_t *db, const void *key, uint32_t key_len, const void *val, uint32_t val_len) {
    if (key_len > MAX_KEY_SIZE) return false;
    lsm_write_batch_t *b = lsm_write_batch_init();
    lsm_write_batch_put(b, key, key_len, val, val_len);
    bool res = lsm_db_write(db, b);
    lsm_write_batch_destroy(b);
    return res;
}

bool lsm_db_delete(lsm_db_t *db, const void *key, uint32_t key_len) {
    if (key_len > MAX_KEY_SIZE) return false;
    lsm_write_batch_t *b = lsm_write_batch_init();
    lsm_write_batch_delete(b, key, key_len);
    bool res = lsm_db_write(db, b);
    lsm_write_batch_destroy(b);
    return res;
}

// [Phase 3 Fix] Extracted helper to prevent scan logic duplication
static int check_sstable_for_get(lsm_db_t *db, sstable_meta_t *meta, int lvl, const void *key, uint32_t key_len, uint64_t read_seq_num, void **disk_val, uint32_t *local_vlen) {
    uint32_t umax = meta->max_key_len > INTERNAL_KEY_TRAILER_SIZE ? meta->max_key_len - INTERNAL_KEY_TRAILER_SIZE : 0;
    uint32_t umin = meta->min_key_len > INTERNAL_KEY_TRAILER_SIZE ? meta->min_key_len - INTERNAL_KEY_TRAILER_SIZE : 0;

    uint32_t min_cmp_len = key_len < umin ? key_len : umin;
    int cmp_min = memcmp(key, meta->min_key, min_cmp_len);
    if (cmp_min == 0) cmp_min = (key_len < umin) ? -1 : (key_len > umin ? 1 : 0);
    if (cmp_min < 0) return 0; // Not in range

    uint32_t max_cmp_len = key_len < umax ? key_len : umax;
    int cmp_max = memcmp(key, meta->max_key, max_cmp_len);
    if (cmp_max == 0) cmp_max = (key_len < umax) ? -1 : (key_len > umax ? 1 : 0);
    if (cmp_max > 0) return 0; // Not in range

    char base_path[512];
    snprintf(base_path, sizeof(base_path), "%s/%06llu", db->db_dir, (unsigned long long)meta->file_id);
    lsm_storage_backend_t *vfs = (lvl >= db->env->router.cold_storage_start_level) ? db->env->router.cold_vfs : db->env->router.hot_vfs;
    sstable_reader_t *r = sstable_reader_init(base_path, vfs, db->env, db->table_id, meta->file_id);
    if (!r) return 0;

    int status = sstable_reader_get(r, key, key_len, read_seq_num, disk_val, local_vlen);
    sstable_reader_destroy(r);
    return status;
}

void *lsm_db_get(lsm_db_t *db, const void *key, uint32_t key_len, uint64_t read_seq_num, uint32_t *out_val_len) {
    pthread_mutex_lock(&db->state_mutex);
    if (db->is_closing) {
        pthread_mutex_unlock(&db->state_mutex);
        return NULL;
    }
    pthread_mutex_unlock(&db->state_mutex);

    lsm_db_retain(db);

    if (key_len > MAX_KEY_SIZE) { lsm_db_release(db); return NULL; }

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
            // [Phase 3 Fix] Scan all overlapping L0 files uniformly
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
            // Binary search for exactly one intersecting file in L1+
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
    lsm_db_release(db);
    return ret_val;
}

// ... iter_cmp, iter_push, iter_pop, advance_source remain unchanged ...
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
    // [Phase 2 Fix] Reject traffic if closing
    pthread_mutex_lock(&db->state_mutex);
    if (db->is_closing) {
        pthread_mutex_unlock(&db->state_mutex);
        return NULL;
    }
    pthread_mutex_unlock(&db->state_mutex);

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
    it->readers = aml_malloc(total_files * sizeof(sstable_reader_t*));
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

        // FILTER: Skip versions newer than the snapshot
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
        sstable_iter_destroy(it->iters[i]); sstable_reader_destroy(it->readers[i]);
    }
    memtable_release(it->active);
    if (it->imm) memtable_release(it->imm);
    lsmc_version_release(it->db->manifest, it->version);

    aml_free(it->ret_val_buf);
    aml_free(it->readers); aml_free(it->iters); aml_free(it->heap);
    lsm_db_release(it->db);
    aml_free(it);
}
