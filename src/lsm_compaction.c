// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-table-store-library/lsm_compaction.h"
#include "a-memory-library/aml_alloc.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

extern uint64_t sstable_builder_current_size(sstable_builder_t *b);

#define INTERNAL_KEY_TRAILER_SIZE 8

static inline uint32_t get_user_key_len(uint32_t internal_key_len) {
    return internal_key_len > INTERNAL_KEY_TRAILER_SIZE ? internal_key_len - INTERNAL_KEY_TRAILER_SIZE : 0;
}

static int lsmc_meta_cmp(const void *a, const void *b) {
    sstable_meta_t *m1 = *(sstable_meta_t **)a;
    sstable_meta_t *m2 = *(sstable_meta_t **)b;
    uint32_t ulen1 = get_user_key_len(m1->min_key_len);
    uint32_t ulen2 = get_user_key_len(m2->min_key_len);
    uint32_t min_len = ulen1 < ulen2 ? ulen1 : ulen2;
    int c = memcmp(m1->min_key, m2->min_key, min_len);
    if (c == 0) c = ulen1 < ulen2 ? -1 : (ulen1 > ulen2 ? 1 : 0);
    return c;
}

static bool lsmc_check_overlap(sstable_meta_t *a, sstable_meta_t *b) {
    uint32_t umaxA = get_user_key_len(a->max_key_len);
    uint32_t uminB = get_user_key_len(b->min_key_len);
    uint32_t min1 = umaxA < uminB ? umaxA : uminB;
    int c1 = memcmp(a->max_key, b->min_key, min1);
    if (c1 == 0) c1 = umaxA < uminB ? -1 : 1;
    if (c1 < 0) return false;

    uint32_t uminA = get_user_key_len(a->min_key_len);
    uint32_t umaxB = get_user_key_len(b->max_key_len);
    uint32_t min2 = uminA < umaxB ? uminA : umaxB;
    int c2 = memcmp(a->min_key, b->max_key, min2);
    if (c2 == 0) c2 = uminA < umaxB ? -1 : 1;
    if (c2 > 0) return false;

    return true;
}

static void lsmc_generate_filepath(char *buf, const char *dir, uint64_t file_id) {
    sprintf(buf, "%s/%06llu.sst", dir, (unsigned long long)file_id);
}

// --- N-Way Merge Heap ---
typedef struct {
    const char *key;  uint32_t key_len;
    const char *val;  uint32_t val_len;
    uint64_t seq;     uint8_t op_type;
    int iterator_idx;
} heap_node_t;

typedef struct {
    heap_node_t *data;
    size_t size;
    size_t capacity;
} min_heap_t;

static int lsmc_heap_cmp(const heap_node_t *a, const heap_node_t *b) {
    uint32_t a_user_len = get_user_key_len(a->key_len);
    uint32_t b_user_len = get_user_key_len(b->key_len);
    uint32_t min_len = a_user_len < b_user_len ? a_user_len : b_user_len;
    int c = memcmp(a->key, b->key, min_len);
    if (c != 0) return c;
    if (a_user_len != b_user_len) return a_user_len < b_user_len ? -1 : 1;
    if (a->seq > b->seq) return -1;
    if (a->seq < b->seq) return 1;
    return 0;
}

static void lsmc_heap_push(min_heap_t *h, heap_node_t node) {
    h->data[h->size] = node;
    size_t i = h->size++;
    while (i > 0) {
        size_t parent = (i - 1) / 2;
        if (lsmc_heap_cmp(&h->data[i], &h->data[parent]) >= 0) break;
        heap_node_t tmp = h->data[i]; h->data[i] = h->data[parent]; h->data[parent] = tmp;
        i = parent;
    }
}

static heap_node_t lsmc_heap_pop(min_heap_t *h) {
    heap_node_t root = h->data[0];
    h->data[0] = h->data[--h->size];
    size_t i = 0;
    while (true) {
        size_t left = 2 * i + 1, right = 2 * i + 2, smallest = i;
        if (left < h->size && lsmc_heap_cmp(&h->data[left], &h->data[smallest]) < 0) smallest = left;
        if (right < h->size && lsmc_heap_cmp(&h->data[right], &h->data[smallest]) < 0) smallest = right;
        if (smallest == i) break;
        heap_node_t tmp = h->data[i]; h->data[i] = h->data[smallest]; h->data[smallest] = tmp;
        i = smallest;
    }
    return root;
}

/* --------------------------------------------------------------------------
 * MVCC & Version Control
 * -------------------------------------------------------------------------- */

lsm_manifest_t *lsmc_manifest_init(const char *db_directory) {
    lsm_manifest_t *m = aml_zalloc(sizeof(lsm_manifest_t));
    m->db_directory = aml_strdup(db_directory);
    m->next_file_id = 1;
    pthread_mutex_init(&m->version_mutex, NULL);

    lsm_version_t *v0 = aml_zalloc(sizeof(lsm_version_t));
    v0->ref_count = 1;
    for (int i = 0; i < MAX_LEVELS; i++) {
        v0->levels[i].level_num = i;
        v0->levels[i].files_capacity = 16;
        v0->levels[i].files = aml_malloc(16 * sizeof(sstable_meta_t*));
    }
    m->current_version = v0;
    return m;
}

lsm_version_t *lsmc_version_retain(lsm_manifest_t *manifest) {
    pthread_mutex_lock(&manifest->version_mutex);
    lsm_version_t *v = manifest->current_version;
    v->ref_count++;
    pthread_mutex_unlock(&manifest->version_mutex);
    return v;
}

void lsmc_version_release(lsm_manifest_t *manifest, lsm_version_t *v) {
    pthread_mutex_lock(&manifest->version_mutex);
    v->ref_count--;

    // GARBAGE COLLECTION: If no queries are reading this version, destroy it!
    if (v->ref_count == 0) {
        for (int i = 0; i < MAX_LEVELS; i++) {
            for (size_t f = 0; f < v->levels[i].num_files; f++) {
                sstable_meta_t *meta = v->levels[i].files[f];
                meta->ref_count--;

                // If no versions reference this file, physically delete it from disk!
                if (meta->ref_count == 0) {
                    char path[512];
                    lsmc_generate_filepath(path, manifest->db_directory, meta->file_id);
                    unlink(path);
                    aml_free(meta);
                }
            }
            aml_free(v->levels[i].files);
        }
        aml_free(v);
    }
    pthread_mutex_unlock(&manifest->version_mutex);
}

bool lsmc_version_edit(lsm_manifest_t *manifest, int source_level, int target_level,
                       sstable_meta_t **deleted_files, size_t num_deleted,
                       sstable_meta_t **added_files, size_t num_added) {

    // (In a real DB, you'd append this edit to the MANIFEST file on disk here)

    pthread_mutex_lock(&manifest->version_mutex);

    // Create the new snapshot
    lsm_version_t *v = aml_zalloc(sizeof(lsm_version_t));
    v->ref_count = 1;

    lsm_version_t *curr = manifest->current_version;

    for (int i = 0; i < MAX_LEVELS; i++) {
        v->levels[i].level_num = i;
        size_t cap = curr->levels[i].num_files + num_added;
        v->levels[i].files_capacity = cap < 16 ? 16 : cap;
        v->levels[i].files = aml_malloc(v->levels[i].files_capacity * sizeof(sstable_meta_t*));

        // Copy files from previous version
        for (size_t f = 0; f < curr->levels[i].num_files; f++) {
            sstable_meta_t *meta = curr->levels[i].files[f];

            // Skip files that were compacted/deleted!
            bool is_deleted = false;
            if (i == source_level || i == target_level) {
                for (size_t d = 0; d < num_deleted; d++) {
                    if (deleted_files[d]->file_id == meta->file_id) { is_deleted = true; break; }
                }
            }
            if (!is_deleted) {
                v->levels[i].files[v->levels[i].num_files++] = meta;
                meta->ref_count++;
            }
        }

        // Apply new additions
        if (i == target_level) {
            for (size_t a = 0; a < num_added; a++) {
                sstable_meta_t *meta = added_files[a];
                meta->ref_count = 1;
                v->levels[i].files[v->levels[i].num_files++] = meta;
            }
            // Sort L1-L6 by key
            if (i > 0) {
                qsort(v->levels[i].files, v->levels[i].num_files, sizeof(sstable_meta_t*), lsmc_meta_cmp);
            }
        }
    }

    // Atomic Swap
    lsm_version_t *old_version = manifest->current_version;
    manifest->current_version = v;

    pthread_mutex_unlock(&manifest->version_mutex);

    // Release the system's hold on the old version
    lsmc_version_release(manifest, old_version);
    return true;
}

/* --------------------------------------------------------------------------
 * The Compactor
 * -------------------------------------------------------------------------- */

bool lsmc_compact_level(lsm_manifest_t *manifest, int source_level) {
    if (source_level >= MAX_LEVELS - 1) return false;

    // 1. PIN the state! We can read these files safely without locking!
    lsm_version_t *v = lsmc_version_retain(manifest);

    lsm_level_t *L_source = &v->levels[source_level];
    lsm_level_t *L_target = &v->levels[source_level + 1];

    if (L_source->num_files == 0) {
        lsmc_version_release(manifest, v);
        return false;
    }

    sstable_meta_t *picked_file = L_source->files[0];
    sstable_meta_t **merge_inputs = aml_malloc((1 + L_target->num_files) * sizeof(sstable_meta_t*));
    size_t num_inputs = 0;
    merge_inputs[num_inputs++] = picked_file;

    for (size_t i = 0; i < L_target->num_files; i++) {
        if (lsmc_check_overlap(picked_file, L_target->files[i])) {
            merge_inputs[num_inputs++] = L_target->files[i];
        }
    }

    sstable_reader_t **readers = aml_malloc(num_inputs * sizeof(sstable_reader_t*));
    sstable_iter_t **iters = aml_malloc(num_inputs * sizeof(sstable_iter_t*));
    char filepath[512];

    min_heap_t heap = { .data = aml_malloc(num_inputs * sizeof(heap_node_t)), .size = 0, .capacity = num_inputs };

    for (size_t i = 0; i < num_inputs; i++) {
        lsmc_generate_filepath(filepath, manifest->db_directory, merge_inputs[i]->file_id);
        readers[i] = sstable_reader_init(filepath);
        if (!readers[i]) {
            for (size_t j = 0; j < i; j++) { sstable_iter_destroy(iters[j]); sstable_reader_destroy(readers[j]); }
            aml_free(merge_inputs); aml_free(readers); aml_free(iters); aml_free(heap.data);
            lsmc_version_release(manifest, v);
            return false;
        }

        iters[i] = sstable_iter_init(readers[i]);
        if (sstable_iter_next(iters[i])) {
            heap_node_t node;
            sstable_iter_get_kv(iters[i], &node.key, &node.key_len, &node.val, &node.val_len);
            sstable_iter_get_meta(iters[i], &node.seq, &node.op_type);
            node.iterator_idx = i;
            lsmc_heap_push(&heap, node);
        }
    }

    /* 3. Execute N-Way Merge to New Files */
    sstable_meta_t **new_files = aml_malloc(1024 * sizeof(sstable_meta_t*));
    size_t num_new_files = 0;
    sstable_builder_t *builder = NULL;

    // Atomically grab new file IDs
    pthread_mutex_lock(&manifest->version_mutex);
    uint64_t current_out_id = manifest->next_file_id++;
    pthread_mutex_unlock(&manifest->version_mutex);

    char last_user_key[MAX_KEY_SIZE];
    uint32_t last_user_key_len = 0;
    bool is_bottom_level = (source_level + 1 == MAX_LEVELS - 1);

    while (heap.size > 0) {
        heap_node_t top = lsmc_heap_pop(&heap);
        uint32_t top_user_len = get_user_key_len(top.key_len);
        bool same_user_key = (last_user_key_len == top_user_len && memcmp(last_user_key, top.key, top_user_len) == 0);

        if (!same_user_key) {
            bool drop = (top.op_type == 1 && is_bottom_level);

            if (!drop) {
                if (builder == NULL) {
                    lsmc_generate_filepath(filepath, manifest->db_directory, current_out_id);
                    builder = sstable_builder_init(filepath, 20000);
                    new_files[num_new_files] = aml_zalloc(sizeof(sstable_meta_t));
                    new_files[num_new_files]->file_id = current_out_id;
                    memcpy(new_files[num_new_files]->min_key, top.key, top.key_len);
                    new_files[num_new_files]->min_key_len = top.key_len;
                }

                sstable_builder_add(builder, top.key, top.key_len, top.val, top.val_len);
                memcpy(new_files[num_new_files]->max_key, top.key, top.key_len);
                new_files[num_new_files]->max_key_len = top.key_len;
                new_files[num_new_files]->num_entries++;

                memcpy(last_user_key, top.key, top_user_len);
                last_user_key_len = top_user_len;

                if (sstable_builder_current_size(builder) >= TARGET_FILE_SIZE) {
                    new_files[num_new_files]->file_size = sstable_builder_finish(builder);
                    builder = NULL;
                    num_new_files++;

                    pthread_mutex_lock(&manifest->version_mutex);
                    current_out_id = manifest->next_file_id++;
                    pthread_mutex_unlock(&manifest->version_mutex);
                }
            }
        }

        int idx = top.iterator_idx;
        if (sstable_iter_next(iters[idx])) {
            heap_node_t next_node;
            sstable_iter_get_kv(iters[idx], &next_node.key, &next_node.key_len, &next_node.val, &next_node.val_len);
            sstable_iter_get_meta(iters[idx], &next_node.seq, &next_node.op_type);
            next_node.iterator_idx = idx;
            lsmc_heap_push(&heap, next_node);
        }
    }

    if (builder != NULL) {
        new_files[num_new_files]->file_size = sstable_builder_finish(builder);
        num_new_files++;
    }

    for (size_t i = 0; i < num_inputs; i++) {
        sstable_iter_destroy(iters[i]);
        sstable_reader_destroy(readers[i]);
    }

    // 4. ATOMIC SWAP: Publish the new state to the database!
    lsmc_version_edit(manifest, source_level, source_level + 1, merge_inputs, num_inputs, new_files, num_new_files);

    // 5. Unpin our old snapshot. If nobody else is reading those old files, they will be deleted instantly!
    lsmc_version_release(manifest, v);

    aml_free(merge_inputs); aml_free(readers); aml_free(iters); aml_free(heap.data); aml_free(new_files);
    return true;
}
