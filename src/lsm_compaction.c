// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-table-store-library/lsm_compaction.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Forward declaration for builder size checking */
extern uint64_t sstable_builder_current_size(sstable_builder_t *b);

/* --------------------------------------------------------------------------
 * Internal Key Unpacking Helper
 * -------------------------------------------------------------------------- */
#define INTERNAL_KEY_TRAILER_SIZE 8

static inline uint32_t get_user_key_len(uint32_t internal_key_len) {
    return internal_key_len > INTERNAL_KEY_TRAILER_SIZE ? internal_key_len - INTERNAL_KEY_TRAILER_SIZE : 0;
}

/* --------------------------------------------------------------------------
 * Overlap Detection & File Helpers
 * -------------------------------------------------------------------------- */

static int lsmc_meta_cmp(const void *a, const void *b) {
    sstable_meta_t *m1 = *(sstable_meta_t **)a;
    sstable_meta_t *m2 = *(sstable_meta_t **)b;

    /* Compare based on the User Key portion of the min_key */
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

/* --------------------------------------------------------------------------
 * Min-Heap (Priority Queue) Implementation for N-Way Merge
 * -------------------------------------------------------------------------- */

typedef struct {
    const char *key;  uint32_t key_len; /* This is the full Internal Key */
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
    /* Compare the USER KEY portion first */
    uint32_t a_user_len = get_user_key_len(a->key_len);
    uint32_t b_user_len = get_user_key_len(b->key_len);
    uint32_t min_len = a_user_len < b_user_len ? a_user_len : b_user_len;

    int c = memcmp(a->key, b->key, min_len);
    if (c != 0) return c;
    if (a_user_len != b_user_len) return a_user_len < b_user_len ? -1 : 1;

    /* User Keys are identical. Sort by SeqNum DESCENDING. */
    if (a->seq > b->seq) return -1;
    if (a->seq < b->seq) return 1;
    return 0;
}

static inline void lsmc_heap_swap(heap_node_t *a, heap_node_t *b) {
    heap_node_t tmp = *a; *a = *b; *b = tmp;
}

static void lsmc_heap_push(min_heap_t *h, heap_node_t node) {
    h->data[h->size] = node;
    size_t i = h->size++;
    while (i > 0) {
        size_t parent = (i - 1) / 2;
        if (lsmc_heap_cmp(&h->data[i], &h->data[parent]) >= 0) break;
        lsmc_heap_swap(&h->data[i], &h->data[parent]);
        i = parent;
    }
}

static heap_node_t lsmc_heap_pop(min_heap_t *h) {
    heap_node_t root = h->data[0];
    h->data[0] = h->data[--h->size];
    size_t i = 0;
    while (true) {
        size_t left = 2 * i + 1;
        size_t right = 2 * i + 2;
        size_t smallest = i;
        if (left < h->size && lsmc_heap_cmp(&h->data[left], &h->data[smallest]) < 0) smallest = left;
        if (right < h->size && lsmc_heap_cmp(&h->data[right], &h->data[smallest]) < 0) smallest = right;
        if (smallest == i) break;
        lsmc_heap_swap(&h->data[i], &h->data[smallest]);
        i = smallest;
    }
    return root;
}

/* --------------------------------------------------------------------------
 * Public API & Execution
 * -------------------------------------------------------------------------- */

lsm_manifest_t *lsmc_manifest_init(const char *db_directory) {
    lsm_manifest_t *manifest = calloc(1, sizeof(lsm_manifest_t));
    manifest->db_directory = db_directory;
    manifest->next_file_id = 1;

    for (int i = 0; i < MAX_LEVELS; i++) {
        manifest->levels[i].level_num = i;
        manifest->levels[i].files_capacity = 16;
        manifest->levels[i].files = malloc(16 * sizeof(sstable_meta_t*));
    }
    return manifest;
}

static bool lsmc_manifest_log_edit(lsm_manifest_t *manifest, int source_level,
                                   sstable_meta_t **deleted_files, size_t num_deleted,
                                   sstable_meta_t **added_files, size_t num_added) {

    char manifest_path[512];
    snprintf(manifest_path, sizeof(manifest_path), "%s/MANIFEST", manifest->db_directory);

    FILE *f = fopen(manifest_path, "ab");
    if (!f) return false;

    uint8_t edit_marker = 0x01;
    fwrite(&edit_marker, 1, 1, f);

    uint32_t ndel = (uint32_t)num_deleted;
    fwrite(&ndel, sizeof(uint32_t), 1, f);

    for (size_t i = 0; i < num_deleted; i++) {
        int level = (i == 0) ? source_level : source_level + 1;
        fwrite(&level, sizeof(int), 1, f);
        fwrite(&deleted_files[i]->file_id, sizeof(uint64_t), 1, f);
    }

    uint32_t nadd = (uint32_t)num_added;
    fwrite(&nadd, sizeof(uint32_t), 1, f);

    int target_level = source_level + 1;
    for (size_t i = 0; i < num_added; i++) {
        sstable_meta_t *m = added_files[i];
        fwrite(&target_level, sizeof(int), 1, f);
        fwrite(&m->file_id, sizeof(uint64_t), 1, f);
        fwrite(&m->file_size, sizeof(uint64_t), 1, f);
        fwrite(&m->num_entries, sizeof(uint32_t), 1, f);

        fwrite(&m->min_key_len, sizeof(uint32_t), 1, f);
        fwrite(m->min_key, 1, m->min_key_len, f);

        fwrite(&m->max_key_len, sizeof(uint32_t), 1, f);
        fwrite(m->max_key, 1, m->max_key_len, f);
    }

    fflush(f);
    if (fsync(fileno(f)) != 0) {
        fclose(f);
        return false;
    }

    fclose(f);
    return true;
}

bool lsmc_compact_level(lsm_manifest_t *manifest, int source_level) {
    if (source_level >= MAX_LEVELS - 1) return false;
    lsm_level_t *L_source = &manifest->levels[source_level];
    lsm_level_t *L_target = &manifest->levels[source_level + 1];

    if (L_source->num_files == 0) return false;

    /* 1. Pick a file and find overlaps */
    sstable_meta_t *picked_file = L_source->files[0];

    sstable_meta_t **merge_inputs = malloc((1 + L_target->num_files) * sizeof(sstable_meta_t*));
    size_t num_inputs = 0;
    merge_inputs[num_inputs++] = picked_file;

    for (size_t i = 0; i < L_target->num_files; i++) {
        if (lsmc_check_overlap(picked_file, L_target->files[i])) {
            merge_inputs[num_inputs++] = L_target->files[i];
        }
    }

    /* 2. Init Iterators and Heap */
    sstable_reader_t **readers = malloc(num_inputs * sizeof(sstable_reader_t*));
    sstable_iter_t **iters = malloc(num_inputs * sizeof(sstable_iter_t*));
    char filepath[512];

    min_heap_t heap = { .data = malloc(num_inputs * sizeof(heap_node_t)), .size = 0, .capacity = num_inputs };

    for (size_t i = 0; i < num_inputs; i++) {
        lsmc_generate_filepath(filepath, manifest->db_directory, merge_inputs[i]->file_id);
        readers[i] = sstable_reader_init(filepath);
        iters[i] = sstable_iter_init(readers[i]);

        if (sstable_iter_next(iters[i])) {
            heap_node_t node;
            sstable_iter_get_kv(iters[i], &node.key, &node.key_len, &node.val, &node.val_len);
            sstable_iter_get_meta(iters[i], &node.seq, &node.op_type);
            node.iterator_idx = i;
            lsmc_heap_push(&heap, node);
        }
    }

    /* 3. Execute N-Way Merge */
    sstable_meta_t **new_files = malloc(1024 * sizeof(sstable_meta_t*));
    size_t num_new_files = 0;
    sstable_builder_t *builder = NULL;
    uint64_t current_out_id = manifest->next_file_id++;

    char last_user_key[MAX_KEY_SIZE];
    uint32_t last_user_key_len = 0;
    bool is_bottom_level = (source_level + 1 == MAX_LEVELS - 1);

    while (heap.size > 0) {
        heap_node_t top = lsmc_heap_pop(&heap);

        uint32_t top_user_len = get_user_key_len(top.key_len);
        bool same_user_key = false;

        /* Check if we've already written a newer version of this User Key */
        if (last_user_key_len == top_user_len) {
            if (memcmp(last_user_key, top.key, top_user_len) == 0) same_user_key = true;
        }

        if (!same_user_key) {
            bool drop = false;
            if (top.op_type == 1 && is_bottom_level) drop = true; // Drop tombstone

            if (!drop) {
                if (builder == NULL) {
                    lsmc_generate_filepath(filepath, manifest->db_directory, current_out_id);
                    builder = sstable_builder_init(filepath, 20000);

                    new_files[num_new_files] = calloc(1, sizeof(sstable_meta_t));
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
                    current_out_id = manifest->next_file_id++;
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

    /* 4. Cleanup & Journal */
    for (size_t i = 0; i < num_inputs; i++) {
        sstable_iter_destroy(iters[i]);
        sstable_reader_destroy(readers[i]);
    }

    bool log_success = lsmc_manifest_log_edit(manifest, source_level,
                                              merge_inputs, num_inputs,
                                              new_files, num_new_files);

    if (!log_success) {
        fprintf(stderr, "FATAL: Failed to sync MANIFEST. Halting compaction.\n");
        return false;
    }

    size_t write_idx = 0;
    for (size_t i = 0; i < L_source->num_files; i++) {
        if (L_source->files[i] != merge_inputs[0]) L_source->files[write_idx++] = L_source->files[i];
    }
    L_source->num_files = write_idx;

    write_idx = 0;
    for (size_t i = 0; i < L_target->num_files; i++) {
        bool to_remove = false;
        for (size_t j = 1; j < num_inputs; j++) {
            if (L_target->files[i] == merge_inputs[j]) { to_remove = true; break; }
        }
        if (!to_remove) L_target->files[write_idx++] = L_target->files[i];
    }
    L_target->num_files = write_idx;

    if (L_target->num_files + num_new_files > L_target->files_capacity) {
        L_target->files_capacity = (L_target->num_files + num_new_files) * 2;
        L_target->files = realloc(L_target->files, L_target->files_capacity * sizeof(sstable_meta_t*));
    }

    for (size_t i = 0; i < num_new_files; i++) {
        L_target->files[L_target->num_files++] = new_files[i];
    }

    qsort(L_target->files, L_target->num_files, sizeof(sstable_meta_t*), lsmc_meta_cmp);

    /* 5. Physical Delete */
    for (size_t i = 0; i < num_inputs; i++) {
        char del_path[512];
        lsmc_generate_filepath(del_path, manifest->db_directory, merge_inputs[i]->file_id);
        unlink(del_path);
        free(merge_inputs[i]);
    }

    free(merge_inputs);
    free(readers);
    free(iters);
    free(heap.data);
    free(new_files);

    return true;
}


