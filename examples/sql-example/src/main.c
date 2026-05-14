// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* The LSM Storage Engine */
#include "a-table-store-library/lsm_db.h"

/* The SQL Execution Engine */
#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_tokenizer.h"
#include "sql-parser-library/sql_query.h"
#include "sql-parser-library/sql_vm.h"

/* --------------------------------------------------------------------------
 * Schema Definition: How data is packed into the LSM byte array
 * -------------------------------------------------------------------------- */
typedef struct {
    int id;
    int age;
    char name[64];
} UserRecord;

/* --------------------------------------------------------------------------
 * SQL -> LSM Binding Callbacks
 * -------------------------------------------------------------------------- */

typedef struct {
    lsm_db_iter_t *iterator;
    void *current_val_ptr;
} sql_lsm_stream_t;

static bool lsm_dataset_next(sql_dataset_t *ds, void **out_row) {
    sql_lsm_stream_t *stream = (sql_lsm_stream_t *)ds->stream_state;

    const void *key, *val;
    uint32_t klen, vlen;

    if (lsm_db_iter_next(stream->iterator, &key, &klen, &val, &vlen)) {
        stream->current_val_ptr = (void *)val;
        *out_row = stream->current_val_ptr;
        return true;
    }
    return false;
}

static void lsm_dataset_rewind(sql_dataset_t *ds) { }

static void lsm_dataset_close(sql_dataset_t *ds) {
    sql_lsm_stream_t *stream = (sql_lsm_stream_t *)ds->stream_state;
    lsm_db_iter_destroy(stream->iterator);
}

static void *lsm_clone_row(sql_dataset_t *ds, void *row, aml_pool_t *pool) {
    UserRecord *r = (UserRecord *)row;
    UserRecord *copy = aml_pool_alloc(pool, sizeof(UserRecord));
    memcpy(copy, r, sizeof(UserRecord));
    return copy;
}

static sql_dataset_t *my_fetch_table(sql_vm_t *vm, const char *table_name) {
    lsm_db_t *db = (lsm_db_t *)vm->user_data;

    if (strcmp(table_name, "users") == 0) {
        sql_lsm_stream_t *stream = aml_pool_zalloc(vm->pool, sizeof(sql_lsm_stream_t));
        stream->iterator = lsm_db_iter_init(db);

        sql_dataset_t *ds = sql_vm_create_streaming_dataset(
            vm, stream, lsm_dataset_next, lsm_dataset_rewind, lsm_dataset_close
        );
        ds->table_name = "users";
        ds->clone_row = lsm_clone_row;
        return ds;
    }
    return NULL;
}

/* --- Corrected Column Extractors --- */

static sql_node_t *extract_id(sql_ctx_t *ctx, sql_node_t *f) {
    /* Cast ctx->row to an array of pointers, then index into our table */
    void **row_array = (void **)ctx->row;
    UserRecord *r = (UserRecord *)row_array[f->column->table_index];

    return sql_int_init(ctx, r->id, false);
}

static sql_node_t *extract_age(sql_ctx_t *ctx, sql_node_t *f) {
    void **row_array = (void **)ctx->row;
    UserRecord *r = (UserRecord *)row_array[f->column->table_index];

    return sql_int_init(ctx, r->age, false);
}

static sql_node_t *extract_name(sql_ctx_t *ctx, sql_node_t *f) {
    void **row_array = (void **)ctx->row;
    UserRecord *r = (UserRecord *)row_array[f->column->table_index];

    return sql_string_init(ctx, r->name, false);
}

static sql_ctx_column_t *my_resolve_column(sql_vm_t *vm, const char *table_name, const char *column_name) {
    if (table_name && strcmp(table_name, "users") != 0) return NULL;

    sql_ctx_column_t *col = aml_pool_zalloc(vm->pool, sizeof(sql_ctx_column_t));
    col->table_name = "users";

    if (strcmp(column_name, "id") == 0) {
        col->name = "id"; col->type = SQL_TYPE_INT; col->func = extract_id;
        return col;
    } else if (strcmp(column_name, "age") == 0) {
        col->name = "age"; col->type = SQL_TYPE_INT; col->func = extract_age;
        return col;
    } else if (strcmp(column_name, "name") == 0) {
        col->name = "name"; col->type = SQL_TYPE_STRING; col->func = extract_name;
        return col;
    }
    return NULL;
}

/* --------------------------------------------------------------------------
 * The Main Entry Point
 * -------------------------------------------------------------------------- */

int main() {
    system("rm -rf /tmp/lsm_test && mkdir -p /tmp/lsm_test");

    lsm_db_t *db = lsm_db_open("/tmp/lsm_test", 64 * 1024 * 1024);

    UserRecord u1 = {1, 30, "Alice"};
    lsm_db_put(db, &u1.id, sizeof(u1.id), &u1, sizeof(UserRecord));

    UserRecord u2 = {2, 17, "Bob"};
    lsm_db_put(db, &u2.id, sizeof(u2.id), &u2, sizeof(UserRecord));

    UserRecord u3 = {3, 45, "Charlie"};
    lsm_db_put(db, &u3.id, sizeof(u3.id), &u3, sizeof(UserRecord));

    aml_pool_t *global_pool = aml_pool_init(1024 * 1024);
    sql_ctx_t ctx = { .pool = global_pool };
    sql_register_ctx(&ctx);

    sql_vm_t *vm = sql_vm_init(&ctx, my_fetch_table, my_resolve_column, db);

    /* Put the WHERE clause back in! It will work perfectly now. */
    const char *query_str = "SELECT name, age FROM users WHERE age >= 30 ORDER BY age DESC";
    printf("Executing SQL: %s\n\n", query_str);

    size_t token_count;
    sql_token_t **tokens = sql_tokenize(&ctx, query_str, &token_count);
    sql_select_t *ast = sql_parse_query(&ctx, tokens, token_count);

    sql_result_set_t *rs = sql_vm_execute(vm, ast);

    if (rs) {
        printf("--- Results (%zu rows) ---\n", rs->count);
        for (size_t i = 0; i < rs->count; i++) {
            sql_result_row_t row = rs->rows[i];

            const char *name = row.columns[0]->value.string_value;
            int age = row.columns[1]->value.int_value;

            printf("Name: %s | Age: %d\n", name, age);
        }
    } else {
        printf("Query failed or returned no results.\n");
    }

    lsm_db_close(db);
    aml_pool_destroy(global_pool);

    return 0;
}
