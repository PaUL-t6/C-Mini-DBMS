#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/table.h"
#include "../include/hashtable.h"
#include "../include/bptree.h"
#include "../include/record.h"


Table *table_create(const char *name)
{
    Table *t = (Table *)malloc(sizeof(Table));
    if (!t) {
        fprintf(stderr, "[table] malloc failed in table_create\n");
        return NULL;
    }

    strncpy(t->name, name, MAX_TABLE_NAME_LEN - 1);
    t->name[MAX_TABLE_NAME_LEN - 1] = '\0';

    t->record_count = 0;
    t->capacity     = TABLE_INITIAL_CAP;

    t->records = (Record **)malloc(sizeof(Record *) * t->capacity);
    if (!t->records) {
        fprintf(stderr, "[table] malloc failed for records array\n");
        free(t);
        return NULL;
    }

    return t;
}


int table_insert(Table *t, Record *r)
{
    if (!t || !r) return -1;

    /* Grow the array if needed */
    if (t->record_count == t->capacity) {
        int new_cap       = t->capacity * 2;
        Record **new_arr  = (Record **)realloc(t->records,
                                               sizeof(Record *) * new_cap);
        if (!new_arr) {
            fprintf(stderr, "[table] realloc failed — insert aborted\n");
            return -1;
        }
        t->records  = new_arr;
        t->capacity = new_cap;
    }

    t->records[t->record_count] = r;
    t->record_count++;
    return 0;
}


Record *table_find_by_id(const Table *t, int id)
{
    if (!t) return NULL;

    for (int i = 0; i < t->record_count; i++) {
        if (t->records[i] && t->records[i]->id == id) {
            return t->records[i];
        }
    }
    return NULL;
}

int table_find_all_by_age(const Table *t, int age,
                          Record **results, int max_results)
{
    if (!t || !results || max_results <= 0) return 0;

    int count = 0;
    for (int i = 0; i < t->record_count && count < max_results; i++) {
        if (t->records[i] && t->records[i]->age == age)
            results[count++] = t->records[i];
    }
    return count;
}


void table_print_header(void)
{
    printf("  %-4s  %-20s  %-4s\n", "ID", "NAME", "AGE");
    printf("  %-4s  %-20s  %-4s\n", "----", "--------------------", "----");
}


void table_print_all(const Table *t)
{
    if (!t) return;

    /* Generic table — delegate to schema-aware printer */
    if (t->is_generic && t->schema) {
        schema_print_all(t->name, t->schema, t->gen_records, t->gen_count, NULL, 0);
        return;
    }

    /* Legacy table */
    printf("\nTable: %s  (%d row%s)\n",
           t->name,
           t->record_count,
           t->record_count == 1 ? "" : "s");

    table_print_header();

    for (int i = 0; i < t->record_count; i++) {
        record_print(t->records[i]);
    }
    printf("\n");
}

void table_free(Table *t)
{
    if (!t) return;

    if (t->is_generic) {
        for (int i = 0; i < t->gen_count; i++) {
            genrec_free(t->gen_records[i]);
        }
        free(t->gen_records);
        if (t->schema) schema_free(t->schema);
        if (t->primary_hash) freeHash((HashTable *)t->primary_hash);
        if (t->primary_bp)   freeBPTree((BPTree *)t->primary_bp);
    } else {
        /* Legacy mode: free the records array and all Records */
        for (int i = 0; i < t->record_count; i++) {
            record_free(t->records[i]);
        }
        free(t->records);
    }

    free(t);
}


int table_delete_by_id(Table *t, int id)
{
    if (!t) return -1;
    for (int i = 0; i < t->record_count; i++) {
        if (t->records[i]->id == id) {
            record_free(t->records[i]);
            /* Compact: shift everything after i left by one */
            for (int j = i; j < t->record_count - 1; j++)
                t->records[j] = t->records[j + 1];
            t->records[t->record_count - 1] = NULL;
            t->record_count--;
            return 0;
        }
    }
    return -1;   /* not found */
}



int table_update_age_by_id(Table *t, int id, int newAge)
{
    if (!t) return -1;
    for (int i = 0; i < t->record_count; i++) {
        if (t->records[i]->id == id) {
            t->records[i]->age = newAge;
            return 0;
        }
    }
    return -1;
}


/* ================================================================
 *  Generic table support (new)
 * ================================================================ */

Table *table_create_generic(const char *name, Schema *schema)
{
    if (!name || !schema) return NULL;

    Table *t = (Table *)calloc(1, sizeof(Table));
    if (!t) {
        fprintf(stderr, "[table] calloc failed in table_create_generic\n");
        return NULL;
    }

    strncpy(t->name, name, MAX_TABLE_NAME_LEN - 1);
    t->name[MAX_TABLE_NAME_LEN - 1] = '\0';

    t->is_generic   = 1;
    t->schema       = schema;
    t->gen_records  = (GenericRecord **)calloc(TABLE_INITIAL_CAP, sizeof(GenericRecord *));
    t->gen_count    = 0;
    t->gen_capacity = TABLE_INITIAL_CAP;

    /* Initialize indexes on the first column (default primary key) */
    t->primary_hash = (void *)createHashTable();
    t->primary_bp   = (void *)createTree();
    t->primary_col_idx = 0;

    return t;
}


int table_insert_generic(Table *t, GenericRecord *r)
{
    if (!t || !r || !t->is_generic) return -1;

    /* Grow the array if needed */
    if (t->gen_count == t->gen_capacity) {
        int new_cap = t->gen_capacity * 2;
        GenericRecord **new_arr = (GenericRecord **)realloc(
            t->gen_records, sizeof(GenericRecord *) * (size_t)new_cap);
        if (!new_arr) {
            fprintf(stderr, "[table] realloc failed for gen_records — insert aborted\n");
            return -1;
        }
        t->gen_records  = new_arr;
        t->gen_capacity = new_cap;
    }

    t->gen_records[t->gen_count] = r;
    t->gen_count++;
    return 0;
}