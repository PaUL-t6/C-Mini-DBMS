#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/table.h"
#include "../include/hashtable.h"
#include "../include/bptree.h"


/* ---- Generic table support ---- */

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
    if (!t || !r) return -1;

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

void table_free(Table *t)
{
    if (!t) return;

    for (int i = 0; i < t->gen_count; i++) {
        genrec_free(t->gen_records[i]);
    }
    free(t->gen_records);
    if (t->schema) schema_free(t->schema);
    if (t->primary_hash) freeHash((HashTable *)t->primary_hash);
    if (t->primary_bp)   freeBPTree((BPTree *)t->primary_bp);

    free(t);
}