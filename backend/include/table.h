#ifndef TABLE_H
#define TABLE_H

/* ============================================================
 *  table.h  –  A named collection of Records
 *
 *  Internals
 *  ---------
 *  records      : dynamically-grown array of Record pointers
 *  record_count : number of records currently stored
 *  capacity     : current allocated slots in `records`
 *
 *  Growth policy: capacity doubles whenever the array is full
 *  (classic amortised O(1) append).
 * ============================================================ */

#include "record.h"
#include "schema.h"

#define MAX_TABLE_NAME_LEN  64
#define TABLE_INITIAL_CAP    8   /* starting capacity for the records array */

typedef struct {
    char            name[MAX_TABLE_NAME_LEN];
    Schema         *schema;        /* column definitions (owned)         */
    GenericRecord **gen_records;    /* heap-allocated array               */
    int             gen_count;     /* number of valid entries            */
    int             gen_capacity;  /* total allocated slots              */

    /* Generic Indexes */
    void           *primary_hash;  /* HashTable* for primary key column  */
    void           *primary_bp;    /* BPTree* for primary key column     */
    int             primary_col_idx; /* which column is the primary key  */
} Table;

/* ---- Generic table functions ---- */

/* Allocate a generic (schema-defined) table.
 * The table takes ownership of `schema` — do NOT free it yourself.
 * Returns NULL on allocation failure. */
Table *table_create_generic(const char *name, Schema *schema);

/* Insert a generic record into the table.
 * The table takes ownership of `r` — do NOT free it yourself.
 * Returns 0 on success, -1 on failure. */
int table_insert_generic(Table *t, GenericRecord *r);

/* Free the table and every record it owns. */
void table_free(Table *t);

#endif /* TABLE_H */