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
    char     name[MAX_TABLE_NAME_LEN];

    /* Legacy mode (is_generic == 0): fixed Record* array */
    Record **records;       /* heap-allocated array of Record pointers */
    int      record_count;  /* number of valid entries                 */
    int      capacity;      /* total allocated slots                   */

    /* Generic mode (is_generic == 1): schema-defined columns */
    int             is_generic;    /* 0 = legacy, 1 = generic           */
    Schema         *schema;        /* column definitions (owned)         */
    GenericRecord **gen_records;    /* heap-allocated array               */
    int             gen_count;     /* number of valid generic entries    */
    int             gen_capacity;  /* total allocated generic slots      */

    /* Generic Indexes */
    void           *primary_hash;  /* HashTable* for primary key column  */
    void           *primary_bp;    /* BPTree* for primary key column     */
    int             primary_col_idx; /* which column is the primary key  */
} Table;

/* Allocate and initialise an empty table with the given name.
 * Returns NULL on allocation failure. */
Table *table_create(const char *name);

/* Insert a record into the table.
 * The table takes ownership of the Record pointer —
 * do NOT free `r` yourself after a successful insert.
 * Returns  0 on success, -1 on failure. */
int table_insert(Table *t, Record *r);

/* Linear scan: return a pointer to the first Record whose id
 * matches, or NULL if not found.
 * The pointer is owned by the table; do not free it. */
Record *table_find_by_id(const Table *t, int id);

/* Linear scan: collect ALL records whose age equals `age`.
 * Results written into caller-supplied `results[]` (size max_results).
 * Returns the number of matches found.
 * Used as O(n) fallback when the age B+ tree index is unavailable. */
int table_find_all_by_age(const Table *t, int age,
                          Record **results, int max_results);

/* Print every record in insertion order. */
void table_print_all(const Table *t);

/* Print a formatted table header (column names + separator). */
void table_print_header(void);

/* Free the table and every Record it owns. */
void table_free(Table *t);

/* ---- Generic table functions ---------------------------------- */

/* Allocate a generic (schema-defined) table.
 * The table takes ownership of `schema` — do NOT free it yourself.
 * Returns NULL on allocation failure. */
Table *table_create_generic(const char *name, Schema *schema);

/* Insert a generic record into the table.
 * The table takes ownership of `r` — do NOT free it yourself.
 * Returns 0 on success, -1 on failure. */
int table_insert_generic(Table *t, GenericRecord *r);

/* Remove the record with the given id from the table's records array.
 * Also frees the Record itself (table owns it).
 * Returns  0 if found and removed, -1 if not found. */
int table_delete_by_id(Table *t, int id);

/* Update the age field of the record with the given id.
 * Returns  0 if found and updated, -1 if not found. */
int table_update_age_by_id(Table *t, int id, int newAge);

#endif /* TABLE_H */