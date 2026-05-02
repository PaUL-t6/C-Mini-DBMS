#ifndef SCHEMA_H
#define SCHEMA_H

/* ================================================================
 *  schema.h  –  User-defined table schemas and generic records
 *
 *  This module enables tables with arbitrary columns instead of
 *  the hardcoded (id, name, age) schema.
 *
 *  A Schema describes the column layout of a table:
 *    - Column name  (e.g. "salary")
 *    - Column type  (COL_INT or COL_VARCHAR)
 *
 *  A GenericRecord stores one row of data for a schema-defined
 *  table.  Values are stored in a ColumnValue union array whose
 *  length matches the Schema's col_count.
 *
 *  GenericValues is a lightweight carrier used inside the Query
 *  struct to transport parsed INSERT values from the parser to
 *  the executor.
 * ================================================================ */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---- Limits --------------------------------------------------- */
#define MAX_COLUMNS      16    /* max columns per table             */
#define MAX_COL_NAME     32    /* max column name length             */
#define MAX_VARCHAR_LEN  256   /* max varchar value length           */
#define MAX_GEN_VALUES   16    /* max VALUES(...) items in INSERT    */

/* ----------------------------------------------------------------
 * ColumnType  –  supported data types
 * ---------------------------------------------------------------- */
typedef enum {
    COL_INT,          /* stored as int                              */
    COL_VARCHAR       /* stored as char[MAX_VARCHAR_LEN]            */
} ColumnType;

/* ----------------------------------------------------------------
 * ColumnDef  –  definition of a single column
 * ---------------------------------------------------------------- */
typedef struct {
    char       name[MAX_COL_NAME];
    ColumnType type;
} ColumnDef;

/* ----------------------------------------------------------------
 * Schema  –  complete column layout for a table
 *
 * Created at CREATE TABLE time, owned by the Table struct.
 * ---------------------------------------------------------------- */
typedef struct {
    ColumnDef  columns[MAX_COLUMNS];
    int        col_count;
} Schema;

/* ----------------------------------------------------------------
 * ColumnValue  –  one cell in a generic row
 * ---------------------------------------------------------------- */
typedef union {
    int  int_val;
    char str_val[MAX_VARCHAR_LEN];
} ColumnValue;

/* ----------------------------------------------------------------
 * GenericRecord  –  one row of a schema-defined table
 *
 * values[] is heap-allocated with col_count elements.
 * Ownership: the Table owns GenericRecords and frees them.
 * ---------------------------------------------------------------- */
typedef struct {
    ColumnValue *values;      /* array of col_count elements        */
    int          col_count;
} GenericRecord;

/* ----------------------------------------------------------------
 * GenericValues  –  temporary carrier for INSERT parsing
 *
 * The parser stores raw string values here; the executor converts
 * them to ColumnValue using the target table's Schema.
 * Heap-allocated by the parser, freed by the executor.
 * ---------------------------------------------------------------- */
typedef struct {
    char  vals[MAX_GEN_VALUES][MAX_VARCHAR_LEN];
    int   count;
} GenericValues;

/* ---- Schema functions ----------------------------------------- */

/* Allocate and return an empty Schema (col_count = 0).
 * Caller must free with schema_free(). */
Schema *schema_create(void);

/* Add a column definition to the schema.
 * Returns 0 on success, -1 if MAX_COLUMNS reached. */
int schema_add_column(Schema *s, const char *name, ColumnType type);

/* Free a heap-allocated Schema. */
void schema_free(Schema *s);

/* Deep-copy a Schema.  Returns a new heap-allocated Schema. */
Schema *schema_copy(const Schema *s);

/* Find column index by name. Returns -1 if not found. */
int schema_find_column(const Schema *s, const char *name);

/* Return the column name string for a ColumnType. */
const char *col_type_name(ColumnType t);

/* Parse a type name string ("int", "varchar") to ColumnType.
 * Returns 0 on success and sets *out.  Returns -1 on unknown type. */
int col_type_parse(const char *str, ColumnType *out);

/* ---- GenericRecord functions ---------------------------------- */

/* Allocate a GenericRecord with `col_count` value slots.
 * All values are zeroed.  Caller frees with genrec_free(). */
GenericRecord *genrec_create(int col_count);

/* Free a GenericRecord and its values array. */
void genrec_free(GenericRecord *r);

/* Print one generic record using the schema for formatting. */
void genrec_print(const GenericRecord *r, const Schema *s);

/* Print the column header + separator line for a schema. */
void schema_print_header(const Schema *s);

/* Print all generic records in a table-like format.
 * `records` is an array of `count` GenericRecord pointers. */
void schema_print_all(const char *tableName, const Schema *s,
                      GenericRecord **records, int count,
                      char (*selectCols)[MAX_COL_NAME], int selectColCount);

/* Merge two schemas into a new one. Caller must free. */
Schema *schema_merge(const Schema *s1, const Schema *s2);

/* Merge two records from different schemas into a new one. Caller must free. */
GenericRecord *genrec_merge(const GenericRecord *r1, const GenericRecord *r2,
                            const Schema *s1, const Schema *s2);

/* ---- GenericValues functions ---------------------------------- */

/* Allocate an empty GenericValues.  Caller must free(). */
GenericValues *genvals_create(void);

#endif /* SCHEMA_H */
