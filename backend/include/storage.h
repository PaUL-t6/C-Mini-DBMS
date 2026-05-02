#ifndef STORAGE_H
#define STORAGE_H

/* ================================================================
 *  storage.h  -  Simple CSV disk persistence layer
 *
 *  Purpose
 *  -------
 *  Gives the DBMS durability: records survive process restarts.
 *  Each table is stored as a plain CSV file:
 *
 *    data/<tableName>.tbl
 *
 *  File format (one record per line, no header row):
 *
 *    id,name,age
 *    1,Alice,20
 *    2,Bob,22
 *    3,Charlie,19
 *
 *  Design rules
 *  ------------
 *  - The storage layer is ADDITIVE: it does not replace or modify
 *    any existing in-memory structure.
 *  - saveRecordToDisk()  is called after a successful in-memory
 *    insert so the file stays in sync.
 *  - loadTableFromDisk() is called once at CREATE TABLE time to
 *    replay persisted rows back into memory (Table + both indexes).
 *  - No locking, no transactions — educational simplicity only.
 *
 *  Directory
 *  ---------
 *  The "data/" directory is created automatically if it does not
 *  exist.  All .tbl files live inside it.
 * ================================================================ */

#include "table.h"
#include "hashtable.h"
#include "bptree.h"

/* Sub-directory (relative to the binary's working directory)
 * where .tbl files are stored.                                    */
#define STORAGE_DIR      "data"

/* Maximum path length for a .tbl file path                        */
#define STORAGE_PATH_LEN 256

/* ----------------------------------------------------------------
 * saveRecordToDisk
 *
 * Append one record as a CSV line to  data/<tableName>.tbl
 *
 * Called by executor.c immediately after a successful INSERT so
 * that every committed record is durably stored.
 *
 * Format written:   id,name,age\n
 *
 * Returns  0 on success.
 * Returns -1 if the file cannot be opened (warning is printed;
 *            the in-memory insert is NOT rolled back).
 * ---------------------------------------------------------------- */
int saveRecordToDisk(const char *tableName, const Record *r);

/* ----------------------------------------------------------------
 * loadTableFromDisk
 *
 * Read  data/<tableName>.tbl  (if it exists) and replay every
 * stored CSV line as an in-memory Record, inserting each one into:
 *   - the Table          (table_insert)
 *   - the hash index     (insertHash)
 *   - the B+ tree index  (insertBPTree)
 *
 * Called once by exec_create() so that a CREATE TABLE command
 * on an already-persisted table restores its data automatically.
 *
 * Silently succeeds (returns 0) if the file does not exist yet
 * (first-ever use of this table name).
 *
 * Returns  0 on success or file-not-found.
 * Returns -1 on a file I/O or parse error.
 * ---------------------------------------------------------------- */
int loadTableFromDisk(const char *tableName, Table *t,
                      HashTable *ht, BPTree *bp, BPTree *ageIdx);

/* ----------------------------------------------------------------
 * buildStoragePath
 *
 * Write the full path for a table's .tbl file into `out`.
 * Example: tableName="students" -> out="data/students.tbl"
 *
 * `out` must point to a buffer of at least STORAGE_PATH_LEN bytes.
 * ---------------------------------------------------------------- */
void buildStoragePath(const char *tableName, char *out);

/* ----------------------------------------------------------------
 * rewriteTableToDisk
 *
 * Overwrite data/<tableName>.tbl with all records currently in
 * the Table.  Used after DELETE or UPDATE to keep the file in sync
 * with in-memory state.
 *
 * Returns  0 on success.
 * Returns -1 on I/O failure.
 * ---------------------------------------------------------------- */
int rewriteTableToDisk(const char *tableName, const Table *t);

/* ----------------------------------------------------------------
 * saveGenericRecordToDisk
 *
 * Append one generic record as a CSV line to data/<tableName>.tbl
 * Returns 0 on success, -1 on failure.
 * ---------------------------------------------------------------- */
int saveGenericRecordToDisk(const char *tableName, const Schema *s, const GenericRecord *r);

/* ----------------------------------------------------------------
 * loadGenericTableFromDisk
 *
 * Load a generic table from disk, including schema detection.
 * Returns 0 on success, -1 on failure.
 * ---------------------------------------------------------------- */
int loadGenericTableFromDisk(const char *tableName, Table *t);

/* ----------------------------------------------------------------
 * rewriteGenericTableToDisk
 *
 * Overwrite data/<tableName>.tbl with all generic records.
 * Returns 0 on success, -1 on failure.
 * ---------------------------------------------------------------- */
int rewriteGenericTableToDisk(const char *tableName, const Table *t);

/* Bootstrap: Scan data directory and load all tables into the database */
void storage_bootstrap(void *dbPtr);

#endif /* STORAGE_H */