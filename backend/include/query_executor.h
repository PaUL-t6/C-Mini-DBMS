#ifndef QUERY_EXECUTOR_H
#define QUERY_EXECUTOR_H

/* ================================================================
 *  query_executor.h  –  Ties every component together
 *
 *  The executor is the "brain" of the DBMS.  It receives a
 *  parsed Query (from parser.h) and carries out the requested
 *  operation against the in-memory data structures built in
 *  previous phases:
 *
 *      Parser  →  Query struct  →  Executor  →  Table / HashTable / BPTree
 *
 *  Database  (the global context)
 *  --------------------------------
 *  A single Database struct holds one Table, one HashTable index,
 *  and one B+ Tree index.  All three are kept in sync on every
 *  INSERT so that either index can be used for lookups.
 *
 *  SELECT WHERE lookup order
 *  --------------------------
 *  1. Search the hash table  → O(1) average
 *  2. If not found, search the B+ Tree  → O(log n)
 *  (In a correct database both will agree; the two-step lookup
 *  demonstrates the purpose of each index structure.)
 *
 *  Return codes for executeQuery
 *  ------------------------------
 *   0  – success
 *  -1  – error (unknown query type, table not initialised, etc.)
 * ================================================================ */

#include "table.h"
#include "hashtable.h"
#include "bptree.h"
#include "parser.h"

/* Maximum number of user-defined (generic) tables in the catalog */
#define MAX_TABLES 16

/* ----------------------------------------------------------------
 * Database  –  the single in-memory database instance
 *
 * isInitialised  : set to 1 after CREATE TABLE succeeds (legacy);
 *                  INSERT and SELECT are rejected until then.
 *
 * tables[]       : catalog of generic (schema-defined) tables.
 *                  These are independent of the legacy table.
 * ---------------------------------------------------------------- */
typedef struct {
    /* Generic table catalog */
    Table     *tables[MAX_TABLES];  /* schema-defined tables              */
    int        tableCount;          /* number of generic tables           */
} Database;

/* Allocate and return an empty Database.
 * isInitialised is 0; all three pointers are NULL until
 * executeQuery handles a QUERY_CREATE command. */
Database *createDatabase(void);

/* Release the Table, both indexes, and the Database itself.
 * Safe to call even if the database was never initialised. */
void freeDatabase(Database *db);

/* Execute one parsed Query against the given Database.
 *
 * Commands and what they do:
 *
 *   QUERY_CREATE
 *     – Allocates Table, HashTable, and BPTree.
 *     – Sets db->isInitialised = 1.
 *     – Prints a confirmation message.
 *
 *   QUERY_INSERT
 *     – Creates a Record with record_create().
 *     – Inserts the Record pointer into all three structures.
 *       (Table takes ownership; indexes borrow the pointer.)
 *     – Prints a confirmation message.
 *
 *   QUERY_SELECT
 *     – Calls table_print_all() to dump every row.
 *
 *   QUERY_SELECT_WHERE
 *     – Searches hash index first  (O(1)).
 *     – Falls back to B+ tree search if hash misses (O(log n)).
 *     – Prints the matching row, or "Record not found".
 *
 *   QUERY_UNKNOWN
 *     – Prints an error and returns -1.
 *
 * Returns 0 on success, -1 on error. */
int executeQuery(Database *db, Query q);

/* Helper to find a table by name in the database catalog. */
Table *db_find_table(Database *db, const char *name);

#endif /* QUERY_EXECUTOR_H */