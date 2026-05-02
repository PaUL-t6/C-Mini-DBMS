#ifndef TRANSACTION_H
#define TRANSACTION_H

/* ================================================================
 *  transaction.h  –  Minimal write-ahead transaction log
 *
 *  Role in the system
 *  ------------------
 *  The transaction layer sits between the REPL (main.c) and the
 *  executor (executor.c).  It intercepts write operations when a
 *  transaction is active and holds them in a log until the user
 *  issues COMMIT or ROLLBACK:
 *
 *    User input
 *        │
 *    main.c REPL
 *        │
 *        ├─ BEGIN / COMMIT / ROLLBACK  ──→  transaction.c
 *        │
 *        ├─ [tx active] write query   ──→  tx_buffer()    (log only)
 *        │
 *        └─ [tx active] read / DDL    ──→  executeQuery() (immediate)
 *           [no tx]     any query     ──→  executeQuery() (immediate)
 *
 *  Write-Ahead Log (WAL) pattern
 *  ------------------------------
 *  Each write is stored as a raw SQL string in the log array
 *  *before* being applied to the database.  This mirrors the core
 *  principle of PostgreSQL WAL and SQLite journaling:
 *
 *    COMMIT   – replay every buffered SQL through parseQuery →
 *               executeQuery in original order.  The database only
 *               sees the writes after a successful COMMIT.
 *
 *    ROLLBACK – discard the log without touching the database.
 *               No undo work is needed because writes were never
 *               applied in the first place.
 *
 *  Auto-commit mode (no BEGIN issued)
 *  ------------------------------------
 *  When no transaction is active every query goes straight to
 *  executeQuery() exactly as before — zero behavioural change to
 *  existing non-transactional code paths.
 *
 *  What is buffered vs what is immediate
 *  ---------------------------------------
 *  Buffered  (DML – modifies data):
 *    INSERT, DELETE WHERE, UPDATE
 *
 *  Always immediate  (reads + DDL):
 *    SELECT / SELECT WHERE / SELECT WHERE age / COUNT(*)
 *      reads have no side effects; buffering them is meaningless.
 *    CREATE TABLE
 *      DDL must execute immediately so subsequent INSERTs in the
 *      same transaction have a table to write into.
 *    EXPLAIN
 *      meta-command; no data modification.
 *
 *  Capacity
 *  ---------
 *  TX_LOG_CAPACITY  max ops bufferable per transaction (128)
 *  TX_SQL_MAX_LEN   max length of one SQL string       (256)
 *
 *  Exceeding TX_LOG_CAPACITY triggers an automatic rollback.
 * ================================================================ */

#include "parser.h"          /* QueryType, Query, parseQuery()  */
#include "query_executor.h"  /* Database,  executeQuery()       */


/* ---- Compile-time limits ---------------------------------------- */
#define TX_LOG_CAPACITY  128   /* max buffered ops per transaction */
#define TX_SQL_MAX_LEN   256   /* max length of one SQL string     */


/* ----------------------------------------------------------------
 * TxEntry  –  one buffered write operation
 *
 * We store the raw SQL string (not a parsed Query struct) so that
 * COMMIT re-runs the full parseQuery → executeQuery pipeline.
 * This keeps the log human-readable and structurally identical to
 * a real on-disk WAL file.
 * ---------------------------------------------------------------- */
typedef struct {
    char sql[TX_SQL_MAX_LEN];
} TxEntry;


/* ----------------------------------------------------------------
 * TxLog  –  the transaction context  (stack-allocated in main.c)
 *
 * active  : 1 while a transaction is open (after BEGIN, before
 *           COMMIT/ROLLBACK); 0 in auto-commit mode.
 * entries : array of buffered SQL strings.
 * count   : number of valid entries  [0 … TX_LOG_CAPACITY].
 * ---------------------------------------------------------------- */
typedef struct {
    int     active;
    TxEntry entries[TX_LOG_CAPACITY];
    int     count;
} TxLog;


/* ----------------------------------------------------------------
 * tx_init
 * Zero-initialise *log.  Call once before the REPL starts.
 * ---------------------------------------------------------------- */
void tx_init(TxLog *log);


/* ----------------------------------------------------------------
 * tx_begin
 * Open a new transaction.
 *   0   success
 *  -1   a transaction is already active (nested BEGIN rejected)
 * ---------------------------------------------------------------- */
int tx_begin(TxLog *log);


/* ----------------------------------------------------------------
 * tx_buffer
 * Copy `sql` into the next log entry.
 *   0   success
 *  -1   log is full; transaction automatically rolled back
 * ---------------------------------------------------------------- */
int tx_buffer(TxLog *log, const char *sql);


/* ----------------------------------------------------------------
 * tx_commit
 * Replay every buffered entry through parseQuery + executeQuery.
 * Clears the log and closes the transaction on completion.
 *   0   all entries applied successfully
 *  -1   one or more entries failed (details printed per entry)
 * ---------------------------------------------------------------- */
int tx_commit(TxLog *log, Database *db);


/* ----------------------------------------------------------------
 * tx_rollback
 * Discard all buffered entries.  Database is never touched.
 * Safe to call with no active transaction (prints a notice).
 * ---------------------------------------------------------------- */
void tx_rollback(TxLog *log);


/* ----------------------------------------------------------------
 * tx_is_write
 * Return 1 if `type` is a DML write that must be buffered;
 * return 0 for reads and DDL (execute immediately).
 *
 *   Buffered   : QUERY_INSERT, QUERY_DELETE_WHERE, QUERY_UPDATE
 *   Immediate  : all other QueryType values
 * ---------------------------------------------------------------- */
int tx_is_write(QueryType type);


/* ----------------------------------------------------------------
 * tx_is_active  (inline accessor for the REPL routing decision)
 * Return 1 if a transaction is currently open, 0 otherwise.
 * ---------------------------------------------------------------- */
static inline int tx_is_active(const TxLog *log)
{
    return (log && log->active) ? 1 : 0;
}

#endif /* TRANSACTION_H */