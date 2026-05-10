#ifndef OPTIMIZER_H
#define OPTIMIZER_H

/* ================================================================
 *  optimizer.h  -  Rule-based query execution planner
 *
 *  Role in the system
 *  ------------------
 *  The optimizer sits between the parser and the executor:
 *
 *    SQL string
 *        |
 *    parseQuery()        -> Query struct
 *        |
 *    chooseExecutionPlan() -> ExecutionPlan   <-- THIS MODULE
 *        |
 *    executeQuery()       uses plan to pick lookup strategy
 *
 *  What it decides
 *  ---------------
 *  For every incoming Query the optimizer picks one of three
 *  execution strategies and records its reasoning:
 *
 *    INDEX_HASH    Use the hash table index.
 *                  Chosen when: the WHERE clause filters on the
 *                  primary key (id), which the hash index maps
 *                  directly.  Expected cost: O(1) average.
 *
 *    INDEX_BPTREE  Use the B+ tree index.
 *                  Chosen when: the query needs ordered traversal
 *                  (range scans), or as an explicit fallback path
 *                  to show the B+ tree being exercised.
 *                  Expected cost: O(log n).
 *
 *    TABLE_SCAN    Iterate every record in the Table array.
 *                  Chosen when: no applicable index exists for the
 *                  filter column (e.g. WHERE age = X), or the query
 *                  retrieves all rows (SELECT *, COUNT(*)).
 *                  Expected cost: O(n).
 *
 *  Decision table (one row per QueryType)
 *  ---------------------------------------
 *  QUERY_SELECT            -> TABLE_SCAN    (full result set needed)
 *  QUERY_SELECT_WHERE      -> INDEX_HASH    (id equality, hash wins)
 *  QUERY_SELECT_WHERE_AGE  -> TABLE_SCAN    (no index on 'age')
 *  QUERY_COUNT             -> TABLE_SCAN    (must visit all rows)
 *  QUERY_DELETE_WHERE      -> INDEX_HASH    (locate by id, hash wins)
 *  QUERY_UPDATE            -> INDEX_HASH    (locate by id, hash wins)
 *  QUERY_INSERT            -> TABLE_SCAN    (no lookup needed)
 *  QUERY_CREATE            -> TABLE_SCAN    (no lookup needed)
 *
 *  Educational note
 *  ----------------
 *  Real optimizers estimate costs using table statistics (row
 *  counts, cardinality, histograms).  This optimizer uses only
 *  the query structure — which is sufficient and clear for an
 *  educational implementation.
 * ================================================================ */

#include "parser.h"

/* ----------------------------------------------------------------
 * PlanType  -  the three possible execution strategies
 * ---------------------------------------------------------------- */
typedef enum {
    INDEX_HASH,    /* O(1)     - hash index on primary key (id)    */
    INDEX_BPTREE,  /* O(log n) - B+ tree index on primary key (id) */
    TABLE_SCAN,    /* O(n)     - linear scan of the records array   */
    NESTED_LOOP    /* O(N*M)   - join of two tables via nested loops */
} PlanType;

/* ----------------------------------------------------------------
 * ExecutionPlan  -  what the optimizer returns
 *
 * planType  : which strategy to use
 * reason    : human-readable explanation (for logging / viva demo)
 * estimatedCost : symbolic cost label shown in the plan output
 * ---------------------------------------------------------------- */
typedef struct {
    PlanType    planType;
    char        reason[256];         /* fixed-size for snprintf safety     */
    char        estimatedCost[64];   /* e.g. "O(1)", "O(log n)", "O(n)"   */
} ExecutionPlan;

/* ----------------------------------------------------------------
 * chooseExecutionPlan
 *
 * Inspect the Query and return the best ExecutionPlan.
 *
 * Rules applied (in priority order):
 *   1. If the query filters on `id` with '=' -> INDEX_HASH
 *      (direct key lookup; hash table was built for exactly this)
 *   2. If the query needs sorted/range access  -> INDEX_BPTREE
 *      (B+ tree leaf chain enables ordered traversal)
 *   3. Otherwise                               -> TABLE_SCAN
 *      (no suitable index; iterate the records array)
 *
 * The returned struct is by value (no heap allocation).
 * ---------------------------------------------------------------- */
ExecutionPlan chooseExecutionPlan(Query q, const void *db);

/* ----------------------------------------------------------------
 * printExecutionPlan
 *
 * Print a formatted one-line summary of the chosen plan.
 * Called by the executor before running the query so the output
 * shows which strategy was selected.
 *
 * Example output:
 *   [optimizer] Plan: INDEX_HASH   | Cost: O(1)     | SELECT * FROM students WHERE id = 3
 * ---------------------------------------------------------------- */
void printExecutionPlan(ExecutionPlan plan, Query q);

/* Return a string label for a PlanType (for logging). */
const char *planTypeName(PlanType pt);

#endif /* OPTIMIZER_H */