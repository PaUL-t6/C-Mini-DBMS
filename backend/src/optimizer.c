#include <stdio.h>
#include <string.h>
#include <strings.h>

#include "../include/optimizer.h"
#include "../include/query_executor.h"
#include "../include/hashtable.h"
#include "../include/bptree.h"
#include "../include/table.h"
#include "../include/schema.h"

/* Helper to find a table in the database catalog */
static Table *opt_find_table(Database *db, const char *name)
{
    if (!db || !name) return NULL;
    if (db->table && strcasecmp(db->table->name, name) == 0) return db->table;
    for (int i = 0; i < db->tableCount; i++) {
        if (db->tables[i] && strcasecmp(db->tables[i]->name, name) == 0)
            return db->tables[i];
    }
    return NULL;
}





const char *planTypeName(PlanType pt)
{
    switch (pt) {
        case INDEX_HASH:   return "INDEX_HASH  ";
        case INDEX_BPTREE: return "INDEX_BPTREE";
        case TABLE_SCAN:   return "TABLE_SCAN  ";
        default:           return "UNKNOWN     ";
    }
}


ExecutionPlan chooseExecutionPlan(Query q, const void *dbPtr)
{
    Database *db = (Database *)dbPtr;
    ExecutionPlan plan;
    memset(&plan, 0, sizeof(plan));

    Table *t = opt_find_table(db, q.tableName);
    int N = t ? (t->is_generic ? t->gen_count : t->record_count) : 0;

    /* Default: scan */
    plan.planType = TABLE_SCAN;
    snprintf(plan.reason, sizeof(plan.reason), "Default strategy for %s", q.tableName);
    snprintf(plan.estimatedCost, sizeof(plan.estimatedCost), "O(N)");

    if (!t) return plan;

    /* ---- Rule 1: CREATE/INSERT/SELECT ALL/COUNT ---- */
    if (q.type == QUERY_CREATE) {
        snprintf(plan.reason, sizeof(plan.reason), "CREATE TABLE allocates structures — no data access");
        snprintf(plan.estimatedCost, sizeof(plan.estimatedCost), "O(1)");
        return plan;
    }
    if (q.type == QUERY_INSERT) {
        snprintf(plan.reason, sizeof(plan.reason), "INSERT appends to array and updates indexes");
        snprintf(plan.estimatedCost, sizeof(plan.estimatedCost), "O(1) amortised");
        return plan;
    }
    if (q.type == QUERY_SELECT) {
        snprintf(plan.reason, sizeof(plan.reason), "Full result set requested — linear scan of %d records", N);
        snprintf(plan.estimatedCost, sizeof(plan.estimatedCost), "O(N)");
        return plan;
    }
    if (q.type == QUERY_COUNT) {
        snprintf(plan.reason, sizeof(plan.reason), "COUNT(*) reads count field — O(1) metadata access");
        snprintf(plan.estimatedCost, sizeof(plan.estimatedCost), "O(1)");
        return plan;
    }

    /* ---- Rule 2: Filter Queries (WHERE) ---- */
    const char *col = q.column;
    if (col[0] == '\0') {
        /* Legacy fallback for missing column name in Query struct */
        if (q.type == QUERY_SELECT_WHERE || q.type == QUERY_DELETE_WHERE || q.type == QUERY_UPDATE)
            col = "id";
        else if (q.type == QUERY_SELECT_WHERE_AGE)
            col = "age";
    }

    HashTable *ht = NULL;
    BPTree    *bp = NULL;

    if (t->is_generic) {
        /* Check if column is the primary key */
        if (t->schema && strcasecmp(t->schema->columns[t->primary_col_idx].name, col) == 0) {
            ht = (HashTable *)t->primary_hash;
            bp = (BPTree *)t->primary_bp;
        }
    } else {
        /* Legacy hardcoded indexes */
        if (strcasecmp(col, "id") == 0) {
            ht = db->hashIndex;
            bp = db->bpIndex;
        } else if (strcasecmp(col, "age") == 0) {
            bp = db->ageIndex;
        }
    }

    /* Decision logic */
    if (ht || bp) {
        int matches = 0;
        int val = (q.type == QUERY_SELECT_WHERE_AGE) ? q.age : q.id;

        if (bp) {
            matches = countBPTreeMatches(bp, val);
        } else if (ht) {
            /* If only hash exists (rare in this DB), assume unique or scan */
            matches = searchHash(ht, val) ? 1 : 0;
        }

        if (ht && matches <= 1) {
            plan.planType = INDEX_HASH;
            snprintf(plan.reason, sizeof(plan.reason), "Unique %s — hash index gives O(1) direct lookup", col);
            snprintf(plan.estimatedCost, sizeof(plan.estimatedCost), "O(1)");
        } else if (bp) {
            plan.planType = INDEX_BPTREE;
            snprintf(plan.reason, sizeof(plan.reason), "%d record%s share %s=%d — B+ Tree leaf chain walk collects duplicates", 
                     matches, matches == 1 ? "" : "s", col, val);
            snprintf(plan.estimatedCost, sizeof(plan.estimatedCost), "O(log N)");
        }
    } else {
        plan.planType = TABLE_SCAN;
        snprintf(plan.reason, sizeof(plan.reason), "No index on %s — performing linear scan of %d records", col, N);
        snprintf(plan.estimatedCost, sizeof(plan.estimatedCost), "O(N)");
    }

    return plan;
}



void printExecutionPlan(ExecutionPlan plan, Query q)
{
   
    char clause[64] = {0};
    switch (q.type) {
        case QUERY_SELECT_WHERE:
            snprintf(clause, sizeof(clause), "WHERE id = %d", q.id);
            break;
        case QUERY_SELECT_WHERE_AGE:
            snprintf(clause, sizeof(clause), "WHERE age = %d", q.age);
            break;
        case QUERY_DELETE_WHERE:
            snprintf(clause, sizeof(clause), "DELETE WHERE id = %d", q.id);
            break;
        case QUERY_UPDATE:
            snprintf(clause, sizeof(clause), "UPDATE SET age=%d WHERE id=%d",
                     q.newAge, q.id);
            break;
        case QUERY_COUNT:
            snprintf(clause, sizeof(clause), "COUNT(*)");
            break;
        case QUERY_INSERT:
            snprintf(clause, sizeof(clause), "INSERT id=%d", q.id);
            break;
        default:
            snprintf(clause, sizeof(clause), "%s", q.tableName);
            break;
    }

    printf("[optimizer] Plan: %s | Cost: %-14s | %s\n",
           planTypeName(plan.planType),
           plan.estimatedCost,
           clause);
    printf("[optimizer] Reason: %s\n", plan.reason);
}