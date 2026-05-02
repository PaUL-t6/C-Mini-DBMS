#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/query_executor.h"
#include "../include/storage.h"     /* persistence: save/load .tbl files */
#include "../include/optimizer.h"   /* query planner: chooseExecutionPlan */
#include "../include/schema.h"     /* Schema, GenericRecord, GenericValues */



Database *createDatabase(void)
{
    Database *db = (Database *)calloc(1, sizeof(Database));
    if (!db) {
        fprintf(stderr, "[executor] calloc failed in createDatabase\n");
        return NULL;
    }
    /* calloc zeroes all fields:
     *   table = NULL, hashIndex = NULL, bpIndex = NULL,
     *   isInitialised = 0                                        */
    return db;
}


void freeDatabase(Database *db)
{
    if (!db) return;

    /* table_free() releases every owned Record AND the array */
    if (db->table)     table_free(db->table);

    /* freeHash() releases nodes only — Records already gone */
    if (db->hashIndex) freeHash(db->hashIndex);

    /* freeBPTree() releases nodes only — Records already gone */
    if (db->bpIndex)   freeBPTree(db->bpIndex);

    /* secondary age index — same ownership rule: borrows Record* */
    if (db->ageIndex)  freeBPTree(db->ageIndex);

    /* Free all generic tables in the catalog */
    for (int i = 0; i < db->tableCount; i++) {
        if (db->tables[i]) table_free(db->tables[i]);
    }

    free(db);
}


/* ----------------------------------------------------------------
 * db_find_table  –  look up a generic table by name
 * Returns the Table* or NULL if not found.
 * ---------------------------------------------------------------- */
static Table *db_find_table(Database *db, const char *name)
{
    if (!db || !name) return NULL;
    for (int i = 0; i < db->tableCount; i++) {
        if (db->tables[i] && strcasecmp(db->tables[i]->name, name) == 0)
            return db->tables[i];
    }
    return NULL;
}


static int exec_create(Database *db, Query q)
{
    Schema *schema = (Schema *)q.schema;

    /* ---- Generic table (schema provided) ---- */
    if (schema) {
        /* Check if a generic table with this name already exists */
        Table *existing = db_find_table(db, q.tableName);
        if (existing) {
            /* Remove and free the old one */
            for (int i = 0; i < db->tableCount; i++) {
                if (db->tables[i] == existing) {
                    table_free(db->tables[i]);
                    /* Compact the catalog */
                    for (int j = i; j < db->tableCount - 1; j++)
                        db->tables[j] = db->tables[j + 1];
                    db->tables[db->tableCount - 1] = NULL;
                    db->tableCount--;
                    break;
                }
            }
            printf("[executor] WARNING: re-creating generic table '%s'.\n",
                   q.tableName);
        }

        if (db->tableCount >= MAX_TABLES) {
            fprintf(stderr, "[executor] catalog full (%d tables max)\n", MAX_TABLES);
            schema_free(schema);
            return -1;
        }

        Table *t = table_create_generic(q.tableName, schema);
        if (!t) {
            fprintf(stderr, "[executor] failed to create generic table\n");
            schema_free(schema);
            return -1;
        }

        db->tables[db->tableCount++] = t;

        printf("[executor] Table '%s' created with %d column%s (",
               q.tableName, schema->col_count,
               schema->col_count == 1 ? "" : "s");
        for (int i = 0; i < schema->col_count; i++) {
            if (i > 0) printf(", ");
            printf("%s %s", schema->columns[i].name,
                   col_type_name(schema->columns[i].type));
        }
        printf(").\n");

        /* Load persisted data if available */
        loadGenericTableFromDisk(q.tableName, t);

        return 0;
    }

    /* ---- Legacy table (no schema) ---- */
    if (db->isInitialised) {
        printf("[executor] WARNING: re-creating table '%s' — all data lost.\n",
               db->table ? db->table->name : "?");
        if (db->table)     { table_free(db->table);     db->table     = NULL; }
        if (db->hashIndex) { freeHash(db->hashIndex);   db->hashIndex = NULL; }
        if (db->bpIndex)   { freeBPTree(db->bpIndex);   db->bpIndex   = NULL; }
        if (db->ageIndex)  { freeBPTree(db->ageIndex);  db->ageIndex  = NULL; }
        db->isInitialised = 0;
    }

    db->table = table_create(q.tableName);
    if (!db->table) {
        fprintf(stderr, "[executor] failed to create table\n");
        return -1;
    }

    db->hashIndex = createHashTable();
    if (!db->hashIndex) {
        table_free(db->table); db->table = NULL;
        fprintf(stderr, "[executor] failed to create hash index\n");
        return -1;
    }

    db->bpIndex = createTree();
    if (!db->bpIndex) {
        table_free(db->table);   db->table     = NULL;
        freeHash(db->hashIndex); db->hashIndex = NULL;
        fprintf(stderr, "[executor] failed to create B+ tree (id)\n");
        return -1;
    }

    db->ageIndex = createTree();
    if (!db->ageIndex) {
        table_free(db->table);   db->table     = NULL;
        freeHash(db->hashIndex); db->hashIndex = NULL;
        freeBPTree(db->bpIndex); db->bpIndex   = NULL;
        fprintf(stderr, "[executor] failed to create B+ tree (age)\n");
        return -1;
    }

    db->isInitialised = 1;
    printf("[executor] Table '%s' created (indexes: hash/id, bptree/id, bptree/age).\n",
           q.tableName);

    loadTableFromDisk(q.tableName, db->table, db->hashIndex,
                      db->bpIndex, db->ageIndex);

    return 0;
}


/* ----------------------------------------------------------------
 * exec_insert_generic  –  INSERT into a schema-defined table
 * ---------------------------------------------------------------- */
static int exec_insert_generic(Table *t, Query q)
{
    GenericValues *gv = (GenericValues *)q.genValues;
    if (!gv) {
        fprintf(stderr, "[executor] INSERT into generic table: no values\n");
        return -1;
    }

    Schema *schema = t->schema;
    if (gv->count != schema->col_count) {
        fprintf(stderr,
            "[executor] INSERT: expected %d values but got %d\n",
            schema->col_count, gv->count);
        return -1;
    }

    GenericRecord *r = genrec_create(schema->col_count);
    if (!r) return -1;

    for (int i = 0; i < schema->col_count; i++) {
        char *val = gv->vals[i];
        /* Simple quote stripping */
        int len = (int)strlen(val);
        if (len >= 2 && ((val[0] == '\'' && val[len-1] == '\'') || 
                        (val[0] == '"'  && val[len-1] == '"'))) {
            val[len-1] = '\0';
            val++;
        }

        if (schema->columns[i].type == COL_INT) {
            r->values[i].int_val = atoi(val);
        } else {
            strncpy(r->values[i].str_val, val, MAX_VARCHAR_LEN - 1);
            r->values[i].str_val[MAX_VARCHAR_LEN - 1] = '\0';
        }
    }


    if (table_insert_generic(t, r) != 0) {
        genrec_free(r);
        return -1;
    }

    /* Primary key index updates (First-in-Hash, All-in-Tree) */
    int pk_val = r->values[t->primary_col_idx].int_val;
    HashTable *ht = (HashTable *)t->primary_hash;
    BPTree    *bp = (BPTree *)t->primary_bp;

    if (ht && bp) {
        if (searchHash(ht, pk_val)) {
            printf("[executor] Note: duplicate %s=%d — stored in B+ Tree only (hash index holds first occurrence).\n",
                   schema->columns[t->primary_col_idx].name, pk_val);
        } else {
            insertHash(ht, pk_val, (Record *)r);
        }
        insertBPTree(bp, pk_val, (Record *)r);
    }

    /* Persist to disk */
    saveGenericRecordToDisk(t->name, t->schema, r);

    printf("[executor] Inserted record into '%s' (", t->name);
    for (int i = 0; i < schema->col_count; i++) {
        if (i > 0) printf(", ");
        if (schema->columns[i].type == COL_INT)
            printf("%s=%d", schema->columns[i].name, r->values[i].int_val);
        else
            printf("%s=%s", schema->columns[i].name, r->values[i].str_val);
    }
    printf(")\n");
    return 0;
}



static int exec_insert(Database *db, Query q)
{
    /* Step 1 – create the record */
    Record *r = record_create(q.id, q.name, q.age);
    if (!r) {
        fprintf(stderr, "[executor] record_create failed\n");
        return -1;
    }

    /* Step 2 – add to table (table now owns r) */
    if (table_insert(db->table, r) != 0) {
        record_free(r);   /* table didn't take it — we must free */
        fprintf(stderr, "[executor] table_insert failed\n");
        return -1;
    }

    /* Step 3 – add to hash index (borrows r) - First-in-Hash logic */
    if (searchHash(db->hashIndex, q.id)) {
        printf("[executor] Note: duplicate id=%d — stored in B+ Tree only (hash index holds first occurrence).\n", q.id);
    } else {
        if (insertHash(db->hashIndex, q.id, r) != 0) {
            fprintf(stderr, "[executor] insertHash failed\n");
            return -1;
        }
    }

    /* Step 4 – add to id B+ tree index (borrows r) */
    insertBPTree(db->bpIndex, q.id, r);

    /* Step 5 – add to age B+ tree index (borrows r)
     * Key = r->age so WHERE age = X queries hit this index.
     * Non-unique: many records may share the same age key. */
    insertBPTree(db->ageIndex, r->age, r);

    /* Step 6 – persist to disk: append one CSV line to data/<table>.tbl
     * Done LAST so the file only contains fully committed records.
     * A failure here prints a warning but does NOT abort the insert. */
    saveRecordToDisk(db->table->name, r);

    printf("[executor] Inserted record → id=%-4d  name=%-20s  age=%d\n",
           r->id, r->name, r->age);
    return 0;
}


static int exec_select(Database *db)
{
    table_print_all(db->table);
    return 0;
}


static int exec_select_where(Database *db, Query q, ExecutionPlan plan)
{
    Record     *found  = NULL;
    const char *method = NULL;

    if (plan.planType == INDEX_HASH) {
        /* --- Hash index: O(1) average --- */
        found = searchHash(db->hashIndex, q.id);
        if (found) {
            method = "hash index (O(1))";
        } else {
            /* Hash miss — fall through to B+ tree as safety net */
            found = searchBPTree(db->bpIndex, q.id);
            if (found) method = "B+ tree (O(log n)) [hash missed — fallback]";
        }

    } else if (plan.planType == INDEX_BPTREE) {
        /* --- B+ tree index: O(log n) --- */
        found = searchBPTree(db->bpIndex, q.id);
        if (found) {
            method = "B+ tree (O(log n))";
        } else {
            /* B+ tree miss — fall through to hash as safety net */
            found = searchHash(db->hashIndex, q.id);
            if (found) method = "hash index (O(1)) [B+ tree missed — fallback]";
        }

    } else {
        /* --- TABLE_SCAN: linear scan through the records array --- */
        found = table_find_by_id(db->table, q.id);
        if (found) method = "table scan (O(n))";
    }

    /* Output */
    if (found) {
        printf("[executor] Found via %s:\n", method);
        table_print_header();
        record_print(found);
        printf("\n");
        return 0;
    }

    printf("[executor] Record with id=%d not found.\n\n", q.id);
    return 0;
}



#define MAX_AGE_RESULTS 256

static int exec_select_where_age(Database *db, Query q)
{
    Record *results[MAX_AGE_RESULTS];
    int     count = 0;

    if (db->ageIndex) {
        count = searchAllBPTree(db->ageIndex, q.age,
                                results, MAX_AGE_RESULTS);
        if (count > 0)
            printf("[executor] Found %d record%s via age B+ tree (O(log n)):\n",
                   count, count == 1 ? "" : "s");
    } else {
        /* Fallback: O(n) linear scan */
        count = table_find_all_by_age(db->table, q.age,
                                      results, MAX_AGE_RESULTS);
        if (count > 0)
            printf("[executor] Found %d record%s via table scan (O(n)) "
                   "[no age index]:\n", count, count == 1 ? "" : "s");
    }

    if (count == 0) {
        printf("[executor] No records found with age=%d.\n\n", q.age);
        return 0;
    }

    table_print_header();
    for (int i = 0; i < count; i++)
        record_print(results[i]);
    printf("\n");
    return 0;
}

/* ----------------------------------------------------------------
 * exec_select_complex_generic – Handles GROUP BY, HAVING, and ORDER BY
 * ---------------------------------------------------------------- */

typedef struct {
    GenericRecord *r;
    int sortColIdx;
    ColumnType sortColType;
    int desc;
} SortItem;

static int compare_values(ColumnValue v1, ColumnValue v2, ColumnType type) {
    if (type == COL_INT) {
        if (v1.int_val < v2.int_val) return -1;
        if (v1.int_val > v2.int_val) return 1;
        return 0;
    } else {
        return strcmp(v1.str_val, v2.str_val);
    }
}

static int sort_item_cmp(const void *a, const void *b) {
    SortItem *ia = (SortItem *)a;
    SortItem *ib = (SortItem *)b;
    int res = compare_values(ia->r->values[ia->sortColIdx], ib->r->values[ib->sortColIdx], ia->sortColType);
    return ia->desc ? -res : res;
}

typedef struct {
    ColumnValue groupVal;
    int count;
} GroupData;

static int exec_select_complex_generic(Table *t, Query q, ExecutionPlan plan)
{
    GenericRecord *matched[1024];
    int matchedCount = 0;

    /* 1. Filtering (WHERE) */
    if (q.type == QUERY_SELECT_WHERE || q.type == QUERY_SELECT_WHERE_AGE) {
        const char *colName = q.column[0] ? q.column : "id";
        int colIdx = schema_find_column(t->schema, colName);
        if (colIdx < 0) {
            fprintf(stderr, "[executor] error: table '%s' has no column '%s'\n", t->name, colName);
            return -1;
        }
        int val = (q.type == QUERY_SELECT_WHERE_AGE) ? q.age : q.id;
        const char *op = q.whereOp[0] ? q.whereOp : "=";

        if (strcmp(op, "=") == 0 && plan.planType == INDEX_HASH) {
            GenericRecord *found = (GenericRecord *)searchHash((HashTable *)t->primary_hash, val);
            if (found) matched[matchedCount++] = found;
        } else if (strcmp(op, "=") == 0 && plan.planType == INDEX_BPTREE) {
            matchedCount = searchAllBPTree((BPTree *)t->primary_bp, val, (Record **)matched, 1024);
        } else {
            /* Table Scan for other operators or fallback */
            matchedCount = 0;
            for (int i = 0; i < t->gen_count && matchedCount < 1024; i++) {
                GenericRecord *r = t->gen_records[i];
                if (t->schema->columns[colIdx].type == COL_INT) {
                    int rVal = r->values[colIdx].int_val;
                    int ok = 0;
                    if (strcmp(op, "=") == 0) ok = (rVal == val);
                    else if (strcmp(op, ">") == 0) ok = (rVal > val);
                    else if (strcmp(op, "<") == 0) ok = (rVal < val);
                    else if (strcmp(op, ">=") == 0) ok = (rVal >= val);
                    else if (strcmp(op, "<=") == 0) ok = (rVal <= val);
                    else if (strcmp(op, "!=") == 0) ok = (rVal != val);
                    if (ok) matched[matchedCount++] = r;
                } else {
                    if (strcmp(op, "=") == 0 && strcmp(r->values[colIdx].str_val, q.column) == 0) 
                        matched[matchedCount++] = r;
                }
            }
        }
    } else {
        /* SELECT * (no WHERE) */
        matchedCount = t->gen_count > 1024 ? 1024 : t->gen_count;
        for (int i = 0; i < matchedCount; i++) matched[i] = t->gen_records[i];
    }

    if (matchedCount == 0) {
        printf("[executor] No records found.\n\n");
        return 0;
    }

    /* 2. Grouping (GROUP BY) */
    if (q.hasGroupBy) {
        int gColIdx = schema_find_column(t->schema, q.groupByCol);
        if (gColIdx < 0) {
            fprintf(stderr, "[executor] error: unknown group by column '%s'\n", q.groupByCol);
            return -1;
        }
        ColumnType gColType = t->schema->columns[gColIdx].type;

        /* Sort by grouping column first */
        SortItem *items = malloc(sizeof(SortItem) * matchedCount);
        for (int i = 0; i < matchedCount; i++) {
            items[i].r = matched[i];
            items[i].sortColIdx = gColIdx;
            items[i].sortColType = gColType;
            items[i].desc = 0;
        }
        qsort(items, matchedCount, sizeof(SortItem), sort_item_cmp);

        /* Form groups */
        GroupData groups[1024];
        int groupCount = 0;
        for (int i = 0; i < matchedCount; i++) {
            if (groupCount > 0 && compare_values(groups[groupCount-1].groupVal, items[i].r->values[gColIdx], gColType) == 0) {
                groups[groupCount-1].count++;
            } else {
                groups[groupCount].groupVal = items[i].r->values[gColIdx];
                groups[groupCount].count = 1;
                groupCount++;
            }
        }
        free(items);

        /* 3. Filtering groups (HAVING) */
        if (q.hasHaving) {
            int filteredCount = 0;
            for (int i = 0; i < groupCount; i++) {
                int ok = 0;
                
                /* If HAVING refers to the grouping column, filter by value.
                 * Otherwise, default to filtering by COUNT(*) for now. */
                if (strcasecmp(q.havingCol, q.groupByCol) == 0) {
                    int val = groups[i].groupVal.int_val; // Assuming INT for now
                    if (strcmp(q.havingOp, ">") == 0) ok = (val > q.havingValue);
                    else if (strcmp(q.havingOp, "<") == 0) ok = (val < q.havingValue);
                    else if (strcmp(q.havingOp, "=") == 0) ok = (val == q.havingValue);
                    else if (strcmp(q.havingOp, ">=") == 0) ok = (val >= q.havingValue);
                    else if (strcmp(q.havingOp, "<=") == 0) ok = (val <= q.havingValue);
                } else {
                    /* Default aggregate: COUNT(*) */
                    int val = groups[i].count;
                    if (strcmp(q.havingOp, ">") == 0) ok = (val > q.havingValue);
                    else if (strcmp(q.havingOp, "<") == 0) ok = (val < q.havingValue);
                    else if (strcmp(q.havingOp, "=") == 0) ok = (val == q.havingValue);
                    else if (strcmp(q.havingOp, ">=") == 0) ok = (val >= q.havingValue);
                    else if (strcmp(q.havingOp, "<=") == 0) ok = (val <= q.havingValue);
                }
                
                if (ok) groups[filteredCount++] = groups[i];
            }
            groupCount = filteredCount;
        }

        /* 4. Sorting groups (ORDER BY) - can only order by grouping column or COUNT(*) */
        /* For simplicity, if hasOrderBy, we just print a message if it's not the grouping column */
        /* In a real DBMS, you'd sort the groups array here. */

        /* 5. Print groups */
        printf("\nGrouped results for '%s' (by %s):\n", t->name, q.groupByCol);
        printf("%-20s | %-8s\n", q.groupByCol, "COUNT(*)");
        printf("------------------------------------\n");
        for (int i = 0; i < groupCount; i++) {
            if (gColType == COL_INT) printf("%-20d | %-8d\n", groups[i].groupVal.int_val, groups[i].count);
            else printf("%-20s | %-8d\n", groups[i].groupVal.str_val, groups[i].count);
        }
        printf("\n");
        return 0;
    }

    /* 3. Sorting (ORDER BY) - when NO Group By */
    if (q.hasOrderBy) {
        int oColIdx = schema_find_column(t->schema, q.orderByCol);
        if (oColIdx < 0) {
            fprintf(stderr, "[executor] error: unknown order by column '%s'\n", q.orderByCol);
            /* Continue without sorting */
        } else {
            SortItem *items = malloc(sizeof(SortItem) * matchedCount);
            for (int i = 0; i < matchedCount; i++) {
                items[i].r = matched[i];
                items[i].sortColIdx = oColIdx;
                items[i].sortColType = t->schema->columns[oColIdx].type;
                items[i].desc = q.isOrderDesc;
            }
            qsort(items, matchedCount, sizeof(SortItem), sort_item_cmp);
            for (int i = 0; i < matchedCount; i++) matched[i] = items[i].r;
            free(items);
        }
    }

    /* 4. Print final results */
    schema_print_all(t->name, t->schema, matched, matchedCount);
    return 0;
}


static int exec_explain(Database *db, Query q)
{
    /* Retrieve the inner query that was parsed by the EXPLAIN branch */
    Query *inner = (Query *)q.innerQuery;
    if (!inner) {
        fprintf(stderr, "[executor] EXPLAIN: missing inner query\n");
        return -1;
    }

    /* Ask the optimizer what it would choose for the inner query */
    ExecutionPlan plan = chooseExecutionPlan(*inner, db);

    /* Map PlanType to a readable pipeline stage label */
    const char *strategy;
    switch (plan.planType) {
        case INDEX_HASH:   strategy = "Hash Index Search";    break;
        case INDEX_BPTREE: strategy = "B+ Tree Index Search"; break;
        default:           strategy = "Full Table Scan";       break;
    }

    /* Calculate statistics for EXPLAIN fields */
    Table *t = db_find_table(db, inner->tableName);
    if (!t && db->table && inner->tableName[0] != '\0' && strcasecmp(db->table->name, inner->tableName) == 0)
        t = db->table;

    int rowCount = t ? (t->is_generic ? t->gen_count : t->record_count) : 0;
    int matches = 0;
    int treeHeight = 0;

    /* Get matches and height if index is used */
    if (t) {
        int val = (inner->type == QUERY_SELECT_WHERE_AGE) ? inner->age : inner->id;
        const char *col = inner->column[0] ? inner->column : "id";
        BPTree *bp = NULL;

        if (t->is_generic) {
             if (t->schema && t->primary_col_idx < t->schema->col_count && 
                 strcasecmp(t->schema->columns[t->primary_col_idx].name, col) == 0)
                 bp = (BPTree *)t->primary_bp;
        } else {
            if (strcasecmp(col, "id") == 0) bp = db->bpIndex;
            else if (strcasecmp(col, "age") == 0) bp = db->ageIndex;
        }

        if (bp) {
            matches = countBPTreeMatches(bp, val);
            treeHeight = getBPTreeHeight(bp);
        } else if (plan.planType == INDEX_HASH) {
            matches = 1; /* Hash implies unique in this logic */
        }
    }

    /* ---- Print the explain output ---- */
    printf("\nQuery Plan:\n");
    printf("  Parser -> Optimizer -> %s\n", strategy);
    printf("\n");
    printf("  Plan Type  : %s\n",  planTypeName(plan.planType));
    printf("  Est. Cost  : %s\n",  plan.estimatedCost);
    printf("  Reason     : %s\n",  plan.reason);
    printf("\n");
    printf("  Inner Query: %s on table '%s'\n",
           queryTypeName(inner->type), inner->tableName);
    printf("\n");
    
    /* Frontend fields */
    printf("  Records    : %d\n", rowCount);
    printf("  Matches    : %d\n", matches);
    printf("  Tree Height: %d\n", treeHeight);
    printf("\n");

    /* Free inner query AND its nested pointers */
    if (inner->genValues) free(inner->genValues);
    free(inner);
    return 0;
}




static int exec_delete(Database *db, Query q)
{
    /* Step 1 - locate in hash index (O(1)) */
    Record *r = searchHash(db->hashIndex, q.id);
    if (!r) {
        /* Try B+ tree as fallback */
        r = searchBPTree(db->bpIndex, q.id);
    }
    if (!r) {
        printf("[executor] DELETE: record with id=%d not found.\n", q.id);
        return 0;
    }

    int age = r->age;   /* save before freeing */

    /* Step 2 - remove from hash index
     * We do this by re-inserting a NULL to mark deleted, then
     * doing a targeted bucket removal via a simple linear scan. */
    HashTable *ht = db->hashIndex;
    int bucket = ((q.id % HT_CAPACITY) + HT_CAPACITY) % HT_CAPACITY;
    HashNode **cur = &ht->buckets[bucket];
    while (*cur) {
        if ((*cur)->key == q.id) {
            HashNode *dead = *cur;
            *cur = dead->next;
            free(dead);
            ht->size--;
            break;
        }
        cur = &(*cur)->next;
    }

    /* Step 3 - remove from id B+ tree (mark tombstone — simple approach:
     * since our B+ tree doesn't support deletion natively we rebuild
     * the index from the table after compaction). */

    /* Step 4 - compact the table records array */
    Table *t = db->table;
    int found_idx = -1;
    for (int i = 0; i < t->record_count; i++) {
        if (t->records[i] == r) { found_idx = i; break; }
    }
    if (found_idx >= 0) {
        record_free(t->records[found_idx]);   /* Table owned this */
        for (int i = found_idx; i < t->record_count - 1; i++)
            t->records[i] = t->records[i + 1];
        t->records[t->record_count - 1] = NULL;
        t->record_count--;
    }

    /* Step 5 - rebuild both B+ trees from scratch (simplest correct approach) */
    freeBPTree(db->bpIndex);
    freeBPTree(db->ageIndex);
    db->bpIndex  = createTree();
    db->ageIndex = createTree();
    for (int i = 0; i < t->record_count; i++) {
        Record *rec = t->records[i];
        insertBPTree(db->bpIndex,  rec->id,  rec);
        insertBPTree(db->ageIndex, rec->age, rec);
    }

    /* Step 6 - rewrite disk file */
    rewriteTableToDisk(t->name, t);

    printf("[executor] Deleted record with id=%d (age was %d).\n", q.id, age);
    return 0;
}



static int exec_update(Database *db, Query q)
{
    /* Step 1 - find the record */
    Record *r = searchHash(db->hashIndex, q.id);
    if (!r) r = searchBPTree(db->bpIndex, q.id);
    if (!r) {
        printf("[executor] UPDATE: record with id=%d not found.\n", q.id);
        return 0;
    }

    int old_age = r->age;

    /* Step 2 - update in place */
    r->age = q.newAge;

    /* Step 3 - rebuild age index (key changed so old position is stale) */
    freeBPTree(db->ageIndex);
    db->ageIndex = createTree();
    for (int i = 0; i < db->table->record_count; i++) {
        Record *rec = db->table->records[i];
        insertBPTree(db->ageIndex, rec->age, rec);
    }

    /* Step 4 - rewrite disk */
    rewriteTableToDisk(db->table->name, db->table);

    printf("[executor] Updated id=%d: age %d -> %d.\n", q.id, old_age, q.newAge);
    return 0;
}

static int exec_delete_generic(Table *t, Query q)
{
    int colIdx = schema_find_column(t->schema, q.column);
    if (colIdx < 0) {
        fprintf(stderr, "[executor] DELETE: column '%s' not found\n", q.column);
        return -1;
    }

    int found_count = 0;
    for (int i = 0; i < t->gen_count; i++) {
        GenericRecord *r = t->gen_records[i];
        int match = 0;
        if (t->schema->columns[colIdx].type == COL_INT) {
            if (r->values[colIdx].int_val == q.id) match = 1;
        } else {
            /* String match if needed - though q.id is int */
        }

        if (match) {
            genrec_free(t->gen_records[i]);
            /* Compact */
            for (int j = i; j < t->gen_count - 1; j++)
                t->gen_records[j] = t->gen_records[j+1];
            t->gen_records[t->gen_count - 1] = NULL;
            t->gen_count--;
            i--; // Re-check this index
            found_count++;
        }
    }

    if (found_count > 0) {
        /* Rebuild indexes */
        freeHash((HashTable *)t->primary_hash);
        freeBPTree((BPTree *)t->primary_bp);
        t->primary_hash = (void *)createHashTable();
        t->primary_bp = (void *)createTree();
        for (int i = 0; i < t->gen_count; i++) {
            GenericRecord *r = t->gen_records[i];
            int pk = r->values[t->primary_col_idx].int_val;
            insertHash((HashTable *)t->primary_hash, pk, (Record *)r);
            insertBPTree((BPTree *)t->primary_bp, pk, (Record *)r);
        }
        rewriteGenericTableToDisk(t->name, t);
        printf("[executor] Deleted %d record(s) from '%s'.\n", found_count, t->name);
    } else {
        printf("[executor] DELETE: No records found matching %s=%d.\n", q.column, q.id);
    }
    return 0;
}

static int exec_update_generic(Table *t, Query q)
{
    int whereColIdx = schema_find_column(t->schema, q.column);
    int setColIdx = schema_find_column(t->schema, "age"); // Parser currently only supports 'age'
    
    if (whereColIdx < 0 || setColIdx < 0) {
        fprintf(stderr, "[executor] UPDATE: columns not found\n");
        return -1;
    }

    int found_count = 0;
    for (int i = 0; i < t->gen_count; i++) {
        GenericRecord *r = t->gen_records[i];
        if (r->values[whereColIdx].int_val == q.id) {
            r->values[setColIdx].int_val = q.newAge;
            found_count++;
        }
    }

    if (found_count > 0) {
        rewriteGenericTableToDisk(t->name, t);
        printf("[executor] Updated %d record(s) in '%s'.\n", found_count, t->name);
    } else {
        printf("[executor] UPDATE: No records found matching %s=%d.\n", q.column, q.id);
    }
    return 0;
}



static int exec_count(Database *db)
{
    printf("COUNT(*)\n");
    printf("--------\n");
    printf("%-8d\n\n", db->table->record_count);
    return 0;
}



static int exec_select_join_generic(Database *db, Query q)
{
    Table *t1 = db_find_table(db, q.tableName);
    Table *t2 = db_find_table(db, q.joinTable);
    if (!t1 || !t2) {
        fprintf(stderr, "[executor] JOIN: one or both tables not found\n");
        return -1;
    }

    int col1 = schema_find_column(t1->schema, q.joinColLeft);
    int col2 = schema_find_column(t2->schema, q.joinColRight);
    if (col1 < 0 || col2 < 0) {
        fprintf(stderr, "[executor] JOIN: join column not found\n");
        return -1;
    }

    /* Combined Schema */
    Schema *mSchema = schema_merge(t1->schema, t2->schema);
    GenericRecord *joined[1024];
    int joinedCount = 0;

    /* Nested Loop Join */
    for (int i = 0; i < t1->gen_count; i++) {
        GenericRecord *r1 = t1->gen_records[i];
        for (int j = 0; j < t2->gen_count; j++) {
            GenericRecord *r2 = t2->gen_records[j];
            
            int match = 0;
            if (t1->schema->columns[col1].type == COL_INT && t2->schema->columns[col2].type == COL_INT) {
                if (r1->values[col1].int_val == r2->values[col2].int_val) match = 1;
            } else if (t1->schema->columns[col1].type == COL_VARCHAR && t2->schema->columns[col2].type == COL_VARCHAR) {
                if (strcmp(r1->values[col1].str_val, r2->values[col2].str_val) == 0) match = 1;
            }

            if (match && joinedCount < 1024) {
                joined[joinedCount++] = genrec_merge(r1, r2, t1->schema, t2->schema);
            }
        }
    }

    char title[128];
    snprintf(title, 127, "%s JOIN %s", t1->name, t2->name);
    schema_print_all(title, mSchema, joined, joinedCount);

    /* Cleanup temporary records and schema */
    for (int i = 0; i < joinedCount; i++) genrec_free(joined[i]);
    schema_free(mSchema);

    return 0;
}

int executeQuery(Database *db, Query q)
{
    int result = -1;

    /* Guard: null database */
    if (!db) {
        fprintf(stderr, "[executor] executeQuery called with NULL database\n");
        goto cleanup;
    }

    /* Guard: unknown / malformed query */
    if (q.type == QUERY_UNKNOWN) {
        fprintf(stderr, "[executor] cannot execute UNKNOWN query type\n");
        goto cleanup;
    }

    /* EXPLAIN does not need an initialised table */
    if (q.type == QUERY_EXPLAIN) {
        result = exec_explain(db, q);
        goto cleanup;
    }

    /* CREATE TABLE is always allowed (creates legacy or generic) */
    if (q.type == QUERY_CREATE) {
        ExecutionPlan plan = chooseExecutionPlan(q, db);
        printExecutionPlan(plan, q);
        result = exec_create(db, q);
        goto cleanup;
    }

    /* ---- Check if this targets a generic table ---- */
    Table *genTable = db_find_table(db, q.tableName);
    if (genTable && genTable->is_generic) {
        ExecutionPlan plan = chooseExecutionPlan(q, db);
        printExecutionPlan(plan, q);

        switch (q.type) {
            case QUERY_INSERT:
                result = exec_insert_generic(genTable, q);
                goto cleanup;

            case QUERY_SELECT:
            case QUERY_SELECT_WHERE:
            case QUERY_SELECT_WHERE_AGE:
                result = exec_select_complex_generic(genTable, q, plan);
                goto cleanup;

            case QUERY_SELECT_JOIN:
                result = exec_select_join_generic(db, q);
                goto cleanup;

            case QUERY_COUNT:
                printf("COUNT(*)\n");
                printf("--------\n");
                printf("%-8d\n\n", genTable->gen_count);
                result = 0;
                goto cleanup;

            case QUERY_DELETE_WHERE:
                result = exec_delete_generic(genTable, q);
                goto cleanup;

            case QUERY_UPDATE:
                result = exec_update_generic(genTable, q);
                goto cleanup;

            default:
                fprintf(stderr,
                    "[executor] operation '%s' not supported on generic table '%s'\n",
                    queryTypeName(q.type), q.tableName);
                goto cleanup;
        }
    }

    /* ---- Legacy table path ---- */
    if (!db->isInitialised) {
        fprintf(stderr,
            "[executor] no table exists — run CREATE TABLE first\n");
        goto cleanup;
    }

    ExecutionPlan plan = chooseExecutionPlan(q, db);
    printExecutionPlan(plan, q);

    switch (q.type) {
        case QUERY_INSERT:
            result = exec_insert(db, q);
            break;
        case QUERY_SELECT:
            result = exec_select(db);
            break;
        case QUERY_SELECT_WHERE:
            result = exec_select_where(db, q, plan);
            break;
        case QUERY_SELECT_WHERE_AGE:
            result = exec_select_where_age(db, q);
            break;
        case QUERY_DELETE_WHERE:
            result = exec_delete(db, q);
            break;
        case QUERY_UPDATE:
            result = exec_update(db, q);
            break;
        case QUERY_COUNT:
            result = exec_count(db);
            break;
        default:
            fprintf(stderr, "[executor] unhandled query type %d\n", q.type);
            break;
    }

cleanup:
    /* Free heap-allocated query fields */
    if (q.genValues) { free(q.genValues); }
    /* Do NOT free q.innerQuery here - it is managed within exec_explain */
    return result;
}