#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "../include/query_executor.h"
#include "../include/storage.h"
#include "../include/optimizer.h"
#include "../include/schema.h"

Database *createDatabase(void)
{
    Database *db = (Database *)calloc(1, sizeof(Database));
    if (!db) {
        fprintf(stderr, "[executor] calloc failed in createDatabase\n");
        return NULL;
    }
    return db;
}

void freeDatabase(Database *db)
{
    if (!db) return;
    for (int i = 0; i < db->tableCount; i++) {
        if (db->tables[i]) table_free(db->tables[i]);
    }
    free(db);
}

Table *db_find_table(Database *db, const char *name)
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
    if (!schema) {
        fprintf(stderr, "[executor] error: generic DBMS requires a schema.\n");
        return -1;
    }

    Table *existing = db_find_table(db, q.tableName);
    if (existing) {
        for (int i = 0; i < db->tableCount; i++) {
            if (db->tables[i] == existing) {
                table_free(db->tables[i]);
                for (int j = i; j < db->tableCount - 1; j++)
                    db->tables[j] = db->tables[j + 1];
                db->tables[db->tableCount - 1] = NULL;
                db->tableCount--;
                break;
            }
        }
    }

    if (db->tableCount >= MAX_TABLES) {
        fprintf(stderr, "[executor] catalog full\n");
        schema_free(schema);
        return -1;
    }

    Table *t = table_create_generic(q.tableName, schema);
    if (!t) {
        schema_free(schema);
        return -1;
    }
    db->tables[db->tableCount++] = t;
    printf("[executor] Table '%s' created.\n", q.tableName);
    loadGenericTableFromDisk(q.tableName, t);
    return 0;
}

static int exec_insert_generic(Table *t, Query q)
{
    GenericValues *gv = (GenericValues *)q.genValues;
    if (!gv || gv->count != t->schema->col_count) {
        fprintf(stderr, "[executor] INSERT: value count mismatch\n");
        return -1;
    }

    GenericRecord *r = genrec_create(t->schema->col_count);
    for (int i = 0; i < t->schema->col_count; i++) {
        char *val = gv->vals[i];
        int len = (int)strlen(val);
        if (len >= 2 && ((val[0] == '\'' && val[len-1] == '\'') || (val[0] == '"' && val[len-1] == '"'))) {
            val[len-1] = '\0';
            val++;
        }
        if (t->schema->columns[i].type == COL_INT) r->values[i].int_val = atoi(val);
        else {
            strncpy(r->values[i].str_val, val, MAX_VARCHAR_LEN - 1);
            r->values[i].str_val[MAX_VARCHAR_LEN - 1] = '\0';
        }
    }

    if (table_insert_generic(t, r) != 0) {
        genrec_free(r);
        return -1;
    }

    int pk_val = r->values[t->primary_col_idx].int_val;
    if (t->primary_hash) {
        if (!searchHash((HashTable *)t->primary_hash, pk_val))
            insertHash((HashTable *)t->primary_hash, pk_val, r);
    }
    if (t->primary_bp) insertBPTree((BPTree *)t->primary_bp, pk_val, r);

    saveGenericRecordToDisk(t->name, t->schema, r);
    printf("[executor] Inserted record into '%s'.\n", t->name);
    return 0;
}

/* ----------------------------------------------------------------
 * Sorting and Complex Select
 * ---------------------------------------------------------------- */
#define SORT_BP_ORDER 4
#define SORT_BP_MAX_KEYS (2 * SORT_BP_ORDER - 1)
typedef struct SortBPNode {
    int isLeaf;
    int numKeys;
    ColumnValue keys[SORT_BP_MAX_KEYS];
    GenericRecord *records[SORT_BP_MAX_KEYS];
    struct SortBPNode *children[SORT_BP_MAX_KEYS + 1];
    struct SortBPNode *nextLeaf;
} SortBPNode;

typedef struct {
    SortBPNode *root;
    int colIdx;
    ColumnType colType;
} SortBPTree;

static int compare_vals(ColumnValue v1, ColumnValue v2, ColumnType type) {
    if (type == COL_INT) return (v1.int_val < v2.int_val) ? -1 : (v1.int_val > v2.int_val);
    return strcmp(v1.str_val, v2.str_val);
}

static SortBPNode *create_sort_node(int isLeaf) {
    SortBPNode *node = (SortBPNode *)calloc(1, sizeof(SortBPNode));
    node->isLeaf = isLeaf;
    return node;
}

static void split_sort_child(SortBPNode *parent, int i, SortBPNode *fullChild, ColumnType type) {
    (void)type;
    int t = SORT_BP_ORDER;
    SortBPNode *sibling = create_sort_node(fullChild->isLeaf);
    if (fullChild->isLeaf) {
        sibling->numKeys = t;
        for (int j = 0; j < t; j++) {
            sibling->keys[j] = fullChild->keys[t - 1 + j];
            sibling->records[j] = fullChild->records[t - 1 + j];
        }
        fullChild->numKeys = t - 1;
        sibling->nextLeaf = fullChild->nextLeaf;
        fullChild->nextLeaf = sibling;
        ColumnValue median = sibling->keys[0];
        for (int j = parent->numKeys; j > i; j--) {
            parent->keys[j] = parent->keys[j-1];
            parent->children[j+1] = parent->children[j];
        }
        parent->keys[i] = median;
        parent->children[i+1] = sibling;
        parent->numKeys++;
    } else {
        sibling->numKeys = t - 1;
        for (int j = 0; j < t - 1; j++) sibling->keys[j] = fullChild->keys[t + j];
        for (int j = 0; j < t; j++) sibling->children[j] = fullChild->children[t + j];
        fullChild->numKeys = t - 1;
        ColumnValue median = fullChild->keys[t - 1];
        for (int j = parent->numKeys; j > i; j--) {
            parent->keys[j] = parent->keys[j-1];
            parent->children[j+1] = parent->children[j];
        }
        parent->keys[i] = median;
        parent->children[i+1] = sibling;
        parent->numKeys++;
    }
}

static void insert_sort_nonfull(SortBPNode *node, ColumnValue key, GenericRecord *rec, ColumnType type) {
    int i = node->numKeys - 1;
    if (node->isLeaf) {
        while (i >= 0 && compare_vals(key, node->keys[i], type) < 0) {
            node->keys[i+1] = node->keys[i];
            node->records[i+1] = node->records[i];
            i--;
        }
        node->keys[i+1] = key;
        node->records[i+1] = rec;
        node->numKeys++;
    } else {
        while (i >= 0 && compare_vals(key, node->keys[i], type) < 0) i--;
        i++;
        if (node->children[i]->numKeys == SORT_BP_MAX_KEYS) {
            split_sort_child(node, i, node->children[i], type);
            if (compare_vals(key, node->keys[i], type) > 0) i++;
        }
        insert_sort_nonfull(node->children[i], key, rec, type);
    }
}

static void insert_sort_tree(SortBPTree *tree, GenericRecord *r) {
    ColumnValue key = r->values[tree->colIdx];
    if (tree->root->numKeys == SORT_BP_MAX_KEYS) {
        SortBPNode *newRoot = create_sort_node(0);
        newRoot->children[0] = tree->root;
        split_sort_child(newRoot, 0, tree->root, tree->colType);
        tree->root = newRoot;
    }
    insert_sort_nonfull(tree->root, key, r, tree->colType);
}

static void collect_sort_records(SortBPNode *node, GenericRecord **out, int *count, int desc) {
    SortBPNode *curr = node;
    while (!curr->isLeaf) curr = curr->children[0];
    while (curr) {
        for (int i = 0; i < curr->numKeys; i++) out[(*count)++] = curr->records[i];
        curr = curr->nextLeaf;
    }
    if (desc) {
        for (int i = 0; i < (*count) / 2; i++) {
            GenericRecord *tmp = out[i];
            out[i] = out[(*count)-1-i];
            out[(*count)-1-i] = tmp;
        }
    }
}

static void free_sort_node(SortBPNode *node) {
    if (!node) return;
    if (!node->isLeaf) for (int i = 0; i <= node->numKeys; i++) free_sort_node(node->children[i]);
    free(node);
}

static int exec_select_complex_generic(Table *t, Query q, ExecutionPlan plan)
{
    GenericRecord *matched[1024];
    int matchedCount = 0;

    const char *colName = q.column[0] ? q.column : "id";
    int colIdx = schema_find_column(t->schema, colName);
    if (colIdx < 0) return -1;
    int val = (q.type == QUERY_SELECT_WHERE_AGE) ? q.age : q.id;
    const char *op = q.whereOp[0] ? q.whereOp : "=";

    if (q.type == QUERY_SELECT_WHERE || q.type == QUERY_SELECT_WHERE_AGE) {
        if (strcmp(op, "=") == 0 && plan.planType == INDEX_HASH) {
            void *found = searchHash((HashTable *)t->primary_hash, val);
            if (found) matched[matchedCount++] = (GenericRecord *)found;
        } else if (strcmp(op, "=") == 0 && plan.planType == INDEX_BPTREE) {
            matchedCount = searchAllBPTree((BPTree *)t->primary_bp, val, (void **)matched, 1024);
        } else {
            for (int i = 0; i < t->gen_count && matchedCount < 1024; i++) {
                GenericRecord *r = t->gen_records[i];
                if (t->schema->columns[colIdx].type == COL_INT) {
                    int rVal = r->values[colIdx].int_val;
                    int ok = 0;
                    if (strcmp(op, "=") == 0) ok = (rVal == val);
                    else if (strcmp(op, ">") == 0) ok = (rVal > val);
                    else if (strcmp(op, "<") == 0) ok = (rVal < val);
                    if (ok) matched[matchedCount++] = r;
                } else if (strcmp(op, "=") == 0 && strcmp(r->values[colIdx].str_val, q.whereValueStr) == 0) {
                    matched[matchedCount++] = r;
                }
            }
        }
    } else {
        matchedCount = (t->gen_count > 1024) ? 1024 : t->gen_count;
        for (int i = 0; i < matchedCount; i++) matched[i] = t->gen_records[i];
    }

    if (q.hasOrderBy) {
        int oIdx = schema_find_column(t->schema, q.orderByCol);
        if (oIdx >= 0) {
            SortBPTree stree;
            stree.root = create_sort_node(1);
            stree.colIdx = oIdx;
            stree.colType = t->schema->columns[oIdx].type;
            for (int i = 0; i < matchedCount; i++) insert_sort_tree(&stree, matched[i]);
            int count = 0;
            collect_sort_records(stree.root, matched, &count, q.isOrderDesc);
            free_sort_node(stree.root);
        }
    }

    schema_print_all(t->name, t->schema, matched, matchedCount, q.selectCols, q.selectColCount);
    return 0;
}

static int exec_select_join_generic(Database *db, Query q)
{
    Table *t1 = db_find_table(db, q.tableName);
    Table *t2 = db_find_table(db, q.joinTable);
    if (!t1 || !t2) return -1;
    int c1 = schema_find_column(t1->schema, q.joinColLeft);
    int c2 = schema_find_column(t2->schema, q.joinColRight);
    if (c1 < 0 || c2 < 0) return -1;

    Schema *ms = schema_merge(t1->schema, t2->schema);
    GenericRecord *results[1024];
    int count = 0;

    for (int i = 0; i < t1->gen_count; i++) {
        for (int j = 0; j < t2->gen_count; j++) {
            if (t1->gen_records[i]->values[c1].int_val == t2->gen_records[j]->values[c2].int_val) {
                if (count < 1024) results[count++] = genrec_merge(t1->gen_records[i], t2->gen_records[j], t1->schema, t2->schema);
            }
        }
    }
    schema_print_all("JOIN RESULT", ms, results, count, NULL, 0);
    for (int i = 0; i < count; i++) genrec_free(results[i]);
    schema_free(ms);
    return 0;
}

static int exec_delete_generic(Table *t, Query q)
{
    int colIdx = schema_find_column(t->schema, q.column);
    if (colIdx < 0) return -1;
    int count = 0;
    for (int i = 0; i < t->gen_count; i++) {
        if (t->gen_records[i]->values[colIdx].int_val == q.id) {
            genrec_free(t->gen_records[i]);
            for (int j = i; j < t->gen_count - 1; j++) t->gen_records[j] = t->gen_records[j+1];
            t->gen_count--; i--; count++;
        }
    }
    if (count > 0) {
        freeHash((HashTable *)t->primary_hash);
        freeBPTree((BPTree *)t->primary_bp);
        t->primary_hash = (void *)createHashTable();
        t->primary_bp = (void *)createTree();
        for (int i = 0; i < t->gen_count; i++) {
            int pk = t->gen_records[i]->values[t->primary_col_idx].int_val;
            insertHash((HashTable *)t->primary_hash, pk, t->gen_records[i]);
            insertBPTree((BPTree *)t->primary_bp, pk, t->gen_records[i]);
        }
        rewriteGenericTableToDisk(t->name, t);
    }
    printf("[executor] Deleted %d records.\n", count);
    return 0;
}

static int exec_update_generic(Table *t, Query q)
{
    int colIdx = schema_find_column(t->schema, q.column);
    int ageIdx = schema_find_column(t->schema, "age");
    if (colIdx < 0 || ageIdx < 0) return -1;
    int count = 0;
    for (int i = 0; i < t->gen_count; i++) {
        if (t->gen_records[i]->values[colIdx].int_val == q.id) {
            t->gen_records[i]->values[ageIdx].int_val = q.newAge;
            count++;
        }
    }
    if (count > 0) rewriteGenericTableToDisk(t->name, t);
    printf("[executor] Updated %d records.\n", count);
    return 0;
}

static int exec_explain(Database *db, Query q)
{
    Query *inner = (Query *)q.innerQuery;
    if (!inner) return -1;
    ExecutionPlan plan = chooseExecutionPlan(*inner, db);
    Table *t = db_find_table(db, inner->tableName);
    int rowCount = t ? t->gen_count : 0;
    int matches = 0, height = 0;
    if (t) {
        int val = (inner->type == QUERY_SELECT_WHERE_AGE) ? inner->age : inner->id;
        if (plan.planType == INDEX_BPTREE) {
            matches = countBPTreeMatches((BPTree *)t->primary_bp, val);
            height = getBPTreeHeight((BPTree *)t->primary_bp);
        } else if (plan.planType == INDEX_HASH) matches = 1;
        else matches = rowCount;
    }
    printf("\nQuery Plan: %s\n", planTypeName(plan.planType));
    printf("  Strategy: %s\n", plan.reason);
    printf("  Records: %d, Matches: %d, Height: %d\n\n", rowCount, matches, height);
    return 0;
}

int executeQuery(Database *db, Query q)
{
    if (!db || q.type == QUERY_UNKNOWN) return -1;
    if (q.type == QUERY_EXPLAIN) return exec_explain(db, q);
    if (q.type == QUERY_CREATE) return exec_create(db, q);

    Table *t = db_find_table(db, q.tableName);
    if (!t) { fprintf(stderr, "[executor] Table '%s' not found.\n", q.tableName); return -1; }

    ExecutionPlan plan = chooseExecutionPlan(q, db);
    printExecutionPlan(plan, q);

    switch (q.type) {
        case QUERY_INSERT: return exec_insert_generic(t, q);
        case QUERY_SELECT:
        case QUERY_SELECT_WHERE:
        case QUERY_SELECT_WHERE_AGE: return exec_select_complex_generic(t, q, plan);
        case QUERY_SELECT_JOIN: return exec_select_join_generic(db, q);
        case QUERY_COUNT: printf("COUNT: %d\n", t->gen_count); return 0;
        case QUERY_DELETE_WHERE: return exec_delete_generic(t, q);
        case QUERY_UPDATE: return exec_update_generic(t, q);
        default: return -1;
    }
}