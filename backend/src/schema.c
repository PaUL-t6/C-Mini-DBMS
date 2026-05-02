#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "../include/schema.h"

/* ================================================================
 *  schema.c  –  Schema, GenericRecord, and GenericValues helpers
 * ================================================================ */


/* ---- Schema --------------------------------------------------- */

Schema *schema_create(void)
{
    Schema *s = (Schema *)calloc(1, sizeof(Schema));
    if (!s) {
        fprintf(stderr, "[schema] calloc failed in schema_create\n");
        return NULL;
    }
    return s;
}


int schema_add_column(Schema *s, const char *name, ColumnType type)
{
    if (!s || !name) return -1;
    if (s->col_count >= MAX_COLUMNS) {
        fprintf(stderr, "[schema] cannot add column '%s': max %d columns reached\n",
                name, MAX_COLUMNS);
        return -1;
    }

    ColumnDef *col = &s->columns[s->col_count];
    strncpy(col->name, name, MAX_COL_NAME - 1);
    col->name[MAX_COL_NAME - 1] = '\0';
    col->type = type;
    s->col_count++;
    return 0;
}


void schema_free(Schema *s)
{
    free(s);   /* free(NULL) is safe */
}


Schema *schema_copy(const Schema *s)
{
    if (!s) return NULL;
    Schema *copy = (Schema *)malloc(sizeof(Schema));
    if (!copy) return NULL;
    memcpy(copy, s, sizeof(Schema));
    return copy;
}


int schema_find_column(const Schema *s, const char *name)
{
    if (!s || !name) return -1;
    for (int i = 0; i < s->col_count; i++) {
        if (strcasecmp(s->columns[i].name, name) == 0) return i;
    }
    return -1;
}



const char *col_type_name(ColumnType t)
{
    switch (t) {
        case COL_INT:     return "int";
        case COL_VARCHAR: return "varchar";
        default:          return "unknown";
    }
}


int col_type_parse(const char *str, ColumnType *out)
{
    if (!str || !out) return -1;
    if (strcasecmp(str, "int") == 0 || strcasecmp(str, "integer") == 0) {
        *out = COL_INT;
        return 0;
    }
    if (strcasecmp(str, "varchar") == 0 || strcasecmp(str, "string") == 0 ||
        strcasecmp(str, "text") == 0) {
        *out = COL_VARCHAR;
        return 0;
    }
    return -1;
}


/* ---- GenericRecord -------------------------------------------- */

GenericRecord *genrec_create(int col_count)
{
    if (col_count <= 0 || col_count > MAX_COLUMNS) return NULL;

    GenericRecord *r = (GenericRecord *)malloc(sizeof(GenericRecord));
    if (!r) {
        fprintf(stderr, "[schema] malloc failed in genrec_create\n");
        return NULL;
    }

    r->values = (ColumnValue *)calloc((size_t)col_count, sizeof(ColumnValue));
    if (!r->values) {
        fprintf(stderr, "[schema] calloc failed for values array\n");
        free(r);
        return NULL;
    }

    r->col_count = col_count;
    return r;
}


void genrec_free(GenericRecord *r)
{
    if (!r) return;
    free(r->values);
    free(r);
}


/* ---- Formatting helpers --------------------------------------- */

/* Compute display width for a column.
 * At least the column name length, at least 4, capped at 20 for varchar. */
static int col_width(const ColumnDef *col)
{
    int name_len = (int)strlen(col->name);
    int min_w = name_len > 4 ? name_len : 4;

    if (col->type == COL_VARCHAR) {
        return min_w > 20 ? min_w : 20;
    }
    return min_w > 6 ? min_w : 6;
}


void schema_print_header(const Schema *s)
{
    if (!s) return;

    /* Column names */
    printf("  ");
    for (int i = 0; i < s->col_count; i++) {
        int w = col_width(&s->columns[i]);
        if (i > 0) printf("  ");
        printf("%-*s", w, s->columns[i].name);
    }
    printf("\n");

    /* Separator dashes */
    printf("  ");
    for (int i = 0; i < s->col_count; i++) {
        int w = col_width(&s->columns[i]);
        if (i > 0) printf("  ");
        for (int j = 0; j < w; j++) printf("-");
    }
    printf("\n");
}


void genrec_print(const GenericRecord *r, const Schema *s)
{
    if (!r || !s) return;

    printf("  ");
    for (int i = 0; i < s->col_count && i < r->col_count; i++) {
        int w = col_width(&s->columns[i]);
        if (i > 0) printf("  ");

        if (s->columns[i].type == COL_INT) {
            printf("%-*d", w, r->values[i].int_val);
        } else {
            printf("%-*s", w, r->values[i].str_val);
        }
    }
    printf("\n");
}


void schema_print_all(const char *tableName, const Schema *s,
                      GenericRecord **records, int count,
                      char (*selectCols)[MAX_COL_NAME], int selectColCount)
{
    if (!s) return;

    printf("\nTable: %s  (%d row%s)\n",
           tableName ? tableName : "?",
           count, count == 1 ? "" : "s");

    /* Map selectCols to indices */
    int indices[MAX_GEN_VALUES];
    int actualCount = 0;

    if (selectColCount == 0) {
        /* Print all */
        actualCount = s->col_count;
        for (int i = 0; i < s->col_count; i++) indices[i] = i;
    } else {
        /* Print only requested */
        for (int i = 0; i < selectColCount; i++) {
            int idx = schema_find_column(s, selectCols[i]);
            if (idx >= 0) indices[actualCount++] = idx;
        }
    }

    if (actualCount == 0) {
        printf("  [No valid columns selected]\n\n");
        return;
    }

    /* Print header */
    printf("  ");
    for (int i = 0; i < actualCount; i++) {
        int idx = indices[i];
        int w = col_width(&s->columns[idx]);
        if (i > 0) printf("  ");
        printf("%-*s", w, s->columns[idx].name);
    }
    printf("\n  ");
    for (int i = 0; i < actualCount; i++) {
        int idx = indices[i];
        int w = col_width(&s->columns[idx]);
        if (i > 0) printf("  ");
        for (int j = 0; j < w; j++) printf("-");
    }
    printf("\n");

    /* Print data */
    for (int r_idx = 0; r_idx < count; r_idx++) {
        GenericRecord *r = records[r_idx];
        printf("  ");
        for (int i = 0; i < actualCount; i++) {
            int idx = indices[i];
            int w = col_width(&s->columns[idx]);
            if (i > 0) printf("  ");

            if (s->columns[idx].type == COL_INT) {
                printf("%-*d", w, r->values[idx].int_val);
            } else {
                printf("%-*s", w, r->values[idx].str_val);
            }
        }
        printf("\n");
    }
    printf("\n");
}


/* ---- GenericValues -------------------------------------------- */

GenericValues *genvals_create(void)
{
    GenericValues *gv = (GenericValues *)calloc(1, sizeof(GenericValues));
    if (!gv) {
        fprintf(stderr, "[schema] calloc failed in genvals_create\n");
    }
    return gv;
}

Schema *schema_merge(const Schema *s1, const Schema *s2)
{
    if (!s1 || !s2) return NULL;
    Schema *m = schema_create();
    if (!m) return NULL;

    for (int i = 0; i < s1->col_count; i++) {
        schema_add_column(m, s1->columns[i].name, s1->columns[i].type);
    }
    for (int i = 0; i < s2->col_count; i++) {
        /* If name collision, prefix with table index? For now, just add. */
        schema_add_column(m, s2->columns[i].name, s2->columns[i].type);
    }
    return m;
}

GenericRecord *genrec_merge(const GenericRecord *r1, const GenericRecord *r2,
                            const Schema *s1, const Schema *s2)
{
    if (!r1 || !r2 || !s1 || !s2) return NULL;
    int total_cols = s1->col_count + s2->col_count;
    GenericRecord *m = genrec_create(total_cols);
    if (!m) return NULL;

    for (int i = 0; i < s1->col_count; i++) {
        m->values[i] = r1->values[i];
    }
    for (int i = 0; i < s2->col_count; i++) {
        m->values[s1->col_count + i] = r2->values[i];
    }
    return m;
}
