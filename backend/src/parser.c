#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>

#include "../include/parser.h"

#define MAX_SQL_LEN 256
#define DELIMS " \t\n;"


static char *strip_quotes(char *str)
{
    if (!str) return str;
    int len = (int)strlen(str);
    if (len >= 2 && ((str[0] == '\'' && str[len-1] == '\'') || 
                    (str[0] == '"'  && str[len-1] == '"'))) {
        str[len-1] = '\0';
        return str + 1;
    }
    return str;
}

static void strip_parens_and_commas(char *token)
{
    if (!token) return;
    int src = 0, dst = 0;
    while (token[src] != '\0') {
        char c = token[src];
        if (c != '(' && c != ')' && c != ',') {
            token[dst] = c;
            dst++;
        }
        src++;
    }
    token[dst] = '\0';
}

static Query make_unknown(const char *reason)
{
    fprintf(stderr, "[parser] error: %s\n", reason);
    Query q;
    memset(&q, 0, sizeof(Query));
    q.type = QUERY_UNKNOWN;
    return q;
}

static Query parse_delete(void)
{
    Query q;
    memset(&q, 0, sizeof(Query));

    char *token = strtok(NULL, DELIMS);
    if (!token || strcasecmp(token, "FROM") != 0)
        return make_unknown("expected FROM after DELETE");

    token = strtok(NULL, DELIMS);
    if (!token)
        return make_unknown("missing table name in DELETE");
    strncpy(q.tableName, token, MAX_TABLE_NAME_LEN - 1);

    token = strtok(NULL, DELIMS);
    if (!token || strcasecmp(token, "WHERE") != 0)
        return make_unknown("DELETE requires a WHERE clause");

    token = strtok(NULL, DELIMS);
    if (!token)
        return make_unknown("missing column name in DELETE WHERE");
    strncpy(q.column, token, MAX_COL_NAME - 1);

    token = strtok(NULL, DELIMS);
    if (!token || strcmp(token, "=") != 0)
        return make_unknown("expected '=' in DELETE WHERE clause");

    token = strtok(NULL, DELIMS);
    if (!token)
        return make_unknown("missing id value in DELETE WHERE");

    q.type = QUERY_DELETE_WHERE;
    q.id   = atoi(strip_quotes(token));
    return q;
}

static Query parse_update(void)
{
    Query q;
    memset(&q, 0, sizeof(Query));

    char *token = strtok(NULL, DELIMS);
    if (!token)
        return make_unknown("missing table name in UPDATE");
    strncpy(q.tableName, token, MAX_TABLE_NAME_LEN - 1);

    token = strtok(NULL, DELIMS);
    if (!token || strcasecmp(token, "SET") != 0)
        return make_unknown("expected SET in UPDATE");

    token = strtok(NULL, DELIMS);
    if (!token || strcasecmp(token, "age") != 0)
        return make_unknown("UPDATE SET only supports 'age' column");

    token = strtok(NULL, DELIMS);
    if (!token || strcmp(token, "=") != 0)
        return make_unknown("expected '=' after column name in UPDATE SET");

    token = strtok(NULL, DELIMS);
    if (!token)
        return make_unknown("missing new value in UPDATE SET");
    q.newAge = atoi(token);

    token = strtok(NULL, DELIMS);
    if (!token || strcasecmp(token, "WHERE") != 0)
        return make_unknown("UPDATE requires a WHERE clause");

    token = strtok(NULL, DELIMS);
    if (!token)
        return make_unknown("missing column name in UPDATE WHERE");
    strncpy(q.column, token, MAX_COL_NAME - 1);

    token = strtok(NULL, DELIMS);
    if (!token || strcmp(token, "=") != 0)
        return make_unknown("expected '=' in UPDATE WHERE clause");

    token = strtok(NULL, DELIMS);
    if (!token)
        return make_unknown("missing id value in UPDATE WHERE");

    q.type = QUERY_UPDATE;
    q.id   = atoi(strip_quotes(token));
    return q;
}

/* Helper to add spaces around operators like '=', '>', '<' if missing */
static void preprocess_sql(const char *sql, char *out, size_t out_len) {
    size_t i = 0, j = 0;
    while (sql[i] && j < out_len - 3) {
        char c = sql[i];
        if (c == '=' || c == '>' || c == '<' || c == '!') {
            /* Add space before if not present */
            if (j > 0 && out[j-1] != ' ') out[j++] = ' ';
            out[j++] = c;
            /* Handle >=, <=, != */
            if ((c == '>' || c == '<' || c == '!') && sql[i+1] == '=') {
                out[j++] = sql[++i];
            }
            /* Add space after if not present */
            if (sql[i+1] != ' ') out[j++] = ' ';
        } else {
            out[j++] = c;
        }
        i++;
    }
    out[j] = '\0';
}

Query parseQuery(const char *sql)
{
    Query q;
    memset(&q, 0, sizeof(Query));

    if (!sql || sql[0] == '\0')
        return make_unknown("empty SQL string");

    char buf[MAX_SQL_LEN];
    preprocess_sql(sql, buf, MAX_SQL_LEN);

    char *token = strtok(buf, DELIMS);
    if (!token)
        return make_unknown("no tokens found");

    if (strcasecmp(token, "CREATE") == 0) {
        token = strtok(NULL, DELIMS);
        if (!token || strcasecmp(token, "TABLE") != 0)
            return make_unknown("expected TABLE after CREATE");

        /* Table name — may have '(' attached: "foo(id" */
        token = strtok(NULL, DELIMS);
        if (!token)
            return make_unknown("missing table name in CREATE");

        /* Split table name from '(' if glued together: "foo(id..." */
        char raw_name[MAX_TABLE_NAME_LEN] = {0};
        strncpy(raw_name, token, MAX_TABLE_NAME_LEN - 1);

        char *paren = strchr(raw_name, '(');
        if (paren) {
            *paren = '\0';   /* terminate the name at '(' */
        }

        q.type = QUERY_CREATE;
        strncpy(q.tableName, raw_name, MAX_TABLE_NAME_LEN - 1);

        /* --- Check for column definitions: CREATE TABLE name (...) --- */

        /* Rebuild the remainder of the SQL after the table name.
         * We need the raw text because strtok has already fragmented buf.
         * Re-scan the original SQL to find everything after the table name. */
        const char *colStart = NULL;
        {
            /* Find '(' in the original sql string */
            const char *p = sql;
            while (*p && *p != '(') p++;
            if (*p == '(') colStart = p;
        }

        if (colStart) {
            /* We have a column list — parse it */
            colStart++;   /* skip '(' */

            /* Find closing ')' */
            const char *colEnd = strchr(colStart, ')');
            if (!colEnd)
                return make_unknown("missing closing ')' in CREATE TABLE column list");

            /* Copy column definitions into a work buffer */
            char colBuf[MAX_SQL_LEN] = {0};
            int colLen = (int)(colEnd - colStart);
            if (colLen <= 0)
                return make_unknown("empty column list in CREATE TABLE");
            if (colLen >= MAX_SQL_LEN - 1) colLen = MAX_SQL_LEN - 2;
            strncpy(colBuf, colStart, (size_t)colLen);
            colBuf[colLen] = '\0';

            /* Parse "col1 type1, col2 type2, ..." */
            Schema *schema = schema_create();
            if (!schema)
                return make_unknown("failed to allocate schema");

            char *col_tok = strtok(colBuf, ",");
            while (col_tok) {
                /* Trim leading whitespace */
                while (*col_tok == ' ' || *col_tok == '\t') col_tok++;

                /* Extract column name and type */
                char colName[MAX_COL_NAME] = {0};
                char colType[MAX_COL_NAME] = {0};

                if (sscanf(col_tok, "%31s %31s", colName, colType) != 2) {
                    schema_free(schema);
                    return make_unknown("bad column definition (expected: name type)");
                }

                ColumnType ct;
                if (col_type_parse(colType, &ct) != 0) {
                    fprintf(stderr, "[parser] unknown type '%s'\n", colType);
                    schema_free(schema);
                    return make_unknown("unknown column type (use int or varchar)");
                }

                if (schema_add_column(schema, colName, ct) != 0) {
                    schema_free(schema);
                    return make_unknown("too many columns in CREATE TABLE");
                }

                col_tok = strtok(NULL, ",");
            }

            if (schema->col_count == 0) {
                schema_free(schema);
                return make_unknown("no columns defined in CREATE TABLE");
            }

            q.schema = (void *)schema;   /* executor takes ownership */
        }
        /* else: no '(' → legacy CREATE TABLE (q.schema remains NULL) */

        return q;
    }

    if (strcasecmp(token, "INSERT") == 0) {
        token = strtok(NULL, DELIMS);
        if (!token || strcasecmp(token, "INTO") != 0)
            return make_unknown("expected INTO after INSERT");

        token = strtok(NULL, DELIMS);
        if (!token)
            return make_unknown("missing table name in INSERT");
        strncpy(q.tableName, token, MAX_TABLE_NAME_LEN - 1);

        token = strtok(NULL, DELIMS);
        if (!token || strcasecmp(token, "VALUES") != 0)
            return make_unknown("expected VALUES keyword in INSERT");

        /* --- Collect ALL comma-separated values generically --- */

        /* Gather the rest of the SQL into one buffer */
        char valBuf[MAX_SQL_LEN] = {0};
        token = strtok(NULL, "");   /* get everything remaining */
        if (!token)
            return make_unknown("missing VALUES data in INSERT");

        /* Trim leading whitespace */
        while (*token == ' ' || *token == '\t') token++;
        strncpy(valBuf, token, MAX_SQL_LEN - 1);

        /* Strip leading '(' and trailing ')' */
        {
            char *p = valBuf;
            while (*p == ' ' || *p == '(') p++;
            char *end = p + strlen(p) - 1;
            while (end > p && (*end == ')' || *end == ' ' || *end == '\n' || *end == '\r'))
                *end-- = '\0';

            /* Shift cleaned content to start of valBuf */
            if (p != valBuf) memmove(valBuf, p, strlen(p) + 1);
        }

        /* Split on commas and collect into GenericValues */
        GenericValues *gv = genvals_create();
        if (!gv)
            return make_unknown("failed to allocate GenericValues");

        char splitBuf[MAX_SQL_LEN];
        strncpy(splitBuf, valBuf, MAX_SQL_LEN - 1);
        splitBuf[MAX_SQL_LEN - 1] = '\0';

        char *vp = strtok(splitBuf, ",");
        while (vp && gv->count < MAX_GEN_VALUES) {
            /* Trim whitespace */
            while (*vp == ' ' || *vp == '\t') vp++;
            char *ve = vp + strlen(vp) - 1;
            while (ve > vp && (*ve == ' ' || *ve == '\t')) *ve-- = '\0';

            /* Strip parentheses from individual tokens */
            {
                char *s = vp;
                int dst = 0;
                while (*s) {
                    if (*s != '(' && *s != ')')
                        gv->vals[gv->count][dst++] = *s;
                    s++;
                }
                gv->vals[gv->count][dst] = '\0';
            }

            gv->count++;
            vp = strtok(NULL, ",");
        }

        if (gv->count == 0) {
            free(gv);
            return make_unknown("no values found in INSERT VALUES");
        }

        q.type          = QUERY_INSERT;
        q.genValues     = (void *)gv;
        q.genValueCount = gv->count;

        /* Backward compat: if exactly 3 values, also populate legacy fields */
        if (gv->count == 3) {
            q.id  = atoi(gv->vals[0]);
            strncpy(q.name, gv->vals[1], MAX_NAME_LEN - 1);
            q.age = atoi(gv->vals[2]);
        }

        return q;
    }

    if (strcasecmp(token, "SELECT") == 0) {
        token = strtok(NULL, DELIMS);
        if (!token)
            return make_unknown("incomplete SELECT statement");

        if (strcasecmp(token, "COUNT(*)") == 0) {
            token = strtok(NULL, DELIMS);
            if (!token || strcasecmp(token, "FROM") != 0)
                return make_unknown("expected FROM after COUNT(*)");

            token = strtok(NULL, DELIMS);
            if (!token)
                return make_unknown("missing table name after FROM in COUNT");

            q.type = QUERY_COUNT;
            strncpy(q.tableName, token, MAX_TABLE_NAME_LEN - 1);
            return q;
        }

        if (strcmp(token, "*") != 0)
            return make_unknown("expected * or COUNT(*) after SELECT");

        token = strtok(NULL, DELIMS);
        if (!token || strcasecmp(token, "FROM") != 0)
            return make_unknown("expected FROM in SELECT");

        token = strtok(NULL, DELIMS);
        if (!token)
            return make_unknown("missing table name in SELECT");
        strncpy(q.tableName, token, MAX_TABLE_NAME_LEN - 1);

        q.type = QUERY_SELECT;

        /* Parse optional clauses: WHERE, GROUP BY, HAVING, ORDER BY */
        token = strtok(NULL, DELIMS);
        while (token) {
            if (strcasecmp(token, "WHERE") == 0) {
                token = strtok(NULL, DELIMS);
                if (!token) return make_unknown("missing column in WHERE");
                strncpy(q.column, token, MAX_COL_NAME - 1);

                token = strtok(NULL, DELIMS);
                if (!token) return make_unknown("missing operator in WHERE");
                strncpy(q.whereOp, token, 3);

                token = strtok(NULL, DELIMS);
                if (!token) return make_unknown("missing value in WHERE");
                char *v = strip_quotes(token);

                /* Store both numeric and string versions of the value */
                q.id = atoi(v);
                strncpy(q.whereValueStr, v, MAX_NAME_LEN - 1);
                q.whereValueStr[MAX_NAME_LEN - 1] = '\0';

                if (strcasecmp(q.column, "id") == 0) {
                    q.type = QUERY_SELECT_WHERE;
                } else if (strcasecmp(q.column, "age") == 0) {
                    q.type = QUERY_SELECT_WHERE_AGE;
                    q.age = q.id;
                } else {
                    /* Generic column (could be int or varchar) */
                    q.type = QUERY_SELECT_WHERE;
                }
            } else if (strcasecmp(token, "JOIN") == 0) {
                token = strtok(NULL, DELIMS);
                if (!token) return make_unknown("missing join table");
                strncpy(q.joinTable, token, MAX_TABLE_NAME_LEN - 1);
                q.type = QUERY_SELECT_JOIN;

                token = strtok(NULL, DELIMS);
                if (!token || strcasecmp(token, "ON") != 0)
                    return make_unknown("expected ON after JOIN");

                token = strtok(NULL, DELIMS);
                if (!token) return make_unknown("missing left column in JOIN ON");
                /* Handle optional table prefix: table.col */
                char *dot = strchr(token, '.');
                strncpy(q.joinColLeft, dot ? dot + 1 : token, MAX_COL_NAME - 1);

                token = strtok(NULL, DELIMS);
                if (!token || strcmp(token, "=") != 0)
                    return make_unknown("expected '=' in JOIN ON");

                token = strtok(NULL, DELIMS);
                if (!token) return make_unknown("missing right column in JOIN ON");
                dot = strchr(token, '.');
                strncpy(q.joinColRight, dot ? dot + 1 : token, MAX_COL_NAME - 1);
            } else if (strcasecmp(token, "GROUP") == 0) {
                token = strtok(NULL, DELIMS);
                if (!token || strcasecmp(token, "BY") != 0)
                    return make_unknown("expected BY after GROUP");
                token = strtok(NULL, DELIMS);
                if (!token) return make_unknown("missing column in GROUP BY");
                q.hasGroupBy = 1;
                strncpy(q.groupByCol, token, MAX_COL_NAME - 1);
            } else if (strcasecmp(token, "HAVING") == 0) {
                token = strtok(NULL, DELIMS);
                if (!token) return make_unknown("missing column in HAVING");
                q.hasHaving = 1;
                strncpy(q.havingCol, token, MAX_COL_NAME - 1);

                token = strtok(NULL, DELIMS);
                if (!token) return make_unknown("missing operator in HAVING");
                strncpy(q.havingOp, token, 3);

                token = strtok(NULL, DELIMS);
                if (!token) return make_unknown("missing value in HAVING");
                q.havingValue = atoi(token);
            } else if (strcasecmp(token, "ORDER") == 0) {
                token = strtok(NULL, DELIMS);
                if (!token || strcasecmp(token, "BY") != 0)
                    return make_unknown("expected BY after ORDER");
                token = strtok(NULL, DELIMS);
                if (!token) return make_unknown("missing column in ORDER BY");
                q.hasOrderBy = 1;
                strncpy(q.orderByCol, token, MAX_COL_NAME - 1);

                /* Optional ASC/DESC */
                char *next = strtok(NULL, DELIMS);
                if (next) {
                    if (strcasecmp(next, "DESC") == 0) {
                        q.isOrderDesc = 1;
                    } else if (strcasecmp(next, "ASC") == 0) {
                        q.isOrderDesc = 0;
                    } else {
                        /* Not ASC/DESC, might be another clause, but for now we consume it if it was ASC/DESC */
                        /* Actually, strtok is stateful, so we need to be careful. */
                        /* If it's not ASC/DESC, it might be the start of another clause. */
                        /* But our simple parser assumes it's the end or followed by another clause keyword. */
                        /* Let's re-handle the 'next' token in the next iteration. */
                        token = next;
                        continue;
                    }
                }
            } else {
                return make_unknown("unexpected token in SELECT");
            }
            token = strtok(NULL, DELIMS);
        }
        return q;
    }

    if (strcasecmp(token, "DELETE") == 0)
        return parse_delete();

    if (strcasecmp(token, "UPDATE") == 0)
        return parse_update();

    if (strcasecmp(token, "EXPLAIN") == 0) {
        char *rest = strtok(NULL, "");
        if (!rest)
            return make_unknown("EXPLAIN requires a SQL statement to explain");

        while (*rest == ' ' || *rest == '\t') rest++;
        if (*rest == '\0')
            return make_unknown("EXPLAIN: inner SQL is empty");

        Query inner = parseQuery(rest);
        if (inner.type == QUERY_UNKNOWN)
            return make_unknown("EXPLAIN: inner SQL could not be parsed");

        if (inner.type == QUERY_EXPLAIN) {
            if (inner.innerQuery) free(inner.innerQuery);
            return make_unknown("EXPLAIN cannot be nested");
        }

        Query *innerPtr = (Query *)malloc(sizeof(Query));
        if (!innerPtr)
            return make_unknown("EXPLAIN: malloc failed for inner query");
        *innerPtr = inner;

        q.type       = QUERY_EXPLAIN;
        q.innerQuery = (void *)innerPtr;
        return q;
    }

    return make_unknown("unrecognised SQL command");
}

const char *queryTypeName(QueryType type)
{
    switch (type) {
        case QUERY_CREATE:           return "CREATE";
        case QUERY_INSERT:           return "INSERT";
        case QUERY_SELECT:           return "SELECT";
        case QUERY_SELECT_WHERE:     return "SELECT_WHERE";
        case QUERY_DELETE_WHERE:     return "DELETE_WHERE";
        case QUERY_UPDATE:           return "UPDATE";
        case QUERY_COUNT:            return "COUNT";
        case QUERY_SELECT_WHERE_AGE: return "SELECT_WHERE_AGE";
        case QUERY_EXPLAIN:          return "EXPLAIN";
        case QUERY_SELECT_JOIN:       return "SELECT_JOIN";
        default:                     return "UNKNOWN";
    }
}