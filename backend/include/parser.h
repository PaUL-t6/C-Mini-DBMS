#ifndef PARSER_H
#define PARSER_H

/* ================================================================
 *  parser.h  -  Minimal SQL parser
 *
 *  Supported SQL syntax
 *  --------------------
 *  EXISTING (unchanged):
 *    CREATE TABLE students
 *    INSERT INTO students VALUES (id, name, age)
 *    SELECT * FROM students
 *    SELECT * FROM students WHERE id = X
 *
 *  NEW (extended):
 *    DELETE FROM students WHERE id = X
 *    UPDATE students SET age = Y WHERE id = X
 *    SELECT COUNT(*) FROM students
 *    SELECT * FROM students WHERE age = X
 *    EXPLAIN <any-supported-sql>
 *
 *  How it works
 *  ------------
 *  parseQuery() receives a raw SQL string, tokenises it with
 *  strtok(), and inspects the first few tokens to identify the
 *  command type.  It then reads the remaining tokens to populate
 *  the relevant fields of a Query struct.
 *
 *  The function returns a Query BY VALUE - no heap allocation
 *  needed by the caller.  On any parse error the returned Query
 *  has type = QUERY_UNKNOWN so the executor can reject it cleanly.
 *
 *  Token diagrams for NEW commands
 *  --------------------------------
 *  DELETE FROM students WHERE id = 3
 *    [0]DELETE  [1]FROM  [2]students  [3]WHERE  [4]id  [5]=  [6]3
 *
 *  UPDATE students SET age = 25 WHERE id = 2
 *    [0]UPDATE  [1]students  [2]SET  [3]age  [4]=  [5]25
 *    [6]WHERE   [7]id        [8]=    [9]2
 *
 *  SELECT COUNT(*) FROM students
 *    [0]SELECT  [1]COUNT(*)  [2]FROM  [3]students
 *
 *  SELECT * FROM students WHERE age = 20
 *    [0]SELECT  [1]*  [2]FROM  [3]students  [4]WHERE  [5]age  [6]=  [7]20
 *    -> distinguished from WHERE id= by checking the column token
 * ================================================================ */

#include "record.h"   /* for MAX_NAME_LEN       */
#include "table.h"    /* for MAX_TABLE_NAME_LEN */
#include "schema.h"   /* for Schema, GenericValues */

/* ----------------------------------------------------------------
 * QueryType  -  which SQL command was parsed
 *
 * EXISTING values keep their numeric positions so that any code
 * doing a direct integer comparison stays binary-compatible.
 * New values are appended at the end.
 * ---------------------------------------------------------------- */
typedef enum {
    /* -- existing (do not reorder) -- */
    QUERY_UNKNOWN,          /* parse failed or unrecognised command      */
    QUERY_CREATE,           /* CREATE TABLE <n>                          */
    QUERY_INSERT,           /* INSERT INTO <n> VALUES (id, name, age)    */
    QUERY_SELECT,           /* SELECT * FROM <n>                         */
    QUERY_SELECT_WHERE,     /* SELECT * FROM <n> WHERE id = X            */

    /* -- new -- */
    QUERY_DELETE_WHERE,     /* DELETE FROM <n> WHERE id = X              */
    QUERY_UPDATE,           /* UPDATE <n> SET age = Y WHERE id = X       */
    QUERY_COUNT,            /* SELECT COUNT(*) FROM <n>                  */
    QUERY_SELECT_WHERE_AGE, /* SELECT * FROM <n> WHERE age = X           */
    QUERY_EXPLAIN,          /* EXPLAIN <any-sql>                         */
    QUERY_SELECT_JOIN       /* SELECT * FROM <n1> JOIN <n2> ON ...       */
} QueryType;

/* ----------------------------------------------------------------
 * Query  -  all information extracted from one SQL statement
 *
 * Field usage per query type:
 *
 *   QUERY_CREATE           -> tableName
 *   QUERY_INSERT           -> tableName, id, name, age
 *   QUERY_SELECT           -> tableName
 *   QUERY_SELECT_WHERE     -> tableName, id
 *   QUERY_DELETE_WHERE     -> tableName, id
 *   QUERY_UPDATE           -> tableName, id, newAge
 *   QUERY_COUNT            -> tableName
 *   QUERY_SELECT_WHERE_AGE -> tableName, age
 *   QUERY_EXPLAIN          -> innerQuery  (heap-allocated; caller must free)
 *   QUERY_UNKNOWN          -> (all fields zeroed/empty)
 *
 * NOTE: `newAge` is a NEW field added for UPDATE.
 *       All existing fields (id, name, age) are preserved so that
 *       executor.c and other callers need no changes.
 *
 * OWNERSHIP of innerQuery:
 *   parseQuery() heap-allocates innerQuery for QUERY_EXPLAIN and
 *   sets all other fields to zero.  The caller (executor.c) is
 *   responsible for calling free(q.innerQuery) after use.
 *   For all other query types innerQuery is NULL — do NOT free it.
 * ---------------------------------------------------------------- */
typedef struct Query_s {
    QueryType  type;
    char       tableName[MAX_TABLE_NAME_LEN];
    int        id;                  /* INSERT pk, SELECT/DELETE WHERE id  */
    char       name[MAX_NAME_LEN];  /* INSERT name value                  */
    int        age;                 /* INSERT age, SELECT WHERE age = X   */
    int        newAge;              /* UPDATE SET age = <newAge> WHERE id  */
    char       whereValueStr[MAX_NAME_LEN]; /* Value for string filters */

    /* EXPLAIN only: heap-allocated inner Query* — caller must free.
     * Cast to (Query *) when using.  NULL for all other query types. */
    void *innerQuery;

    /* Generic table support (new):
     *   schema    – heap-allocated Schema* for CREATE TABLE with columns.
     *               Ownership transfers to the Table on creation.
     *   genValues – heap-allocated GenericValues* for INSERT into a
     *               schema-defined table.  Executor frees after use.
     *   genValueCount – number of values (convenience; also in genValues).
     * Both are NULL for legacy queries. */
    void *schema;
    void *genValues;
    int   genValueCount;
    char  column[MAX_COL_NAME];    /* WHERE <column> = X */
    char  whereOp[4];              /* "=", ">", "<", ">=", "<=", "!=" */

    /* Complex SELECT clauses (new): */
    int   hasGroupBy;
    char  groupByCol[MAX_COL_NAME];
    
    int   hasHaving;
    char  havingCol[MAX_COL_NAME]; /* Currently supports COUNT(*) */
    char  havingOp[4];             /* "=", ">", "<", ">=", "<=" */
    int   havingValue;

    int   hasOrderBy;
    char  orderByCol[MAX_COL_NAME];
    int   isOrderDesc;             /* 1 for DESC, 0 for ASC */

    /* JOIN support (new): */
    char  joinTable[MAX_TABLE_NAME_LEN];
    char  joinColLeft[MAX_COL_NAME];
    char  joinColRight[MAX_COL_NAME];

    /* Column selection support (new): */
    char  selectCols[MAX_GEN_VALUES][MAX_COL_NAME];
    int   selectColCount;
} Query;

/* Parse a SQL string and return a populated Query struct.
 *
 * NOTE: strtok() modifies the string it operates on, so a local
 * copy of `sql` is made internally before tokenising.
 *
 * Returns a Query with type = QUERY_UNKNOWN on any error. */
Query parseQuery(const char *sql);

/* Return a human-readable name for a QueryType (useful for
 * debugging and executor log messages). */
const char *queryTypeName(QueryType type);

#endif /* PARSER_H */