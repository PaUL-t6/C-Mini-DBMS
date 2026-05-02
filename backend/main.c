#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "include/parser.h"
#include "include/query_executor.h"
#include "include/transaction.h"   



#define MAX_INPUT_LEN 256



static char *trim(char *s)
{
    while (isspace((unsigned char)*s)) s++;
    if (*s == '\0') return s;
    char *end = s + strlen(s) - 1;
    while (end > s && isspace((unsigned char)*end)) end--;
    *(end + 1) = '\0';
    return s;
}


/* ----------------------------------------------------------------
 * print_banner
 * ---------------------------------------------------------------- */
static void print_banner(void)
{
   printf("=====================================\n");
printf("    Mini In-Memory DBMS (ADS Project)\n");
printf("  Indexes: B+ Tree + Hash Table\n");
printf("=====================================\n");
printf("Type HELP for commands, EXIT to quit.\n");
}


/* ----------------------------------------------------------------
 * print_help
 * ---------------------------------------------------------------- */
static void print_help(void)
{
    printf("\n  Supported commands\n");
    printf("  ------------------\n");
    printf("  CREATE TABLE students\n");
    printf("  INSERT INTO students VALUES (id, name, age)\n");
    printf("  SELECT * FROM students\n");
    printf("  SELECT * FROM students WHERE id   = X\n");
    printf("  SELECT * FROM students WHERE age  = X\n");
    printf("  SELECT COUNT(*) FROM students\n");
    printf("  EXPLAIN <sql>            — show execution plan\n");
    
    printf("  HELP                     — show this message\n");
    printf("  EXIT / QUIT              — quit\n\n");
}


/* ================================================================
 *  main  –  Read-Parse-Execute loop
 * ================================================================ */
int main(void)
{
    print_banner();

    Database *db = createDatabase();
    if (!db) {
        fprintf(stderr, "Fatal: could not allocate database.\n");
        return EXIT_FAILURE;
    }

    /* TxLog is stack-allocated — no heap allocation required */
    TxLog txlog;
    tx_init(&txlog);

    char input[MAX_INPUT_LEN];

    /* ---- REPL: Read → Route → Execute ---- */
    while (1) {

        /* Prompt: show buffered count while inside a transaction */
        if (tx_is_active(&txlog))
            printf("[TX +%d]> ", txlog.count);
        else
            printf("> ");
        fflush(stdout);

        if (!fgets(input, sizeof(input), stdin)) {
            printf("\n[dbms] EOF — shutting down.\n");
            break;
        }

        char *sql = trim(input);
        if (sql[0] == '\0') continue;


        /* ---- Built-in control commands --------------------------------- */

        if (strcasecmp(sql, "EXIT") == 0 || strcasecmp(sql, "QUIT") == 0) {
            if (tx_is_active(&txlog)) {
                printf("[dbms] Open transaction discarded on exit.\n");
                tx_rollback(&txlog);
            }
            printf("[dbms] Shutting down. Goodbye!\n");
            break;
        }

        if (strcasecmp(sql, "HELP") == 0) { print_help(); continue; }

        

        /* ---- SQL path: Parse → Buffer or Execute ----------------------- */

        Query q = parseQuery(sql);

        
        if (tx_is_active(&txlog) && tx_is_write(q.type)) {
            tx_buffer(&txlog, sql);
        } else {
            executeQuery(db, q);
        }
    }

    freeDatabase(db);
    return EXIT_SUCCESS;
}