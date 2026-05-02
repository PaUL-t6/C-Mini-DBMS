#include <stdio.h>
#include <string.h>

#include "../include/transaction.h"




void tx_init(TxLog *log)
{
    if (!log) return;
    memset(log, 0, sizeof(TxLog));
    /* active=0, count=0, all entry strings zeroed */
}



int tx_begin(TxLog *log)
{
    if (!log) return -1;

    if (log->active) {
        fprintf(stderr,
            "[transaction] ERROR: a transaction is already active.\n"
            "              Issue COMMIT or ROLLBACK before BEGIN.\n");
        return -1;
    }

    log->active = 1;
    log->count  = 0;

    printf("[transaction] BEGIN — transaction opened.\n"
           "              DML writes will be buffered until COMMIT.\n");
    return 0;
}



int tx_buffer(TxLog *log, const char *sql)
{
    if (!log || !sql) return -1;

    /* Safety: should never be called outside a transaction */
    if (!log->active) {
        fprintf(stderr, "[transaction] BUG: tx_buffer called with no active tx\n");
        return -1;
    }

    if (log->count >= TX_LOG_CAPACITY) {
        fprintf(stderr,
            "[transaction] ERROR: log full (%d/%d entries).\n"
            "              Rolling back automatically.\n",
            log->count, TX_LOG_CAPACITY);
        tx_rollback(log);
        return -1;
    }

    /* Copy the SQL string — caller's buffer is immediately reusable */
    strncpy(log->entries[log->count].sql, sql, TX_SQL_MAX_LEN - 1);
    log->entries[log->count].sql[TX_SQL_MAX_LEN - 1] = '\0';
    log->count++;

    printf("[transaction] Buffered  [%2d] : %s\n", log->count, sql);
    return 0;
}



int tx_commit(TxLog *log, Database *db)
{
    if (!log || !db) return -1;

    if (!log->active) {
        fprintf(stderr,
            "[transaction] WARNING: COMMIT with no active transaction.\n");
        return -1;
    }

    /* Capture before reset */
    int total = log->count;

    if (total == 0) {
        printf("[transaction] COMMIT — nothing to apply (empty transaction).\n");
        log->active = 0;
        return 0;
    }

    printf("[transaction] COMMIT — replaying %d buffered operation%s:\n",
           total, total == 1 ? "" : "s");

    int applied = 0;

    for (int i = 0; i < total; i++) {

        const char *sql = log->entries[i].sql;

        /* Re-parse — full validation is repeated, just as in real WAL replay */
        Query q = parseQuery(sql);

        if (q.type == QUERY_UNKNOWN) {
            /* Should not happen: we only buffer queries that parsed OK */
            fprintf(stderr,
                "[transaction] COMMIT error at entry [%d]: "
                "re-parse failed for: %s\n", i + 1, sql);
            continue;
        }

        /* Apply to the live database */
        if (executeQuery(db, q) == 0) {
            applied++;
        } else {
            fprintf(stderr,
                "[transaction] COMMIT error at entry [%d]: "
                "execute failed for: %s\n", i + 1, sql);
        }
    }

    /* Reset log regardless of partial failure */
    log->count  = 0;
    log->active = 0;

    printf("[transaction] COMMIT complete — %d/%d operation%s applied.\n",
           applied, total, applied == 1 ? "" : "s");

    return (applied == total) ? 0 : -1;
}


void tx_rollback(TxLog *log)
{
    if (!log) return;

    if (!log->active) {
        printf("[transaction] ROLLBACK — no active transaction.\n");
        return;
    }

    int discarded = log->count;

    /* Wipe entries so no stale SQL is left in memory */
    memset(log->entries, 0, sizeof(TxEntry) * (size_t)discarded);
    log->count  = 0;
    log->active = 0;

    printf("[transaction] ROLLBACK — discarded %d buffered operation%s.\n"
           "              Database is unchanged.\n",
           discarded, discarded == 1 ? "" : "s");
}



int tx_is_write(QueryType type)
{
    switch (type) {
        case QUERY_INSERT:
        case QUERY_DELETE_WHERE:
        case QUERY_UPDATE:
            return 1;

        case QUERY_CREATE:
        case QUERY_SELECT:
        case QUERY_SELECT_WHERE:
        case QUERY_SELECT_WHERE_AGE:
        case QUERY_COUNT:
        case QUERY_EXPLAIN:
        case QUERY_UNKNOWN:
        default:
            return 0;
    }
}