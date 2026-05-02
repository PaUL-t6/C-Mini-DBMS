#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

/* mkdir: POSIX uses sys/stat.h with a mode argument.
 * MinGW/Windows exposes _mkdir (no mode) in direct.h.
 * We wrap both behind a single make_dir() macro so the
 * rest of the file stays platform-neutral.             */
#if defined(_WIN32) || defined(__MINGW32__)
#  include <direct.h>
#  define make_dir(path)  _mkdir(path)
#else
#  include <sys/stat.h>
#  define make_dir(path)  mkdir((path), 0755)
#endif

#include "../include/storage.h"
#include "../include/record.h"
#include "../include/table.h"
#include "../include/hashtable.h"
#include "../include/bptree.h"





static void ensure_data_dir(void)
{
    /* mkdir returns -1 with errno=EEXIST if directory already exists;
     * we treat that as success.  Mode 0755 = rwxr-xr-x.           */
    if (make_dir(STORAGE_DIR) == -1 && errno != EEXIST) {
        fprintf(stderr, "[storage] warning: could not create '%s/': %s\n",
                STORAGE_DIR, strerror(errno));
    }
}



void buildStoragePath(const char *tableName, char *out)
{
    snprintf(out, STORAGE_PATH_LEN, "%s/%s.tbl", STORAGE_DIR, tableName);
}



int saveRecordToDisk(const char *tableName, const Record *r)
{
    if (!tableName || !r) return -1;

    ensure_data_dir();

    char path[STORAGE_PATH_LEN];
    buildStoragePath(tableName, path);

    FILE *fp = fopen(path, "a");   /* append mode */
    if (!fp) {
        fprintf(stderr, "[storage] warning: could not open '%s' for append: %s\n",
                path, strerror(errno));
        return -1;
    }

    /* Write one CSV line: id,name,age */
    fprintf(fp, "%d,%s,%d\n", r->id, r->name, r->age);
    fclose(fp);

    return 0;
}


int loadTableFromDisk(const char *tableName, Table *t,
                      HashTable *ht, BPTree *bp, BPTree *ageIdx)
{
    if (!tableName || !t || !ht || !bp) return -1;

    char path[STORAGE_PATH_LEN];
    buildStoragePath(tableName, path);

    FILE *fp = fopen(path, "r");   /* read mode */
    if (!fp) {
        if (errno == ENOENT) {
            /* File doesn't exist yet — first use, nothing to load */
            printf("[storage] No existing data file for '%s' (fresh table).\n",
                   tableName);
            return 0;
        }
        fprintf(stderr, "[storage] error opening '%s': %s\n",
                path, strerror(errno));
        return -1;
    }

    printf("[storage] Loading '%s' from disk...\n", path);

    char  line[MAX_NAME_LEN + 32];   /* enough for "id,name,age\n" */
    int   id, age;
    char  name[MAX_NAME_LEN];
    int   loaded = 0;
    int   skipped = 0;

    while (fgets(line, sizeof(line), fp) != NULL) {

        /* Strip trailing newline/carriage-return */
        line[strcspn(line, "\r\n")] = '\0';

        /* Skip blank lines */
        if (line[0] == '\0') continue;

        /* Parse CSV: id,name,age
         * %63[^,] reads up to 63 chars that are NOT a comma,
         * so it safely handles names with spaces.               */
        if (sscanf(line, "%d,%63[^,],%d", &id, name, &age) != 3) {
            fprintf(stderr, "[storage] warning: skipping malformed line: '%s'\n",
                    line);
            skipped++;
            continue;
        }

        /* Recreate the record in memory */
        Record *r = record_create(id, name, age);
        if (!r) {
            fprintf(stderr, "[storage] record_create failed during load\n");
            fclose(fp);
            return -1;
        }

        /* Insert into all three structures (same order as exec_insert) */
        if (table_insert(t, r) != 0) {
            record_free(r);
            fprintf(stderr, "[storage] table_insert failed during load\n");
            fclose(fp);
            return -1;
        }
        insertHash(ht, id, r);                        /* hash: id -> r   */
        insertBPTree(bp, id, r);                      /* bptree: id -> r */
        if (ageIdx) insertBPTree(ageIdx, age, r);     /* bptree: age -> r*/

        loaded++;
    }

    fclose(fp);

    printf("[storage] Loaded %d record%s from '%s'",
           loaded, loaded == 1 ? "" : "s", path);
    if (skipped > 0)
        printf(" (%d line%s skipped)", skipped, skipped == 1 ? "" : "s");
    printf(".\n");

    return 0;
}



int rewriteTableToDisk(const char *tableName, const Table *t)
{
    if (!tableName || !t) return -1;

    char path[STORAGE_PATH_LEN];
    buildStoragePath(tableName, path);

    FILE *f = fopen(path, "w");   /* "w" truncates existing file */
    if (!f) {
        fprintf(stderr, "[storage] cannot open '%s' for rewrite: ", path);
        perror("");
        return -1;
    }

    for (int i = 0; i < t->record_count; i++) {
        const Record *r = t->records[i];
        fprintf(f, "%d,%s,%d\n", r->id, r->name, r->age);
    }

    fclose(f);
    printf("[storage] Rewrote '%s' (%d record%s).\n",
           path, t->record_count, t->record_count == 1 ? "" : "s");
    return 0;
}


/* ================================================================
 *  Generic table storage (new)
 * ================================================================ */

static void write_schema_header(FILE *fp, const Schema *s)
{
    if (!fp || !s) return;
    fprintf(fp, "#schema:");
    for (int i = 0; i < s->col_count; i++) {
        fprintf(fp, "%s:%s%s",
                s->columns[i].name,
                col_type_name(s->columns[i].type),
                (i == s->col_count - 1) ? "" : ",");
    }
    fprintf(fp, "\n");
}


int saveGenericRecordToDisk(const char *tableName, const Schema *s, const GenericRecord *r)
{
    if (!tableName || !s || !r) return -1;

    ensure_data_dir();
    char path[STORAGE_PATH_LEN];
    buildStoragePath(tableName, path);

    /* Check if file exists to see if we need to write the header */
    FILE *test = fopen(path, "r");
    int needsHeader = (test == NULL);
    if (test) fclose(test);

    FILE *fp = fopen(path, "a");
    if (!fp) return -1;

    if (needsHeader) {
        write_schema_header(fp, s);
    }

    for (int i = 0; i < r->col_count; i++) {
        if (s->columns[i].type == COL_INT) {
            fprintf(fp, "%d", r->values[i].int_val);
        } else {
            fprintf(fp, "%s", r->values[i].str_val);
        }
        fprintf(fp, "%s", (i == r->col_count - 1) ? "" : ",");
    }
    fprintf(fp, "\n");

    fclose(fp);
    return 0;
}


int loadGenericTableFromDisk(const char *tableName, Table *t)
{
    if (!tableName || !t || !t->is_generic) return -1;

    char path[STORAGE_PATH_LEN];
    buildStoragePath(tableName, path);

    FILE *fp = fopen(path, "r");
    if (!fp) {
        if (errno == ENOENT) return 0;
        return -1;
    }

    printf("[storage] Loading generic table '%s' from disk...\n", tableName);

    char line[1024];
    int loaded = 0;
    int firstLine = 1;

    while (fgets(line, sizeof(line), fp)) {
        line[strcspn(line, "\r\n")] = '\0';
        if (line[0] == '\0') continue;

        if (firstLine) {
            firstLine = 0;
            if (strncmp(line, "#schema:", 8) == 0) {
                /* We could verify the schema here, but for now we assume it matches
                 * the Table's schema which was just created in exec_create. */
                continue;
            }
        }

        /* Generic CSV parse */
        GenericRecord *r = genrec_create(t->schema->col_count);
        if (!r) break;

        char *ptr = line;
        for (int i = 0; i < t->schema->col_count; i++) {
            char *comma = strchr(ptr, ',');
            if (comma) *comma = '\0';

            if (t->schema->columns[i].type == COL_INT) {
                r->values[i].int_val = atoi(ptr);
            } else {
                strncpy(r->values[i].str_val, ptr, MAX_VARCHAR_LEN - 1);
            }

            if (comma) ptr = comma + 1;
            else break;
        }

        table_insert_generic(t, r);

        /* Update primary indexes if they exist */
        if (t->primary_hash && t->primary_bp) {
            int pk_val = r->values[t->primary_col_idx].int_val;
            if (!searchHash((HashTable *)t->primary_hash, pk_val)) {
                insertHash((HashTable *)t->primary_hash, pk_val, (Record *)r);
            }
            insertBPTree((BPTree *)t->primary_bp, pk_val, (Record *)r);
        }

        loaded++;
    }

    fclose(fp);
    printf("[storage] Loaded %d generic record%s from '%s'.\n",
           loaded, loaded == 1 ? "" : "s", path);
    return 0;
}


int rewriteGenericTableToDisk(const char *tableName, const Table *t)
{
    if (!tableName || !t || !t->is_generic) return -1;

    char path[STORAGE_PATH_LEN];
    buildStoragePath(tableName, path);

    FILE *f = fopen(path, "w");
    if (!f) return -1;

    write_schema_header(f, t->schema);

    for (int i = 0; i < t->gen_count; i++) {
        GenericRecord *r = t->gen_records[i];
        for (int j = 0; j < r->col_count; j++) {
            if (t->schema->columns[j].type == COL_INT) {
                fprintf(f, "%d", r->values[j].int_val);
            } else {
                fprintf(f, "%s", r->values[j].str_val);
            }
            fprintf(f, "%s", (j == r->col_count - 1) ? "" : ",");
        }
        fprintf(f, "\n");
    }

    fclose(f);
    return 0;
}