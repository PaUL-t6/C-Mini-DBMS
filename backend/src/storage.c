#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include <dirent.h>

#ifdef _WIN32
#include <direct.h>
#define make_dir(path) _mkdir(path)
#else
#define make_dir(path) mkdir(path, 0777)
#endif

#include "../include/storage.h"
#include "../include/hashtable.h"
#include "../include/bptree.h"
#include "../include/query_executor.h"

#define DATA_DIR "data"

static void ensure_data_dir(void)
{
    if (make_dir(DATA_DIR) == -1) {
        if (errno != EEXIST) {
            fprintf(stderr, "[storage] could not create data directory\n");
        }
    }
}

static void buildStoragePath(const char *tableName, char *outPath)
{
    snprintf(outPath, STORAGE_PATH_LEN, "%s/%s.tbl", DATA_DIR, tableName);
}

int saveGenericRecordToDisk(const char *tableName, const Schema *s, const GenericRecord *r)
{
    if (!tableName || !s || !r) return -1;
    ensure_data_dir();
    char path[STORAGE_PATH_LEN];
    buildStoragePath(tableName, path);
    FILE *f = fopen(path, "a");
    if (!f) return -1;
    for (int i = 0; i < s->col_count; i++) {
        if (s->columns[i].type == COL_INT) fprintf(f, "%d", r->values[i].int_val);
        else fprintf(f, "%s", r->values[i].str_val);
        if (i < s->col_count - 1) fprintf(f, ",");
    }
    fprintf(f, "\n");
    fclose(f);
    return 0;
}

int loadGenericTableFromDisk(const char *tableName, Table *t)
{
    if (!tableName || !t) return -1;
    char path[STORAGE_PATH_LEN];
    buildStoragePath(tableName, path);
    FILE *fp = fopen(path, "r");
    if (!fp) return -1;
    char line[1024];
    int loaded = 0;
    while (fgets(line, sizeof(line), fp)) {
        line[strcspn(line, "\r\n")] = 0;
        if (strlen(line) == 0) continue;
        GenericRecord *r = genrec_create(t->schema->col_count);
        char *ptr = line;
        for (int i = 0; i < t->schema->col_count; i++) {
            char *comma = strchr(ptr, ',');
            if (comma) *comma = '\0';
            if (t->schema->columns[i].type == COL_INT) r->values[i].int_val = atoi(ptr);
            else {
                strncpy(r->values[i].str_val, ptr, MAX_VARCHAR_LEN - 1);
                r->values[i].str_val[MAX_VARCHAR_LEN - 1] = '\0';
            }
            if (comma) ptr = comma + 1; else break;
        }
        table_insert_generic(t, r);
        if (t->primary_hash && t->primary_bp) {
            int pk = r->values[t->primary_col_idx].int_val;
            if (!searchHash((HashTable *)t->primary_hash, pk))
                insertHash((HashTable *)t->primary_hash, pk, r);
            insertBPTree((BPTree *)t->primary_bp, pk, r);
        }
        loaded++;
    }
    fclose(fp);
    printf("[storage] Loaded %d records for '%s'.\n", loaded, tableName);
    return 0;
}

int rewriteGenericTableToDisk(const char *tableName, const Table *t)
{
    if (!tableName || !t) return -1;
    char path[STORAGE_PATH_LEN];
    buildStoragePath(tableName, path);
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    for (int i = 0; i < t->gen_count; i++) {
        GenericRecord *r = t->gen_records[i];
        for (int j = 0; j < t->schema->col_count; j++) {
            if (t->schema->columns[j].type == COL_INT) fprintf(f, "%d", r->values[j].int_val);
            else fprintf(f, "%s", r->values[j].str_val);
            if (j < t->schema->col_count - 1) fprintf(f, ",");
        }
        fprintf(f, "\n");
    }
    fclose(f);
    return 0;
}

void storage_bootstrap(void *dbPtr)
{
    Database *db = (Database *)dbPtr;
    DIR *d = opendir(DATA_DIR);
    if (!d) return;
    struct dirent *dir;
    while ((dir = readdir(d)) != NULL) {
        if (strstr(dir->d_name, ".tbl")) {
            char tableName[64];
            size_t len = strlen(dir->d_name);
            if (len < 5) continue;
            strncpy(tableName, dir->d_name, len - 4);
            tableName[len - 4] = '\0';
            if (db_find_table(db, tableName)) continue;
            /* Skipping auto-load without schema as per design */
        }
    }
    closedir(d);
}