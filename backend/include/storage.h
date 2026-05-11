#ifndef STORAGE_H
#define STORAGE_H

#include "table.h"
#include "schema.h"

#define STORAGE_PATH_LEN 256

/* Append one generic record as a CSV line to data/<tableName>.tbl */
int saveGenericRecordToDisk(const char *tableName, const Schema *s, const GenericRecord *r);

/* Read data/<tableName>.tbl and replay every stored CSV line as an in-memory GenericRecord */
int loadGenericTableFromDisk(const char *tableName, Table *t);

/* Overwrite data/<tableName>.tbl with all generic records currently in the Table */
int rewriteGenericTableToDisk(const char *tableName, const Table *t);

/* Persist schema to data/<tableName>.schema */
int saveSchemaToDisk(const char *tableName, const Schema *s);

/* Load schema from data/<tableName>.schema */
Schema *loadSchemaFromDisk(const char *tableName);

/* Scan data directory and load all tables into the database */
void storage_bootstrap(void *db);

#endif /* STORAGE_H */