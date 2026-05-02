#ifndef RECORD_H
#define RECORD_H

/* ============================================================
 *  record.h  –  A single row stored in a table
 *
 *  Fields
 *  ------
 *  id   : integer primary key  (used by B+ tree index & hash table)
 *  name : student name         (fixed-size buffer, MAX_NAME_LEN chars)
 *  age  : student age
 * ============================================================ */

#define MAX_NAME_LEN 64

typedef struct {
    int  id;
    char name[MAX_NAME_LEN];
    int  age;
} Record;

/* Allocate a new Record on the heap and populate it.
 * Caller is responsible for calling record_free() when done. */
Record *record_create(int id, const char *name, int age);

/* Deep-copy src into dst (both must already be allocated). */
void record_copy(Record *dst, const Record *src);

/* Print a single record to stdout in a readable format. */
void record_print(const Record *r);

/* Release heap memory for a record created with record_create(). */
void record_free(Record *r);

#endif /* RECORD_H */