#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/record.h"


Record *record_create(int id, const char *name, int age)
{
    Record *r = (Record *)malloc(sizeof(Record));
    if (!r) {
        fprintf(stderr, "[record] malloc failed in record_create\n");
        return NULL;
    }

    r->id  = id;
    r->age = age;

    /* strncpy + explicit NUL terminator: safe even when name is long */
    strncpy(r->name, name, MAX_NAME_LEN - 1);
    r->name[MAX_NAME_LEN - 1] = '\0';

    return r;
}


void record_copy(Record *dst, const Record *src)
{
    if (!dst || !src) return;

    dst->id  = src->id;
    dst->age = src->age;
    strncpy(dst->name, src->name, MAX_NAME_LEN - 1);
    dst->name[MAX_NAME_LEN - 1] = '\0';
}


void record_print(const Record *r)
{
    if (!r) {
        printf("  [NULL record]\n");
        return;
    }
    
    printf("  %-4d  %-20s  %-4d\n", r->id, r->name, r->age);
}


void record_free(Record *r)
{
    free(r);   /* free(NULL) is a no-op per the C standard */
}