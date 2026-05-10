#ifndef HASHTABLE_H
#define HASHTABLE_H

/* ============================================================
 *  hashtable.h  –  Hash index mapping  id -> void*
 * ============================================================ */

#define HT_CAPACITY 101

typedef struct HashNode {
    int         key;        /* record id                        */
    void       *value;      /* pointer to the data (not owned)  */
    struct HashNode *next;  /* next node in the same bucket     */
} HashNode;

typedef struct {
    HashNode **buckets;     /* array of HT_CAPACITY bucket heads */
    int        size;        /* total number of entries inserted  */
} HashTable;

HashTable *createHashTable(void);
int hashFunction(int key);
int insertHash(HashTable *ht, int key, void *value);
void *searchHash(const HashTable *ht, int key);
void freeHash(HashTable *ht);

#endif /* HASHTABLE_H */