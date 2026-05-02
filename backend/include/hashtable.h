#ifndef HASHTABLE_H
#define HASHTABLE_H

/* ============================================================
 *  hashtable.h  –  Hash index mapping  id -> Record*
 *
 *  Strategy : Separate Chaining
 *  ----------------------------
 *  Each bucket is the head of a singly-linked list of HashNodes.
 *  When two keys hash to the same bucket they form a chain;
 *  lookup walks the chain comparing keys until it finds a match.
 *
 *  Complexity (average case)
 *  -------------------------
 *  Insert  : O(1)
 *  Search  : O(1)   ← the whole point of adding this index
 *  (worst case O(n) if every key collides, but rare with a
 *   good hash function and a reasonable load factor)
 *
 *  Ownership note
 *  --------------
 *  The hash table stores POINTERS to Records that are owned by
 *  the Table (see table.h).  freeHash() only releases the nodes
 *  and bucket array — it never frees the Record objects themselves.
 * ============================================================ */

#include "record.h"

/* Number of buckets.  A prime number reduces clustering. */
#define HT_CAPACITY 101

/* ------------------------------------------------------------
 * HashNode  –  one link in a bucket's collision chain
 * ------------------------------------------------------------ */
typedef struct HashNode {
    int         key;        /* record id                        */
    Record     *value;      /* pointer to the Record (not owned)*/
    struct HashNode *next;  /* next node in the same bucket     */
} HashNode;

/* ------------------------------------------------------------
 * HashTable  –  the index structure itself
 * ------------------------------------------------------------ */
typedef struct {
    HashNode **buckets;     /* array of HT_CAPACITY bucket heads */
    int        size;        /* total number of entries inserted  */
} HashTable;

/* Allocate and return an empty HashTable.
 * All bucket heads are initialised to NULL.
 * Returns NULL on allocation failure. */
HashTable *createHashTable(void);

/* Map a key (record id) to a bucket index in [0, HT_CAPACITY).
 * Uses the division method:  bucket = key % HT_CAPACITY
 * Handles negative ids safely via absolute value. */
int hashFunction(int key);

/* Insert a key->value mapping into the table.
 * If the key already exists its value pointer is updated in place
 * (no duplicate nodes are created).
 * Returns  0 on success, -1 on allocation failure. */
int insertHash(HashTable *ht, int key, Record *value);

/* Return the Record* associated with key, or NULL if not found.
 * This is the fast-path used by SELECT … WHERE id = X. */
Record *searchHash(const HashTable *ht, int key);

/* Free every HashNode and the bucket array, then the HashTable.
 * Records pointed to by the nodes are NOT freed here —
 * that is the Table's responsibility. */
void freeHash(HashTable *ht);

#endif /* HASHTABLE_H */