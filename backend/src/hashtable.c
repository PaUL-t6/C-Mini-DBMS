#include <stdio.h>
#include <stdlib.h>

#include "../include/hashtable.h"


HashTable *createHashTable(void)
{
    HashTable *ht = (HashTable *)malloc(sizeof(HashTable));
    if (!ht) {
        fprintf(stderr, "[hash] malloc failed in createHashTable\n");
        return NULL;
    }

    /* Allocate the bucket array — HT_CAPACITY pointers, all NULL */
    ht->buckets = (HashNode **)calloc(HT_CAPACITY, sizeof(HashNode *));
    if (!ht->buckets) {
        fprintf(stderr, "[hash] calloc failed for bucket array\n");
        free(ht);
        return NULL;
    }

    ht->size = 0;
    return ht;
}


int hashFunction(int key)
{
    /* abs() handles unlikely negative ids gracefully */
    int k = (key < 0) ? -key : key;
    return k % HT_CAPACITY;
}



int insertHash(HashTable *ht, int key, Record *value)
{
    if (!ht) return -1;

    int index = hashFunction(key);

    /* --- Step 2: check for existing key in the chain --- */
    HashNode *current = ht->buckets[index];
    while (current != NULL) {
        if (current->key == key) {
            /* Key already present — update value in place */
            current->value = value;
            return 0;
        }
        current = current->next;
    }

    /* --- Step 3: new key — allocate a node and prepend --- */
    HashNode *node = (HashNode *)malloc(sizeof(HashNode));
    if (!node) {
        fprintf(stderr, "[hash] malloc failed in insertHash\n");
        return -1;
    }

    node->key   = key;
    node->value = value;

    /* Prepend: new node points to old head, bucket now points to new node */
    node->next          = ht->buckets[index];
    ht->buckets[index]  = node;

    ht->size++;
    return 0;
}



Record *searchHash(const HashTable *ht, int key)
{
    if (!ht) return NULL;

    int index = hashFunction(key);

    /* Walk the chain at this bucket */
    HashNode *current = ht->buckets[index];
    while (current != NULL) {
        if (current->key == key) {
            return current->value;   /* found */
        }
        current = current->next;
    }

    return NULL;   /* not found */
}


void freeHash(HashTable *ht)
{
    if (!ht) return;

    /* Walk every bucket and free its chain */
    for (int i = 0; i < HT_CAPACITY; i++) {
        HashNode *current = ht->buckets[i];
        while (current != NULL) {
            HashNode *next = current->next;   /* save before free */
            free(current);
            current = next;
        }
    }

    free(ht->buckets);
    free(ht);
}