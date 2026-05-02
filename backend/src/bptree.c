#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/bptree.h"




BPTreeNode *createNode(int isLeaf)
{
    BPTreeNode *node = (BPTreeNode *)calloc(1, sizeof(BPTreeNode));
    if (!node) {
        fprintf(stderr, "[bptree] calloc failed in createNode\n");
        return NULL;
    }
    node->isLeaf  = isLeaf;
    node->numKeys = 0;
    node->nextLeaf = NULL;
    return node;
}



BPTree *createTree(void)
{
    BPTree *tree = (BPTree *)malloc(sizeof(BPTree));
    if (!tree) {
        fprintf(stderr, "[bptree] malloc failed in createTree\n");
        return NULL;
    }
    tree->root = createNode(1);   /* empty leaf = initial root */
    return tree;
}



void splitChild(BPTreeNode *parent, int i, BPTreeNode *fullChild)
{
    int t = BP_ORDER;   /* shorthand */

    /* Allocate the new right-sibling node (same type as fullChild) */
    BPTreeNode *sibling = createNode(fullChild->isLeaf);

    
    if (fullChild->isLeaf) {
        sibling->numKeys = t;                  /* sibling holds t keys    */

        /* Copy upper half (starting at index t-1) to sibling */
        for (int j = 0; j < t; j++) {
            sibling->keys[j]    = fullChild->keys[t - 1 + j];
            sibling->records[j] = fullChild->records[t - 1 + j];
        }

        fullChild->numKeys = t - 1;            /* left leaf keeps t-1 keys*/

        /* Stitch sibling into the leaf linked list */
        sibling->nextLeaf   = fullChild->nextLeaf;
        fullChild->nextLeaf = sibling;

        /* The separator pushed to parent is a COPY of sibling's first key */
        int medianKey = sibling->keys[0];

        /* Make room in parent for the new separator and new child pointer */
        for (int j = parent->numKeys; j > i; j--) {
            parent->keys[j]         = parent->keys[j - 1];
            parent->children[j + 1] = parent->children[j];
        }
        parent->keys[i]         = medianKey;
        parent->children[i + 1] = sibling;
        parent->numKeys++;

  
    } else {
        sibling->numKeys = t - 1;              /* sibling holds t-1 keys  */

        /* Copy upper keys (after median) to sibling */
        for (int j = 0; j < t - 1; j++) {
            sibling->keys[j] = fullChild->keys[t + j];
        }

        /* Copy upper children to sibling */
        for (int j = 0; j < t; j++) {
            sibling->children[j] = fullChild->children[t + j];
        }

        fullChild->numKeys = t - 1;            /* median is consumed       */
        int medianKey = fullChild->keys[t - 1];

        /* Make room in parent */
        for (int j = parent->numKeys; j > i; j--) {
            parent->keys[j]         = parent->keys[j - 1];
            parent->children[j + 1] = parent->children[j];
        }
        parent->keys[i]         = medianKey;
        parent->children[i + 1] = sibling;
        parent->numKeys++;
    }
}


void insertNonFull(BPTreeNode *node, int key, Record *record)
{
    int i = node->numKeys - 1;   /* start from the rightmost key */

    /* ---- Leaf: insert key + record pointer in sorted position ---- */
    if (node->isLeaf) {
        /* Shift keys/records right until we find where key belongs */
        while (i >= 0 && key < node->keys[i]) {
            node->keys[i + 1]    = node->keys[i];
            node->records[i + 1] = node->records[i];
            i--;
        }
        
        node->keys[i + 1]    = key;
        node->records[i + 1] = record;
        node->numKeys++;

   
    } else {
        
        while (i >= 0 && key < node->keys[i]) {
            i--;
        }
        i++;   

        
        if (node->children[i]->numKeys == BP_MAX_KEYS) {
            splitChild(node, i, node->children[i]);

            
            if (key > node->keys[i]) {
                i++;
            }
        }
        insertNonFull(node->children[i], key, record);
    }
}



void insertBPTree(BPTree *tree, int key, Record *record)
{
    if (!tree || !record) return;

    BPTreeNode *root = tree->root;

    /* --- Root is full: split it and grow the tree --- */
    if (root->numKeys == BP_MAX_KEYS) {
        BPTreeNode *newRoot = createNode(0);   /* new internal root */
        newRoot->children[0] = root;           /* old root = first child */
        splitChild(newRoot, 0, root);          /* split old root          */
        tree->root = newRoot;
        insertNonFull(newRoot, key, record);   /* insert into new tree    */
    } else {
        /* Root has room: just insert */
        insertNonFull(root, key, record);
    }
}



Record *searchBPTree(const BPTree *tree, int key)
{
    if (!tree || !tree->root) return NULL;

    BPTreeNode *node = tree->root;

    /* Descend until we reach a leaf */
    while (!node->isLeaf) {
        /* Find the first key greater than `key` to pick the child */
        int i = 0;
        while (i < node->numKeys && key >= node->keys[i]) {
            i++;
        }
        /* children[i] is the subtree that may contain `key` */
        node = node->children[i];
    }

    /* We are now at the leaf — scan for an exact match */
    for (int i = 0; i < node->numKeys; i++) {
        if (node->keys[i] == key) {
            return node->records[i];   /* found */
        }
    }

    return NULL;   /* not found */
}


int searchAllBPTree(const BPTree *tree, int key,
                    Record **results, int max_results)
{
    if (!tree || !tree->root || !results || max_results <= 0) return 0;

    BPTreeNode *node = tree->root;
    int count = 0;

    /* Step 1 — descend to the correct leaf */
    while (!node->isLeaf) {
        int i = 0;
        while (i < node->numKeys && key >= node->keys[i]) i++;
        node = node->children[i];
    }

    /* Step 2 & 3 — walk the leaf chain collecting all matches */
    while (node != NULL) {
        for (int i = 0; i < node->numKeys; i++) {
            if (node->keys[i] == key) {
                if (count < max_results)
                    results[count++] = node->records[i];
            } else if (node->keys[i] > key) {
                return count;   /* keys are sorted — no more matches */
            }
        }
        node = node->nextLeaf;
    }

    return count;
}


int countBPTreeMatches(const BPTree *tree, int key)
{
    if (!tree || !tree->root) return 0;

    BPTreeNode *node = tree->root;
    int count = 0;

    /* Step 1 — descend to the correct leaf */
    while (!node->isLeaf) {
        int i = 0;
        while (i < node->numKeys && key >= node->keys[i]) i++;
        node = node->children[i];
    }

    /* Step 2 — walk the leaf chain */
    while (node != NULL) {
        for (int i = 0; i < node->numKeys && i < BP_MAX_KEYS; i++) {
            if (node->keys[i] == key) {
                count++;
            } else if (node->keys[i] > key) {
                return count;
            }
        }
        node = node->nextLeaf;
    }

    return count;
}


int getBPTreeHeight(const BPTree *tree)
{
    if (!tree || !tree->root) return 0;
    BPTreeNode *node = tree->root;
    int height = 1;
    while (node && !node->isLeaf) {
        node = node->children[0];
        if (!node) break;
        height++;
    }
    return height;
}




/* Recursive post-order node freeing */
static void freeNode(BPTreeNode *node)
{
    if (!node) return;

    if (!node->isLeaf) {
        /* Free all children first (post-order) */
        for (int i = 0; i <= node->numKeys; i++) {
            freeNode(node->children[i]);
        }
    }
    /* Records are owned by Table — do NOT free them here */
    free(node);
}

void freeBPTree(BPTree *tree)
{
    if (!tree) return;
    freeNode(tree->root);
    free(tree);
}