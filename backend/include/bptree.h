#ifndef BPTREE_H
#define BPTREE_H

/* ================================================================
 *  bptree.h  –  B+ Tree index on primary key (record id)
 *
 *  What is a B+ Tree?
 *  ------------------
 *  A B+ Tree is a self-balancing tree where:
 *    - INTERNAL nodes  : hold only keys; guide the search down
 *    - LEAF nodes      : hold keys + Record pointers (actual data)
 *    - All leaves are linked left-to-right via `nextLeaf`
 *      (useful for range scans, e.g. WHERE id BETWEEN 3 AND 10)
 *
 *  Order (t = 4)
 *  -------------
 *  Every node obeys these capacity rules:
 *
 *                      min keys    max keys    max children
 *  Internal node  :      t-1=3       2t-1=7       2t=8
 *  Leaf node      :      t-1=3       2t-1=7        -
 *
 *  A node is "full" when it holds 2t-1 = 7 keys.
 *  When a full node would receive one more key it must be SPLIT
 *  before insertion proceeds.
 *
 *  Visual example (order 4, after several inserts):
 *
 *              [ 20 | 40 ]           ← internal node
 *             /     |     \
 *       [10|15]  [25|30]  [45|50]   ← leaf nodes (linked →)
 *
 * ================================================================ */

#include "record.h"

/* B+ Tree order.  Changing this constant resizes every node. */
#define BP_ORDER     4
#define BP_MAX_KEYS  (2 * BP_ORDER - 1)   /* 7  – max keys per node  */
#define BP_MAX_CHILD (2 * BP_ORDER)        /* 8  – max children (internal) */

/* ----------------------------------------------------------------
 * BPTreeNode  –  used for BOTH internal nodes and leaf nodes
 *
 *  isLeaf == 0  →  internal node
 *    keys[i]       : separator keys that guide the search
 *    children[i]   : child BPTreeNode pointers
 *    nextLeaf      : unused (NULL)
 *
 *  isLeaf == 1  →  leaf node
 *    keys[i]       : stored record ids
 *    records[i]    : pointer to the matching data (NOT owned here)
 *    children[i]   : unused (NULL)
 *    nextLeaf      : pointer to the next leaf (linked list)
 * ---------------------------------------------------------------- */
typedef struct BPTreeNode {
    int     isLeaf;                      /* 1 = leaf, 0 = internal        */
    int     numKeys;                     /* how many keys are currently set*/
    int     keys[BP_MAX_KEYS];           /* separator / data keys          */

    /* Internal nodes use children[]; leaf nodes use records[].
     * They share the same index positions (children[i] ↔ records[i]). */
    struct BPTreeNode *children[BP_MAX_CHILD]; /* internal: child pointers */
    void              *records[BP_MAX_KEYS];   /* leaf: Data pointers      */

    struct BPTreeNode *nextLeaf;         /* leaf linked-list pointer       */
} BPTreeNode;

/* ----------------------------------------------------------------
 * BPTree  –  the tree handle (just wraps the root pointer)
 * ---------------------------------------------------------------- */
/* ============================================================
 *  bptree.h  –  B+ Tree index mapping  key -> void*
 * ============================================================ */

#define BP_ORDER 4

typedef struct BPNode {
    int     isLeaf;
    int     numKeys;
    int     keys[2 * BP_ORDER - 1];
    void   *values[2 * BP_ORDER - 1];
    struct BPNode *children[2 * BP_ORDER];
    struct BPNode *nextLeaf;
} BPNode;

typedef struct {
    BPNode *root;
} BPTree;

BPTree *createTree(void);
int insertBPTree(BPTree *tree, int key, void *value);
void *searchBPTree(const BPTree *tree, int key);
int searchAllBPTree(const BPTree *tree, int key, void **results, int max_results);
int countBPTreeMatches(const BPTree *tree, int key);
int getBPTreeHeight(const BPTree *tree);
void freeBPTree(BPTree *tree);

#endif /* BPTREE_H */