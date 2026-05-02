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
 *    records[i]    : pointer to the matching Record (NOT owned here)
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
    Record            *records[BP_MAX_KEYS];   /* leaf: Record pointers    */

    struct BPTreeNode *nextLeaf;         /* leaf linked-list pointer       */
} BPTreeNode;

/* ----------------------------------------------------------------
 * BPTree  –  the tree handle (just wraps the root pointer)
 * ---------------------------------------------------------------- */
typedef struct {
    BPTreeNode *root;
} BPTree;

/* Allocate a new node.  isLeaf controls which fields are active.
 * All pointers are zeroed; numKeys starts at 0. */
BPTreeNode *createNode(int isLeaf);

/* Allocate and return an empty B+ Tree with a single empty leaf root. */
BPTree *createTree(void);

/* Insert key + Record pointer into the tree.
 * Handles root-split when the root is full.
 * Does nothing (prints a warning) on duplicate keys. */
void insertBPTree(BPTree *tree, int key, Record *record);

/* When a child node is full before insertion, split it in two.
 * parent  : the internal node that owns the full child
 * i       : index in parent->children[] of the full child
 * The median key is pushed up into parent, and a new sibling
 * node is created to hold the upper half of the old child's keys. */
void splitChild(BPTreeNode *parent, int i, BPTreeNode *fullChild);

/* Insert key into the subtree rooted at `node`, which is guaranteed
 * to be non-full at the time of the call (pre-split on the way down).*/
void insertNonFull(BPTreeNode *node, int key, Record *record);

/* Search for a key in the tree.
 * Returns the Record* if found, NULL otherwise.
 * Time complexity: O(log n) */
Record *searchBPTree(const BPTree *tree, int key);

/* Search for ALL records whose key equals `key`.
 * Used by the age secondary index where multiple records can share
 * the same age value (non-unique key).
 *
 * Results are written into caller-supplied `results[]` array.
 * `max_results` is the capacity of that array.
 * Returns the number of matches found (0 if none).
 *
 * Strategy: descend to the first matching leaf via normal tree
 * walk, then follow the nextLeaf linked-list to collect all
 * duplicates that span across leaf boundaries.
 * Time complexity: O(log n + k) where k = number of matches. */
int searchAllBPTree(const BPTree *tree, int key,
                    Record **results, int max_results);

/* Return the number of records with the given key (O(log n + k)). */
int countBPTreeMatches(const BPTree *tree, int key);

/* Return the depth of the tree (root to leaf). O(height). */
int getBPTreeHeight(const BPTree *tree);

/* Free every node in the tree.
 * Records pointed to by leaves are NOT freed (owned by Table). */
void freeBPTree(BPTree *tree);

#endif /* BPTREE_H */