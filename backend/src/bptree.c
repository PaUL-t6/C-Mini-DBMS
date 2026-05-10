#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/bptree.h"


BPNode *createNode(int isLeaf)
{
    BPNode *node = (BPNode *)calloc(1, sizeof(BPNode));
    if (!node) {
        fprintf(stderr, "[bptree] calloc failed in createNode\n");
        return NULL;
    }
    node->isLeaf  = isLeaf;
    return node;
}

BPTree *createTree(void)
{
    BPTree *tree = (BPTree *)malloc(sizeof(BPTree));
    if (!tree) {
        fprintf(stderr, "[bptree] malloc failed in createTree\n");
        return NULL;
    }
    tree->root = createNode(1);
    return tree;
}

void splitChild(BPNode *parent, int i, BPNode *fullChild)
{
    int t = BP_ORDER;
    BPNode *sibling = createNode(fullChild->isLeaf);

    if (fullChild->isLeaf) {
        sibling->numKeys = t;
        for (int j = 0; j < t; j++) {
            sibling->keys[j]    = fullChild->keys[t - 1 + j];
            sibling->values[j]  = fullChild->values[t - 1 + j];
        }
        fullChild->numKeys = t - 1;
        sibling->nextLeaf   = fullChild->nextLeaf;
        fullChild->nextLeaf = sibling;

        int medianKey = sibling->keys[0];
        for (int j = parent->numKeys; j > i; j--) {
            parent->keys[j]         = parent->keys[j - 1];
            parent->children[j + 1] = parent->children[j];
        }
        parent->keys[i]         = medianKey;
        parent->children[i + 1] = sibling;
        parent->numKeys++;
    } else {
        sibling->numKeys = t - 1;
        for (int j = 0; j < t - 1; j++) sibling->keys[j] = fullChild->keys[t + j];
        for (int j = 0; j < t; j++) sibling->children[j] = fullChild->children[t + j];
        fullChild->numKeys = t - 1;
        int medianKey = fullChild->keys[t - 1];
        for (int j = parent->numKeys; j > i; j--) {
            parent->keys[j]         = parent->keys[j - 1];
            parent->children[j + 1] = parent->children[j];
        }
        parent->keys[i]         = medianKey;
        parent->children[i + 1] = sibling;
        parent->numKeys++;
    }
}

void insertNonFull(BPNode *node, int key, void *value)
{
    int i = node->numKeys - 1;
    if (node->isLeaf) {
        while (i >= 0 && key < node->keys[i]) {
            node->keys[i + 1]    = node->keys[i];
            node->values[i + 1]  = node->values[i];
            i--;
        }
        node->keys[i + 1]    = key;
        node->values[i + 1]  = value;
        node->numKeys++;
    } else {
        while (i >= 0 && key < node->keys[i]) i--;
        i++;
        if (node->children[i]->numKeys == 2 * BP_ORDER - 1) {
            splitChild(node, i, node->children[i]);
            if (key > node->keys[i]) i++;
        }
        insertNonFull(node->children[i], key, value);
    }
}

int insertBPTree(BPTree *tree, int key, void *value)
{
    if (!tree || !value) return -1;
    BPNode *root = tree->root;
    if (root->numKeys == 2 * BP_ORDER - 1) {
        BPNode *newRoot = createNode(0);
        newRoot->children[0] = root;
        splitChild(newRoot, 0, root);
        tree->root = newRoot;
        insertNonFull(newRoot, key, value);
    } else {
        insertNonFull(root, key, value);
    }
    return 0;
}

void *searchBPTree(const BPTree *tree, int key)
{
    if (!tree || !tree->root) return NULL;
    BPNode *node = tree->root;
    while (!node->isLeaf) {
        int i = 0;
        while (i < node->numKeys && key >= node->keys[i]) i++;
        node = node->children[i];
    }
    for (int i = 0; i < node->numKeys; i++) {
        if (node->keys[i] == key) return node->values[i];
    }
    return NULL;
}

int searchAllBPTree(const BPTree *tree, int key, void **results, int max_results)
{
    if (!tree || !tree->root || !results || max_results <= 0) return 0;
    BPNode *node = tree->root;
    int count = 0;
    while (!node->isLeaf) {
        int i = 0;
        while (i < node->numKeys && key >= node->keys[i]) i++;
        node = node->children[i];
    }
    while (node != NULL) {
        for (int i = 0; i < node->numKeys; i++) {
            if (node->keys[i] == key) {
                if (count < max_results) results[count++] = node->values[i];
            } else if (node->keys[i] > key) return count;
        }
        node = node->nextLeaf;
    }
    return count;
}

int countBPTreeMatches(const BPTree *tree, int key)
{
    if (!tree || !tree->root) return 0;
    BPNode *node = tree->root;
    int count = 0;
    while (!node->isLeaf) {
        int i = 0;
        while (i < node->numKeys && key >= node->keys[i]) i++;
        node = node->children[i];
    }
    while (node != NULL) {
        for (int i = 0; i < node->numKeys; i++) {
            if (node->keys[i] == key) count++;
            else if (node->keys[i] > key) return count;
        }
        node = node->nextLeaf;
    }
    return count;
}

int getBPTreeHeight(const BPTree *tree)
{
    if (!tree || !tree->root) return 0;
    BPNode *node = tree->root;
    int height = 1;
    while (node && !node->isLeaf) {
        node = node->children[0];
        height++;
    }
    return height;
}

static void freeNode(BPNode *node)
{
    if (!node) return;
    if (!node->isLeaf) {
        for (int i = 0; i <= node->numKeys; i++) freeNode(node->children[i]);
    }
    free(node);
}

void freeBPTree(BPTree *tree)
{
    if (!tree) return;
    freeNode(tree->root);
    free(tree);
}