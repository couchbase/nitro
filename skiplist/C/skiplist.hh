#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <stdlib.h>
#include <inttypes.h>
#include <stdbool.h>
#include <limits.h>
#include <strings.h>
#include <algorithm>
#include <iostream>

using namespace std;

static int MaxLevel = 32;
static float p = 0.25;

void *skiplist_malloc(size_t sz) {
    return malloc(sz);
}

void skiplist_free(void *p) {
    free(p);
}

struct Node;

typedef struct NodeRef {
    struct Node *ptr;
    bool deleted;
} NodeRef;

typedef struct Item {
    int l;
    void *data;
} Item;

typedef struct Node {
    NodeRef * volatile *next;
    Item *itm;
    uint16_t level;
} Node;

typedef struct Skiplist {
    Node *head;
    Node *tail;
    uint16_t level;
    unsigned int randSeed;
} Skiplist;

Item *newItem(void *data, int l) {
    Item *i = (Item *) skiplist_malloc(sizeof(Item));
    i->data = data;
    i->l = l;
    return i;
}

int Item_Compare(Item *itm1, Item *itm2) {
    if (itm2 == NULL || itm2->l == INT_MAX) {
        return 1;
    }

    if (itm1->l == INT_MIN) {
        return -1;
    }

    if (itm1->l == INT_MAX) {
        return 1;
    }

    int l = min(itm1->l,itm2->l);
    return memcmp(itm1->data, itm2->data, l);
}

NodeRef *newRef(Node *ptr, bool deleted) {
    NodeRef *n = (NodeRef *) skiplist_malloc(sizeof(NodeRef));
    n->ptr = ptr;
    n->deleted = deleted;
    return n;
}

Node *newNode(Item *itm, int level) {
    Node *n = (Node *) skiplist_malloc(sizeof(Node));
    n->level = (uint16_t) level;
    n->itm = itm;
    n->next = (NodeRef **) skiplist_malloc((sizeof(NodeRef*)) * level+1);

    return n;
}

void Node_setNext(Node *n, int level, Node *ptr, bool deleted) {
    n->next[level] = newRef(ptr, deleted);
}

NodeRef Node_getNext(Node *n, int level) {
    NodeRef null;
    NodeRef *ref = (NodeRef *) __atomic_load_n(&n->next[level], __ATOMIC_RELAXED);
    if (ref != NULL) {
        return *ref;
    }

    return null;
}

bool Node_dcasNext(Node *n, int level, Node *prevPtr, Node *newPtr,
        bool prevIsdeleted, bool newIsdeleted) {

    bool swapped = false;
    NodeRef * volatile *addr = &n->next[level];
    NodeRef *ref = (NodeRef *) __atomic_load_n(addr, __ATOMIC_RELAXED);

    if (ref != NULL) {
        if (ref->ptr == prevPtr && ref->deleted == prevIsdeleted) {
            swapped = __sync_bool_compare_and_swap(addr, ref, newRef(newPtr, newIsdeleted));
        }
    }

    return swapped;
}

Skiplist *newSkiplist() {
    Skiplist *s;
    Item *minItem, *maxItem;
    Node *head, *tail;

    srand(time(NULL));

    minItem = newItem(NULL, INT_MIN);
    maxItem = newItem(NULL, INT_MAX);

    head = newNode(minItem, MaxLevel);
    tail = newNode(maxItem, MaxLevel);

    for (int i=0; i <= MaxLevel; i++) {
        Node_setNext(head, i, tail, false);
    }

    s = (Skiplist *) skiplist_malloc(sizeof(Skiplist));
    s->head = head;
    s->tail = tail;
    s->level = 0;

    return s;
}

float Skiplist_randFloat(Skiplist *s) {
    return (float)rand_r(&s->randSeed) / (float)RAND_MAX;
}

int Skiplist_randomLevel(Skiplist *s) {
    int nextLevel = 0;
    int level;

    for (; Skiplist_randFloat(s) < p; nextLevel++) {
    }

    if (nextLevel > MaxLevel) {
        nextLevel = MaxLevel;
    }

    level = (int) __atomic_load_n(&s->level, __ATOMIC_RELAXED);
    if (nextLevel > level) {
        __sync_bool_compare_and_swap(&s->level, level, level+1);
        nextLevel = level + 1;
    }
    return nextLevel;
}

bool Skiplist_findPath(Skiplist *s, Item *itm, Node *preds[], Node *succs[]) {
    int cmpVal = 1;
    int level;
    Node *prev, *curr;
    NodeRef curRef, nextRef;

retry:
    prev = s->head;
    level = (int) __atomic_load_n(&s->level, __ATOMIC_RELAXED);
    for (int i=level; i>=0; i--) {
        curRef = Node_getNext(prev, i);
levelSearch:
        while (1) {
            curr = curRef.ptr;
            nextRef = Node_getNext(curr, i);

            cmpVal = Item_Compare(curr->itm, itm);
            if (cmpVal < 0) {
                prev = curr;
                curRef = Node_getNext(prev, i);
                curr = curRef.ptr;
            } else {
                break;
            }
        }

        preds[i] = prev;
        succs[i] = curr;
    }

    if (cmpVal == 0) {
        return true;
    }

    return false;
}


void Skiplist_Insert(Skiplist *s, Item *itm) {
    int itemLevel  = Skiplist_randomLevel(s);
    Node *x = newNode(itm, itemLevel);
    Node *preds[MaxLevel], *succs[MaxLevel];

retry:
    Skiplist_findPath(s, itm, preds, succs);

    Node_setNext(x, 0, succs[0], false);
    if (!Node_dcasNext(preds[0], 0, succs[0], x, false, false)) {
            goto retry;
    }

    for (int i=1; i <= int(itemLevel); i++) {
fixThisLevel:
        while (1) {
            Node_setNext(x, i, succs[i], false);
            if (Node_dcasNext(preds[i], i, succs[i], x, false, false)) {
                break;
            }
            Skiplist_findPath(s, itm, preds, succs);
        }
    }
}


#endif
