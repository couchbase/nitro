// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.
#include "skiplist.hh"
#include <vector>
#include <thread>
#include <iostream>

static unsigned int seed;

void insert(Skiplist *s, int n, bool is_rand) {
    for (int x=0; x < n; x++) {
        unsigned r;
        if (is_rand) {
            r = rand_r(&seed);
        } else {
            r = x;
        }
        int *v = (int *) skiplist_malloc(sizeof(int));
        *v = r;
        Item *itm = newItem(v, sizeof(int));
        Skiplist_Insert(s, itm);
    }
}

void lookup(Skiplist *s, int n) {
    Node *preds[MaxLevel], *succs[MaxLevel];
    for (int x=0; x < n; x++) {
        unsigned r = rand_r(&seed);
        int *v = (int *) skiplist_malloc(sizeof(int));
        *v = r % n;
        Item *itm = newItem(v, sizeof(int));
        Skiplist_findPath(s, itm, preds, succs);
        skiplist_free(itm);
    }
}

int main() {

    srand(time(NULL));
    int i = 100;
    Skiplist *s = newSkiplist();
    std::vector<std::thread> threads;

    insert(s, 10000000, false);

    time_t t0 = time(NULL);
    /*
    for (int x=0; x < 8; x++) {
        threads.push_back(std::thread(&insert,s, 1000000, true));
    }
    */
    for (int x=0; x < 8; x++) {
        threads.push_back(std::thread(&lookup,s, 1000000));
    }

    for (auto& th : threads) th.join();
    std::cout<<"took "<<(time(NULL)-t0)<<"s"<<std::endl;

    exit(0);
    int count = 0;
    Node *p = s->head;
    while (p) {
        if (p->itm->l == 4) {
            count++;
//            std::cout<<"itm "<<count<<" - "<<*((int *)(p->itm->data))<<std::endl;
        }

        NodeRef r = Node_getNext(p, 0);
        p = r.ptr;
    }

    std::cout<<count<<std::endl;
}
