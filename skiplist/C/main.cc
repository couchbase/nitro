#include "skiplist.hh"
#include <vector>
#include <thread>
#include <iostream>

static unsigned int seed;

void insert(Skiplist *s, int n) {
    for (int x=0; x < n; x++) {
        unsigned r = rand_r(&seed);
        int *v = (int *) skiplist_malloc(sizeof(int));
        *v = r;
        Item *itm = newItem(v, sizeof(int));
        Skiplist_Insert(s, itm);
    }
}

int main() {

    srand(time(NULL));
    int i = 100;
    Skiplist *s = newSkiplist();
    std::vector<std::thread> threads;

    for (int x=0; x < 8; x++) {
        threads.push_back(std::thread(&insert,s, 1000000));
    }

    for (auto& th : threads) th.join();

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
