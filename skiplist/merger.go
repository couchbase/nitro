// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package skiplist

import "container/heap"
import "unsafe"

// MergeIterator aggregates multiple iterators
type MergeIterator struct {
	iters []*Iterator
	h     nodeHeap
	curr  *Node
}

type heapItem struct {
	iter *Iterator
	n    *Node
}

type nodeHeap []heapItem

func (h nodeHeap) Len() int           { return len(h) }
func (h nodeHeap) Less(i, j int) bool { return h[i].iter.cmp(h[i].n.Item(), h[j].n.Item()) < 0 }
func (h nodeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *nodeHeap) Push(x interface{}) {
	*h = append(*h, x.(heapItem))
}

func (h *nodeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// NewMergeIterator creates an iterator that merges multiple iterators
func NewMergeIterator(iters []*Iterator) *MergeIterator {
	return &MergeIterator{
		iters: iters,
	}
}

// SeekFirst moves cursor to the first item
func (mit *MergeIterator) SeekFirst() {
	for _, it := range mit.iters {
		it.SeekFirst()
		if it.Valid() {
			n := it.GetNode()
			mit.h = append(mit.h, heapItem{iter: it, n: n})
		}
	}

	heap.Init(&mit.h)
	mit.Next()
}

// Valid returns false when cursor reaches end
func (mit *MergeIterator) Valid() bool {
	return mit.curr != nil
}

// Next moves cursor to the next item
func (mit *MergeIterator) Next() {
	mit.curr = nil
	if mit.h.Len() == 0 {
		return
	}

	o := heap.Pop(&mit.h)
	hi := o.(heapItem)
	mit.curr = hi.n
	hi.iter.Next()
	if hi.iter.Valid() {
		hi.n = hi.iter.GetNode()
		heap.Push(&mit.h, hi)
	}
}

// Seek moves cursor to the specified item, if present
func (mit *MergeIterator) Seek(itm unsafe.Pointer) bool {
	var found bool
	for _, it := range mit.iters {
		if it.Seek(itm) {
			found = true
		}
		if it.Valid() {
			n := it.GetNode()
			mit.h = append(mit.h, heapItem{iter: it, n: n})
		}
	}

	heap.Init(&mit.h)
	mit.Next()

	return found
}

// Get returns current item
func (mit *MergeIterator) Get() unsafe.Pointer {
	return mit.curr.Item()
}

// GetNode returns node for the current item
func (mit *MergeIterator) GetNode() *Node {
	return mit.curr
}
