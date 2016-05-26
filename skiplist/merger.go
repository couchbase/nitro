// Copyright (c) 2016 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.
package skiplist

import "container/heap"
import "unsafe"

type mIterator struct {
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

func NewMergeIterator(iters []*Iterator) *mIterator {
	return &mIterator{
		iters: iters,
	}
}

func (mit *mIterator) SeekFirst() {
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

func (mit *mIterator) Valid() bool {
	return mit.curr != nil
}

func (mit *mIterator) Next() {
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

func (mit *mIterator) Seek(itm unsafe.Pointer) bool {
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

func (mit *mIterator) Get() unsafe.Pointer {
	return mit.curr.Item()
}

func (mit *mIterator) GetNode() *Node {
	return mit.curr
}
