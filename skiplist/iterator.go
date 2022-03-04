// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package skiplist

import "sync/atomic"
import "unsafe"

// Iterator is used for lookup and range operations on skiplist
type Iterator struct {
	cmp        CompareFn
	s          *Skiplist
	prev, curr *Node
	valid      bool
	buf        *ActionBuffer
	deleted    bool

	bs          *BarrierSession
	count       uint
	smrInterval uint
}

// NewIterator creates an iterator for skiplist
func (s *Skiplist) NewIterator(cmp CompareFn,
	buf *ActionBuffer) *Iterator {
	it := s.NewIterator2(cmp, buf)
	it.bs = s.barrier.Acquire()
	return it
}

func (s *Skiplist) NewIterator2(cmp CompareFn,
	buf *ActionBuffer) *Iterator {
	return &Iterator{
		cmp:         cmp,
		s:           s,
		buf:         buf,
		smrInterval: ^uint(0),
	}
}

// SeekFirst moves cursor to the start
func (it *Iterator) SeekFirst() {
	it.prev = it.s.head
	it.curr, _ = it.s.head.getNext(0)
	it.valid = true
}

// SeekWithCmp moves iterator to a provided item by using custom comparator
func (it *Iterator) SeekWithCmp(itm unsafe.Pointer, cmp CompareFn, eqCmp CompareFn) bool {
	var found bool
	if found = it.s.findPath(itm, cmp, it.buf, &it.s.Stats) != nil; found {
		it.prev = it.buf.preds[0]
		it.curr = it.buf.succs[0]
	} else {
		if found = eqCmp != nil && compare(eqCmp, itm, it.buf.preds[0].Item()) == 0; found {
			it.prev = nil
			it.curr = it.buf.preds[0]
		}
	}
	return found
}

// Seek moves iterator to a provided item
func (it *Iterator) Seek(itm unsafe.Pointer) bool {
	it.valid = true
	found := it.s.findPath(itm, it.cmp, it.buf, &it.s.Stats) != nil
	it.prev = it.buf.preds[0]
	it.curr = it.buf.succs[0]
	return found
}

// Valid returns true when iterator reaches the end
func (it *Iterator) Valid() bool {
	if it.valid && it.curr == it.s.tail {
		it.valid = false
	}

	return it.valid
}

// Get returns the current item
func (it *Iterator) Get() unsafe.Pointer {
	return it.curr.Item()
}

// GetNode returns node which holds the current item
func (it *Iterator) GetNode() *Node {
	return it.curr
}

// Next moves iterator to the next item
func (it *Iterator) Next() {
	if it.deleted {
		it.deleted = false
		return
	}

retry:
	it.valid = true
	next, deleted := it.curr.getNext(0)
	if deleted {
		// Current node is deleted. Unlink current node from the level
		// and make next node as current node.
		// If it fails, refresh the path buffer and obtain new current node.
		if it.s.helpDelete(0, it.prev, it.curr, next, &it.s.Stats) {
			it.curr = next
		} else {
			atomic.AddUint64(&it.s.Stats.readConflicts, 1)
			found := it.s.findPath(it.curr.Item(), it.cmp, it.buf, &it.s.Stats) != nil
			last := it.curr
			it.prev = it.buf.preds[0]
			it.curr = it.buf.succs[0]
			if found && last == it.curr {
				goto retry
			}
		}
	} else {
		it.prev = it.curr
		it.curr = next
	}

	it.count++
	if it.count%it.smrInterval == 0 {
		it.Refresh()
	}
}

// Close is a destructor
func (it *Iterator) Close() {
	if it.bs != nil {
		it.s.barrier.Release(it.bs)
	}
}

func (it *Iterator) SetRefreshInterval(interval int) {
	it.smrInterval = uint(interval)
}

func (it *Iterator) Refresh() {
	if it.Valid() {
		currBs := it.bs
		itm := it.Get()
		it.bs = it.s.barrier.Acquire()
		it.Seek(itm)
		it.s.barrier.Release(currBs)
	}
}

func (it *Iterator) Pause() {
	if it.bs != nil {
		it.s.barrier.Release(it.bs)
		it.bs = nil
	}
}

func (it *Iterator) Resume() {
	it.bs = it.s.barrier.Acquire()
}
