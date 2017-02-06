// Copyright (c) 2016 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package nitro

import (
	"github.com/couchbase/nitro/skiplist"
	"unsafe"
)

// Iterator implements Nitro snapshot iterator
type Iterator struct {
	count       int
	refreshRate int

	snap *Snapshot
	iter *skiplist.Iterator
	buf  *skiplist.ActionBuffer
}

func (it *Iterator) skipUnwanted() {
loop:
	if !it.iter.Valid() {
		return
	}
	itm := (*Item)(it.iter.Get())
	if itm.bornSn > it.snap.sn || (itm.deadSn > 0 && itm.deadSn <= it.snap.sn) {
		it.iter.Next()
		it.count++
		goto loop
	}
}

// SeekFirst moves cursor to the beginning
func (it *Iterator) SeekFirst() {
	it.iter.SeekFirst()
	it.skipUnwanted()
}

// Seek to a specified key or the next bigger one if an item with key does not
// exist.
func (it *Iterator) Seek(bs []byte) {
	itm := it.snap.db.newItem(bs, false)
	it.iter.Seek(unsafe.Pointer(itm))
	it.skipUnwanted()
}

// Valid eturns false when the iterator has reached the end.
func (it *Iterator) Valid() bool {
	return it.iter.Valid()
}

// Get eturns the current item data from the iterator.
func (it *Iterator) Get() []byte {
	return (*Item)(it.iter.Get()).Bytes()
}

// GetNode eturns the current skiplist node which holds current item.
func (it *Iterator) GetNode() *skiplist.Node {
	return it.iter.GetNode()
}

// Next moves iterator cursor to the next item
func (it *Iterator) Next() {
	it.iter.Next()
	it.count++
	it.skipUnwanted()
	if it.refreshRate > 0 && it.count > it.refreshRate {
		it.Refresh()
		it.count = 0
	}
}

// Refresh is a helper API to call refresh accessor tokens manually
// This would enable SMR to reclaim objects faster if an iterator is
// alive for a longer duration of time.
func (it *Iterator) Refresh() {
	if it.Valid() {
		itm := it.snap.db.ptrToItem(it.GetNode().Item())
		it.iter.Close()
		it.iter = it.snap.db.store.NewIterator(it.snap.db.iterCmp, it.buf)
		it.iter.Seek(unsafe.Pointer(itm))
	}
}

// SetRefreshRate sets automatic refresh frequency. By default, it is unlimited
// If this is set, the iterator SMR accessor will be refreshed
// after every `rate` items.
func (it *Iterator) SetRefreshRate(rate int) {
	it.refreshRate = rate
}

// Close executes destructor for iterator
func (it *Iterator) Close() {
	it.snap.Close()
	it.snap.db.store.FreeBuf(it.buf)
	it.iter.Close()
}

// NewIterator creates an iterator for a Nitro snapshot
func (m *Nitro) NewIterator(snap *Snapshot) *Iterator {
	if !snap.Open() {
		return nil
	}
	buf := snap.db.store.MakeBuf()
	return &Iterator{
		snap: snap,
		iter: m.store.NewIterator(m.iterCmp, buf),
		buf:  buf,
	}
}
