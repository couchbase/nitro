// Copyright (c) 2017 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package plasma

import (
	"github.com/couchbase/nitro/skiplist"
	"math/rand"
	"testing"
	"unsafe"
)

type storePtr struct {
	p interface{}
}

func newTestPage() (*page, *storePtr) {
	sp := new(storePtr)
	pg := &page{
		ctx: &wCtx{
			pgBuffers: make([]*Buffer, maxCtxBuffers),
		},
		allocCtx: new(allocCtx),
		low:      skiplist.MinItem,
		head: &pageDelta{
			op:           opMetaDelta,
			hiItm:        skiplist.MaxItem,
			rightSibling: nil,
		},
	}

	pg.storeCtx = &storeCtx{
		itemSize: func(x unsafe.Pointer) uintptr {
			if x == skiplist.MinItem || x == skiplist.MaxItem {
				return 0
			}
			return unsafe.Sizeof(new(skiplist.IntKeyItem))
		},
		cmp: skiplist.CompareInt,
		getPageId: func(unsafe.Pointer, *wCtx) PageId {
			return nil
		},
		getCompactFilter: func() ItemFilter {
			return new(defaultFilter)
		},
		getLookupFilter: func() ItemFilter {
			return &nilFilter
		},
		copyItem: memcopy,
	}

	pg.storeCtx.copyItemRun = func(srcItms, dstItms []unsafe.Pointer, data unsafe.Pointer) {
		var offset uintptr
		for i, itm := range srcItms {
			dstItm := unsafe.Pointer(uintptr(data) + offset)
			sz := unsafe.Sizeof(new(skiplist.IntKeyItem))
			memcopy(dstItm, itm, int(sz))
			dstItms[i] = dstItm
			offset += sz
		}
	}

	pg.storeCtx.itemRunSize = func(itms []unsafe.Pointer) uintptr {
		return uintptr(len(itms)) * unsafe.Sizeof(new(skiplist.IntKeyItem))
	}

	pg.ctx.Plasma = &Plasma{storeCtx: pg.storeCtx}

	return pg, sp
}

//  Based on MB-23572
func TestPageMergeCorrectness2(t *testing.T) {
	pg1, _ := newTestPage()
	pg1.head.hiItm = pg1.dup(skiplist.NewIntKeyItem(515434))
	pg1.Compact()

	pg2, _ := newTestPage()
	pg2.head.hiItm = pg2.dup(skiplist.NewIntKeyItem(515413))
	pg2.Compact()

	pg3, _ := newTestPage()
	pg3.head.hiItm = pg3.dup(skiplist.NewIntKeyItem(515434))
	pg3.Insert(skiplist.NewIntKeyItem(515414))
	pg3.Compact()

	pg2.Merge(pg3)
	pg2.Insert(skiplist.NewIntKeyItem(515415))

	pg1.Merge(pg2)
	pg1.Insert(skiplist.NewIntKeyItem(515334))
	pg1.Insert(skiplist.NewIntKeyItem(515336))

	itr := pg1.NewIterator()
	var last unsafe.Pointer
	count := 0
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		if last != nil && skiplist.IntFromItem(last) >= skiplist.IntFromItem(itr.Get()) {
			t.Errorf("Expected sort order")
		}
		last = itr.Get()

		count++
	}

	if count != 4 {
		t.Errorf("got only %d items", count)
	}

}

func TestPageMergeCorrectness(t *testing.T) {
	pg, sp := newTestPage()
	for i := 0; i < 1000; i++ {
		bk := skiplist.NewIntKeyItem(i)
		pg.Insert(bk)
	}

	pg.Compact()

	split := pg.Split(sp)

	for i := 500; i < 1000; i++ {
		bk := skiplist.NewIntKeyItem(i)
		split.Delete(bk)
	}

	pg.Merge(split)

	for i := 500; i < 1000; i++ {
		bk := skiplist.NewIntKeyItem(i)
		itm := pg.Lookup(bk)
		if itm != nil {
			t.Errorf("expected missing, found %d", skiplist.IntFromItem(itm))
		}
	}
}

func TestPageMarshalFull(t *testing.T) {
	pg1, sp := newTestPage()
	for i := 0; i < 1000; i++ {
		bk := skiplist.NewIntKeyItem(i)
		pg1.Insert(bk)
	}

	pg1.Compact()

	b := newBuffer(0)
	_, l1, _, numSegs1 := pg1.Marshal(b, 100)
	pg1.Split(sp)
	pg1.AddFlushRecord(0, l1, numSegs1)

	_, l2, _, numSegs2 := pg1.Marshal(b, 100)
	pg1.AddFlushRecord(0, l2, numSegs2)

	_, l3, old, _ := pg1.Marshal(b, FullMarshal)

	if old != l1+l2 || l3 > old {
		t.Errorf("expected %d == %d+%d", old, l1, l2)
	}

	pg1.AddFlushRecord(0, l3, FullMarshal)
	bk := skiplist.NewIntKeyItem(1)
	pg1.Delete(bk)
	_, l4, _, numSegs4 := pg1.Marshal(b, 100)
	pg1.AddFlushRecord(0, l4, numSegs4)

	_, _, old2, _ := pg1.Marshal(b, FullMarshal)

	if old2 != l3+l4 {
		t.Errorf("expected %d == %d+%d", old2, l3, l4)
	}
}

func TestPageMergeMarshal(t *testing.T) {
	pg1, sp := newTestPage()
	for i := 0; i < 1000; i++ {
		bk := skiplist.NewIntKeyItem(i)
		pg1.Insert(bk)
	}

	pg1.Compact()
	pg2 := pg1.Split(sp)
	pg3 := pg2.Split(sp)

	pg2.Delete(skiplist.NewIntKeyItem(501))
	pg2.Delete(skiplist.NewIntKeyItem(502))

	pg3.Delete(skiplist.NewIntKeyItem(900))
	pg3.Delete(skiplist.NewIntKeyItem(901))

	pg3.Close()
	pg2.Merge(pg3)
	pg2.Close()
	pg1.Merge(pg2)

	var itmsE, itmsG []unsafe.Pointer
	itr := pg1.NewIterator()
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		itmsE = append(itmsE, itr.Get())
	}

	if len(itmsE) != 996 {
		t.Errorf("expected 996 items, got %d", len(itmsE))
	}

	b := newBuffer(0)
	encb := b.Get(0, 1024*1024)
	encb, _, _, _ = pg1.Marshal(b, 100)

	newPg, _ := newTestPage()
	newPg.Unmarshal(encb, nil)

	i := 0
	itr = newPg.NewIterator()
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		itmsG = append(itmsG, itr.Get())

		e := skiplist.IntFromItem(itmsE[i])
		g := skiplist.IntFromItem(itmsG[i])
		if e != g {
			t.Errorf("expected %d, got %d", e, g)
		}
		i++
	}

	if len(itmsE) != len(itmsG) {
		t.Errorf("expected %d, got %d", len(itmsE), len(itmsG))
	}
}

func TestPageOperations(t *testing.T) {
	pg, sp := newTestPage()
	for i := 0; i < 1000; i++ {
		bk := skiplist.NewIntKeyItem(i)
		pg.Insert(bk)
	}

	verify := func(pg Page, start, end int, missing bool) {
		for i := start; i < end; i++ {
			bk := skiplist.NewIntKeyItem(i)
			itm := pg.Lookup(bk)
			if missing {
				if itm != nil {
					v := skiplist.IntFromItem(itm)
					t.Errorf("expected missing for %d, got %d", i, v)
				}
			} else {
				if itm == nil {
					t.Errorf("unexpected nil for %d", i)
				} else {
					v := skiplist.IntFromItem(itm)
					if v != i {
						t.Errorf("expected %d, got %d", i, v)
					}
				}
			}
		}
	}

	verify(pg, 0, 1000, false)

	if !pg.NeedCompaction(500) {
		t.Errorf("expected compaction")
	}

	pg.Compact()
	verify(pg, 0, 1000, false)

	if pg.NeedCompaction(500) {
		t.Errorf("unexpected compaction")
	}

	if !pg.NeedSplit(500) {
		t.Errorf("expected split")
	}

	split := pg.Split(sp).(*page)
	sp.p = split.head

	if pg.NeedSplit(500) {
		t.Errorf("unexpected split")
	}

	if split.NeedSplit(500) {
		t.Errorf("unexpected split")
	}

	verify(pg, 0, 500, false)
	verify(split, 500, 1000, false)

	split.Close()
	pg.Merge(split)

	verify(pg, 0, 1000, false)

	for i := 100; i < 400; i++ {
		bk := skiplist.NewIntKeyItem(i)
		pg.Delete(bk)
	}

	for i := 500; i < 800; i++ {
		bk := skiplist.NewIntKeyItem(i)
		pg.Delete(bk)
	}

	verify(pg, 0, 100, false)
	verify(pg, 100, 400, true)
	verify(pg, 400, 500, false)
	verify(pg, 500, 800, true)
	verify(pg, 800, 1000, false)
	pg.Compact()
	verify(pg, 0, 100, false)
	verify(pg, 100, 400, true)
	verify(pg, 400, 500, false)
	verify(pg, 500, 800, true)
	verify(pg, 800, 1000, false)
}

func TestPageIterator(t *testing.T) {
	pg, _ := newTestPage()
	for i := 0; i < 1000; i++ {
		bk := skiplist.NewIntKeyItem(i)
		pg.Insert(bk)
	}

	i := 0
	itr := pg.NewIterator()
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		v := skiplist.IntFromItem(itr.Get())
		if v != i {
			t.Errorf("expected %d, got %d", i, v)
		}
		i++
	}
	if i != 1000 {
		t.Errorf("expected 1000 items")
	}

	i = 550
	itr = pg.NewIterator()
	for itr.Seek(skiplist.NewIntKeyItem(i)); itr.Valid(); itr.Next() {
		v := skiplist.IntFromItem(itr.Get())
		if v != i {
			t.Errorf("expected %d, got %d", i, v)
		}
		i++
	}

	if i != 1000 {
		t.Errorf("expected 1000 items")
	}

	pg.Insert(skiplist.NewIntKeyItem(1500))
	pg.Insert(skiplist.NewIntKeyItem(1600))
	pg.Insert(skiplist.NewIntKeyItem(1601))
	itr = pg.NewIterator()
	itr.Seek(skiplist.NewIntKeyItem((1510)))
	v := skiplist.IntFromItem(itr.Get())
	if v != 1600 {
		t.Errorf("expected %d, got %d", 1600, v)
	}
}

func TestPageMarshal(t *testing.T) {
	pg, _ := newTestPage()
	for i := 0; i < 1000; i++ {
		pg.Insert(skiplist.NewIntKeyItem(i))
	}

	pg.Compact()
	for i := 300; i < 700; i++ {
		pg.Delete(skiplist.NewIntKeyItem(i))
	}

	b := newBuffer(0)
	encb, _, _, _ := pg.Marshal(b, 100)
	newPg, _ := newTestPage()
	newPg.Unmarshal(encb, nil)

	x := 699
	y := 0
	for pd := newPg.head; pd != nil; pd = pd.next {
		if pd.op != opBasePage {
			if pd.op == opInsertDelta || pd.op == opDeleteDelta {
				v := skiplist.IntFromItem((*recordDelta)(unsafe.Pointer(pd)).itm)
				if pd.op != opDeleteDelta || x != v {
					t.Errorf("expected op:%d, val:%d, got op:%d, val:%d",
						opDeleteDelta, x, pd.op, v)
				}
				x--
			}
		} else {
			bp := (*basePage)(unsafe.Pointer(pd))
			for _, itm := range bp.items {
				v := skiplist.IntFromItem(itm)
				if v != y {
					t.Errorf("expected %d, got %d", y, v)
				}
				y++
			}
			break
		}
	}

	if x != 299 {
		t.Errorf("expected 299 items, got %d", x)
	}

	if y != 1000 {
		t.Errorf("expected 1000 items, got %d", y)
	}
}

func BenchmarkPageIterator(b *testing.B) {
	pg, _ := newTestPage()
	for i := 0; i < 400; i++ {
		pg.Insert(skiplist.NewIntKeyItem(rand.Int()))
	}

	pg.Compact()
	for i := 0; i < 200; i++ {
		pg.Insert(skiplist.NewIntKeyItem(rand.Int()))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seekItm := skiplist.NewIntKeyItem(rand.Int())
		var sts pgOpIteratorStats
		itr := newPgOpIterator(pg.head, pg.cmp, seekItm, pg.head.hiItm, new(defaultFilter), pg.ctx, &sts)
		for itr.Init(); itr.Valid(); itr.Next() {
		}
		itr.Close()
	}
}

func TestPageMergeCorrectness3(t *testing.T) {
	pg, sp := newTestPage()
	for i := 0; i < 1000; i++ {
		bk := skiplist.NewIntKeyItem(i)
		pg.Insert(bk)
	}

	pg.Compact()

	readPg, _ := newTestPage()
	var invalidRead = false
	pg.ctx.pageReader = func(offset LSSOffset, ctx *wCtx,
		aCtx *allocCtx, sCtx *storeCtx) (*page, error) {
		if offset == 1000 || offset == 2000 {
			invalidRead = true
		}
		return readPg, nil
	}

	split := pg.Split(sp)
	pgPtr := pg.head
	splitPgPtr := split.(*page).head

	pg.Evict(1000, 1)
	split.Evict(2000, 1)

	// Swapin both parent and child page before the merge
	split.SwapIn(splitPgPtr)
	pg.SwapIn(pgPtr)
	pg.Merge(split)

	readPg.head = pg.head
	pg.Evict(3000, 1)

	i := 0
	itr := pg.NewIterator()
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		v := skiplist.IntFromItem(itr.Get())
		if v != i {
			t.Errorf("expected %d, got %d", i, v)
		}
		i++
	}

	if invalidRead {
		t.Errorf("Expected no invalid swapped out page reads")
	}
}
