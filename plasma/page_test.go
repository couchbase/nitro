package plasma

import (
	"github.com/t3rm1n4l/nitro/skiplist"
	"testing"
	"unsafe"
)

type storePtr struct {
	p interface{}
}

func newTestPage() (*page, *storePtr) {
	sp := new(storePtr)
	pg := &page{
		low: skiplist.MinItem,
		head: &pageDelta{
			op:           opMetaDelta,
			hiItm:        skiplist.MaxItem,
			rightSibling: nil,
		},
	}

	pg.storeCtx = &storeCtx{
		itemSize: func(unsafe.Pointer) uintptr {
			return unsafe.Sizeof(new(skiplist.IntKeyItem))
		},
		cmp: skiplist.CompareInt,
		getPageId: func(unsafe.Pointer, *wCtx) PageId {
			return nil
		},

		getItem: func(PageId) unsafe.Pointer {
			return skiplist.MaxItem
		},
		getCompactFilter: func() ItemFilter {
			return new(defaultFilter)
		},
		getLookupFilter: func() ItemFilter {
			return &nilFilter
		},
	}

	return pg, sp
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

	buf := make([]byte, 1024*1024)
	_, l1, _, numSegs1 := pg1.Marshal(buf, 100)
	pg1.Split(sp)
	pg1.AddFlushRecord(0, l1, numSegs1)

	_, l2, _, numSegs2 := pg1.Marshal(buf, 100)
	pg1.AddFlushRecord(0, l2, numSegs2)

	_, l3, old, _ := pg1.Marshal(buf, FullMarshal)

	if old != l1+l2 || l3 > old {
		t.Errorf("expected %d == %d+%d", old, l1, l2)
	}

	pg1.AddFlushRecord(0, l3, FullMarshal)
	bk := skiplist.NewIntKeyItem(1)
	pg1.Delete(bk)
	_, l4, _, numSegs4 := pg1.Marshal(buf, 100)
	pg1.AddFlushRecord(0, l4, numSegs4)

	_, _, old2, _ := pg1.Marshal(buf, FullMarshal)

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

	encb := make([]byte, 1024*1024)
	encb, _, _, _ = pg1.Marshal(encb, 100)

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
	buf := make([]byte, 1024*1024)
	for i := 0; i < 1000; i++ {
		pg.Insert(skiplist.NewIntKeyItem(i))
	}

	pg.Compact()
	for i := 300; i < 700; i++ {
		pg.Delete(skiplist.NewIntKeyItem(i))
	}

	encb, _, _, _ := pg.Marshal(buf, 100)
	newPg, _ := newTestPage()
	newPg.Unmarshal(encb, nil)

	x := 699
	y := 0
	for pd := newPg.head; pd != nil; pd = pd.next {
		if pd.op != opBasePage {
			v := skiplist.IntFromItem((*recordDelta)(unsafe.Pointer(pd)).itm)
			if pd.op != opDeleteDelta || x != v {
				t.Errorf("expected op:%d, val:%d, got op:%d, val:%d",
					opDeleteDelta, x, pd.op, v)
			}
			x--
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
