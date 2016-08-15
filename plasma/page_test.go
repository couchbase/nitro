package plasma

import (
	"testing"
	"unsafe"
)

type storePtr struct {
	p interface{}
}

func newTestPage() (*page, *storePtr) {
	sp := new(storePtr)
	return &page{
		storeCtx: &storeCtx{
			itemSize: func(unsafe.Pointer) uintptr {
				return unsafe.Sizeof(new(IntKeyItem))
			},
			cmp: CompareInt,
			getDeltas: func(PageId) *pageDelta {
				return sp.p.(*pageDelta)
			},
		},
	}, sp
}

func TestPageOperations(t *testing.T) {
	pg, sp := newTestPage()
	for i := 0; i < 1000; i++ {
		bk := NewIntKeyItem(i)
		pg.Insert(bk)
	}

	verify := func(pg Page, start, end int, missing bool) {
		for i := start; i < end; i++ {
			bk := NewIntKeyItem(i)
			itm := pg.Lookup(bk)
			if missing {
				if itm != nil {
					v := IntFromItem(itm)
					t.Errorf("expected missing for %d, got %d", i, v)
				}
			} else {
				if itm == nil {
					t.Errorf("unexpected nil for %d", i)
				} else {
					v := IntFromItem(itm)
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
		bk := NewIntKeyItem(i)
		pg.Delete(bk)
	}

	for i := 500; i < 800; i++ {
		bk := NewIntKeyItem(i)
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
		bk := NewIntKeyItem(i)
		pg.Insert(bk)
	}

	i := 0
	for itr := pg.NewIterator(); itr.Valid(); itr.Next() {
		v := IntFromItem(itr.Get())
		if v != i {
			t.Errorf("expected %d, got %d", i, v)
		}
		i++
	}
	if i != 1000 {
		t.Errorf("expected 1000 items")
	}

	i = 550
	itr := pg.NewIterator()
	for itr.Seek(NewIntKeyItem(i)); itr.Valid(); itr.Next() {
		v := IntFromItem(itr.Get())
		if v != i {
			t.Errorf("expected %d, got %d", i, v)
		}
		i++
	}

	if i != 1000 {
		t.Errorf("expected 1000 items")
	}

	pg.Insert(NewIntKeyItem(1500))
	pg.Insert(NewIntKeyItem(1600))
	pg.Insert(NewIntKeyItem(1601))
	itr = pg.NewIterator()
	itr.Seek(NewIntKeyItem((1510)))
	v := IntFromItem(itr.Get())
	if v != 1600 {
		t.Errorf("expected %d, got %d", 1600, v)
	}
}
