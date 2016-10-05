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
	return &page{
		storeCtx: &storeCtx{
			itemSize: func(unsafe.Pointer) uintptr {
				return unsafe.Sizeof(new(skiplist.IntKeyItem))
			},
			cmp: skiplist.CompareInt,
			getDeltas: func(PageId) *pageDelta {
				return sp.p.(*pageDelta)
			},
			getPageId: func(unsafe.Pointer, *wCtx) PageId {
				return nil
			},

			getItem: func(PageId) unsafe.Pointer {
				return skiplist.MaxItem
			},
		},
	}, sp
}

func TestPageMergeCorrectness(t *testing.T) {
	pg, sp := newTestPage()
	for i := 0; i < 1000; i++ {
		bk := skiplist.NewIntKeyItem(i)
		pg.Insert(bk)
	}

	pg.Compact()

	split := pg.Split(sp).(*page)

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
	for itr := pg.NewIterator(); itr.Valid(); itr.Next() {
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
	itr := pg.NewIterator()
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

	encb, _ := pg.Marshal(buf)
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
