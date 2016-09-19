package plasma

import (
	"github.com/t3rm1n4l/nitro/skiplist"
	"math/rand"
	"sync/atomic"
	"unsafe"
)

type PageTable interface {
	AllocPageId() PageId
	FreePageId(PageId)

	CreateMapping(PageId, Page)
	UpdateMapping(PageId, Page) bool
	ReadPage(PageId) Page
}

type pageTable struct {
	*storeCtx
	*skiplist.Skiplist
	randFn func() float32
}

func newPageTable(sl *skiplist.Skiplist, itmSize ItemSizeFn,
	cmp skiplist.CompareFn) *pageTable {

	pt := &pageTable{
		Skiplist: sl,
		randFn:   rand.New(rand.NewSource(int64(rand.Int()))).Float32,
	}

	pt.storeCtx = &storeCtx{
		cmp:      cmp,
		itemSize: itmSize,
		getDeltas: func(pid PageId) *pageDelta {
			return pt.ReadPage(pid).(*page).head
		},
		getPageId: func(itm unsafe.Pointer, ctx *wCtx) PageId {
			var pid PageId
			if itm == skiplist.MinItem {
				pid = sl.HeadNode()
			} else if itm == skiplist.MaxItem {
				pid = sl.TailNode()
			}
			_, pid, found := sl.Lookup(itm, cmp, ctx.buf, ctx.slSts)
			if !found {
				panic("should not happen")
			}

			return pid
		},
		getItem: func(pid PageId) unsafe.Pointer {
			if pid == nil {
				return skiplist.MaxItem
			}
			return pid.(*skiplist.Node).Item()
		},
	}

	return pt
}

func (s *pageTable) AllocPageId() PageId {
	itemLevel := s.Skiplist.NewLevel(s.randFn)
	return s.Skiplist.NewNode(itemLevel)
}

func (s *pageTable) FreePageId(pid PageId) {
	s.Skiplist.FreeNode(pid.(*skiplist.Node), &s.Skiplist.Stats)
}

func (s *pageTable) CreateMapping(pid PageId, pg Page) {
	n := pid.(*skiplist.Node)
	pgi := pg.(*page)

	newPtr := unsafe.Pointer(pgi.head)
	n.SetItem(pgi.low)
	n.DataPtr = newPtr
	pgi.prevHeadPtr = newPtr
}

func (s *pageTable) UpdateMapping(pid PageId, pg Page) bool {
	n := pid.(*skiplist.Node)
	pgi := pg.(*page)

	newPtr := unsafe.Pointer(pgi.head)
	if atomic.CompareAndSwapPointer(&n.DataPtr, pgi.prevHeadPtr, newPtr) {
		pgi.prevHeadPtr = newPtr
		return true
	}

	return false
}

func (s *pageTable) ReadPage(pid PageId) Page {
	n := pid.(*skiplist.Node)
	ptr := atomic.LoadPointer(&n.DataPtr)
	return &page{
		storeCtx:    s.storeCtx,
		low:         n.Item(),
		head:        (*pageDelta)(ptr),
		prevHeadPtr: ptr,
	}
}
