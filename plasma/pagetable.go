package plasma

import (
	"github.com/t3rm1n4l/nitro/skiplist"
	"math/rand"
	"sync/atomic"
	"unsafe"
)

const evictMask = uint64(0x8000000000000000)

type PageTable interface {
	AllocPageId() PageId
	FreePageId(PageId)

	CreateMapping(PageId, Page)
	UpdateMapping(PageId, Page) bool
	ReadPage(PageId, PageReader, swapin bool) (Page, error)

	EvictPage(PageId, Page, LSSOffset) bool
}

type pageTable struct {
	*storeCtx
	*skiplist.Skiplist
	sts *Stats
}

func newPageTable(sl *skiplist.Skiplist, itmSize ItemSizeFn,
	cmp skiplist.CompareFn, getCompactFilter, getLookupFilter FilterGetter, sts *Stats) *pageTable {

	pt := &pageTable{
		Skiplist: sl,
		sts:      sts,
	}

	pt.storeCtx = &storeCtx{
		cmp:      cmp,
		itemSize: itmSize,
		getPageId: func(itm unsafe.Pointer, ctx *wCtx) PageId {
			var pid PageId
			if itm == skiplist.MinItem {
				pid = sl.HeadNode()
			} else if itm == skiplist.MaxItem {
				pid = sl.TailNode()
			} else {
				var found bool
				_, pid, found = sl.Lookup(itm, cmp, ctx.buf, ctx.slSts)
				if !found {
					return nil
				}
			}

			return pid
		},
		// TODO: depreciate
		getItem: func(pid PageId) unsafe.Pointer {
			if pid == nil {
				return skiplist.MaxItem
			}
			return pid.(*skiplist.Node).Item()
		},
		getCompactFilter: getCompactFilter,
		getLookupFilter:  getLookupFilter,
	}

	return pt
}

func (s *pageTable) AllocPageId() PageId {
	itemLevel := s.Skiplist.NewLevel(rand.Float32)
	return s.Skiplist.NewNode(itemLevel)
}

func (s *pageTable) FreePageId(pid PageId) {
	s.Skiplist.FreeNode(pid.(*skiplist.Node), &s.Skiplist.Stats)
}

func (s *pageTable) CreateMapping(pid PageId, pg Page) {
	n := pid.(*skiplist.Node)
	pgi := pg.(*page)

	newPtr := unsafe.Pointer(pgi.head)
	n.SetItem(pgi.dup(pgi.low))
	n.Link = newPtr
	pgi.prevHeadPtr = newPtr
}

func (s *pageTable) UpdateMapping(pid PageId, pg Page) bool {
	n := pid.(*skiplist.Node)
	pgi := pg.(*page)

	newPtr := unsafe.Pointer(pgi.head)
	if atomic.CompareAndSwapPointer(&n.Link, pgi.prevHeadPtr, newPtr) {
		pgi.prevHeadPtr = newPtr
		return true
	}

	return false
}

func (s *pageTable) ReadPage(pid PageId, pgRdr PageReader, swapin bool) (Page, error) {
	var pg Page
	n := pid.(*skiplist.Node)

retry:
	ptr := atomic.LoadPointer(&n.Link)

	if offset := uint64(uintptr(ptr)); offset&evictMask > 0 {
		if pgRdr == nil {
			pg = newPage(s.storeCtx, n.Item(), nil)
			pg.SetNext(NextPid(pid))
			return pg, nil
		}

		var err error
		off := LSSOffset(offset & ^evictMask)
		pg, err = pgRdr(off)
		if err != nil {
			return nil, err
		}

		if swapin {
			if !s.UpdateMapping(pid, pg) {
				atomic.AddInt64(&s.sts.SwapInConflicts, 1)
				goto retry
			}

			pg.InCache(true)
			atomic.AddInt64(&s.sts.NumPagesSwapIn, 1)
			atomic.AddInt64(&s.sts.MemSz, int64(pg.ComputeMemUsed()))
		}
	} else {
		pg = newPage(s.storeCtx, n.Item(), ptr)
	}

	return pg, nil
}

func (s *pageTable) EvictPage(pid PageId, pg Page, offset LSSOffset) bool {
	n := pid.(*skiplist.Node)
	pgi := pg.(*page)

	newPtr := unsafe.Pointer(uintptr(uint64(offset) | evictMask))
	if atomic.CompareAndSwapPointer(&n.Link, pgi.prevHeadPtr, newPtr) {
		pgi.prevHeadPtr = newPtr
		if pg.IsInCache() {
			atomic.AddInt64(&s.sts.NumPagesSwapOut, 1)
			atomic.AddInt64(&s.sts.MemSz, -int64(pg.ComputeMemUsed()))
		}
		return true
	}

	return false
}
