package plasma

import (
	"github.com/couchbase/nitro/mm"
	"github.com/couchbase/nitro/skiplist"
	"math/rand"
	"reflect"
	"sync/atomic"
	"unsafe"
)

const evictMask = uint64(0x8000000000000000)

type storeCtx struct {
	useMemMgmt       bool
	itemSize         ItemSizeFn
	cmp              skiplist.CompareFn
	getPageId        func(unsafe.Pointer, *wCtx) PageId
	getCompactFilter FilterGetter
	getLookupFilter  FilterGetter
}

func (ctx *storeCtx) alloc(sz uintptr) unsafe.Pointer {
	b := make([]byte, int(sz))
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	return unsafe.Pointer(hdr.Data)
}

func (ctx *storeCtx) dup(itm unsafe.Pointer) unsafe.Pointer {
	if itm == skiplist.MinItem || itm == skiplist.MaxItem {
		return itm
	}

	l := ctx.itemSize(itm)
	p := ctx.alloc(l)
	memcopy(p, itm, int(l))
	return p
}

func (ctx *storeCtx) allocMM(sz uintptr) unsafe.Pointer {
	return mm.Malloc(int(sz))
}

func (ctx *storeCtx) freeMM(ptr unsafe.Pointer) {

}

func newStoreContext(indexLayer *skiplist.Skiplist, itemSize ItemSizeFn,
	cmp skiplist.CompareFn, getCompactFilter, getLookupFilter FilterGetter) *storeCtx {

	return &storeCtx{
		//useMemMgmt: true,
		cmp:      cmp,
		itemSize: itemSize,
		getPageId: func(itm unsafe.Pointer, ctx *wCtx) PageId {
			var pid PageId
			if itm == skiplist.MinItem {
				pid = indexLayer.HeadNode()
			} else if itm == skiplist.MaxItem {
				pid = indexLayer.TailNode()
			} else {
				var found bool
				_, pid, found = indexLayer.Lookup(itm, cmp, ctx.buf, ctx.slSts)
				if !found {
					return nil
				}
			}

			return pid
		},
		getCompactFilter: getCompactFilter,
		getLookupFilter:  getLookupFilter,
	}
}

func (s *Plasma) AllocPageId(*wCtx) PageId {
	itemLevel := s.Skiplist.NewLevel(rand.Float32)
	return s.Skiplist.NewNode(itemLevel)
}

func (s *Plasma) FreePageId(pid PageId, ctx *wCtx) {
	s.Skiplist.FreeNode(pid.(*skiplist.Node), &s.Skiplist.Stats)
}

func (s *Plasma) CreateMapping(pid PageId, pg Page, ctx *wCtx) {
	n := pid.(*skiplist.Node)
	pgi := pg.(*page)

	newPtr := unsafe.Pointer(pgi.head)
	n.SetItem(pgi.dup(pgi.low))
	n.Link = newPtr
	pgi.prevHeadPtr = newPtr

	_, _, memUsed := pg.GetMallocOps()
	ctx.sts.MemSz += int64(memUsed)
}

func (s *Plasma) UpdateMapping(pid PageId, pg Page, ctx *wCtx) bool {
	n := pid.(*skiplist.Node)
	pgi := pg.(*page)

	beforeInCache := pg.InCache()
	allocs, frees, memUsed := pg.GetMallocOps()
	newPtr := unsafe.Pointer(pgi.head)
	if atomic.CompareAndSwapPointer(&n.Link, pgi.prevHeadPtr, newPtr) {
		pgi.prevHeadPtr = newPtr

		if pg.InCache() {
			ctx.sts.MemSz += int64(memUsed)
		} else if beforeInCache {
			ctx.sts.NumPagesSwapOut += 1
		}

		ctx.freePages(frees)
		return true
	}

	s.discardDeltas(allocs)
	return false
}

func (s *Plasma) discardDeltas(allocs []*pageDelta) {
	if s.useMemMgmt {
		for _, a := range allocs {
			s.freeMM(unsafe.Pointer(a))
		}
	}
}

func (s *Plasma) ReadPage(pid PageId, pgRdr PageReader, swapin bool, ctx *wCtx) (Page, error) {
	var pg Page
	n := pid.(*skiplist.Node)

retry:
	ptr := atomic.LoadPointer(&n.Link)

	if offset := uint64(uintptr(ptr)); offset&evictMask > 0 {
		if pgRdr == nil {
			pg = newPage(ctx, n.Item(), nil)
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
			if !s.UpdateMapping(pid, pg, ctx) {
				ctx.sts.SwapInConflicts += 1
				goto retry
			}

			ctx.sts.NumPagesSwapIn += 1
			ctx.sts.MemSz += int64(pg.ComputeMemUsed())
		}
	} else {
		pg = newPage(ctx, n.Item(), ptr)
	}

	return pg, nil
}
