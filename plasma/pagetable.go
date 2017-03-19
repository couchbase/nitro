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
	copyItem         ItemCopyFn
	copyIndexKey     ItemCopyFn
	indexKeySize     ItemSizeFn
	itemRunSize      ItemRunSizeFn
	copyItemRun      ItemRunCopyFn
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
	ctx.copyItem(p, itm, int(l))
	return p
}

func (ctx *storeCtx) allocMM(sz uintptr) unsafe.Pointer {
	return mm.Malloc(int(sz))
}

func (ctx *storeCtx) freeMM(ptr unsafe.Pointer) {
	mm.Free(ptr)
}

func newStoreContext(indexLayer *skiplist.Skiplist, cfg Config,
	getCompactFilter, getLookupFilter FilterGetter) *storeCtx {

	return &storeCtx{
		useMemMgmt:   cfg.UseMemoryMgmt,
		cmp:          cfg.Compare,
		itemSize:     cfg.ItemSize,
		copyItem:     cfg.CopyItem,
		indexKeySize: cfg.IndexKeySize,
		copyItemRun:  cfg.CopyItemRun,
		itemRunSize:  cfg.ItemRunSize,
		copyIndexKey: cfg.CopyIndexKey,
		getPageId: func(itm unsafe.Pointer, ctx *wCtx) PageId {
			var pid PageId
			if itm == skiplist.MinItem {
				n := indexLayer.HeadNode()
				if n.Link != nil {
					pid = n
				}
			} else if itm == skiplist.MaxItem {
				pid = indexLayer.TailNode()
			} else {
				var found bool
				_, pid, found = indexLayer.Lookup(itm, cfg.Compare, ctx.buf, ctx.slSts)
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
	if s.useMemMgmt {
		n := pid.(*skiplist.Node)
		ptr := n.Item()
		if ptr != nil {
			s.freeMM(ptr)
		}
		s.freeMM(unsafe.Pointer(n))
	}
}

func (s *Plasma) newIndexKey(itm unsafe.Pointer) unsafe.Pointer {
	if itm == nil {
		return nil
	}

	if s.useMemMgmt {
		size := s.IndexKeySize(itm)
		key := s.allocMM(size)
		s.CopyIndexKey(key, itm, int(size))
		return key
	}

	return s.dup(itm)
}

func (s *Plasma) CreateMapping(pid PageId, pg Page, ctx *wCtx) {
	n := pid.(*skiplist.Node)
	pgi := pg.(*page)

	newPtr := unsafe.Pointer(pgi.head)
	n.SetItem(s.newIndexKey(pgi.low))
	n.Link = newPtr
	pgi.prevHeadPtr = newPtr
}

func (s *Plasma) UpdateMapping(pid PageId, pg Page, ctx *wCtx) bool {
	n := pid.(*skiplist.Node)
	pgi := pg.(*page)

	allocs, frees, nra, nrs, memUsed := pg.GetAllocOps()
	newPtr := unsafe.Pointer(pgi.head)
	if atomic.CompareAndSwapPointer(&n.Link, pgi.prevHeadPtr, newPtr) {
		pgi.prevHeadPtr = newPtr

		ctx.sts.AllocSz += int64(memUsed)
		ctx.sts.NumRecordAllocs += int64(nra)
		ctx.sts.NumRecordSwapIn += int64(nrs)

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
	pg = newPage(ctx, n.Item(), ptr)

	if swapin {
		if s.tryPageSwapin(pg) && !s.UpdateMapping(pid, pg, ctx) {
			goto retry
		}
	}

	return pg, nil
}
