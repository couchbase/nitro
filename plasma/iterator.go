package plasma

import (
	"github.com/t3rm1n4l/nitro/skiplist"
	"sort"
	"unsafe"
)

type Iterator struct {
	store *Plasma
	*wCtx
	currPid   PageId
	nextPid   PageId
	currPgItr pgOpIterator
}

func (s *Plasma) NewIterator() ItemIterator {
	return &Iterator{
		store: s,
		wCtx: &wCtx{
			buf:   s.Skiplist.MakeBuf(),
			slSts: &s.Skiplist.Stats,
			// TODO: merge with plasma store stats
			sts: new(Stats),
		},
	}
}

func (itr *Iterator) initPgIterator(pid PageId, seekItm unsafe.Pointer) {
	itr.currPid = pid
	pg := itr.store.ReadPage(pid).(*page)
	if pg.head != nil {
		itr.nextPid = pg.head.rightSibling
		itr.currPgItr, _ = newPgOpIterator(pg.head, pg.cmp, seekItm, pg.head.hiItm, true)
		itr.currPgItr.Init()
	}
}

func (itr *Iterator) SeekFirst() {
	itr.initPgIterator(itr.store.Skiplist.HeadNode(), nil)

}

func (itr *Iterator) Seek(itm unsafe.Pointer) {
	var pid PageId
	if prev, curr, found := itr.store.Skiplist.Lookup(itm, itr.store.cmp, itr.wCtx.buf, itr.wCtx.slSts); found {
		pid = curr
	} else {
		pid = prev
	}
	itr.initPgIterator(pid, itm)
}

func (itr *Iterator) Get() unsafe.Pointer {
	itm, _ := itr.currPgItr.Get()
	return itm
}

func (itr *Iterator) Valid() bool {
	return itr.currPgItr.Valid()
}

func (itr *Iterator) Next() {
	itr.currPgItr.Next()
	if !itr.currPgItr.Valid() {
		if itr.nextPid != nil {
			itr.initPgIterator(itr.nextPid, nil)
		}
	}
}

// Delta chain sorted iterator
type pdIterator struct {
	deltas []PageItem
	i      int
}

func (pdi *pdIterator) Init() {}

func (pdi *pdIterator) Get() (unsafe.Pointer, bool) {
	return pdi.deltas[pdi.i].Item(), pdi.deltas[pdi.i].IsInsert()
}

func (pdi *pdIterator) Valid() bool {
	return pdi.i < len(pdi.deltas)
}

func (pdi *pdIterator) Next() {
	pdi.i++
}

// Base page interator
type basePgIterator struct {
	cmp       skiplist.CompareFn
	low, high unsafe.Pointer
	bp        *basePage
	i, j      int
}

func (bpi *basePgIterator) Init() {
	n := len(bpi.bp.items)
	bpi.i = sort.Search(n, func(i int) bool {
		return bpi.cmp(bpi.bp.items[i], bpi.low) >= 0
	})

	bpi.j = sort.Search(n, func(i int) bool {
		return bpi.cmp(bpi.bp.items[i], bpi.high) >= 0
	})
}

func (bpi *basePgIterator) Get() (unsafe.Pointer, bool) {
	return bpi.bp.items[bpi.i], true
}

func (bpi *basePgIterator) Valid() bool {
	return bpi.i < bpi.j
}

func (bpi *basePgIterator) Next() {
	bpi.i++
}

// Iterator merger
type pdMergeIterator struct {
	itrs    [2]pgOpIterator
	lastIt  pgOpIterator
	cmp     skiplist.CompareFn
	doDedup bool
}

func (pdm *pdMergeIterator) valid(x int) bool {
	return pdm.itrs[x] != nil && pdm.itrs[x].Valid()
}

func (pdm *pdMergeIterator) Init() {
	if pdm.itrs[0] != nil {
		pdm.itrs[0].Init()
	}

	if pdm.itrs[1] != nil {
		pdm.itrs[1].Init()
	}
	pdm.Next()
}

func (pdm *pdMergeIterator) Next() {
	var lastItm unsafe.Pointer
skip:
	if pdm.lastIt != nil {
		lastItm, _ = pdm.lastIt.Get()
		pdm.lastIt.Next()
	}

	if pdm.valid(0) && pdm.valid(1) {
		itm0, _ := pdm.itrs[0].Get()
		itm1, _ := pdm.itrs[1].Get()

		if pdm.cmp(itm0, itm1) < 0 {
			pdm.lastIt = pdm.itrs[0]
		} else {
			pdm.lastIt = pdm.itrs[1]
		}
	} else if pdm.valid(0) {
		pdm.lastIt = pdm.itrs[0]
	} else if pdm.valid(1) {
		pdm.lastIt = pdm.itrs[1]
	}

	if pdm.doDedup && pdm.lastIt != nil && pdm.lastIt.Valid() {
		currItm, ok := pdm.lastIt.Get()
		if !ok || pdm.cmp(lastItm, currItm) == 0 {
			goto skip
		}
	}
}

func (pdm *pdMergeIterator) Get() (unsafe.Pointer, bool) {
	if pdm.lastIt == nil {
		return nil, false
	}
	return pdm.lastIt.Get()
}

func (pdm *pdMergeIterator) Valid() bool {
	return pdm.valid(0) || pdm.valid(1)
}

type pgOpIterator interface {
	Init()
	Get() (unsafe.Pointer, bool)
	Next()
	Valid() bool
}

func newPgOpIterator(pd *pageDelta, cmp skiplist.CompareFn,
	low, high unsafe.Pointer, doDedup bool) (iter pgOpIterator, fdSz int) {

	var hasReloc bool
	m := &pdMergeIterator{cmp: cmp, doDedup: doDedup}
	startPd := pd
	pdCount := 0

loop:
	for {
		switch {
		case pd == nil:
			break loop
		case pd.op == opRelocPageDelta:
			fpd := (*flushPageDelta)(unsafe.Pointer(pd))
			if !hasReloc {
				fdSz = int(fpd.flushDataSz)
				hasReloc = true
			}
		case pd.op == opFlushPageDelta:
			if !hasReloc {
				fpd := (*flushPageDelta)(unsafe.Pointer(pd))
				fdSz += int(fpd.flushDataSz)
			}
		case pd.op == opPageSplitDelta:
			high = (*splitPageDelta)(unsafe.Pointer(pd)).itm
		case pd.op == opPageMergeDelta:
			itr1, fdSz1 := newPgOpIterator(pd.next, cmp, low, high, false)
			itr2, fdSz2 := newPgOpIterator((*mergePageDelta)(unsafe.Pointer(pd)).mergeSibling,
				cmp, low, high, false)

			if !hasReloc {
				fdSz += fdSz1 + fdSz2
			}

			m.itrs[0] = &pdMergeIterator{
				itrs: [2]pgOpIterator{itr1, itr2},
				cmp:  cmp,
			}
			break loop
		case pd.op == opBasePage:
			m.itrs[0] = &basePgIterator{
				bp:   (*basePage)(unsafe.Pointer(pd)),
				cmp:  cmp,
				low:  low,
				high: high,
			}

			break loop
		}

		pdCount++
		pd = pd.next
	}

	pdi := &pdIterator{}
	if pdCount > 0 {
		pdi.deltas = make([]PageItem, 0, pdCount)
		for x := startPd; x != pd; x = x.next {
			if x.op == opInsertDelta || x.op == opDeleteDelta {
				rec := (*recordDelta)(unsafe.Pointer(x))
				if cmp(rec.itm, high) < 0 && cmp(rec.itm, low) >= 0 {
					pdi.deltas = append(pdi.deltas, x)
				}
			}
		}

		s := pageItemSorter{itms: pdi.deltas, cmp: cmp}
		sort.Stable(&s)
		pdi.deltas = s.itms

		m.itrs[1] = pdi
	}

	return m, fdSz
}
