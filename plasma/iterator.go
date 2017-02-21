package plasma

import (
	"github.com/couchbase/nitro/skiplist"
	"sort"
	"unsafe"
)

type ItemFilter interface {
	Process(PageItem) PageItemsList
	AddFilter(interface{})
	Reset()
}

type pgOpIterator interface {
	Init()
	Get() PageItem
	Next()
	Valid() bool
}

type acceptAllFilter struct{}

func (f *acceptAllFilter) Process(itm PageItem) PageItemsList { return itm }
func (f *acceptAllFilter) AddFilter(interface{})              {}
func (f *acceptAllFilter) Reset()                             {}

var nilFilter acceptAllFilter

type defaultFilter struct {
	skip bool
}

func (f *defaultFilter) Process(itm PageItem) PageItemsList {
	if !itm.IsInsert() {
		f.skip = true
		return nilPageItemsList
	}

	if f.skip {
		f.skip = false
		return nilPageItemsList
	}

	return itm
}

func (f *defaultFilter) AddFilter(interface{}) {}

func (f *defaultFilter) Reset() {}

type Iterator struct {
	store *Plasma
	*wCtx
	currPid   PageId
	nextPid   PageId
	currPgItr pgOpIterator
	filter    ItemFilter

	err error
}

func (s *Plasma) NewIterator() ItemIterator {
	return &Iterator{
		store:  s,
		filter: new(defaultFilter),
		// TODO: merge with plasma store stats
		wCtx: s.newWCtx2(),
	}
}

func (itr *Iterator) initPgIterator(pid PageId, seekItm unsafe.Pointer) {
	itr.currPid = pid
	if pgPtr, err := itr.store.ReadPage(pid, itr.wCtx.pgRdrFn, true, itr.wCtx); err == nil {
		pg := pgPtr.(*page)
		if err == nil {
			if pg.IsEmpty() {
				panic("an empty page found")
			}

			itr.nextPid = pg.Next()
			itr.filter.Reset()
			itr.currPgItr, _ = newPgOpIterator(pg.head, pg.cmp, seekItm, pg.head.hiItm, itr.filter)
			itr.currPgItr.Init()
		} else {
			itr.err = err
		}
	}
}

func (itr *Iterator) SeekFirst() error {
	itr.initPgIterator(itr.store.Skiplist.HeadNode(), nil)
	itr.tryNextPg()
	return itr.err

}

func (itr *Iterator) Seek(itm unsafe.Pointer) error {
	var pid PageId
	if prev, curr, found := itr.store.Skiplist.Lookup(itm, itr.store.cmp, itr.wCtx.buf, itr.wCtx.slSts); found {
		pid = curr
	} else {
		pid = prev
	}

	itr.initPgIterator(pid, itm)
	itr.tryNextPg()
	return itr.err
}

func (itr *Iterator) Get() unsafe.Pointer {
	return itr.currPgItr.Get().Item()
}

func (itr *Iterator) Valid() bool {
	return itr.currPgItr.Valid()
}

// If the current page has no valid item, move to next page
func (itr *Iterator) tryNextPg() {
	for !itr.currPgItr.Valid() {
		if itr.nextPid == itr.store.EndPageId() {
			break
		}
		itr.initPgIterator(itr.nextPid, nil)
	}
}

func (itr *Iterator) Next() error {
	itr.currPgItr.Next()
	itr.tryNextPg()

	return itr.err
}

// Delta chain sorted iterator
type pdIterator struct {
	deltas []PageItem
	i      int
}

func (pdi *pdIterator) Init() {}

func (pdi *pdIterator) Get() PageItem {
	return pdi.deltas[pdi.i]
}

func (pdi *pdIterator) Valid() bool {
	return pdi.i < len(pdi.deltas)
}

func (pdi *pdIterator) Next() {
	pdi.i++
}

type basePageItem struct{}

func (i *basePageItem) Item() unsafe.Pointer {
	return unsafe.Pointer(i)
}

func (i *basePageItem) IsInsert() bool {
	return true
}

func (i *basePageItem) Len() int {
	return 1
}

func (i *basePageItem) At(int) PageItem {
	return i
}

type insertPageItem struct{}

func (pi *insertPageItem) IsInsert() bool {
	return true
}

func (pi *insertPageItem) Len() int {
	return 1
}

func (pi *insertPageItem) At(int) PageItem {
	return pi
}

func (pi *insertPageItem) Item() unsafe.Pointer {
	return unsafe.Pointer(pi)
}

type removePageItem struct{}

func (pi *removePageItem) IsInsert() bool {
	return false
}

func (pi *removePageItem) Len() int {
	return 1
}

func (pi *removePageItem) At(int) PageItem {
	return pi
}

func (pi *removePageItem) Item() unsafe.Pointer {
	return unsafe.Pointer(pi)
}

// Base page iterator
type basePgIterator struct {
	cmp       skiplist.CompareFn
	low, high unsafe.Pointer
	items     []unsafe.Pointer
	i, j      int
}

func (bpi *basePgIterator) Init() {
	n := len(bpi.items)
	bpi.i = sort.Search(n, func(i int) bool {
		return bpi.cmp(bpi.items[i], bpi.low) >= 0
	})

	bpi.j = sort.Search(n, func(i int) bool {
		return bpi.cmp(bpi.items[i], bpi.high) >= 0
	})
}

func (bpi *basePgIterator) Get() PageItem {
	return (*basePageItem)(bpi.items[bpi.i])
}

func (bpi *basePgIterator) Valid() bool {
	return bpi.i < bpi.j
}

func (bpi *basePgIterator) Next() {
	bpi.i++
}

// Merge two disjoint sorted sets
type pdJoinIterator struct {
	itrs [2]pgOpIterator
	i    int

	currIt pgOpIterator
}

func (pdj *pdJoinIterator) Init() {
	pdj.itrs[0].Init()
	pdj.itrs[1].Init()
}

func (pdj *pdJoinIterator) Valid() bool {
	return pdj.itrs[pdj.i].Valid()
}

func (pdj *pdJoinIterator) Next() {
	pdj.itrs[pdj.i].Next()
	if pdj.i == 0 && !pdj.itrs[pdj.i].Valid() {
		pdj.i++
	}
}

func (pdj *pdJoinIterator) Get() PageItem {
	return pdj.itrs[pdj.i].Get()
}

// Iterator merger
type pdMergeIterator struct {
	itrs   [2]pgOpIterator
	lastIt pgOpIterator
	cmp    skiplist.CompareFn
	ItemFilter

	items  PageItemsList
	offset int
}

func (pdm *pdMergeIterator) Init() {
	pdm.itrs[0].Init()
	pdm.itrs[1].Init()
	pdm.fetchMin()
}

func (pdm *pdMergeIterator) Next() {
	pdm.offset++

	if pdm.offset >= pdm.items.Len() {
		pdm.next()
	}
}

func (pdm *pdMergeIterator) next() {
	if pdm.valid() {
		pdm.lastIt.Next()
		pdm.fetchMin()
	}
}

func (pdm *pdMergeIterator) fetchMin() {
	valid1 := pdm.itrs[0].Valid()
	valid2 := pdm.itrs[1].Valid()

	if valid1 && valid2 {
		itm0 := pdm.itrs[0].Get().Item()
		itm1 := pdm.itrs[1].Get().Item()

		cmpv := pdm.cmp(itm0, itm1)
		if cmpv < 0 {
			pdm.lastIt = pdm.itrs[0]
		} else if cmpv == 0 {
			pdm.lastIt = pdm.itrs[0]
		} else {
			pdm.lastIt = pdm.itrs[1]
		}
	} else if valid1 {
		pdm.lastIt = pdm.itrs[0]
	} else if valid2 {
		pdm.lastIt = pdm.itrs[1]
	}

	pdm.items = nilPageItemsList
	pdm.offset = 0
	if pdm.ItemFilter != nil && pdm.valid() {
		if pdm.items = pdm.Process(pdm.lastIt.Get()); pdm.items == nilPageItemsList {
			pdm.next()
		}
	}
}

func (pdm *pdMergeIterator) Get() PageItem {
	if pdm.lastIt == nil {
		return nil
	}

	if pdm.items.Len() > 0 {
		return pdm.items.At(pdm.offset)
	}

	return pdm.lastIt.Get()
}

func (pdm *pdMergeIterator) valid() bool {
	return pdm.itrs[0].Valid() || pdm.itrs[1].Valid()
}

func (pdm *pdMergeIterator) Valid() bool {
	return pdm.offset < pdm.items.Len()
}

func newPgOpIterator(head *pageDelta, cmp skiplist.CompareFn,
	low, high unsafe.Pointer, filter ItemFilter) (iter pgOpIterator, fdSz int) {

	var hasReloc bool
	m := &pdMergeIterator{cmp: cmp, ItemFilter: filter}
	pdCount := 0

	pdi := &pdIterator{}
	pw := newPgDeltaWalker(head)
loop:
	for ; !pw.End(); pw.Next() {
		op := pw.Op()
		switch op {
		case opRelocPageDelta:
			if !hasReloc {
				_, d, _ := pw.FlushInfo()
				fdSz = int(d)
				hasReloc = true
			}
		case opFlushPageDelta:
			if !hasReloc {
				_, d, _ := pw.FlushInfo()
				fdSz += int(d)
			}
		case opPageSplitDelta:
			sitm := pw.Item()
			if cmp(sitm, high) < 0 {
				high = sitm
			}
		case opPageMergeDelta:
			deltaItr, fdSz1 := newPgOpIterator(pw.NextPd(), cmp, low, high, filter)
			mergeItr, fdSz2 := newPgOpIterator(
				pw.MergeSibling(),
				cmp, low, high, filter)

			if !hasReloc {
				fdSz += fdSz1 + fdSz2
			}

			m.itrs[1] = &pdJoinIterator{
				itrs: [2]pgOpIterator{deltaItr, mergeItr},
			}
			break loop
		case opBasePage:
			m.itrs[1] = &basePgIterator{
				items: pw.BaseItems(),
				cmp:   cmp,
				low:   low,
				high:  high,
			}

			break loop
		case opInsertDelta, opDeleteDelta:
			pdCount++
		case opRollbackDelta:
			filter.AddFilter(pw.RollbackFilter())
		}
	}

	if pdCount > 0 {
		pdi.deltas = make([]PageItem, 0, pdCount)
		for pw.SetEndAndRestart(); !pw.End(); pw.Next() {
			op := pw.Op()
			if op == opInsertDelta || op == opDeleteDelta {
				itm := pw.Item()
				if cmp(itm, high) < 0 && cmp(itm, low) >= 0 {
					pdi.deltas = append(pdi.deltas, pw.PageItem())
				}
			}
		}

		s := pageItemSorter{itms: pdi.deltas, cmp: cmp}
		pdi.deltas = s.Run()
	}
	m.itrs[0] = pdi
	if m.itrs[1] == nil {
		m.itrs[1] = &pdIterator{}
	}

	return m, fdSz
}
