package plasma

import (
	"fmt"
	"github.com/t3rm1n4l/nitro/skiplist"
	"reflect"
	"sort"
	"unsafe"
)

type pageOp uint16

const (
	opBasePage pageOp = iota

	opInsertDelta
	opDeleteDelta

	opPageSplitDelta
	opPageRemoveDelta
	opPageMergeDelta
)

type PageId interface{}

type Page interface {
	Insert(itm unsafe.Pointer)
	Delete(itm unsafe.Pointer)
	Lookup(itm unsafe.Pointer) unsafe.Pointer
	NewIterator() ItemIterator

	InRange(itm unsafe.Pointer) bool

	NeedCompaction(int) bool
	NeedMerge(int) bool
	NeedSplit(int) bool
	NeedRemoval() bool

	Close()
	Split(PageId) Page
	Merge(Page)
	Compact()
}

type ItemIterator interface {
	Seek(unsafe.Pointer)
	Get() unsafe.Pointer
	Valid() bool
	Next()
}

type PageItem interface {
	IsInsert() bool
	Item() unsafe.Pointer
}

type pageItem struct {
	itm unsafe.Pointer
}

func (pi *pageItem) IsInsert() bool {
	return true
}

func (pi *pageItem) Item() unsafe.Pointer {
	return pi.itm
}

var pageDeltaHdrSize = unsafe.Sizeof(*new(pageDelta))

type pageDelta struct {
	op       pageOp
	chainLen uint16
	numItems uint16

	next *pageDelta

	hiItm        unsafe.Pointer
	rightSibling PageId
}

type basePage struct {
	op       pageOp
	chainLen uint16
	numItems uint16

	data unsafe.Pointer

	hiItm        unsafe.Pointer
	rightSibling PageId
	items        []unsafe.Pointer
}

type recordDelta struct {
	pageDelta
	itm unsafe.Pointer
}

func (rd *recordDelta) IsInsert() bool {
	return rd.op == opInsertDelta
}

func (rd *recordDelta) Item() unsafe.Pointer {
	return rd.itm
}

type splitPageDelta struct {
	pageDelta
	itm unsafe.Pointer
}

type mergePageDelta struct {
	pageDelta
	itm          unsafe.Pointer
	mergeSibling *pageDelta
}

type removePageDelta pageDelta

type storeCtx struct {
	itemSize  func(unsafe.Pointer) uintptr
	cmp       skiplist.CompareFn
	getDeltas func(PageId) *pageDelta
}

type page struct {
	*storeCtx

	low         unsafe.Pointer
	prevHeadPtr unsafe.Pointer
	head        *pageDelta
}

func (pg *page) newRecordDelta(op pageOp, itm unsafe.Pointer) *pageDelta {
	pd := new(recordDelta)
	var hiItm unsafe.Pointer
	if pg.head == nil {
		hiItm = skiplist.MaxItem
	} else {
		*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
		hiItm = pg.head.hiItm
	}

	pd.next = pg.head
	pd.chainLen++

	pd.op = op
	pd.itm = itm
	pd.hiItm = hiItm
	return (*pageDelta)(unsafe.Pointer(pd))
}

func (pg *page) newSplitPageDelta(itm unsafe.Pointer, pid PageId) *pageDelta {
	pd := new(splitPageDelta)
	if pg.head != nil {
		*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	}
	pd.next = pg.head
	pd.op = opPageSplitDelta
	pd.itm = itm
	pd.rightSibling = pid
	return (*pageDelta)(unsafe.Pointer(pd))
}

func (pg *page) newMergePageDelta(itm unsafe.Pointer, sibl *pageDelta) *pageDelta {
	pd := new(mergePageDelta)
	pd.op = opPageMergeDelta
	pd.itm = itm
	pd.next = pg.head
	pd.mergeSibling = sibl
	pd.rightSibling = sibl.rightSibling
	return (*pageDelta)(unsafe.Pointer(pd))
}

func (pg *page) newRemovePageDelta() *pageDelta {
	pd := new(removePageDelta)
	*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	pd.op = opPageRemoveDelta
	pd.next = pg.head
	return (*pageDelta)(unsafe.Pointer(pd))
}

func (pg *page) newBasePage(itms []unsafe.Pointer) *pageDelta {
	var sz uintptr
	for _, itm := range itms {
		sz += pg.itemSize(itm)
	}

	bp := &basePage{op: opBasePage, numItems: uint16(len(itms))}
	bp.items = make([]unsafe.Pointer, len(itms))

	bp.data = pg.alloc(sz)
	var offset uintptr
	for i, itm := range itms {
		itmsz := pg.itemSize(itm)
		dstItm := unsafe.Pointer(uintptr(bp.data) + offset)
		memcopy(dstItm, itm, int(itmsz))
		bp.items[i] = dstItm
		offset += itmsz
	}

	bp.numItems = uint16(len(itms))
	bp.rightSibling = pg.head.rightSibling
	bp.hiItm = pg.head.hiItm

	return (*pageDelta)(unsafe.Pointer(bp))
}

func (pg *page) InRange(itm unsafe.Pointer) bool {
	if pg.head != nil && pg.cmp(itm, pg.head.hiItm) >= 0 {
		return false
	}

	return true
}

func (pg *page) alloc(sz uintptr) unsafe.Pointer {
	b := make([]byte, int(sz))
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	return unsafe.Pointer(hdr.Data)
}

func (pg *page) Insert(itm unsafe.Pointer) {
	pg.head = pg.newRecordDelta(opInsertDelta, itm)
}

func (pg *page) Delete(itm unsafe.Pointer) {
	pg.head = pg.newRecordDelta(opDeleteDelta, itm)
}

func (pg *page) Lookup(itm unsafe.Pointer) unsafe.Pointer {
	pd := pg.head

	if pd == nil {
		return nil
	} else if pg.cmp(itm, pd.hiItm) >= 0 {
		pd = pg.getDeltas(pd.rightSibling)
	}

loop:
	for pd != nil {
		switch pd.op {
		case opInsertDelta:
			pdr := (*recordDelta)(unsafe.Pointer(pd))
			if pg.cmp(pdr.itm, itm) == 0 {
				return pdr.itm
			}
		case opDeleteDelta:
			pdr := (*recordDelta)(unsafe.Pointer(pd))
			if pg.cmp(pdr.itm, itm) == 0 {
				return nil
			}
		case opBasePage:
			bp := (*basePage)(unsafe.Pointer(pd))
			n := int(bp.numItems)
			index := sort.Search(n, func(i int) bool {
				return pg.cmp(bp.items[i], itm) >= 0
			})

			if index < n && pg.cmp(bp.items[index], itm) == 0 {
				return bp.items[index]
			}

			return nil
		case opPageSplitDelta:
		case opPageMergeDelta:
			pdm := (*mergePageDelta)(unsafe.Pointer(pd))
			if pg.cmp(itm, pdm.itm) >= 0 {
				pd = pdm.mergeSibling
				continue loop
			}
		default:
			panic(fmt.Sprint("should not happen op:", pd.op))
		}
		pd = pd.next
	}

	return nil
}

func (pg *page) NeedCompaction(threshold int) bool {
	return pg.head != nil && int(pg.head.chainLen) > threshold
}

func (pg *page) NeedSplit(threshold int) bool {
	return pg.head != nil && int(pg.head.numItems) > threshold
}

func (pg *page) NeedMerge(threshold int) bool {
	return pg.head != nil && int(pg.head.numItems) < threshold
}

func (pg *page) NeedRemoval() bool {
	return pg.head != nil && pg.head.op == opPageRemoveDelta
}

func (pg *page) Close() {
	pg.head = pg.newRemovePageDelta()
}

func (pg *page) Split(pid PageId) Page {
	newPage := new(page)
	*newPage = *pg
	newPage.prevHeadPtr = nil
	head := pg.head
	curr := head
	for ; curr != nil && curr.op != opBasePage; curr = curr.next {
	}

	bp := (*basePage)(unsafe.Pointer(curr))
	mid := len(bp.items) / 2
	for mid > 0 {
		if pg.cmp(bp.items[mid], head.hiItm) < 0 {
			break
		}
		mid--
	}

	if mid > 0 {
		itms := pg.collectItems(head, bp.items[mid], head.hiItm)
		newPage.head = pg.newBasePage(itms)
		newPage.low = (*basePage)(unsafe.Pointer(newPage.head)).items[0]
		pg.head = pg.newSplitPageDelta(bp.items[mid], pid)
		pg.head.hiItm = bp.items[mid]
		pg.head.numItems = uint16(len(bp.items[:mid]))
		return newPage
	}

	return nil
}

func (pg *page) Compact() {
	itms := pg.collectItems(pg.head, nil, pg.head.hiItm)
	pg.head = pg.newBasePage(itms)
}

func (pg *page) Merge(sp Page) {
	siblPage := (sp.(*page)).head.next
	pdm := pg.newMergePageDelta(pg.head.hiItm, siblPage)
	pdm.next = pg.head
	pg.head = pdm
	pg.head.hiItm = siblPage.hiItm
}

func (pg *page) newPageItemSorter(head *pageDelta) pageItemSorter {
	chainLen := 0
	if head != nil {
		chainLen = int(head.chainLen)
	}

	return pageItemSorter{
		cmp:  pg.cmp,
		itms: make([]PageItem, 0, chainLen),
	}
}

func (pg *page) inRange(lo, hi unsafe.Pointer, itm unsafe.Pointer) bool {
	return pg.cmp(itm, hi) < 0 && pg.cmp(itm, lo) >= 0
}

func (pg *page) collectPageItems(head *pageDelta, loItm, hiItm unsafe.Pointer) []PageItem {
	sorter := pg.newPageItemSorter(head)
	for pd := head; pd != nil; pd = pd.next {
		switch pd.op {
		case opInsertDelta, opDeleteDelta:
			rec := (*recordDelta)(unsafe.Pointer(pd))
			if pg.inRange(loItm, hiItm, rec.itm) {
				sorter.Add(rec)
			}
		case opPageSplitDelta:
		case opPageMergeDelta:
			pds := (*mergePageDelta)(unsafe.Pointer(pd))
			sorter.Add(pg.collectPageItems(pds.mergeSibling, loItm, hiItm)...)
		case opBasePage:
			bp := (*basePage)(unsafe.Pointer(pd))
			var pgItms []PageItem
			for _, itm := range bp.items {
				if pg.inRange(loItm, hiItm, itm) {
					pgItms = append(pgItms, &pageItem{itm: itm})
				}
			}

			merger := pg.newPageItemSorter(nil)
			merger.Init(pgItms)
			return merger.Merge(sorter.Run())
		}
	}

	return sorter.Run()
}

func (pg *page) collectItems(head *pageDelta, loItm, hiItm unsafe.Pointer) []unsafe.Pointer {
	var itms []unsafe.Pointer
	for _, itm := range pg.collectPageItems(head, loItm, hiItm) {
		if itm.IsInsert() {
			itms = append(itms, itm.Item())
		}
	}

	return itms
}

type pageIterator struct {
	cmp  skiplist.CompareFn
	itms []unsafe.Pointer
	i    int
}

func (pi *pageIterator) Get() unsafe.Pointer {
	return pi.itms[pi.i]
}

func (pi *pageIterator) Valid() bool {
	return pi.i < len(pi.itms)
}

func (pi *pageIterator) Next() {
	pi.i++
}

func (pi *pageIterator) Seek(itm unsafe.Pointer) {
	pi.i = sort.Search(len(pi.itms), func(i int) bool {
		return pi.cmp(pi.itms[i], itm) >= 0
	})

}

func (pg *page) NewIterator() ItemIterator {
	return &pageIterator{
		itms: pg.collectItems(pg.head, nil, pg.head.hiItm),
		cmp:  pg.cmp,
	}
}
