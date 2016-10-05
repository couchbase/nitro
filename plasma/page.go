package plasma

import (
	"encoding/binary"
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

	opFlushPageDelta
)

var inFlushingOffset = lssOffset(0xffffffffffffffff)

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
	Compact() (fdSize int)

	PrependDeltas(Page)
	Marshal([]byte) (bs []byte, fdSize int)
}

type ItemIterator interface {
	SeekFirst()
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
	op          pageOp
	chainLen    uint16
	numItems    uint16
	pageVersion uint16

	next *pageDelta

	hiItm        unsafe.Pointer
	rightSibling PageId
}

type basePage struct {
	op          pageOp
	chainLen    uint16
	numItems    uint16
	pageVersion uint16

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

type flushPageDelta struct {
	pageDelta
	offset      lssOffset
	flushDataSz int32
}

type removePageDelta pageDelta

type ItemSizeFn func(unsafe.Pointer) uintptr

type storeCtx struct {
	itemSize  ItemSizeFn
	cmp       skiplist.CompareFn
	getDeltas func(PageId) *pageDelta
	getPageId func(unsafe.Pointer, *wCtx) PageId
	getItem   func(PageId) unsafe.Pointer
}

type page struct {
	*storeCtx

	low         unsafe.Pointer
	version     uint16
	prevHeadPtr unsafe.Pointer
	head        *pageDelta
	tail        *pageDelta
}

func (pg *page) Reset() {
	pg.low = nil
	pg.head = nil
	pg.tail = nil
	pg.prevHeadPtr = nil
}

func (pg *page) newFlushPageDelta(dataSz int) *flushPageDelta {
	pd := new(flushPageDelta)
	*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	pd.next = pg.head
	pd.offset = inFlushingOffset
	pd.op = opFlushPageDelta
	pd.flushDataSz = int32(dataSz)
	return pd
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
	pd.chainLen++
	pd.rightSibling = pid
	return (*pageDelta)(unsafe.Pointer(pd))
}

func (pg *page) newMergePageDelta(itm unsafe.Pointer, sibl *pageDelta) *pageDelta {
	pd := new(mergePageDelta)
	pd.op = opPageMergeDelta
	pd.itm = itm
	pd.next = pg.head
	pd.mergeSibling = sibl
	if pg.head != nil {
		pd.chainLen = pg.head.chainLen
		pd.numItems = pg.head.numItems
	}
	pd.chainLen += sibl.chainLen + 1
	pd.numItems += sibl.numItems
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
	if pg.head != nil {
		bp.rightSibling = pg.head.rightSibling
		bp.hiItm = pg.head.hiItm
	}

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
		case opFlushPageDelta:
		case opPageRemoveDelta:
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
	curr := pg.head
	for ; curr != nil && curr.op != opBasePage; curr = curr.next {
	}

	bp := (*basePage)(unsafe.Pointer(curr))
	mid := len(bp.items) / 2
	for mid > 0 {
		if pg.cmp(bp.items[mid], pg.head.hiItm) < 0 {
			break
		}
		mid--
	}

	if mid > 0 {
		numItems := len(bp.items[:mid])
		return pg.doSplit(bp.items[mid], pid, numItems)
	}

	return nil
}

func (pg *page) doSplit(itm unsafe.Pointer, pid PageId, numItems int) *page {
	newPage := new(page)
	*newPage = *pg
	newPage.prevHeadPtr = nil
	itms, _ := pg.collectItems(pg.head, itm, pg.head.hiItm)
	newPage.head = pg.newBasePage(itms)
	newPage.low = (*basePage)(unsafe.Pointer(newPage.head)).items[0]
	pg.head = pg.newSplitPageDelta(itm, pid)
	pg.head.hiItm = itm
	pg.head.numItems = uint16(numItems)
	return newPage
}

func (pg *page) Compact() int {
	var pageVersion uint16
	if pg.head != nil {
		pageVersion = pg.head.pageVersion
	}

	itms, fdataSz := pg.collectItems(pg.head, nil, pg.head.hiItm)
	pg.head = pg.newBasePage(itms)
	pg.head.pageVersion = pageVersion + 1
	return fdataSz
}

func (pg *page) Merge(sp Page) {
	siblPage := (sp.(*page)).head
	pdm := pg.newMergePageDelta(pg.head.hiItm, siblPage)
	pdm.next = pg.head
	pg.head = pdm
	pg.head.hiItm = siblPage.hiItm
}

func (pg *page) PrependDeltas(p Page) {
	dPg := p.(*page)
	if dPg.tail != nil {
		dPg.tail.next = pg.head
		pg.head = dPg.head
	}
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

func (pg *page) prettyPrint(pd *pageDelta, stringify func(unsafe.Pointer) string) {
loop:
	for ; pd != nil; pd = pd.next {
		switch pd.op {
		case opInsertDelta, opDeleteDelta:
			rec := (*recordDelta)(unsafe.Pointer(pd))
			fmt.Printf("Delta op:%d, itm:%s\n", pd.op, stringify(rec.itm))
		case opBasePage:
			bp := (*basePage)(unsafe.Pointer(pd))
			for _, itm := range bp.items {
				fmt.Printf("Basepage itm:%s\n", stringify(itm))
			}
			break loop
		case opFlushPageDelta:
			fmt.Println("-------flush------")
		case opPageSplitDelta:
			fmt.Println("-------split------")
		case opPageMergeDelta:
			pds := (*mergePageDelta)(unsafe.Pointer(pd))
			fmt.Printf("-------merge-siblings-------")
			pg.prettyPrint(pds.mergeSibling, stringify)
			fmt.Println("-----------")
		}
	}
}

func (pg *page) collectPageItems(head *pageDelta,
	loItm, hiItm unsafe.Pointer) (items []PageItem, dataSz int) {
	sorter := pg.newPageItemSorter(head)
	for pd := head; pd != nil; pd = pd.next {
		switch pd.op {
		case opInsertDelta, opDeleteDelta:
			rec := (*recordDelta)(unsafe.Pointer(pd))
			if pg.inRange(loItm, hiItm, rec.itm) {
				sorter.Add(rec)
			}
		case opPageSplitDelta:
			pds := (*splitPageDelta)(unsafe.Pointer(pd))
			hiItm = pds.hiItm
		case opPageMergeDelta:
			pdm := (*mergePageDelta)(unsafe.Pointer(pd))
			items, _ := pg.collectPageItems(pdm.mergeSibling, loItm, hiItm)
			sorter.Add(items...)
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
			return merger.Merge(sorter.Run()), dataSz
		case opFlushPageDelta:
			fpd := (*flushPageDelta)(unsafe.Pointer(pd))
			dataSz += int(fpd.flushDataSz)
		}
	}

	return sorter.Run(), dataSz
}

func (pg *page) collectItems(head *pageDelta,
	loItm, hiItm unsafe.Pointer) (itx []unsafe.Pointer, dataSz int) {
	var itms []unsafe.Pointer
	items, dataSz := pg.collectPageItems(head, loItm, hiItm)
	for _, itm := range items {
		if itm.IsInsert() {
			itms = append(itms, itm.Item())
		}
	}

	return itms, dataSz
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

func (pi *pageIterator) SeekFirst() {}

func (pi *pageIterator) Seek(itm unsafe.Pointer) {
	pi.i = sort.Search(len(pi.itms), func(i int) bool {
		return pi.cmp(pi.itms[i], itm) >= 0
	})

}

func (pg *page) NewIterator() ItemIterator {
	itms, _ := pg.collectItems(pg.head, nil, pg.head.hiItm)
	return &pageIterator{
		itms: itms,
		cmp:  pg.cmp,
	}
}

func (pg *page) Marshal(buf []byte) (bs []byte, flushDataSz int) {
	woffset := 0
	pd := pg.head
	if pd != nil {
		// pageVersion
		binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(pd.pageVersion))
		woffset += 2

		if pg.low == skiplist.MinItem {
			binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(0))
			woffset += 2
		} else {
			l := int(pg.itemSize(pg.low))
			binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(l))
			woffset += 2
			memcopy(unsafe.Pointer(&buf[woffset]), pg.low, l)
			woffset += l
		}

		// chainlen
		binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(pd.chainLen))
		woffset += 2

		// numItems
		binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(pd.numItems))
		woffset += 2

		// hiItm
		if pd.hiItm == skiplist.MaxItem {
			binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(0))
			woffset += 2
		} else {
			l := int(pg.itemSize(pd.hiItm))
			binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(l))
			woffset += 2
			memcopy(unsafe.Pointer(&buf[woffset]), pd.hiItm, l)
			woffset += l
		}

		// rightSibling
		nkey := pg.getItem(pd.rightSibling)
		if nkey == skiplist.MaxItem {
			binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(0))
			woffset += 2
		} else {
			l := int(pg.itemSize(nkey))
			binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(l))
			woffset += 2
			memcopy(unsafe.Pointer(&buf[woffset]), nkey, l)
			woffset += l
		}
	}

loop:
	for ; pd != nil; pd = pd.next {
		switch pd.op {
		case opInsertDelta, opDeleteDelta:
			rpd := (*recordDelta)(unsafe.Pointer(pd))
			if pg.InRange(rpd.itm) {
				binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(pd.op))
				woffset += 2
				sz := int(pg.itemSize(rpd.itm))
				binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(sz))
				woffset += 2
				memcopy(unsafe.Pointer(&buf[woffset]), rpd.itm, sz)
				woffset += sz
			}
		case opBasePage:
			bp := (*basePage)(unsafe.Pointer(pd))
			binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(pd.op))
			woffset += 2
			bufnitm := buf[woffset : woffset+2]
			nItms := 0
			woffset += 2
			for _, itm := range bp.items {
				if pg.InRange(itm) {
					sz := int(pg.itemSize(itm))
					binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(sz))
					woffset += 2
					memcopy(unsafe.Pointer(&buf[woffset]), itm, sz)
					woffset += sz
					nItms++
				}
			}
			binary.BigEndian.PutUint16(bufnitm, uint16(nItms))
			break loop
		case opFlushPageDelta:
			fpd := (*flushPageDelta)(unsafe.Pointer(pd))
			binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(pd.op))
			woffset += 2
			binary.BigEndian.PutUint64(buf[woffset:woffset+8], uint64(fpd.offset))
			woffset += 8
			break loop
		}
	}

	return buf[:woffset], woffset
}

func getLSSPageMeta(data []byte) (itm unsafe.Pointer, pv uint16) {
	roffset := 0
	pv = binary.BigEndian.Uint16(data[roffset : roffset+2])
	roffset += 2

	l := int(binary.BigEndian.Uint16(data[roffset : roffset+2]))
	roffset += 2
	if l == 0 {
		itm = skiplist.MaxItem
	} else {
		itm = unsafe.Pointer(&data[roffset])
	}

	return
}

func (pg *page) Unmarshal(data []byte, ctx *wCtx) {
	roffset := 0

	pageVersion := int(binary.BigEndian.Uint16(data[roffset : roffset+2]))
	roffset += 2

	pg.version = uint16(pageVersion)

	l := int(binary.BigEndian.Uint16(data[roffset : roffset+2]))
	roffset += 2
	if l == 0 {
		pg.low = skiplist.MinItem
	} else {
		pg.low = pg.alloc(uintptr(l))
		memcopy(pg.low, unsafe.Pointer(&data[roffset]), l)
		roffset += l
	}

	chainLen := int(binary.BigEndian.Uint16(data[roffset : roffset+2]))
	roffset += 2

	numItems := int(binary.BigEndian.Uint16(data[roffset : roffset+2]))
	roffset += 2

	l = int(binary.BigEndian.Uint16(data[roffset : roffset+2]))
	roffset += 2

	var hiItm unsafe.Pointer
	if l == 0 {
		hiItm = skiplist.MaxItem
	} else {
		hiItm = pg.alloc(uintptr(l))
		memcopy(hiItm, unsafe.Pointer(&data[roffset]), l)
		roffset += l
	}

	var rightSibling PageId
	l = int(binary.BigEndian.Uint16(data[roffset : roffset+2]))

	// TODO: rightSibling needs to be fixed later (help recovery now)
	roffset += 2
	if l == 0 {
		// rightSibling = pg.getPageId(skiplist.MaxItem, ctx)
	} else {
		// rightSibling = pg.getPageId(unsafe.Pointer(&data[roffset]), ctx)
		roffset += l
	}

	var pd, lastPd *pageDelta
loop:
	for roffset < len(data) {
		op := pageOp(binary.BigEndian.Uint16(data[roffset : roffset+2]))
		roffset += 2

		switch op {
		case opInsertDelta, opDeleteDelta:
			l := int(binary.BigEndian.Uint16(data[roffset : roffset+2]))
			roffset += 2
			itm := append([]byte(nil), data[roffset:roffset+l]...)
			roffset += l
			rpd := &recordDelta{
				pageDelta: pageDelta{
					op:           op,
					chainLen:     uint16(chainLen),
					numItems:     uint16(numItems),
					pageVersion:  uint16(pageVersion),
					hiItm:        hiItm,
					rightSibling: rightSibling,
				},
				itm: unsafe.Pointer(&itm[0]),
			}

			chainLen--
			pd = (*pageDelta)(unsafe.Pointer(rpd))

		case opBasePage:
			nItms := int(binary.BigEndian.Uint16(data[roffset : roffset+2]))
			roffset += 2
			var itms []unsafe.Pointer
			size := 0
			for i := 0; i < nItms; i++ {
				l := int(binary.BigEndian.Uint16(data[roffset : roffset+2]))
				roffset += 2
				itms = append(itms, unsafe.Pointer(&data[roffset]))
				roffset += l
				size += l
			}

			bp := pg.newBasePage(itms)
			bp.pageVersion = uint16(pageVersion)
			bp.hiItm = hiItm
			bp.rightSibling = rightSibling
			pd = (*pageDelta)(unsafe.Pointer(bp))
		case opFlushPageDelta:
			// TODO: handle later during eviction impl
			break loop
		}

		if pg.head == nil {
			pg.head = pd
		} else {
			lastPd.next = pd
		}
		lastPd = pd
	}

	pg.tail = lastPd
}

func (pg *page) addFlushDelta(dataSz int) *lssOffset {
	fd := pg.newFlushPageDelta(dataSz)
	pg.head = (*pageDelta)(unsafe.Pointer(fd))

	return &fd.offset
}

func encodeMetaBlock(target *page, buf []byte) []byte {
	woffset := 0

	l := int(target.itemSize(target.low))
	binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(l))
	woffset += 2
	memcopy(unsafe.Pointer(&buf[woffset]), target.low, l)
	woffset += l

	return buf[:woffset]
}

func (pg *page) GetFlushDataSize() int {
	flushDataSz := 0
loop:
	for pd := pg.head; pd != nil; pd = pd.next {
		switch pd.op {
		case opBasePage:
			break loop
		case opFlushPageDelta:
			fpd := (*flushPageDelta)(unsafe.Pointer(pd))
			flushDataSz += int(fpd.flushDataSz)
		}
	}

	return flushDataSz
}
