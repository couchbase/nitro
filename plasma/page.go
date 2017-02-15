package plasma

import (
	"encoding/binary"
	"fmt"
	"github.com/couchbase/nitro/skiplist"
	"sort"
	"unsafe"
)

type pageOp uint16

const (
	opBasePage pageOp = iota

	opMetaDelta

	opInsertDelta
	opDeleteDelta

	opPageSplitDelta
	opPageRemoveDelta
	opPageMergeDelta

	opFlushPageDelta
	opRelocPageDelta

	opRollbackDelta
)

const (
	FullMarshal   = 0
	NoFullMarshal = ^0
)

var pageHeaderSize = int(unsafe.Sizeof(*new(pageDelta)))

type PageId interface{}

// TODO: Identify corner cases
func NextPid(pid PageId) PageId {
	return PageId(pid.(*skiplist.Node).GetNext())
}

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
	Rollback(s, end uint64)

	Append(Page)
	Marshal(buf []byte, maxSegments int) (bs []byte, fdSz int, staleFdSz int, numSegments int)

	GetVersion() uint16
	IsFlushed() bool
	NeedsFlush() bool
	IsEvictable() bool
	InCache(bool)
	IsInCache() bool
	MaxItem() unsafe.Pointer
	MinItem() unsafe.Pointer
	SetNext(PageId)
	Next() PageId

	GetMallocOps() ([]*pageDelta, []*pageDelta, int)
	GetFlushDataSize() int
	ComputeMemUsed() int
	AddFlushRecord(off LSSOffset, dataSz int, numSegments int)

	// TODO: Clean up later
	IsEmpty() bool
	GetLSSOffset() (LSSOffset, int)
	SetNumSegments(int)
}

type ItemIterator interface {
	SeekFirst() error
	Seek(unsafe.Pointer) error
	Get() unsafe.Pointer
	Valid() bool
	Next() error
}

type PageItemsList interface {
	Len() int
	At(i int) PageItem
}

var nilPageItemsList = (*pageItemsList)(&[]PageItem{})

type PageItem interface {
	IsInsert() bool
	Item() unsafe.Pointer

	PageItemsList
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

func (pi *pageItem) Len() int {
	return 1
}

func (pi *pageItem) At(i int) PageItem {
	return pi
}

type pageItemsList []PageItem

func (pil *pageItemsList) Len() int {
	return len(*pil)
}

func (pil *pageItemsList) At(i int) PageItem {
	return (*pil)[i]
}

type pageState uint16

func (ps *pageState) GetVersion() uint16 {
	return uint16(*ps & 0x7fff)
}

func (ps *pageState) IsFlushed() bool {
	return *ps&0x8000 == 0x8000
}

func (ps *pageState) SetFlushed() {
	*ps |= 0x8000
}

func (ps *pageState) IncrVersion() {
	v := uint16(*ps & 0x7fff)
	*ps = pageState((v + 1) & 0x7fff)
}

type metaPageDelta pageDelta

type pageDelta struct {
	op       pageOp
	chainLen uint16
	numItems uint16
	state    pageState

	next *pageDelta

	hiItm        unsafe.Pointer
	rightSibling PageId
}

func (pd *pageDelta) IsInsert() bool {
	return pd.op == opInsertDelta
}

func (pd *pageDelta) Item() unsafe.Pointer {
	return (*recordDelta)(unsafe.Pointer(pd)).itm
}

func (pd *pageDelta) Len() int {
	return 1
}

func (pd *pageDelta) At(int) PageItem {
	return pd
}

type basePage struct {
	op       pageOp
	chainLen uint16
	numItems uint16
	state    pageState

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
	offset      LSSOffset
	flushDataSz int32
	numSegments int32
}

type removePageDelta pageDelta

type rollbackDelta struct {
	pageDelta
	rb rollbackSn
}

func (rpd *rollbackDelta) Filter() interface{} {
	return &rpd.rb
}

type ItemSizeFn func(unsafe.Pointer) uintptr
type FilterGetter func() ItemFilter

type page struct {
	*storeCtx
	*allocCtx

	nextPid     PageId
	low         unsafe.Pointer
	state       pageState
	prevHeadPtr unsafe.Pointer
	head        *pageDelta
	tail        *pageDelta

	inCache bool
	memUsed int
}

func (pg *page) SetNext(pid PageId) {
	if pg.head == nil {
		pg.nextPid = pid
	} else {
		pg.head.rightSibling = pid
	}
}

func (pg *page) InCache(in bool) {
	pg.inCache = in
}

func (pg *page) IsInCache() bool {
	return pg.inCache
}

func (pg *page) Reset() {
	pg.memUsed = 0
	pg.inCache = false
	pg.nextPid = nil
	pg.low = nil
	pg.head = nil
	pg.tail = nil
	pg.prevHeadPtr = nil
}

func (pg *page) newFlushPageDelta(offset LSSOffset, dataSz int, numSegments int) *flushPageDelta {
	pd := pg.allocFlushPageDelta()

	*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	pd.next = pg.head
	reloc := numSegments == -1
	if reloc {
		pd.op = opRelocPageDelta
		pd.state.IncrVersion()
		pd.numSegments = 1
	} else {
		pd.op = opFlushPageDelta
		pd.numSegments = int32(numSegments + 1)
	}

	pd.offset = offset
	pd.state.SetFlushed()
	pd.flushDataSz = int32(dataSz)
	return pd
}

func (pg *page) newRecordDelta(op pageOp, itm unsafe.Pointer) *pageDelta {
	pd := pg.allocRecordDelta(itm)
	*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	pd.next = pg.head

	pd.op = op
	pd.chainLen++
	return (*pageDelta)(unsafe.Pointer(pd))
}

func (pg *page) newSplitPageDelta(itm unsafe.Pointer, pid PageId) *pageDelta {
	pd := pg.allocSplitPageDelta(itm)
	itm = pd.hiItm
	*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	pd.next = pg.head

	pd.op = opPageSplitDelta
	pd.itm = itm
	pd.hiItm = itm
	pd.chainLen++
	pd.rightSibling = pid
	return (*pageDelta)(unsafe.Pointer(pd))
}

func (pg *page) newMergePageDelta(itm unsafe.Pointer, sibl *pageDelta) *pageDelta {
	pd := pg.allocMergePageDelta(sibl.hiItm)
	hiItm := pd.hiItm
	*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	pd.next = pg.head

	pd.op = opPageMergeDelta
	pd.mergeSibling = sibl
	pd.itm = itm
	pd.hiItm = hiItm
	pd.chainLen += sibl.chainLen + 1
	pd.numItems += sibl.numItems
	pd.rightSibling = sibl.rightSibling
	return (*pageDelta)(unsafe.Pointer(pd))
}

func (pg *page) newRemovePageDelta() *pageDelta {
	pd := pg.allocRemovePageDelta()
	*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	pd.next = pg.head

	pd.op = opPageRemoveDelta
	return (*pageDelta)(unsafe.Pointer(pd))
}

func (pg *page) newBasePage(itms []unsafe.Pointer) *pageDelta {
	var sz uintptr
	var hiItm unsafe.Pointer

	if pg.head != nil {
		hiItm = pg.head.hiItm
	}

	n := len(itms)
	for _, itm := range itms {
		sz += pg.itemSize(itm)
	}

	bp := pg.allocBasePage(n, sz, hiItm)
	bp.op = opBasePage
	bp.numItems = uint16(n)

	var offset uintptr
	for i, itm := range itms {
		itmsz := pg.itemSize(itm)
		dstItm := unsafe.Pointer(uintptr(bp.data) + offset)
		memcopy(dstItm, itm, int(itmsz))
		bp.items[i] = dstItm
		offset += itmsz
	}

	bp.numItems = uint16(n)
	if pg.head != nil {
		bp.rightSibling = pg.head.rightSibling
	}

	return (*pageDelta)(unsafe.Pointer(bp))
}

// TODO: Fix the low bound check ?
func (pg *page) InRange(itm unsafe.Pointer) bool {
	if pg.cmp(itm, pg.head.hiItm) >= 0 {
		return false
	}

	return true
}

func (pg *page) Insert(itm unsafe.Pointer) {
	pg.head = pg.newRecordDelta(opInsertDelta, itm)
}

func (pg *page) Delete(itm unsafe.Pointer) {
	pg.head = pg.newRecordDelta(opDeleteDelta, itm)
}

func (pg *page) equal(itm0, itm1, hi unsafe.Pointer) bool {
	return pg.cmp(itm0, itm1) == 0 && pg.cmp(itm0, hi) < 0
}

func (pg *page) Lookup(itm unsafe.Pointer) unsafe.Pointer {
	hiItm := pg.MaxItem()
	filter := pg.getLookupFilter()
	head := pg.head

loop:
	for pw := newPgDeltaWalker(head); !pw.End(); pw.Next() {
		op := pw.Op()
		switch op {
		case opInsertDelta:
			ritm := pw.Item()
			pgItm := pw.PageItem()
			if filter.Process(pgItm).Len() > 0 && pg.equal(ritm, itm, hiItm) {
				return ritm
			}
		case opDeleteDelta:
			ritm := pw.Item()
			pgItm := pw.PageItem()
			if filter.Process(pgItm).Len() > 0 && pg.equal(ritm, itm, hiItm) {
				return nil
			}
		case opBasePage:
			items := pw.BaseItems()
			n := len(items)
			index := sort.Search(n, func(i int) bool {
				return pg.cmp(items[i], itm) >= 0
			})

			for ; index < n && pg.equal(items[index], itm, hiItm); index++ {
				bpItm := (*basePageItem)(items[index])
				if filter.Process(bpItm).Len() > 0 {
					return items[index]
				}
			}

			return nil
		case opPageSplitDelta:
			sitm := pw.Item()
			if pg.cmp(sitm, hiItm) < 0 {
				hiItm = sitm
			}
		case opPageMergeDelta:
			if pg.cmp(itm, pw.Item()) >= 0 {
				head = pw.MergeSibling()
				goto loop
			}

		case opRollbackDelta:
			filter.AddFilter(pw.RollbackFilter())

		case opFlushPageDelta:
		case opRelocPageDelta:
		case opPageRemoveDelta:
		case opMetaDelta:
		default:
			panic(fmt.Sprint("should not happen op:", op))
		}
	}

	return nil
}

func (pg *page) NeedCompaction(threshold int) bool {
	return int(pg.head.chainLen) > threshold
}

func (pg *page) NeedSplit(threshold int) bool {
	return int(pg.head.numItems) > threshold
}

func (pg *page) NeedMerge(threshold int) bool {
	return int(pg.head.numItems) < threshold
}

func (pg *page) NeedRemoval() bool {
	return pg.head.op == opPageRemoveDelta
}

func (pg *page) Close() {
	pg.head = pg.newRemovePageDelta()
}

func (pg *page) Split(pid PageId) Page {
	curr := pg.head
	for ; curr != nil && curr.op != opBasePage; curr = curr.next {
	}

	var mid int
	bp := (*basePage)(unsafe.Pointer(curr))
	if bp != nil {
		mid = len(bp.items) / 2
		for mid > 0 {
			// Make sure that split is performed by different key boundary
			if pg.cmp(bp.items[mid], pg.head.hiItm) < 0 {
				if mid-1 >= 0 && pg.cmp(bp.items[mid], bp.items[mid-1]) > 0 {
					break
				}
			}
			mid--
		}
	}

	if mid > 0 {
		numItems := len(bp.items[:mid])
		return pg.doSplit(bp.items[mid], pid, numItems)
	}

	return nil
}

func (pg *page) doSplit(itm unsafe.Pointer, pid PageId, numItems int) *page {
	splitPage := new(page)
	*splitPage = *pg
	splitPage.prevHeadPtr = nil
	itms, _ := pg.collectItems(pg.head, itm, pg.head.hiItm)
	splitPage.head = pg.newBasePage(itms)

	splitPage.low = itm
	pg.head = pg.newSplitPageDelta(itm, pid)

	if numItems >= 0 {
		pg.head.numItems = uint16(numItems)
	} else {
		// During recovery
		pg.head.numItems /= 2
	}
	return splitPage
}

func (pg *page) Compact() int {
	state := pg.head.state

	itms, fdataSz := pg.collectItems(pg.head, nil, pg.head.hiItm)
	pg.freePg(pg.head)
	pg.head = pg.newBasePage(itms)
	state.IncrVersion()
	pg.head.state = state
	return fdataSz
}

func (pg *page) Merge(sp Page) {
	siblPage := (sp.(*page)).head
	pdm := pg.newMergePageDelta(pg.head.hiItm, siblPage)
	pdm.next = pg.head
	pg.head = pdm
}

func (pg *page) Append(p Page) {
	aPg := p.(*page)
	if pg.tail == nil {
		*pg = *aPg
	} else {
		pg.tail.next = aPg.head
		pg.tail = aPg.tail
	}
}

func (pg *page) inRange(lo, hi unsafe.Pointer, itm unsafe.Pointer) bool {
	return pg.cmp(itm, hi) < 0 && pg.cmp(itm, lo) >= 0
}

func prettyPrint(head *pageDelta, stringify func(unsafe.Pointer) string) {
loop:
	for pw := newPgDeltaWalker(head); !pw.End(); pw.Next() {
		op := pw.Op()
		switch op {
		case opInsertDelta, opDeleteDelta:
			fmt.Printf("Delta op:%d, itm:%s\n", op, stringify(pw.Item()))
		case opBasePage:
			for _, itm := range pw.BaseItems() {
				fmt.Printf("Basepage itm:%s\n", stringify(itm))
			}
			break loop
		case opFlushPageDelta:
			offset, _, _ := pw.FlushInfo()
			fmt.Printf("-------flush------ max:%s, offset:%d\n", stringify(pw.HighItem()), offset)
		case opPageSplitDelta:
			fmt.Println("-------split------ ", stringify(pw.Item()))
		case opPageMergeDelta:
			fmt.Println("-------merge-siblings------- ", stringify(pw.Item()))
			prettyPrint(pw.MergeSibling(), stringify)
			fmt.Println("-----------")
		case opPageRemoveDelta:
			fmt.Println("---remove-delta---")
		case opRollbackDelta:
			start, end := pw.RollbackInfo()
			fmt.Println("-----rollback----", start, end)
		}
	}
}

func (pg *page) collectItems(head *pageDelta,
	loItm, hiItm unsafe.Pointer) (itx []unsafe.Pointer, dataSz int) {

	it, fdSz := newPgOpIterator(pg.head, pg.cmp, loItm, hiItm, pg.getCompactFilter())
	var itms []unsafe.Pointer
	for it.Init(); it.Valid(); it.Next() {
		itm := it.Get()
		itms = append(itms, itm.Item())
	}

	return itms, fdSz
}

type pageIterator struct {
	pg   *page
	itms []unsafe.Pointer
	i    int
}

func (pi *pageIterator) Get() unsafe.Pointer {
	return pi.itms[pi.i]
}

func (pi *pageIterator) Valid() bool {
	return pi.i < len(pi.itms)
}

func (pi *pageIterator) Next() error {
	pi.i++
	return nil
}

func (pi *pageIterator) SeekFirst() error {
	pi.itms, _ = pi.pg.collectItems(pi.pg.head, nil, pi.pg.head.hiItm)
	return nil
}

func (pi *pageIterator) Seek(itm unsafe.Pointer) error {
	pi.itms, _ = pi.pg.collectItems(pi.pg.head, itm, pi.pg.head.hiItm)
	return nil

}

// This method is only used by page_tests
// TODO: Cleanup implementation by using pageOpIterator
func (pg *page) NewIterator() ItemIterator {
	return &pageIterator{
		pg: pg,
	}
}

func (pg *page) Marshal(buf []byte, maxSegments int) (bs []byte, dataSz, staleFdSz int, numSegments int) {
	hiItm := pg.MaxItem()
	offset, staleFdSz, numSegments := pg.marshal(buf, 0, pg.head, hiItm, false, maxSegments)
	return buf[:offset], offset, staleFdSz, numSegments
}

func (pg *page) marshalIndexKey(key unsafe.Pointer, woffset int, buf []byte) int {
	if key == skiplist.MinItem || key == skiplist.MaxItem {
		binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(0))
		woffset += 2
	} else {
		return pg.marshalItem(key, woffset, buf)
	}

	return woffset
}

func (pg *page) marshalItem(itm unsafe.Pointer, woffset int, buf []byte) int {
	l := int(pg.itemSize(itm))
	binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(l))
	woffset += 2
	memcopy(unsafe.Pointer(&buf[woffset]), itm, l)
	woffset += l

	return woffset
}

func (pg *page) marshal(buf []byte, woffset int, head *pageDelta,
	hiItm unsafe.Pointer, child bool, maxSegments int) (offset int, staleFdSz int, numSegments int) {

	if head == nil {
		return
	}

	var isFullMarshal bool = maxSegments == 0
	stateBuf := buf[woffset : woffset+2]
	hasReloc := false

	if !child {
		woffset += 2

		// pageLow
		woffset = pg.marshalIndexKey(pg.MinItem(), woffset, buf)

		// chainlen
		binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(head.chainLen))
		woffset += 2

		// numItems
		binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(head.numItems))
		woffset += 2

		// pageHigh
		woffset = pg.marshalIndexKey(pg.MaxItem(), woffset, buf)
	}

loop:
	for pw := newPgDeltaWalker(head); !pw.End(); pw.Next() {
		op := pw.Op()
		switch op {
		case opInsertDelta, opDeleteDelta:
			itm := pw.Item()
			if pg.cmp(itm, hiItm) < 0 {
				binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(op))
				woffset += 2
				woffset = pg.marshalItem(itm, woffset, buf)
			}
		case opPageSplitDelta:
			itm := pw.Item()
			if pg.cmp(itm, hiItm) < 0 {
				hiItm = itm
			}
			binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(op))
			woffset += 2
		case opPageMergeDelta:
			mergeSibling := pw.MergeSibling()
			var fdSz int
			woffset, fdSz, _ = pg.marshal(buf, woffset, mergeSibling, hiItm, true, 0)
			if !hasReloc {
				staleFdSz += fdSz
			}
		case opBasePage:
			if child {
				// Encode items as insertDelta
				for _, itm := range pw.BaseItems() {
					if pg.cmp(itm, hiItm) < 0 {
						binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(opInsertDelta))
						woffset += 2

						woffset = pg.marshalItem(itm, woffset, buf)
					}
				}
			} else {
				binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(op))
				woffset += 2
				bufnitm := buf[woffset : woffset+2]
				nItms := 0
				woffset += 2
				for _, itm := range pw.BaseItems() {
					if pg.cmp(itm, hiItm) < 0 {
						woffset = pg.marshalItem(itm, woffset, buf)
						nItms++
					}
				}
				binary.BigEndian.PutUint16(bufnitm, uint16(nItms))
			}
			break loop
		case opFlushPageDelta, opRelocPageDelta:
			offset, dataSz, numSegs := pw.FlushInfo()
			if int(numSegs) > maxSegments {
				isFullMarshal = true
			} else if !isFullMarshal {
				binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(op))
				woffset += 2
				binary.BigEndian.PutUint64(buf[woffset:woffset+8], uint64(offset))
				woffset += 8
				numSegments = int(numSegs)
				break loop
			}

			if !hasReloc {
				staleFdSz += int(dataSz)
			}

			if op == opRelocPageDelta {
				hasReloc = true
			}
		case opRollbackDelta:
			start, end := pw.RollbackInfo()
			binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(op))
			woffset += 2
			binary.BigEndian.PutUint64(buf[woffset:woffset+8], uint64(start))
			woffset += 8
			binary.BigEndian.PutUint64(buf[woffset:woffset+8], uint64(end))
			woffset += 8
		case opPageRemoveDelta, opMetaDelta:
		default:
			panic(fmt.Sprintf("unknown delta %d", op))
		}
	}

	if !child {
		// pageVersion
		state := head.state
		if isFullMarshal {
			state.IncrVersion()
			numSegments = -1
		}
		binary.BigEndian.PutUint16(stateBuf, uint16(state))
	}

	return woffset, staleFdSz, numSegments
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
	pg.unmarshalDelta(data, ctx)
}

func (pg *page) unmarshalDelta(data []byte, ctx *wCtx) (offset LSSOffset, hasChain bool) {
	roffset := 0
	state := pageState(binary.BigEndian.Uint16(data[roffset : roffset+2]))
	state.SetFlushed()

	roffset += 2
	pg.state = state

	l := int(binary.BigEndian.Uint16(data[roffset : roffset+2]))
	roffset += 2
	if l == 0 {
		pg.low = skiplist.MinItem
	} else {
		pg.low = pg.alloc(uintptr(l))
		memcopy(pg.low, unsafe.Pointer(&data[roffset]), l)
		roffset += l
	}

	chainLen := binary.BigEndian.Uint16(data[roffset : roffset+2])
	roffset += 2

	numItems := binary.BigEndian.Uint16(data[roffset : roffset+2])
	roffset += 2

	l = int(binary.BigEndian.Uint16(data[roffset : roffset+2]))
	roffset += 2

	var hiItm unsafe.Pointer
	if l == 0 {
		hiItm = skiplist.MaxItem
	} else {
		hiItm = unsafe.Pointer(&data[roffset])
		roffset += l
	}

	lastPd := (*pageDelta)(unsafe.Pointer(pg.allocMetaDelta(hiItm)))
	lastPd.op = opMetaDelta
	lastPd.state = state
	lastPd.numItems = numItems
	lastPd.chainLen = chainLen
	lastPd.next = nil
	pg.head = lastPd

	var pd *pageDelta
loop:
	for roffset < len(data) {
		op := pageOp(binary.BigEndian.Uint16(data[roffset : roffset+2]))
		roffset += 2

		switch op {
		case opInsertDelta, opDeleteDelta:
			l := int(binary.BigEndian.Uint16(data[roffset : roffset+2]))
			roffset += 2
			itm := unsafe.Pointer(&data[roffset])
			roffset += l
			rpd := pg.allocRecordDelta(itm)
			*(*pageDelta)(unsafe.Pointer(rpd)) = *pg.head
			rpd.op = op
			pd = (*pageDelta)(unsafe.Pointer(rpd))
		case opPageSplitDelta:
			spd := pg.allocSplitPageDelta(nil)
			*(*pageDelta)(unsafe.Pointer(spd)) = *pg.head
			spd.op = op
			spd.itm = pg.head.hiItm
			pd = (*pageDelta)(unsafe.Pointer(spd))
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
			bp.state = state
			pd = (*pageDelta)(unsafe.Pointer(bp))
		case opFlushPageDelta, opRelocPageDelta:
			offset = LSSOffset(binary.BigEndian.Uint64(data[roffset : roffset+8]))
			hasChain = true
			break loop
		case opRollbackDelta:
			rpd := pg.allocRollbackPageDelta()
			*(*pageDelta)(unsafe.Pointer(rpd)) = *pg.head
			rpd.rb = rollbackSn{
				start: binary.BigEndian.Uint64(data[roffset : roffset+8]),
				end:   binary.BigEndian.Uint64(data[roffset+8 : roffset+16]),
			}

			rpd.op = op
			pd = (*pageDelta)(unsafe.Pointer(rpd))
			roffset += 16
		}

		lastPd.next = pd
		lastPd = pd
	}

	pg.tail = lastPd
	return
}

func (pg *page) AddFlushRecord(offset LSSOffset, dataSz int, numSegments int) {
	fd := pg.newFlushPageDelta(offset, dataSz, numSegments)
	pg.head = (*pageDelta)(unsafe.Pointer(fd))
}

func (pg *page) Rollback(startSn, endSn uint64) {
	pd := pg.allocRollbackPageDelta()
	*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	pd.next = pg.head

	pd.op = opRollbackDelta
	pd.chainLen++
	pd.rb.start = startSn
	pd.rb.end = endSn
	pg.head = (*pageDelta)(unsafe.Pointer(pd))
}

func marshalPageSMO(pg Page, buf []byte) []byte {
	woffset := 0

	target := pg.(*page)
	l := int(target.itemSize(target.low))
	binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(l))
	woffset += 2
	memcopy(unsafe.Pointer(&buf[woffset]), target.low, l)
	woffset += l

	return buf[:woffset]
}

func unmarshalPageSMO(p Page, data []byte) unsafe.Pointer {
	pg := p.(*page)
	roffset := 0
	l := int(binary.BigEndian.Uint16(data[roffset : roffset+2]))
	roffset += 2

	ptr := pg.alloc(uintptr(l))
	memcopy(ptr, unsafe.Pointer(&data[roffset]), l)
	return ptr
}

func (pg *page) GetFlushDataSize() int {
	return getFdSize(pg.head)
}

func getFdSize(head *pageDelta) int {
	hasReloc := false
	flushDataSz := 0
loop:
	for pw := newPgDeltaWalker(head); !pw.End(); pw.Next() {
		op := pw.Op()
		switch op {
		case opBasePage:
			break loop
		case opFlushPageDelta, opRelocPageDelta:
			if !hasReloc {
				_, d, _ := pw.FlushInfo()
				flushDataSz += int(d)
			}

			if op == opRelocPageDelta {
				hasReloc = true
			}
		case opPageMergeDelta:
			fdSz := getFdSize(pw.MergeSibling())
			if !hasReloc {
				flushDataSz += fdSz
			}
		}

	}

	return flushDataSz
}

func decodePageState(data []byte) (state pageState, key unsafe.Pointer) {
	roffset := 0
	state = pageState(binary.BigEndian.Uint16(data[roffset : roffset+2]))
	roffset += 2

	l := int(binary.BigEndian.Uint16(data[roffset : roffset+2]))
	roffset += 2
	if l == 0 {
		key = skiplist.MinItem
	} else {
		key = unsafe.Pointer(&data[roffset])
	}

	return
}

func (pg *page) GetVersion() uint16 {
	if pg.head == nil {
		return 0
	}

	return pg.head.state.GetVersion()
}

func (pg *page) IsFlushed() bool {
	if pg.head == nil {
		return false
	}

	return pg.head.state.IsFlushed()
}

func (pg *page) MaxItem() unsafe.Pointer {
	if pg.head == nil {
		return skiplist.MaxItem
	}

	return pg.head.hiItm
}

func (pg *page) MinItem() unsafe.Pointer {
	return pg.low
}

func (pg *page) Next() PageId {
	if pg.head == nil {
		return pg.nextPid
	}

	return pg.head.rightSibling
}

func newPage(ctx *wCtx, low unsafe.Pointer, ptr unsafe.Pointer) Page {
	pg := &page{
		storeCtx:    ctx.storeCtx,
		allocCtx:    ctx.pgAllocCtx,
		head:        (*pageDelta)(ptr),
		low:         low,
		inCache:     true,
		prevHeadPtr: ptr,
	}

	return pg
}

func (s *Plasma) newSeedPage(ctx *wCtx) Page {
	pg := newPage(ctx, skiplist.MinItem, nil).(*page)
	d := pg.allocMetaDelta(skiplist.MaxItem)
	d.op = opMetaDelta
	d.rightSibling = s.EndPageId()

	pg.head = (*pageDelta)(unsafe.Pointer(d))
	return pg
}

// TODO: Depreciate
func (pg *page) IsEmpty() bool {
	return pg.head == nil
}

func (pg *page) IsEvictable() bool {
	if pg.head == nil {
		return false
	}

	switch pg.head.op {
	case opFlushPageDelta, opRelocPageDelta:
		return true
	}

	return false
}

func (pg *page) NeedsFlush() bool {
	if pg.head == nil {
		return false
	}

	switch pg.head.op {
	case opFlushPageDelta, opRelocPageDelta, opPageRemoveDelta:
		return false
	}

	return true
}

func (pg *page) GetLSSOffset() (LSSOffset, int) {
	if pg.head.op == opFlushPageDelta || pg.head.op == opRelocPageDelta {
		fpd := (*flushPageDelta)(unsafe.Pointer(pg.head))
		return fpd.offset, int(fpd.numSegments)
	} else if pg.head.op == opMetaDelta {
		return 0, 0
	}

	panic(fmt.Sprintf("invalid delta op:%d", pg.head.op))
}

func (pg *page) SetNumSegments(n int) {
	if pg.head.op == opFlushPageDelta {
		fpd := (*flushPageDelta)(unsafe.Pointer(pg.head))
		fpd.numSegments = int32(n)
		return
	}

	panic(fmt.Sprintf("invalid delta op:%d", pg.head.op))
}

func (pg *page) GetMallocOps() ([]*pageDelta, []*pageDelta, int) {
	a := pg.allocDeltaList
	f := pg.freePageList
	m := pg.memUsed

	pg.memUsed = 0
	pg.allocDeltaList = pg.allocDeltaList[:0]
	pg.freePageList = pg.freePageList[:0]
	return a, f, m
}

func (pg *page) ComputeMemUsed() int {
	return computeMemUsed(pg.head, pg.itemSize)
}

func computeMemUsed(pd *pageDelta, itemSize ItemSizeFn) int {
	var size int
loop:
	for ; pd != nil; pd = pd.next {
		switch pd.op {
		case opBasePage:
			bp := (*basePage)(unsafe.Pointer(pd))
			for _, itm := range bp.items {
				size += int(itemSize(itm))
			}

			size += int(itemSize(bp.hiItm)) + int(unsafe.Sizeof(bp.items)) + pageHeaderSize
			break loop
		case opInsertDelta, opDeleteDelta:
			rpd := (*recordDelta)(unsafe.Pointer(pd))
			size += pageHeaderSize + int(itemSize(rpd.itm)) + 8
		case opPageRemoveDelta:
			size += pageHeaderSize
		case opPageSplitDelta:
			spd := (*splitPageDelta)(unsafe.Pointer(pd))
			size += pageHeaderSize + 8 + int(itemSize(spd.itm))
		case opPageMergeDelta:
			pdm := (*mergePageDelta)(unsafe.Pointer(pd))
			size += pageHeaderSize + 8 + 8 + int(itemSize(pdm.hiItm))
			size += computeMemUsed(pdm.mergeSibling, itemSize)
		case opFlushPageDelta, opRelocPageDelta:
			size += pageHeaderSize + 8 + 8
		case opRollbackDelta:
			size += pageHeaderSize + 8 + 8
		case opMetaDelta:
		default:
			panic(fmt.Sprintf("unsupported delta %d", pd.op))
		}
	}

	return size
}
