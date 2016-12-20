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
	MarshalFull([]byte) (bs []byte, fdSz int, staleFdSz int)
	Marshal([]byte) (bs []byte, fdSz int)

	GetVersion() uint16
	IsFlushed() bool
	NeedsFlush() bool
	IsEvictable() bool
	MaxItem() unsafe.Pointer
	MinItem() unsafe.Pointer
	SetNext(PageId)
	Next() PageId

	GetFlushDataSize() int
	AddFlushRecord(off lssOffset, dataSz int, reloc bool)

	// TODO: Clean up later
	IsEmpty() bool
	GetLSSOffset() lssOffset
}

type ItemIterator interface {
	SeekFirst() error
	Seek(unsafe.Pointer) error
	Get() unsafe.Pointer
	Valid() bool
	Next() error
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
	offset      lssOffset
	flushDataSz int32
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

type storeCtx struct {
	itemSize         ItemSizeFn
	cmp              skiplist.CompareFn
	getPageId        func(unsafe.Pointer, *wCtx) PageId
	getItem          func(PageId) unsafe.Pointer
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

type page struct {
	*storeCtx

	meta *pageDelta

	nextPid     PageId
	low         unsafe.Pointer
	state       pageState
	prevHeadPtr unsafe.Pointer
	head        *pageDelta
	tail        *pageDelta
}

func (pg *page) SetNext(pid PageId) {
	if pg.head == nil {
		pg.nextPid = pid
	} else {
		pg.head.rightSibling = pid
	}
}

func (pg *page) Reset() {
	pg.nextPid = nil
	pg.low = nil
	pg.head = nil
	pg.tail = nil
	pg.prevHeadPtr = nil
}

func (pg *page) newFlushPageDelta(offset lssOffset, dataSz int, reloc bool) *flushPageDelta {
	pd := new(flushPageDelta)
	var meta *pageDelta
	if pg.head == nil {
		meta = pg.meta
	} else {
		meta = pg.head
	}

	*(*pageDelta)(unsafe.Pointer(pd)) = *meta
	pd.next = pg.head
	if reloc {
		pd.op = opRelocPageDelta
		pd.state.IncrVersion()
	} else {
		pd.op = opFlushPageDelta
	}

	pd.offset = offset
	pd.state.SetFlushed()
	pd.flushDataSz = int32(dataSz)
	return pd
}

func (pg *page) newRecordDelta(op pageOp, itm unsafe.Pointer) *pageDelta {
	pd := new(recordDelta)
	*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	pd.next = pg.head

	pd.chainLen++

	pd.op = op
	pd.itm = itm
	return (*pageDelta)(unsafe.Pointer(pd))
}

func (pg *page) newSplitPageDelta(itm unsafe.Pointer, pid PageId) *pageDelta {
	pd := new(splitPageDelta)
	*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	pd.next = pg.head

	itm = pg.dup(itm)
	pd.op = opPageSplitDelta
	pd.itm = itm
	pd.hiItm = itm
	pd.chainLen++
	pd.rightSibling = pid
	return (*pageDelta)(unsafe.Pointer(pd))
}

func (pg *page) newMergePageDelta(itm unsafe.Pointer, sibl *pageDelta) *pageDelta {
	pd := new(mergePageDelta)
	*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	pd.next = pg.head

	pd.op = opPageMergeDelta
	pd.itm = itm
	pd.mergeSibling = sibl
	pd.chainLen += sibl.chainLen + 1
	pd.numItems += sibl.numItems
	pd.rightSibling = sibl.rightSibling
	pd.hiItm = pg.dup(sibl.hiItm)
	return (*pageDelta)(unsafe.Pointer(pd))
}

func (pg *page) newRemovePageDelta() *pageDelta {
	pd := new(removePageDelta)
	*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	pd.next = pg.head

	pd.op = opPageRemoveDelta
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
		bp.hiItm = pg.dup(pg.head.hiItm)
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
	pd := pg.head
	hiItm := pg.MaxItem()
	filter := pg.getLookupFilter()

loop:
	for pd != nil {
		switch pd.op {
		case opInsertDelta:
			pdr := (*recordDelta)(unsafe.Pointer(pd))
			if filter.Accept(pdr.itm, true) && pg.equal(pdr.itm, itm, hiItm) {
				return pdr.itm
			}
		case opDeleteDelta:
			pdr := (*recordDelta)(unsafe.Pointer(pd))
			if filter.Accept(pdr.itm, false) && pg.equal(pdr.itm, itm, hiItm) {
				return nil
			}
		case opBasePage:
			bp := (*basePage)(unsafe.Pointer(pd))
			n := int(bp.numItems)
			index := sort.Search(n, func(i int) bool {
				return pg.cmp(bp.items[i], itm) >= 0
			})

			for ; index < n && pg.equal(bp.items[index], itm, hiItm); index++ {
				if filter.Accept(bp.items[index], true) {
					return bp.items[index]
				}

			}

			return nil
		case opPageSplitDelta:
			pds := (*splitPageDelta)(unsafe.Pointer(pd))
			if pg.cmp(pds.itm, hiItm) < 0 {
				hiItm = pds.itm
			}
		case opPageMergeDelta:
			pdm := (*mergePageDelta)(unsafe.Pointer(pd))
			if pg.cmp(itm, pdm.itm) >= 0 {
				pd = pdm.mergeSibling
				continue loop
			}

		case opRollbackDelta:
			rpd := (*rollbackDelta)(unsafe.Pointer(pd))
			filter.AddFilter(rpd.Filter())

		case opFlushPageDelta:
		case opRelocPageDelta:
		case opPageRemoveDelta:
		case opMetaDelta:
		default:
			panic(fmt.Sprint("should not happen op:", pd.op))
		}
		pd = pd.next
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
	splitPage.low = pg.dup(itm)
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
	}
}

func (pg *page) inRange(lo, hi unsafe.Pointer, itm unsafe.Pointer) bool {
	return pg.cmp(itm, hi) < 0 && pg.cmp(itm, lo) >= 0
}

func prettyPrint(pd *pageDelta, stringify func(unsafe.Pointer) string) {
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
			fpd := (*flushPageDelta)(unsafe.Pointer(pd))
			fmt.Printf("-------flush------ max:%s, offset:%d\n", stringify(pd.hiItm), fpd.offset)
		case opPageSplitDelta:
			pds := (*splitPageDelta)(unsafe.Pointer(pd))
			fmt.Println("-------split------ ", stringify(pds.itm))
		case opPageMergeDelta:
			pdm := (*mergePageDelta)(unsafe.Pointer(pd))
			fmt.Println("-------merge-siblings------- ", stringify(pdm.itm))
			prettyPrint(pdm.mergeSibling, stringify)
			fmt.Println("-----------")
		}
	}
}

func (pg *page) collectItems(head *pageDelta,
	loItm, hiItm unsafe.Pointer) (itx []unsafe.Pointer, dataSz int) {

	it, fdSz := newPgOpIterator(pg.head, pg.cmp, loItm, hiItm, pg.getCompactFilter())
	var itms []unsafe.Pointer
	for it.Init(); it.Valid(); it.Next() {
		itm, _ := it.Get()
		itms = append(itms, itm)
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

func (pg *page) MarshalFull(buf []byte) (bs []byte, dataSz, staleSz int) {
	hiItm := pg.MaxItem()
	pd := pg.head

	offset, staleSz := pg.marshal(buf, 0, pd, hiItm, false, true)
	return buf[:offset], offset, staleSz
}

func (pg *page) Marshal(buf []byte) (bs []byte, dataSz int) {
	hiItm := pg.MaxItem()
	pd := pg.head

	offset, _ := pg.marshal(buf, 0, pd, hiItm, false, false)
	return buf[:offset], offset
}

func (pg *page) marshal(buf []byte, woffset int, pd *pageDelta,
	hiItm unsafe.Pointer, child bool, full bool) (offset int, staleFdSz int) {

	hasReloc := false
	if !child {
		if pd != nil {
			// pageVersion
			state := pd.state
			if full {
				state.IncrVersion()
			}

			binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(state))
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
		}
	}

loop:
	for ; pd != nil; pd = pd.next {
		switch pd.op {
		case opInsertDelta, opDeleteDelta:
			rpd := (*recordDelta)(unsafe.Pointer(pd))
			if pg.cmp(rpd.itm, hiItm) < 0 {
				binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(pd.op))
				woffset += 2
				sz := int(pg.itemSize(rpd.itm))
				binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(sz))
				woffset += 2
				memcopy(unsafe.Pointer(&buf[woffset]), rpd.itm, sz)
				woffset += sz
			}
		case opPageSplitDelta:
			pds := (*splitPageDelta)(unsafe.Pointer(pd))
			if pg.cmp(pds.itm, hiItm) < 0 {
				hiItm = pds.itm
			}
		case opPageMergeDelta:
			pdm := (*mergePageDelta)(unsafe.Pointer(pd))
			var fdSz int
			woffset, fdSz = pg.marshal(buf, woffset, pdm.mergeSibling, hiItm, true, full)
			if !hasReloc {
				staleFdSz += fdSz
			}
		case opBasePage:
			bp := (*basePage)(unsafe.Pointer(pd))

			if child {
				// Encode items as insertDelta
				for _, itm := range bp.items {
					if pg.cmp(itm, hiItm) < 0 {
						binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(opInsertDelta))
						woffset += 2
						sz := int(pg.itemSize(itm))
						binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(sz))
						woffset += 2
						memcopy(unsafe.Pointer(&buf[woffset]), itm, sz)
						woffset += sz
					}
				}
			} else {
				binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(pd.op))
				woffset += 2
				bufnitm := buf[woffset : woffset+2]
				nItms := 0
				woffset += 2
				for _, itm := range bp.items {
					if pg.cmp(itm, hiItm) < 0 {
						sz := int(pg.itemSize(itm))
						binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(sz))
						woffset += 2
						memcopy(unsafe.Pointer(&buf[woffset]), itm, sz)
						woffset += sz
						nItms++
					}
				}
				binary.BigEndian.PutUint16(bufnitm, uint16(nItms))
			}
			break loop
		case opFlushPageDelta, opRelocPageDelta:
			fpd := (*flushPageDelta)(unsafe.Pointer(pd))
			if !full {
				binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(pd.op))
				woffset += 2
				binary.BigEndian.PutUint64(buf[woffset:woffset+8], uint64(fpd.offset))
				woffset += 8
				break loop
			}

			if !hasReloc {
				staleFdSz += int(fpd.flushDataSz)
			}

			if pd.op == opRelocPageDelta {
				hasReloc = true
			}
		case opRollbackDelta:
			rpd := (*rollbackDelta)(unsafe.Pointer(pd))
			binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(pd.op))
			woffset += 2
			binary.BigEndian.PutUint64(buf[woffset:woffset+8], uint64(rpd.rb.start))
			woffset += 8
			binary.BigEndian.PutUint64(buf[woffset:woffset+8], uint64(rpd.rb.end))
			woffset += 8
		}
	}

	return woffset, staleFdSz
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

func (pg *page) unmarshalDelta(data []byte, ctx *wCtx) (offset lssOffset, hasChain bool) {
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
					op:       op,
					chainLen: uint16(chainLen),
					numItems: uint16(numItems),
					state:    state,
					hiItm:    hiItm,
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
			bp.state = state
			bp.hiItm = hiItm
			pd = (*pageDelta)(unsafe.Pointer(bp))
		case opFlushPageDelta, opRelocPageDelta:
			offset = lssOffset(binary.BigEndian.Uint64(data[roffset : roffset+8]))
			hasChain = true
			break loop
		case opRollbackDelta:
			chainLen++
			rpd := &rollbackDelta{
				pageDelta: pageDelta{
					op:       op,
					chainLen: uint16(chainLen),
					numItems: uint16(numItems),
					state:    state,
					hiItm:    hiItm,
				},
				rb: rollbackSn{
					start: binary.BigEndian.Uint64(data[roffset : roffset+8]),
					end:   binary.BigEndian.Uint64(data[roffset+8 : roffset+16]),
				},
			}

			roffset += 16
			pd = (*pageDelta)(unsafe.Pointer(rpd))
		}

		if lastPd == nil {
			pg.head = pd
		} else {
			lastPd.next = pd
		}

		lastPd = pd
	}

	pg.tail = lastPd

	if lastPd == nil {
		pg.meta = &pageDelta{
			op:       opMetaDelta,
			chainLen: uint16(chainLen),
			numItems: uint16(numItems),
			state:    state,
			hiItm:    hiItm,
		}
	}

	return
}

func (pg *page) AddFlushRecord(offset lssOffset, dataSz int, reloc bool) {
	fd := pg.newFlushPageDelta(offset, dataSz, reloc)
	pg.head = (*pageDelta)(unsafe.Pointer(fd))
}

func (pg *page) Rollback(startSn, endSn uint64) {
	pd := new(rollbackDelta)
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

func getFdSize(pd *pageDelta) int {
	hasReloc := false
	flushDataSz := 0
loop:
	for ; pd != nil; pd = pd.next {
		switch pd.op {
		case opBasePage:
			break loop
		case opFlushPageDelta, opRelocPageDelta:
			fpd := (*flushPageDelta)(unsafe.Pointer(pd))
			if !hasReloc {
				flushDataSz += int(fpd.flushDataSz)
			}

			if pd.op == opRelocPageDelta {
				hasReloc = true
			}
		case opPageMergeDelta:
			pdm := (*mergePageDelta)(unsafe.Pointer(pd))
			fdSz := getFdSize(pdm.mergeSibling)
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

func newPage(ctx *storeCtx, low unsafe.Pointer, ptr unsafe.Pointer) Page {
	pg := &page{
		storeCtx:    ctx,
		head:        (*pageDelta)(ptr),
		low:         low,
		prevHeadPtr: ptr,
	}

	return pg
}

func newSeedPage() Page {
	return &page{
		head: &pageDelta{
			op:           opMetaDelta,
			hiItm:        skiplist.MaxItem,
			rightSibling: nil,
		},
	}
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

func (pg *page) GetLSSOffset() lssOffset {
	if pg.head.op == opFlushPageDelta || pg.head.op == opRelocPageDelta {
		fpd := (*flushPageDelta)(unsafe.Pointer(pg.head))
		return fpd.offset
	}

	panic("invalid usage")
}
