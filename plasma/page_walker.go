package plasma

import (
	"unsafe"
)

type pageWalker struct {
	head     *pageDelta
	currPd   *pageDelta
	count    int
	maxCount int
}

func newPgDeltaWalker(pd *pageDelta) pageWalker {
	return pageWalker{
		head:     pd,
		currPd:   pd,
		maxCount: 1000000000,
	}
}

func (w *pageWalker) NextPd() *pageDelta {
	return w.currPd.next
}

func (w *pageWalker) Op() pageOp {
	return w.currPd.op
}

func (w *pageWalker) HighItem() unsafe.Pointer {
	return w.currPd.hiItm
}

func (w *pageWalker) Item() unsafe.Pointer {
	itmDelta := (*recordDelta)(unsafe.Pointer(w.currPd))
	return itmDelta.itm
}

func (w *pageWalker) PageItem() PageItem {
	if w.Op() == opInsertDelta {
		return (*insertPageItem)(w.Item())
	}

	return (*removePageItem)(w.Item())
}

func (w *pageWalker) BaseItems() []unsafe.Pointer {
	return (*basePage)(unsafe.Pointer(w.currPd)).items
}

func (w *pageWalker) MergeSibling() *pageDelta {
	return (*mergePageDelta)(unsafe.Pointer(w.currPd)).mergeSibling
}

func (w *pageWalker) FlushInfo() (LSSOffset, int32, int32) {
	fd := (*flushPageDelta)(unsafe.Pointer(w.currPd))
	return fd.offset, fd.flushDataSz, fd.numSegments
}

func (w *pageWalker) RollbackFilter() interface{} {
	return (*rollbackDelta)(unsafe.Pointer(w.currPd)).Filter()
}

func (w *pageWalker) RollbackInfo() (uint64, uint64) {
	rb := (*rollbackDelta)(unsafe.Pointer(w.currPd)).rb
	return rb.start, rb.end
}

func (w *pageWalker) Next() {
	if w.currPd.op == opBasePage {
		w.maxCount = w.count
	} else {
		w.currPd = w.currPd.next
		w.count++
	}
}

func (w *pageWalker) End() bool {
	return w.currPd == nil || w.count == w.maxCount
}

func (w *pageWalker) SetEndAndRestart() {
	w.maxCount = w.count
	w.count = 0
	w.currPd = w.head
}
