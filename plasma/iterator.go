package plasma

import (
	"unsafe"
)

type Iterator struct {
	store *Plasma
	*wCtx
	currPid   PageId
	nextPid   PageId
	currPgItr ItemIterator
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

func (itr *Iterator) initPgIterator(pid PageId) {
	itr.currPid = pid
	pg := itr.store.ReadPage(pid).(*page)
	if pg.head != nil {
		itr.nextPid = pg.head.rightSibling
	}
	itr.currPgItr = pg.NewIterator()
}

func (itr *Iterator) SeekFirst() {
	itr.initPgIterator(itr.store.Skiplist.HeadNode())

}

func (itr *Iterator) Seek(itm unsafe.Pointer) {
	var pid PageId
	if prev, curr, found := itr.store.Skiplist.Lookup(itm, itr.store.cmp, itr.wCtx.buf, itr.wCtx.slSts); found {
		pid = curr
	} else {
		pid = prev
	}
	itr.initPgIterator(pid)
	itr.currPgItr.Seek(itm)
}

func (itr *Iterator) Get() unsafe.Pointer {
	return itr.currPgItr.Get()
}

func (itr *Iterator) Valid() bool {
	return itr.currPgItr.Valid()
}

func (itr *Iterator) Next() {
	itr.currPgItr.Next()
	if !itr.currPgItr.Valid() {
		if itr.nextPid != nil {
			itr.initPgIterator(itr.nextPid)
		}
	}
}
