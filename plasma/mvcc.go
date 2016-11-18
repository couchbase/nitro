package plasma

import (
	"sync/atomic"
	"unsafe"
)

type Snapshot struct {
	sn       uint64
	refCount int32
	child    *Snapshot
	db       *Plasma
}

// Used by snapshot iterator
type snAcceptor struct {
	sn   uint64
	skip bool
}

func (a *snAcceptor) Accept(o unsafe.Pointer, _ bool) bool {
	itm := (*item)(o)
	if a.skip || itm.Sn() > a.sn {
		a.skip = false
		return false
	}

	if !itm.IsInsert() {
		a.skip = true
		return false
	}

	return true
}

// Used by page compactor to GC dead snapshot items
type gcAcceptor struct {
	gcSn uint64
	skip bool
}

func (a *gcAcceptor) Accept(o unsafe.Pointer, _ bool) bool {
	itm := (*item)(o)

	if a.skip {
		a.skip = false
		return false
	}

	if !itm.IsInsert() && itm.Sn() <= a.gcSn {
		a.skip = true
		return false
	}

	return true
}

func (s *Snapshot) Close() {
	if atomic.AddInt32(&s.refCount, -1) == 0 {
		atomic.AddUint64(&s.db.gcSn, 1)
		s.child.Close()
	}
}

type MVCCIterator struct {
	snap *Snapshot
	ItemIterator
}

func (itr *MVCCIterator) Seek(k []byte) {
	sn := atomic.LoadUint64(&itr.snap.db.currSn)
	itm := unsafe.Pointer(itr.snap.db.newItem(k, nil, sn, false))
	itr.ItemIterator.Seek(itm)
}

func (itr *MVCCIterator) Key() []byte {
	return (*item)(itr.Get()).Key()
}

func (itr *MVCCIterator) Value() []byte {
	return (*item)(itr.Get()).Value()
}

func (itr *MVCCIterator) Close() {
	itr.snap.Close()
}

func (s *Snapshot) NewIterator() *MVCCIterator {
	s.Open()
	itr := s.db.NewIterator().(*Iterator)
	itr.acceptor = &snAcceptor{
		sn: s.sn,
	}

	return &MVCCIterator{
		snap:         s,
		ItemIterator: itr,
	}
}

func (s *Snapshot) Open() {
	atomic.AddInt32(&s.refCount, 1)
}

func (s *Plasma) NewSnapshot() (snap *Snapshot) {
	if !s.EnableShapshots {
		panic("snapshots not enabled")
	}

	snap = s.currSnapshot

	nextSnap := &Snapshot{
		sn:       atomic.AddUint64(&s.currSn, 1),
		refCount: 2,
		db:       s,
	}

	s.currSnapshot.child = nextSnap
	s.currSnapshot = nextSnap

	return
}

func (w *Writer) InsertKV(k, v []byte) {
	sn := atomic.LoadUint64(&w.currSn)
	itm := w.newItem(k, v, sn, false)
	w.Insert(unsafe.Pointer(itm))
}

func (w *Writer) DeleteKV(k []byte) {
	sn := atomic.LoadUint64(&w.currSn)
	itm := w.newItem(k, nil, sn, true)
	w.Insert(unsafe.Pointer(itm))
}
