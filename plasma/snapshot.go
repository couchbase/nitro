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

func (s *Snapshot) Close() {
	if atomic.AddInt32(&s.refCount, -1) == 0 {
		atomic.AddUint64(&s.db.gcSn, 1)
		s.child.Close()
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
