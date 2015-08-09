package memdb

import (
	"bytes"
	"github.com/t3rm1n4l/memdb/skiplist"
	"math/rand"
	"sync/atomic"
)

type Item struct {
	bornSn, deadSn uint32
	data           []byte
}

func NewItem(data []byte) *Item {
	return &Item{
		data: data,
	}
}

func InsertCompare(this skiplist.Item, that skiplist.Item) int {
	var v int
	if v = DataCompare(this, that); v == 0 {
		thisItem := this.(*Item)
		thatItem := that.(*Item)
		v = int(thisItem.bornSn - thatItem.bornSn)
	}

	return v
}

func DataCompare(this skiplist.Item, that skiplist.Item) int {
	var l int
	thisItem := this.(*Item)
	thatItem := that.(*Item)

	l1 := len(thisItem.data)
	l2 := len(thatItem.data)
	if l1 < l2 {
		l = l1
	} else {
		l = l2
	}
	return bytes.Compare(thisItem.data[:l], thatItem.data[:l])
}

//
//compare item,sn
type Writer struct {
	rand *rand.Rand
	buf  *skiplist.ActionBuffer
	iter *skiplist.Iterator
	*MemDB
}

func (w *Writer) Put(x *Item) {
	x.bornSn = w.getCurrSn()
	w.store.Insert2(x, InsertCompare, w.buf, w.rand.Float32)
}

// find first item, seek until dead=0, mark dead=sn
func (w *Writer) Delete(x *Item) bool {
	gotItem := w.Get(x)
	if gotItem != nil {
		sn := w.getCurrSn()
		return atomic.CompareAndSwapUint32(&gotItem.deadSn, 0, sn)
	}

	return false
}

func (w *Writer) Get(x *Item) *Item {
	var curr *Item
	found := w.iter.Seek(x)
	if !found {
		return nil
	}

	// Seek until most recent item for key is found
	curr = w.iter.Get().(*Item)
	for {
		w.iter.Next()
		if !w.iter.Valid() {
			break
		}
		next := w.iter.Get().(*Item)
		if DataCompare(next, curr) != 0 {
			break
		}

		curr = next
	}

	if curr.deadSn != 0 {
		return nil
	}

	return curr
}

type MemDB struct {
	store       *skiplist.Skiplist
	currSn      uint32
	snapshots   *skiplist.Skiplist
	isGCRunning int32
	lastGCSn    uint32
}

func New() *MemDB {
	return &MemDB{
		store:     skiplist.New(),
		snapshots: skiplist.New(),
		currSn:    1,
	}
}

func (m *MemDB) getCurrSn() uint32 {
	return atomic.LoadUint32(&m.currSn)
}

func (m *MemDB) NewWriter() *Writer {
	buf := m.store.MakeBuf()

	return &Writer{
		rand:  rand.New(rand.NewSource(int64(rand.Int()))),
		buf:   buf,
		iter:  m.store.NewIterator(DataCompare, buf),
		MemDB: m,
	}
}

type Snapshot struct {
	sn       uint32
	refCount int32
	db       *MemDB
}

func (s *Snapshot) Open() bool {
	if atomic.LoadInt32(&s.refCount) == 0 {
		return false
	}
	atomic.AddInt32(&s.refCount, 1)
	return true
}

func (s *Snapshot) Close() {
	newRefcount := atomic.AddInt32(&s.refCount, -1)
	if newRefcount == 0 {
		buf := s.db.snapshots.MakeBuf()
		defer s.db.snapshots.FreeBuf(buf)
		s.db.snapshots.Delete(s, CompareSnapshot, buf)
		if atomic.CompareAndSwapInt32(&s.db.isGCRunning, 0, 1) {
			go s.db.GC()
		}
	}
}

func CompareSnapshot(this skiplist.Item, that skiplist.Item) int {
	thisItem := this.(*Snapshot)
	thatItem := that.(*Snapshot)

	return int(thisItem.sn) - int(thatItem.sn)
}

func (m *MemDB) NewSnapshot() *Snapshot {
	buf := m.snapshots.MakeBuf()
	defer m.snapshots.FreeBuf(buf)

	snap := &Snapshot{db: m, sn: m.getCurrSn(), refCount: 1}
	m.snapshots.Insert(snap, CompareSnapshot, buf)
	atomic.AddUint32(&m.currSn, 1)
	return snap
}

type Iterator struct {
	snap *Snapshot
	iter *skiplist.Iterator
	buf  *skiplist.ActionBuffer
}

func (it *Iterator) skipUnwanted() {
loop:
	if !it.iter.Valid() {
		return
	}
	itm := it.iter.Get().(*Item)
	if itm.bornSn > it.snap.sn || (itm.deadSn > 0 && itm.deadSn <= it.snap.sn) {
		it.iter.Next()
		goto loop
	}
}

func (it *Iterator) SeekFirst() {
	it.iter.SeekFirst()
	it.skipUnwanted()
}

func (it *Iterator) Seek(itm *Item) {
	it.iter.Seek(itm)
	it.skipUnwanted()
}

func (it *Iterator) Valid() bool {
	return it.iter.Valid()
}

func (it *Iterator) Get() *Item {
	return it.iter.Get().(*Item)
}

func (it *Iterator) Next() {
	it.iter.Next()
	it.skipUnwanted()
}

func (it *Iterator) Close() {
	it.snap.Close()
	it.snap.db.store.FreeBuf(it.buf)
}

func (m *MemDB) NewIterator(snap *Snapshot) *Iterator {
	if !snap.Open() {
		return nil
	}
	buf := snap.db.store.MakeBuf()
	return &Iterator{
		snap: snap,
		iter: m.store.NewIterator(DataCompare, buf),
		buf:  buf,
	}
}

func (m *MemDB) collectDead(sn uint32) {
	buf1 := m.snapshots.MakeBuf()
	buf2 := m.snapshots.MakeBuf()
	defer m.snapshots.FreeBuf(buf1)
	defer m.snapshots.FreeBuf(buf2)
	iter := m.store.NewIterator(DataCompare, buf1)
	iter.SeekFirst()
	for ; iter.Valid(); iter.Next() {
		itm := iter.Get().(*Item)
		if itm.deadSn > 0 && itm.deadSn <= sn {
			m.store.Delete(itm, DataCompare, buf2)
		}
	}
}

func (m *MemDB) GC() {
	buf := m.snapshots.MakeBuf()
	defer m.snapshots.FreeBuf(buf)

	iter := m.snapshots.NewIterator(CompareSnapshot, buf)
	iter.SeekFirst()
	if iter.Valid() {
		snap := iter.Get().(*Snapshot)
		if snap.sn != m.lastGCSn && snap.sn > 1 {
			m.lastGCSn = snap.sn - 1
			m.collectDead(m.lastGCSn)
		}
	}

	atomic.CompareAndSwapInt32(&m.isGCRunning, 1, 0)
}

func (m *MemDB) GetSnapshots() []*Snapshot {
	var snaps []*Snapshot
	buf := m.snapshots.MakeBuf()
	defer m.snapshots.FreeBuf(buf)
	iter := m.snapshots.NewIterator(CompareSnapshot, buf)
	iter.SeekFirst()
	for ; iter.Valid(); iter.Next() {
		snaps = append(snaps, iter.Get().(*Snapshot))
	}

	return snaps
}
