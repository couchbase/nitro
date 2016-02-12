package memdb

import (
	"github.com/t3rm1n4l/memdb/skiplist"
	"unsafe"
)

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
	itm := (*Item)(it.iter.Get())
	if itm.bornSn > it.snap.sn || (itm.deadSn > 0 && itm.deadSn <= it.snap.sn) {
		it.iter.Next()
		goto loop
	}
}

func (it *Iterator) SeekFirst() {
	it.iter.SeekFirst()
	it.skipUnwanted()
}

func (it *Iterator) Seek(bs []byte) {
	itm := it.snap.db.newItem(bs, false)
	it.iter.Seek(unsafe.Pointer(itm))
	it.skipUnwanted()
}

func (it *Iterator) Valid() bool {
	return it.iter.Valid()
}

func (it *Iterator) Get() []byte {
	return (*Item)(it.iter.Get()).Bytes()
}

func (it *Iterator) GetNode() *skiplist.Node {
	return it.iter.GetNode()
}

func (it *Iterator) Next() {
	it.iter.Next()
	it.skipUnwanted()
}

func (it *Iterator) Close() {
	it.snap.Close()
	it.snap.db.store.FreeBuf(it.buf)
	it.iter.Close()
}

func (m *MemDB) NewIterator(snap *Snapshot) *Iterator {
	if !snap.Open() {
		return nil
	}
	buf := snap.db.store.MakeBuf()
	return &Iterator{
		snap: snap,
		iter: m.store.NewIterator(m.iterCmp, buf),
		buf:  buf,
	}
}
