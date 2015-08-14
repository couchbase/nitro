package memdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/t3rm1n4l/memdb/skiplist"
	"io"
	"math/rand"
	"os"
	"path"
	"runtime"
	"sync"
	"sync/atomic"
)

const DiskBlockSize = 512 * 1024

var (
	ErrNotEnoughSpace = errors.New("Not enough space in the buffer")
)

type KeyCompare func([]byte, []byte) int

type Item struct {
	bornSn, deadSn uint32
	data           []byte
}

func (itm *Item) Encode(buf []byte, w io.Writer) error {
	l := 2
	if len(buf) < l {
		return ErrNotEnoughSpace
	}

	binary.BigEndian.PutUint16(buf[0:2], uint16(len(itm.data)))
	if _, err := w.Write(buf[0:2]); err != nil {
		return err
	}
	if _, err := w.Write(itm.data); err != nil {
		return err
	}

	return nil
}

func (itm *Item) Decode(buf []byte, r io.Reader) error {
	if _, err := io.ReadFull(r, buf[0:2]); err != nil {
		return err
	}
	l := binary.BigEndian.Uint16(buf[0:2])
	itm.data = make([]byte, int(l))
	_, err := io.ReadFull(r, itm.data)

	return err
}

func NewItem(data []byte) *Item {
	return &Item{
		data: data,
	}
}

func newInsertCompare(keyCmp KeyCompare) skiplist.CompareFn {
	return func(this skiplist.Item, that skiplist.Item) int {
		var v int
		thisItem := this.(*Item)
		thatItem := that.(*Item)
		if v = keyCmp(thisItem.data, thatItem.data); v == 0 {
			v = int(thisItem.bornSn - thatItem.bornSn)
		}

		return v
	}
}

func newIterCompare(keyCmp KeyCompare) skiplist.CompareFn {
	return func(this skiplist.Item, that skiplist.Item) int {
		thisItem := this.(*Item)
		thatItem := that.(*Item)
		return keyCmp(thisItem.data, thatItem.data)
	}
}

func defaultKeyCmp(this []byte, that []byte) int {
	var l int

	l1 := len(this)
	l2 := len(that)
	if l1 < l2 {
		l = l1
	} else {
		l = l2
	}

	return bytes.Compare(this[:l], that[:l])
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
	w.store.Insert2(x, w.insCmp, w.buf, w.rand.Float32)
}

// Find first item, seek until dead=0, mark dead=sn
func (w *Writer) Delete(x *Item) bool {
	gotItem := w.Get(x)
	if gotItem != nil {
		sn := w.getCurrSn()
		if gotItem.bornSn == sn {
			return w.store.Delete(x, w.iterCmp, w.buf)
		}

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
		if w.iterCmp(next, curr) != 0 {
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

	keyCmp  KeyCompare
	insCmp  skiplist.CompareFn
	iterCmp skiplist.CompareFn
}

func New() *MemDB {
	m := &MemDB{
		store:     skiplist.New(),
		snapshots: skiplist.New(),
		currSn:    1,
	}

	m.SetKeyComparator(defaultKeyCmp)
	return m
}

func (m *MemDB) SetKeyComparator(cmp KeyCompare) {
	m.keyCmp = cmp
	m.insCmp = newInsertCompare(cmp)
	m.iterCmp = newIterCompare(cmp)
}

func (m *MemDB) getCurrSn() uint32 {
	return atomic.LoadUint32(&m.currSn)
}

func (m *MemDB) NewWriter() *Writer {
	buf := m.store.MakeBuf()

	return &Writer{
		rand:  rand.New(rand.NewSource(int64(rand.Int()))),
		buf:   buf,
		iter:  m.store.NewIterator(m.iterCmp, buf),
		MemDB: m,
	}
}

type Snapshot struct {
	sn       uint32
	refCount int32
	db       *MemDB
}

func (s *Snapshot) Encode(buf []byte, w io.Writer) error {
	l := 4
	if len(buf) < l {
		return ErrNotEnoughSpace
	}

	binary.BigEndian.PutUint32(buf[0:4], s.sn)
	if _, err := w.Write(buf[0:4]); err != nil {
		return err
	}

	return nil

}

func (s *Snapshot) Decode(buf []byte, r io.Reader) error {
	if _, err := io.ReadFull(r, buf[0:4]); err != nil {
		return err
	}
	s.sn = binary.BigEndian.Uint32(buf[0:4])
	return nil
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

func (s *Snapshot) NewIterator() *Iterator {
	return s.db.NewIterator(s)
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
		iter: m.store.NewIterator(m.iterCmp, buf),
		buf:  buf,
	}
}

func (m *MemDB) collectDead(sn uint32) {
	buf1 := m.snapshots.MakeBuf()
	buf2 := m.snapshots.MakeBuf()
	defer m.snapshots.FreeBuf(buf1)
	defer m.snapshots.FreeBuf(buf2)
	iter := m.store.NewIterator(m.iterCmp, buf1)
	iter.SeekFirst()
	for ; iter.Valid(); iter.Next() {
		itm := iter.Get().(*Item)
		if itm.deadSn > 0 && itm.deadSn <= sn {
			m.store.Delete(itm, m.iterCmp, buf2)
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

func (m *MemDB) StoreToDisk(dir string, snap *Snapshot) error {
	os.MkdirAll(dir, 0755)
	file, err := os.OpenFile(path.Join(dir, "records.data"), os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriterSize(file, DiskBlockSize)
	defer w.Flush()

	buf := make([]byte, 4)
	itr := m.NewIterator(snap)
	if itr == nil {
		return errors.New("Invalid snapshot")
	}
	defer itr.Close()

	if err := snap.Encode(buf, w); err != nil {
		return err
	}

	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		itm := itr.Get()
		if err := itm.Encode(buf, w); err != nil {
			return err
		}
	}

	endItem := &Item{
		data: []byte(nil),
	}

	if err := endItem.Encode(buf, w); err != nil {
		return err
	}

	w.Flush()

	return nil
}

func (m *MemDB) LoadFromDisk(dir string) (*Snapshot, error) {
	var wg sync.WaitGroup
	file, err := os.OpenFile(path.Join(dir, "records.data"), os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buf := make([]byte, 4)
	r := bufio.NewReaderSize(file, DiskBlockSize)
	snap := &Snapshot{db: m, refCount: 1}
	if err := snap.Decode(buf, r); err != nil {
		return nil, err
	}
	m.currSn = snap.sn

	ch := make(chan *Item, 100000)
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			w := m.NewWriter()
			for itm := range ch {
				w.Put(itm)
			}
		}(&wg)
	}

loop:
	for {
		itm := &Item{}
		itm.Decode(buf, r)
		if len(itm.data) == 0 {
			break loop
		}

		ch <- itm
	}

	close(ch)
	wg.Wait()

	snbuf := m.snapshots.MakeBuf()
	defer m.snapshots.FreeBuf(snbuf)
	m.snapshots.Insert(snap, CompareSnapshot, snbuf)
	m.currSn++

	return snap, nil
}
