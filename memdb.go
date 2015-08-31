package memdb

import (
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

type KeyCompare func([]byte, []byte) int
type ItemCallback func(*Item)

type FileType int

const (
	encodeBufSize = 4
	readerBufSize = 10000
)

const (
	ForestdbFile FileType = iota
	RawdbFile
)

func DefaultConfig() Config {
	var cfg Config
	cfg.SetKeyComparator(defaultKeyCmp)
	cfg.SetFileType(RawdbFile)
	return cfg
}

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

func (itm *Item) Bytes() []byte {
	return itm.data
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
			v = int(thisItem.bornSn) - int(thatItem.bornSn)
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
	atomic.AddInt64(&w.count, 1)
}

// Find first item, seek until dead=0, mark dead=sn
func (w *Writer) Delete(x *Item) (success bool) {
	defer func() {
		if success {
			atomic.AddInt64(&w.count, -1)
		}
	}()

	gotItem := w.Get(x)
	if gotItem != nil {
		sn := w.getCurrSn()
		if gotItem.bornSn == sn {
			success = w.store.Delete(gotItem, w.insCmp, w.buf)
			return
		}

		success = atomic.CompareAndSwapUint32(&gotItem.deadSn, 0, sn)
		return
	}

	return
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

type Config struct {
	keyCmp  KeyCompare
	insCmp  skiplist.CompareFn
	iterCmp skiplist.CompareFn

	fileType FileType
}

func (cfg *Config) SetKeyComparator(cmp KeyCompare) {
	cfg.keyCmp = cmp
	cfg.insCmp = newInsertCompare(cmp)
	cfg.iterCmp = newIterCompare(cmp)
}

func (cfg *Config) SetFileType(t FileType) error {
	switch t {
	case ForestdbFile, RawdbFile:
	default:
		return errors.New("Invalid format")
	}

	cfg.fileType = t
	return nil
}

type MemDB struct {
	store        *skiplist.Skiplist
	currSn       uint32
	snapshots    *skiplist.Skiplist
	isGCRunning  int32
	lastGCSn     uint32
	leastUnrefSn uint32
	count        int64

	Config
}

func NewWithConfig(cfg Config) *MemDB {
	m := &MemDB{
		store:     skiplist.New(),
		snapshots: skiplist.New(),
		currSn:    1,
		Config:    cfg,
	}

	return m

}

func New() *MemDB {
	return NewWithConfig(DefaultConfig())
}

func (m *MemDB) Reset() {
	m.store = skiplist.New()
	m.snapshots = skiplist.New()
	m.currSn = 1
}

func (m *MemDB) getCurrSn() uint32 {
	return atomic.LoadUint32(&m.currSn)
}

func (m *MemDB) setLeastUnrefSn() {
	buf := m.snapshots.MakeBuf()
	defer m.snapshots.FreeBuf(buf)
	iter := m.snapshots.NewIterator(CompareSnapshot, buf)
	iter.SeekFirst()
	if iter.Valid() {
		snap := iter.Get().(*Snapshot)
		atomic.StoreUint32(&m.leastUnrefSn, snap.sn-1)
	}
}

func (m *MemDB) getLeastUnrefSn() uint32 {
	return atomic.LoadUint32(&m.leastUnrefSn)
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
	count    int64
}

func (s Snapshot) Count() int64 {
	return s.count
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
		s.db.setLeastUnrefSn()
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

	snap := &Snapshot{db: m, sn: m.getCurrSn(), refCount: 1, count: m.ItemsCount()}
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

func (m *MemDB) ItemsCount() int64 {
	return atomic.LoadInt64(&m.count)
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
		if itm.deadSn > 0 && itm.deadSn <= m.getLeastUnrefSn() {
			m.store.Delete(itm, m.insCmp, buf2)
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

func (m *MemDB) StoreToDisk(dir string, snap *Snapshot, callb ItemCallback) error {
	os.MkdirAll(dir, 0755)
	datafile := path.Join(dir, "records.data")
	w := newFileWriter(m.fileType)
	if err := w.Open(datafile); err != nil {
		return err
	}
	defer w.Close()

	itr := m.NewIterator(snap)
	if itr == nil {
		return errors.New("Invalid snapshot")
	}
	defer itr.Close()

	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		itm := itr.Get()
		if err := w.WriteItem(itm); err != nil {
			return err
		}

		if callb != nil {
			callb(itm)
		}
	}

	return nil
}

func (m *MemDB) LoadFromDisk(dir string, callb ItemCallback) (*Snapshot, error) {
	var wg sync.WaitGroup
	datafile := path.Join(dir, "records.data")
	r := newFileReader(m.fileType)
	if err := r.Open(datafile); err != nil {
		return nil, err
	}

	defer r.Close()

	ch := make(chan *Item, readerBufSize)
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			w := m.NewWriter()
			for itm := range ch {
				w.Put(itm)
				if callb != nil {
					callb(itm)
				}
			}
		}(&wg)
	}

loop:
	for {
		itm, err := r.ReadItem()
		if err != nil {
			return nil, err
		}

		if itm == nil {
			break loop
		}
		ch <- itm
	}

	close(ch)
	wg.Wait()

	snap := m.NewSnapshot()
	return snap, nil
}

func (m *MemDB) DumpStats() string {
	return m.store.GetStats().String()
}
