package memdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/t3rm1n4l/memdb/mm"
	"github.com/t3rm1n4l/memdb/skiplist"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var (
	ErrMaxSnapshotsLimitReached = fmt.Errorf("Maximum snapshots limit reached")
	ErrShutdown                 = fmt.Errorf("MemDB instance has been shutdown")
)

type KeyCompare func([]byte, []byte) int

type VisitorCallback func(*Item, int) error

type ItemEntry struct {
	itm *Item
	n   *skiplist.Node
}

func (e *ItemEntry) Item() *Item {
	return e.itm
}

func (e *ItemEntry) Node() *skiplist.Node {
	return e.n
}

type ItemCallback func(*ItemEntry)
type CheckPointCallback func()

type FileType int

const (
	encodeBufSize      = 4
	readerBufSize      = 10000
	defaultRefreshRate = 10000
)

const (
	ForestdbFile FileType = iota
	RawdbFile
)

const gcchanBufSize = 256

var (
	dbInstances      *skiplist.Skiplist
	dbInstancesCount int64
)

func init() {
	dbInstances = skiplist.New()
}

func CompareMemDB(this unsafe.Pointer, that unsafe.Pointer) int {
	thisItem := (*MemDB)(this)
	thatItem := (*MemDB)(that)

	return int(thisItem.id - thatItem.id)
}

func DefaultConfig() Config {
	var cfg Config
	cfg.SetKeyComparator(defaultKeyCmp)
	cfg.SetFileType(RawdbFile)
	cfg.useMemoryMgmt = false
	cfg.refreshRate = defaultRefreshRate
	return cfg
}

func newInsertCompare(keyCmp KeyCompare) skiplist.CompareFn {
	return func(this, that unsafe.Pointer) int {
		var v int
		thisItem := (*Item)(this)
		thatItem := (*Item)(that)
		if v = keyCmp(thisItem.Bytes(), thatItem.Bytes()); v == 0 {
			v = int(thisItem.bornSn) - int(thatItem.bornSn)
		}

		return v
	}
}

func newIterCompare(keyCmp KeyCompare) skiplist.CompareFn {
	return func(this, that unsafe.Pointer) int {
		thisItem := (*Item)(this)
		thatItem := (*Item)(that)
		return keyCmp(thisItem.Bytes(), thatItem.Bytes())
	}
}

func newExistCompare(keyCmp KeyCompare) skiplist.CompareFn {
	return func(this, that unsafe.Pointer) int {
		thisItem := (*Item)(this)
		thatItem := (*Item)(that)
		if thisItem.deadSn != 0 || thatItem.deadSn != 0 {
			return 1
		}
		return keyCmp(thisItem.Bytes(), thatItem.Bytes())
	}
}

func defaultKeyCmp(this, that []byte) int {
	return bytes.Compare(this, that)
}

const (
	dwStateInactive = iota
	dwStateInit
	dwStateActive
	dwStateTerminate
)

type deltaWrContext struct {
	state        int
	closed       chan struct{}
	notifyStatus chan error
	sn           uint32
	fw           FileWriter
	err          error
}

func (ctx *deltaWrContext) Init() {
	ctx.state = dwStateInactive
	ctx.notifyStatus = make(chan error)
	ctx.closed = make(chan struct{})
}

type Writer struct {
	dwrCtx deltaWrContext // Used for cooperative disk snapshotting

	rand   *rand.Rand
	buf    *skiplist.ActionBuffer
	gchead *skiplist.Node
	gctail *skiplist.Node
	next   *Writer
	// Local skiplist stats for writer, gcworker and freeworker
	slSts1, slSts2, slSts3 skiplist.Stats
	resSts                 restoreStats
	count                  int64

	*MemDB
}

func (w *Writer) doCheckpoint() {
	ctx := &w.dwrCtx
	switch ctx.state {
	case dwStateInit:
		ctx.state = dwStateActive
		ctx.notifyStatus <- nil
		ctx.err = nil
	case dwStateTerminate:
		ctx.state = dwStateInactive
		ctx.notifyStatus <- ctx.err
	}
}

func (w *Writer) doDeltaWrite(itm *Item) {
	ctx := &w.dwrCtx
	if ctx.state == dwStateActive {
		if itm.bornSn <= ctx.sn && itm.deadSn > ctx.sn {
			if err := ctx.fw.WriteItem(itm); err != nil {
				ctx.err = err
			}
		}
	}
}

func (w *Writer) Put(bs []byte) {
	w.Put2(bs)
}

func (w *Writer) Put2(bs []byte) (n *skiplist.Node) {
	var success bool
	x := w.newItem(bs, w.useMemoryMgmt)
	x.bornSn = w.getCurrSn()
	n, success = w.store.Insert2(unsafe.Pointer(x), w.insCmp, w.existCmp, w.buf,
		w.rand.Float32, &w.slSts1)
	if success {
		w.count += 1
	} else {
		w.freeItem(x)
	}
	return
}

// Find first item, seek until dead=0, mark dead=sn
func (w *Writer) Delete(bs []byte) (success bool) {
	_, success = w.Delete2(bs)
	return
}

func (w *Writer) GetNode(bs []byte) *skiplist.Node {
	iter := w.store.NewIterator(w.iterCmp, w.buf)
	defer iter.Close()

	x := w.newItem(bs, false)
	x.bornSn = w.getCurrSn()

	if found := iter.SeekWithCmp(unsafe.Pointer(x), w.insCmp, w.existCmp); found {
		return iter.GetNode()
	}

	return nil
}

func (w *Writer) Delete2(bs []byte) (n *skiplist.Node, success bool) {
	if n := w.GetNode(bs); n != nil {
		return n, w.DeleteNode(n)
	}

	return nil, false
}

func (w *Writer) DeleteNode(x *skiplist.Node) (success bool) {
	defer func() {
		if success {
			w.count -= 1
		}
	}()

	sn := w.getCurrSn()
	gotItem := (*Item)(x.Item())
	if gotItem.bornSn == sn {
		success = w.store.DeleteNode(x, w.insCmp, w.buf, &w.slSts1)

		barrier := w.store.GetAccesBarrier()
		barrier.FlushSession(unsafe.Pointer(x))
		return
	}

	success = atomic.CompareAndSwapUint32(&gotItem.deadSn, 0, sn)
	if success {
		x.GClink = nil
		if w.gctail == nil {
			w.gctail = x
			w.gchead = w.gctail
		} else {
			w.gctail.GClink = x
			w.gctail = x
		}
	}
	return
}

type Config struct {
	keyCmp      KeyCompare
	insCmp      skiplist.CompareFn
	iterCmp     skiplist.CompareFn
	existCmp    skiplist.CompareFn
	refreshRate int

	ignoreItemSize bool

	fileType FileType

	useMemoryMgmt bool
	useDeltaFiles bool
	mallocFun     skiplist.MallocFn
	freeFun       skiplist.FreeFn
}

func (cfg *Config) SetKeyComparator(cmp KeyCompare) {
	cfg.keyCmp = cmp
	cfg.insCmp = newInsertCompare(cmp)
	cfg.iterCmp = newIterCompare(cmp)
	cfg.existCmp = newExistCompare(cmp)
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

func (cfg *Config) IgnoreItemSize() {
	cfg.ignoreItemSize = true
}

func (cfg *Config) UseMemoryMgmt(malloc skiplist.MallocFn, free skiplist.FreeFn) {
	if runtime.GOARCH == "amd64" {
		cfg.useMemoryMgmt = true
		cfg.mallocFun = malloc
		cfg.freeFun = free
	}
}

func (cfg *Config) UseDeltaInterleaving() {
	cfg.useDeltaFiles = true
}

type restoreStats struct {
	DeltaRestored      uint64
	DeltaRestoreFailed uint64
}

type MemDB struct {
	id           int
	store        *skiplist.Skiplist
	currSn       uint32
	snapshots    *skiplist.Skiplist
	gcsnapshots  *skiplist.Skiplist
	isGCRunning  int32
	lastGCSn     uint32
	leastUnrefSn uint32
	itemsCount   int64

	wlist    *Writer
	gcchan   chan *skiplist.Node
	freechan chan *skiplist.Node

	hasShutdown bool
	shutdownWg1 sync.WaitGroup // GC workers and StoreToDisk task
	shutdownWg2 sync.WaitGroup // Free workers

	Config
	restoreStats
}

func NewWithConfig(cfg Config) *MemDB {
	m := &MemDB{
		snapshots:   skiplist.New(),
		gcsnapshots: skiplist.New(),
		currSn:      1,
		Config:      cfg,
		gcchan:      make(chan *skiplist.Node, gcchanBufSize),
		id:          int(atomic.AddInt64(&dbInstancesCount, 1)),
	}

	m.freechan = make(chan *skiplist.Node, gcchanBufSize)
	m.store = skiplist.NewWithConfig(m.newStoreConfig())
	m.initSizeFuns()

	buf := dbInstances.MakeBuf()
	defer dbInstances.FreeBuf(buf)
	dbInstances.Insert(unsafe.Pointer(m), CompareMemDB, buf, &dbInstances.Stats)

	return m

}

func (m *MemDB) newStoreConfig() skiplist.Config {
	slCfg := skiplist.DefaultConfig()
	if m.useMemoryMgmt {
		slCfg.UseMemoryMgmt = true
		slCfg.Malloc = m.mallocFun
		slCfg.Free = m.freeFun
		slCfg.BarrierDestructor = m.newBSDestructor()

	}
	return slCfg
}

func (m *MemDB) newBSDestructor() skiplist.BarrierSessionDestructor {
	return func(ref unsafe.Pointer) {
		// If gclist is not empty
		if ref != nil {
			freelist := (*skiplist.Node)(ref)
			m.freechan <- freelist
		}
	}
}

func (m *MemDB) initSizeFuns() {
	m.snapshots.SetItemSizeFunc(SnapshotSize)
	m.gcsnapshots.SetItemSizeFunc(SnapshotSize)
	if !m.ignoreItemSize {
		m.store.SetItemSizeFunc(ItemSize)
	}
}

func New() *MemDB {
	return NewWithConfig(DefaultConfig())
}

func (m *MemDB) MemoryInUse() int64 {
	storeStats := m.aggrStoreStats()
	return storeStats.Memory + m.snapshots.MemoryInUse() + m.gcsnapshots.MemoryInUse()
}

func (m *MemDB) Close() {
	m.hasShutdown = true

	// Acquire gc chan ownership
	// This will make sure that no other goroutine will write to gcchan
	for !atomic.CompareAndSwapInt32(&m.isGCRunning, 0, 1) {
		time.Sleep(time.Millisecond)
	}
	close(m.gcchan)

	buf := dbInstances.MakeBuf()
	defer dbInstances.FreeBuf(buf)
	dbInstances.Delete(unsafe.Pointer(m), CompareMemDB, buf, &dbInstances.Stats)

	if m.useMemoryMgmt {
		buf := m.snapshots.MakeBuf()
		defer m.snapshots.FreeBuf(buf)

		m.shutdownWg1.Wait()
		close(m.freechan)
		m.shutdownWg2.Wait()

		// Manually free up all nodes
		iter := m.store.NewIterator(m.iterCmp, buf)
		defer iter.Close()
		var lastNode *skiplist.Node

		iter.SeekFirst()
		if iter.Valid() {
			lastNode = iter.GetNode()
			iter.Next()
		}

		for lastNode != nil {
			m.freeItem((*Item)(lastNode.Item()))
			m.store.FreeNode(lastNode, &m.store.Stats)
			lastNode = nil

			if iter.Valid() {
				lastNode = iter.GetNode()
				iter.Next()
			}
		}
	}
}

func (m *MemDB) getCurrSn() uint32 {
	return atomic.LoadUint32(&m.currSn)
}

func (m *MemDB) newWriter() *Writer {
	w := &Writer{
		rand:  rand.New(rand.NewSource(int64(rand.Int()))),
		buf:   m.store.MakeBuf(),
		MemDB: m,
	}

	w.slSts1.IsLocal(true)
	w.slSts2.IsLocal(true)
	w.slSts3.IsLocal(true)
	return w
}

func (m *MemDB) NewWriter() *Writer {
	w := m.newWriter()
	w.next = m.wlist
	m.wlist = w
	w.dwrCtx.Init()

	m.shutdownWg1.Add(1)
	go m.collectionWorker(w)
	if m.useMemoryMgmt {
		m.shutdownWg2.Add(1)
		go m.freeWorker(w)
	}

	return w
}

type Snapshot struct {
	sn       uint32
	refCount int32
	db       *MemDB
	count    int64

	gclist *skiplist.Node
}

func SnapshotSize(p unsafe.Pointer) int {
	s := (*Snapshot)(p)
	return int(unsafe.Sizeof(s.sn) + unsafe.Sizeof(s.refCount) + unsafe.Sizeof(s.db) +
		unsafe.Sizeof(s.count) + unsafe.Sizeof(s.gclist))
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

		// Move from live snapshot list to dead list
		s.db.snapshots.Delete(unsafe.Pointer(s), CompareSnapshot, buf, &s.db.snapshots.Stats)
		s.db.gcsnapshots.Insert(unsafe.Pointer(s), CompareSnapshot, buf, &s.db.snapshots.Stats)
		s.db.GC()

	}
}

func (s *Snapshot) NewIterator() *Iterator {
	return s.db.NewIterator(s)
}

func CompareSnapshot(this, that unsafe.Pointer) int {
	thisItem := (*Snapshot)(this)
	thatItem := (*Snapshot)(that)

	return int(thisItem.sn) - int(thatItem.sn)
}

func (m *MemDB) NewSnapshot() (*Snapshot, error) {
	buf := m.snapshots.MakeBuf()
	defer m.snapshots.FreeBuf(buf)

	// Stitch all local gclists from all writers to create snapshot gclist
	var head, tail *skiplist.Node

	for w := m.wlist; w != nil; w = w.next {
		if tail == nil {
			head = w.gchead
			tail = w.gctail
		} else if w.gchead != nil {
			tail.GClink = w.gchead
			tail = w.gctail
		}

		w.gchead = nil
		w.gctail = nil

		// Update global stats
		m.store.Stats.Merge(&w.slSts1)
		atomic.AddInt64(&m.itemsCount, w.count)
		w.count = 0
	}

	snap := &Snapshot{db: m, sn: m.getCurrSn(), refCount: 1, count: m.ItemsCount()}
	m.snapshots.Insert(unsafe.Pointer(snap), CompareSnapshot, buf, &m.snapshots.Stats)
	snap.gclist = head
	newSn := atomic.AddUint32(&m.currSn, 1)
	if newSn == math.MaxUint32 {
		return nil, ErrMaxSnapshotsLimitReached
	}

	return snap, nil
}

func (m *MemDB) ItemsCount() int64 {
	return atomic.LoadInt64(&m.itemsCount)
}

func (m *MemDB) collectionWorker(w *Writer) {
	buf := m.store.MakeBuf()
	defer m.store.FreeBuf(buf)
	defer m.shutdownWg1.Done()

	for {
		select {
		case <-w.dwrCtx.notifyStatus:
			w.doCheckpoint()
		case gclist, ok := <-m.gcchan:
			if !ok {
				close(w.dwrCtx.closed)
				return
			}
			for n := gclist; n != nil; n = n.GClink {
				w.doDeltaWrite((*Item)(n.Item()))
				m.store.DeleteNode(n, m.insCmp, buf, &w.slSts2)
			}

			m.store.Stats.Merge(&w.slSts2)

			barrier := m.store.GetAccesBarrier()
			barrier.FlushSession(unsafe.Pointer(gclist))
		}
	}
}

func (m *MemDB) freeWorker(w *Writer) {
	for freelist := range m.freechan {
		for n := freelist; n != nil; {
			dnode := n
			n = n.GClink

			itm := (*Item)(dnode.Item())
			m.freeItem(itm)
			m.store.FreeNode(dnode, &w.slSts3)
		}

		m.store.Stats.Merge(&w.slSts3)
	}

	m.shutdownWg2.Done()
}

// Invarient: Each snapshot n is dependent on snapshot n-1.
// Unless snapshot n-1 is collected, snapshot n cannot be collected.
func (m *MemDB) collectDead() {
	buf1 := m.snapshots.MakeBuf()
	buf2 := m.snapshots.MakeBuf()
	defer m.snapshots.FreeBuf(buf1)
	defer m.snapshots.FreeBuf(buf2)

	iter := m.gcsnapshots.NewIterator(CompareSnapshot, buf1)
	defer iter.Close()

	for iter.SeekFirst(); iter.Valid(); iter.Next() {
		node := iter.GetNode()
		sn := (*Snapshot)(node.Item())
		if sn.sn != m.lastGCSn+1 {
			return
		}

		m.lastGCSn = sn.sn
		m.gcchan <- sn.gclist
		m.gcsnapshots.DeleteNode(node, CompareSnapshot, buf2, &m.gcsnapshots.Stats)
	}
}

func (m *MemDB) GC() {
	if atomic.CompareAndSwapInt32(&m.isGCRunning, 0, 1) {
		m.collectDead()
		atomic.CompareAndSwapInt32(&m.isGCRunning, 1, 0)
	}
}

func (m *MemDB) GetSnapshots() []*Snapshot {
	var snaps []*Snapshot
	buf := m.snapshots.MakeBuf()
	defer m.snapshots.FreeBuf(buf)
	iter := m.snapshots.NewIterator(CompareSnapshot, buf)
	iter.SeekFirst()
	for ; iter.Valid(); iter.Next() {
		snaps = append(snaps, (*Snapshot)(iter.Get()))
	}

	return snaps
}

func (m *MemDB) ptrToItem(itmPtr unsafe.Pointer) *Item {
	o := (*Item)(itmPtr)
	itm := m.newItem(o.Bytes(), false)
	*itm = *o

	return itm
}

func (m *MemDB) Visitor(snap *Snapshot, callb VisitorCallback, shards int, concurrency int) error {
	var wg sync.WaitGroup
	var pivotItems []*Item

	wch := make(chan int, shards)

	if snap == nil {
		panic("snapshot cannot be nil")
	}

	func() {
		tmpIter := m.NewIterator(snap)
		if tmpIter == nil {
			panic("iterator cannot be nil")
		}
		defer tmpIter.Close()

		barrier := m.store.GetAccesBarrier()
		token := barrier.Acquire()
		defer barrier.Release(token)

		pivotItems = append(pivotItems, nil) // start item
		pivotPtrs := m.store.GetRangeSplitItems(shards)
		for _, itmPtr := range pivotPtrs {
			itm := m.ptrToItem(itmPtr)
			tmpIter.Seek(itm.Bytes())
			if tmpIter.Valid() {
				prevItm := pivotItems[len(pivotItems)-1]
				// Find bigger item than prev pivot
				if prevItm == nil || m.insCmp(unsafe.Pointer(itm), unsafe.Pointer(prevItm)) > 0 {
					pivotItems = append(pivotItems, itm)
				}
			}
		}
		pivotItems = append(pivotItems, nil) // end item
	}()

	errors := make([]error, len(pivotItems)-1)

	// Run workers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()

			for shard := range wch {
				startItem := pivotItems[shard]
				endItem := pivotItems[shard+1]

				itr := m.NewIterator(snap)
				if itr == nil {
					panic("iterator cannot be nil")
				}
				defer itr.Close()

				itr.SetRefreshRate(m.refreshRate)
				if startItem == nil {
					itr.SeekFirst()
				} else {
					itr.Seek(startItem.Bytes())
				}
			loop:
				for ; itr.Valid(); itr.Next() {
					if endItem != nil && m.insCmp(itr.GetNode().Item(), unsafe.Pointer(endItem)) >= 0 {
						break loop
					}

					itm := (*Item)(itr.GetNode().Item())
					if err := callb(itm, shard); err != nil {
						errors[shard] = err
						return
					}
				}
			}
		}(&wg)
	}

	// Provide work and wait
	for shard := 0; shard < len(pivotItems)-1; shard++ {
		wch <- shard
	}
	close(wch)

	wg.Wait()

	for _, err := range errors {
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *MemDB) numWriters() int {
	var count int
	for w := m.wlist; w != nil; w = w.next {
		count++
	}

	return count
}

func (m *MemDB) changeDeltaWrState(state int,
	writers []FileWriter, snap *Snapshot) error {

	var err error

	for id, w := 0, m.wlist; w != nil; w, id = w.next, id+1 {
		w.dwrCtx.state = state
		if state == dwStateInit {
			w.dwrCtx.sn = snap.sn
			w.dwrCtx.fw = writers[id]
		}

		// send
		select {
		case w.dwrCtx.notifyStatus <- nil:
			break
		case <-w.dwrCtx.closed:
			return ErrShutdown
		}

		// receive
		select {
		case e := <-w.dwrCtx.notifyStatus:
			if e != nil {
				err = e
			}
			break
		case <-w.dwrCtx.closed:
			return ErrShutdown
		}
	}

	return err
}

func (m *MemDB) StoreToDisk(dir string, snap *Snapshot, concurr int, itmCallback ItemCallback) (err error) {

	var snapClosed bool
	defer func() {
		if !snapClosed {
			snap.Close()
		}
	}()

	if m.useMemoryMgmt {
		m.shutdownWg1.Add(1)
		defer m.shutdownWg1.Done()
	}

	datadir := filepath.Join(dir, "data")
	os.MkdirAll(datadir, 0755)
	shards := runtime.NumCPU()

	writers := make([]FileWriter, shards)
	files := make([]string, shards)
	defer func() {
		for _, w := range writers {
			if w != nil {
				w.Close()
			}
		}
	}()

	for shard := 0; shard < shards; shard++ {
		w := m.newFileWriter(m.fileType)
		file := fmt.Sprintf("shard-%d", shard)
		datafile := filepath.Join(datadir, file)
		if err := w.Open(datafile); err != nil {
			return err
		}

		writers[shard] = w
		files[shard] = file
	}

	// Initialize and setup delta processing
	if m.useDeltaFiles {
		deltaWriters := make([]FileWriter, m.numWriters())
		deltaFiles := make([]string, m.numWriters())
		defer func() {
			for _, w := range deltaWriters {
				if w != nil {
					w.Close()
				}
			}
		}()

		deltadir := filepath.Join(dir, "delta")
		os.MkdirAll(deltadir, 0755)
		for id := 0; id < m.numWriters(); id++ {
			dw := m.newFileWriter(m.fileType)
			file := fmt.Sprintf("shard-%d", id)
			deltafile := filepath.Join(deltadir, file)
			if err = dw.Open(deltafile); err != nil {
				return err
			}
			deltaWriters[id] = dw
			deltaFiles[id] = file
		}

		if err = m.changeDeltaWrState(dwStateInit, deltaWriters, snap); err != nil {
			return err
		}

		// Create a placeholder snapshot object. We are decoupled from holding snapshot items
		// The fakeSnap object is to use the same iterator without any special handling for
		// usual refcount based freeing.

		snap.Close()
		snapClosed = true
		fakeSnap := *snap
		fakeSnap.refCount = 1
		snap = &fakeSnap

		defer func() {
			if err = m.changeDeltaWrState(dwStateTerminate, nil, nil); err == nil {
				bs, _ := json.Marshal(deltaFiles)
				ioutil.WriteFile(filepath.Join(deltadir, "files.json"), bs, 0660)
			}
		}()
	}

	visitorCallback := func(itm *Item, shard int) error {
		if m.hasShutdown {
			return ErrShutdown
		}

		w := writers[shard]
		if err := w.WriteItem(itm); err != nil {
			return err
		}

		if itmCallback != nil {
			itmCallback(&ItemEntry{itm: itm, n: nil})
		}

		return nil
	}

	if err = m.Visitor(snap, visitorCallback, shards, concurr); err == nil {
		bs, _ := json.Marshal(files)
		ioutil.WriteFile(filepath.Join(datadir, "files.json"), bs, 0660)
	}

	return err
}

func (m *MemDB) LoadFromDisk(dir string, concurr int, callb ItemCallback) (*Snapshot, error) {
	var wg sync.WaitGroup
	datadir := filepath.Join(dir, "data")
	var files []string

	if bs, err := ioutil.ReadFile(filepath.Join(datadir, "files.json")); err != nil {
		return nil, err
	} else {
		json.Unmarshal(bs, &files)
	}

	var nodeCallb skiplist.NodeCallback
	wchan := make(chan int)
	b := skiplist.NewBuilderWithConfig(m.newStoreConfig())
	b.SetItemSizeFunc(ItemSize)
	segments := make([]*skiplist.Segment, len(files))
	readers := make([]FileReader, len(files))
	errors := make([]error, len(files))

	if callb != nil {
		nodeCallb = func(n *skiplist.Node) {
			callb(&ItemEntry{itm: (*Item)(n.Item()), n: n})
		}
	}

	defer func() {
		for _, r := range readers {
			if r != nil {
				r.Close()
			}
		}
	}()

	for i, file := range files {
		segments[i] = b.NewSegment()
		segments[i].SetNodeCallback(nodeCallb)
		r := m.newFileReader(m.fileType)
		datafile := filepath.Join(datadir, file)
		if err := r.Open(datafile); err != nil {
			return nil, err
		}

		readers[i] = r
	}

	for i := 0; i < concurr; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()

			for shard := range wchan {
				r := readers[shard]
			loop:
				for {
					itm, err := r.ReadItem()
					if err != nil {
						errors[shard] = err
						return
					}

					if itm == nil {
						break loop
					}
					segments[shard].Add(unsafe.Pointer(itm))
				}
			}
		}(&wg)
	}

	for i, _ := range files {
		wchan <- i
	}
	close(wchan)
	wg.Wait()

	for _, err := range errors {
		if err != nil {
			return nil, err
		}
	}

	m.store = b.Assemble(segments...)

	// Delta processing
	if m.useDeltaFiles {
		m.DeltaRestoreFailed = 0
		m.DeltaRestored = 0

		wchan := make(chan int)
		deltadir := filepath.Join(dir, "delta")
		var files []string
		if bs, err := ioutil.ReadFile(filepath.Join(deltadir, "files.json")); err == nil {
			json.Unmarshal(bs, &files)
		}

		readers := make([]FileReader, len(files))
		errors := make([]error, len(files))
		writers := make([]*Writer, concurr)

		defer func() {
			for _, r := range readers {
				if r != nil {
					r.Close()
				}
			}
		}()

		for i, file := range files {
			r := m.newFileReader(m.fileType)
			deltafile := filepath.Join(deltadir, file)
			if err := r.Open(deltafile); err != nil {
				return nil, err
			}

			readers[i] = r
		}

		for i := 0; i < concurr; i++ {
			writers[i] = m.newWriter()
			wg.Add(1)
			go func(wg *sync.WaitGroup, id int) {
				defer wg.Done()

				for shard := range wchan {
					r := readers[shard]
				loop:
					for {
						itm, err := r.ReadItem()
						if err != nil {
							errors[shard] = err
							return
						}

						if itm == nil {
							break loop
						}

						w := writers[id]
						if n, success := w.store.Insert2(unsafe.Pointer(itm),
							w.insCmp, w.existCmp, w.buf, w.rand.Float32, &w.slSts1); success {

							w.resSts.DeltaRestored += 1
							if nodeCallb != nil {
								nodeCallb(n)
							}
						} else {
							w.freeItem(itm)
							w.resSts.DeltaRestoreFailed += 1
						}
					}
				}

				// Aggregate stats
				w := writers[id]
				m.store.Stats.Merge(&w.slSts1)
				atomic.AddUint64(&m.restoreStats.DeltaRestored, w.resSts.DeltaRestored)
				atomic.AddUint64(&m.restoreStats.DeltaRestoreFailed, w.resSts.DeltaRestoreFailed)
			}(&wg, i)
		}

		for i, _ := range files {
			wchan <- i
		}
		close(wchan)
		wg.Wait()

		for _, err := range errors {
			if err != nil {
				return nil, err
			}
		}
	}

	stats := m.store.GetStats()
	m.itemsCount = int64(stats.NodeCount)
	return m.NewSnapshot()
}

func (m *MemDB) DumpStats() string {
	return m.aggrStoreStats().String()
}

func (m *MemDB) aggrStoreStats() skiplist.StatsReport {
	sts := m.store.GetStats()
	for w := m.wlist; w != nil; w = w.next {
		sts.Apply(&w.slSts1)
		sts.Apply(&w.slSts2)
		sts.Apply(&w.slSts3)
	}

	return sts
}

func MemoryInUse() (sz int64) {
	buf := dbInstances.MakeBuf()
	defer dbInstances.FreeBuf(buf)
	iter := dbInstances.NewIterator(CompareMemDB, buf)
	for iter.SeekFirst(); iter.Valid(); iter.Next() {
		db := (*MemDB)(iter.Get())
		sz += db.MemoryInUse()
	}

	return
}

func Debug(flag bool) {
	skiplist.Debug = flag
	mm.Debug = flag
}
