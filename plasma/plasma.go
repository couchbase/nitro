package plasma

import (
	"fmt"
	"github.com/couchbase/nitro/mm"
	"github.com/couchbase/nitro/skiplist"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type PageReader func(offset LSSOffset) (Page, error)

const maxCtxBuffers = 4

var (
	memQuota       int64
	maxMemoryQuota = int64(1024 * 1024 * 1024 * 1024)
	dbInstances    *skiplist.Skiplist
)

func init() {
	dbInstances = skiplist.New()
	SetMemoryQuota(maxMemoryQuota)
}

type Plasma struct {
	Config
	*skiplist.Skiplist
	wlist                           []*Writer
	lss                             LSS
	lssCleanerWriter                *Writer
	persistWriters                  []*Writer
	evictWriters                    []*Writer
	stoplssgc, stopswapper, stopmon chan struct{}
	sync.RWMutex

	// MVCC data structures
	mvcc         sync.RWMutex
	currSn       uint64
	numSnCreated int
	gcSn         uint64
	currSnapshot *Snapshot

	lastMaxSn uint64

	rpSns          unsafe.Pointer
	rpVersion      uint16
	recoveryPoints []*RecoveryPoint

	hasMemoryPressure bool

	smrWg   sync.WaitGroup
	smrChan chan unsafe.Pointer

	*storeCtx
}

type Stats struct {
	Compacts int64
	Splits   int64
	Merges   int64
	Inserts  int64
	Deletes  int64

	CompactConflicts int64
	SplitConflicts   int64
	MergeConflicts   int64
	InsertConflicts  int64
	DeleteConflicts  int64
	SwapInConflicts  int64

	BytesIncoming int64
	BytesWritten  int64

	FlushDataSz int64

	MemSz      int64
	MemSzIndex int64

	AllocSz   int64
	FreeSz    int64
	ReclaimSz int64

	AllocSzIndex   int64
	FreeSzIndex    int64
	ReclaimSzIndex int64

	NumCachedPages  int64
	NumPages        int64
	NumPagesSwapOut int64
	NumPagesSwapIn  int64

	LSSFrag      int
	LSSDataSize  int64
	LSSUsedSpace int64
	NumLSSReads  int64
	LSSReadBytes int64

	NumLSSCleanerReads  int64
	LSSCleanerReadBytes int64
}

func (s *Stats) Merge(o *Stats) {
	s.Compacts += o.Compacts
	s.Splits += o.Splits
	s.Merges += o.Merges
	s.Inserts += o.Inserts
	s.Deletes += o.Deletes

	s.CompactConflicts += o.CompactConflicts
	s.SplitConflicts += o.SplitConflicts
	s.MergeConflicts += o.MergeConflicts
	s.InsertConflicts += o.InsertConflicts
	s.DeleteConflicts += o.DeleteConflicts
	s.SwapInConflicts += o.SwapInConflicts

	s.AllocSz += o.AllocSz
	s.FreeSz += o.FreeSz
	s.ReclaimSz += o.ReclaimSz

	s.AllocSzIndex += o.AllocSzIndex
	s.FreeSzIndex += o.FreeSzIndex
	o.ReclaimSzIndex += o.ReclaimSzIndex

	s.NumPagesSwapOut += o.NumPagesSwapOut
	s.NumPagesSwapIn += o.NumPagesSwapIn

	s.BytesIncoming += o.BytesIncoming

	s.NumLSSReads += o.NumLSSReads
	s.LSSReadBytes += o.LSSReadBytes
}

func (s Stats) String() string {
	return fmt.Sprintf("===== Stats =====\n"+
		"memory_quota      = %d\n"+
		"count             = %d\n"+
		"compacts          = %d\n"+
		"splits            = %d\n"+
		"merges            = %d\n"+
		"inserts           = %d\n"+
		"deletes           = %d\n"+
		"compact_conflicts = %d\n"+
		"split_conflicts   = %d\n"+
		"merge_conflicts   = %d\n"+
		"insert_conflicts  = %d\n"+
		"delete_conflicts  = %d\n"+
		"swapin_conflicts  = %d\n"+
		"memory_size       = %d\n"+
		"memory_size_index = %d\n"+
		"allocated         = %d\n"+
		"freed             = %d\n"+
		"reclaimed         = %d\n"+
		"allocated_index   = %d\n"+
		"freed_index       = %d\n"+
		"reclaimed_index   = %d\n"+
		"num_cached_pages  = %d\n"+
		"num_pages         = %d\n"+
		"num_pages_swapout = %d\n"+
		"num_pages_swapin  = %d\n"+
		"bytes_incoming    = %d\n"+
		"bytes_written     = %d\n"+
		"write_amp         = %.2f\n"+
		"lss_fragmentation = %d%%\n"+
		"lss_data_size     = %d\n"+
		"lss_used_space    = %d\n"+
		"lss_num_reads     = %d\n"+
		"lss_read_bs       = %d\n"+
		"lss_gc_num_reads  = %d\n"+
		"lss_gc_reads_bs   = %d\n",
		atomic.LoadInt64(&memQuota),
		s.Inserts-s.Deletes,
		s.Compacts, s.Splits, s.Merges,
		s.Inserts, s.Deletes, s.CompactConflicts,
		s.SplitConflicts, s.MergeConflicts,
		s.InsertConflicts, s.DeleteConflicts,
		s.SwapInConflicts, s.MemSz, s.MemSzIndex,
		s.AllocSz, s.FreeSz, s.ReclaimSz,
		s.AllocSzIndex, s.FreeSzIndex, s.ReclaimSzIndex,
		s.NumCachedPages, s.NumPages,
		s.NumPagesSwapOut, s.NumPagesSwapIn,
		s.BytesIncoming, s.BytesWritten,
		float64(s.BytesWritten)/float64(s.BytesIncoming),
		s.LSSFrag, s.LSSDataSize, s.LSSUsedSpace,
		s.NumLSSReads, s.LSSReadBytes,
		s.NumLSSCleanerReads, s.LSSCleanerReadBytes)
}

func New(cfg Config) (*Plasma, error) {
	var err error

	cfg = applyConfigDefaults(cfg)

	s := &Plasma{Config: cfg}
	slCfg := skiplist.DefaultConfig()
	if cfg.UseMemoryMgmt {
		s.smrChan = make(chan unsafe.Pointer, smrChanBufSize)
		slCfg.UseMemoryMgmt = true
		slCfg.Malloc = mm.Malloc
		slCfg.Free = mm.Free
		slCfg.BarrierDestructor = s.newBSDestroyCallback()
	}

	sl := skiplist.NewWithConfig(slCfg)
	s.Skiplist = sl

	var cfGetter, lfGetter FilterGetter
	if cfg.EnableShapshots {
		cfGetter = func() ItemFilter {
			gcSn := atomic.LoadUint64(&s.gcSn) + 1
			rpSns := (*[]uint64)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&s.rpSns))))

			var gcPos int
			for _, sn := range *rpSns {
				if sn < gcSn {
					gcPos++
				} else {
					break
				}
			}

			var snIntervals []uint64
			if gcPos == 0 {
				snIntervals = []uint64{0, gcSn}
			} else {
				snIntervals = make([]uint64, gcPos+2)
				copy(snIntervals[1:], (*rpSns)[:gcPos])
				snIntervals[gcPos+1] = gcSn
			}

			return &gcFilter{snIntervals: snIntervals}
		}

		lfGetter = func() ItemFilter {
			return &rollbackFilter{}
		}
	} else {
		cfGetter = func() ItemFilter {
			return new(defaultFilter)
		}

		lfGetter = func() ItemFilter {
			return &nilFilter
		}
	}

	s.storeCtx = newStoreContext(sl, cfg.UseMemoryMgmt, cfg.ItemSize,
		cfg.Compare, cfGetter, lfGetter)

	gWr := s.NewWriter()
	pid := s.StartPageId()
	pg := s.newSeedPage(gWr.wCtx)
	s.CreateMapping(pid, pg, gWr.wCtx)

	if s.shouldPersist {
		commitDur := time.Duration(cfg.SyncInterval) * time.Second
		s.lss, err = NewLSStore(cfg.File, cfg.LSSLogSegmentSize, cfg.FlushBufferSize, 2, commitDur)
		if err != nil {
			return nil, err
		}

		err = s.doRecovery()
	}

	s.doInit()

	if s.shouldPersist {
		s.persistWriters = make([]*Writer, runtime.NumCPU())
		s.evictWriters = make([]*Writer, runtime.NumCPU())
		for i, _ := range s.persistWriters {
			s.persistWriters[i] = s.NewWriter()
			s.evictWriters[i] = s.NewWriter()
		}
		s.lssCleanerWriter = s.NewWriter()

		s.stoplssgc = make(chan struct{})
		s.stopswapper = make(chan struct{})
		s.stopmon = make(chan struct{})

		if cfg.AutoLSSCleaning {
			go s.lssCleanerDaemon()
		}

		if cfg.AutoSwapper {
			go s.swapperDaemon()
		}
	}

	sbuf := dbInstances.MakeBuf()
	defer dbInstances.FreeBuf(sbuf)
	dbInstances.Insert(unsafe.Pointer(s), ComparePlasma, sbuf, &dbInstances.Stats)

	go s.monitorMemUsage()
	return s, err
}

func (s *Plasma) monitorMemUsage() {
	sctx := s.newWCtx().SwapperContext()
	for {
		select {
		case <-s.stopmon:
			return
		default:
		}
		s.hasMemoryPressure = s.TriggerSwapper(sctx)
		time.Sleep(time.Millisecond * 100)
	}
}

func (s *Plasma) doInit() {
	if s.EnableShapshots {
		if s.currSn == 0 {
			s.currSn = 1
		}

		s.currSnapshot = &Snapshot{
			sn:       s.currSn,
			refCount: 1,
			db:       s,
		}

		s.updateMaxSn(s.currSn, true)
		s.updateRecoveryPoints(s.recoveryPoints)
		s.updateRPSns(s.recoveryPoints)
	}
}

func (s *Plasma) doRecovery() error {
	w := s.NewWriter()
	pg := newPage(w.wCtx, nil, nil).(*page)

	buf := w.wCtx.GetBuffer(0)

	fn := func(offset LSSOffset, bs []byte) (bool, error) {
		typ := getLSSBlockType(bs)
		bs = bs[lssBlockTypeSize:]
		switch typ {
		case lssDiscard:
		case lssRecoveryPoints:
			s.rpVersion, s.recoveryPoints = unmarshalRPs(bs)
		case lssMaxSn:
			s.currSn = decodeMaxSn(bs)
		case lssPageRemove:
			rmPglow := getRmPageLow(bs)
			pid := s.getPageId(rmPglow, w.wCtx)
			if pid != nil {
				currPg, err := s.ReadPage(pid, w.wCtx.pgRdrFn, true, w.wCtx)
				if err != nil {
					return false, err
				}

				w.sts.FlushDataSz -= int64(currPg.GetFlushDataSize())
				s.unindexPage(pid, w.wCtx)
			}
		case lssPageData, lssPageReloc, lssPageUpdate:
			pg.Unmarshal(bs, w.wCtx)

			newPageData := (typ == lssPageData || typ == lssPageReloc)
			if pid := s.getPageId(pg.low, w.wCtx); pid == nil {
				if newPageData {
					pid = s.AllocPageId(w.wCtx)
					s.CreateMapping(pid, pg, w.wCtx)
					s.indexPage(pid, w.wCtx)
				}
			} else {
				flushDataSz := len(bs)
				w.sts.FlushDataSz += int64(flushDataSz)

				currPg, err := s.ReadPage(pid, w.wCtx.pgRdrFn, true, w.wCtx)
				if err != nil {
					return false, err
				}

				if newPageData {
					w.sts.FlushDataSz -= int64(currPg.GetFlushDataSize())
					w.sts.FreeSz += int64(currPg.ComputeMemUsed())
					w.wCtx.destroyPg(currPg.(*page).head)
					pg.AddFlushRecord(offset, flushDataSz, 0)
				} else {
					_, numSegments := currPg.GetLSSOffset()
					pg.Append(currPg)
					pg.AddFlushRecord(offset, flushDataSz, numSegments)
				}

				s.CreateMapping(pid, pg, w.wCtx)
			}
		}

		pg.Reset()
		return true, nil
	}

	err := s.lss.Visitor(fn, buf)
	if err != nil {
		return err
	}

	// Initialize rightSiblings for all pages
	var lastPg Page
	callb := func(pid PageId, partn RangePartition) error {
		pg, err := s.ReadPage(pid, w.pgRdrFn, true, w.wCtx)
		if lastPg != nil {
			if err == nil && s.cmp(lastPg.MaxItem(), pg.MinItem()) != 0 {
				panic("found missing page")
			}

			lastPg.SetNext(pid)
		}

		lastPg = pg
		return err
	}

	s.PageVisitor(callb, 1)
	s.gcSn = s.currSn

	if lastPg != nil {
		lastPg.SetNext(s.EndPageId())
		if lastPg.MaxItem() != skiplist.MaxItem {
			panic("invalid last page")
		}
	}

	return err
}

func (s *Plasma) Close() {
	close(s.stopmon)
	if s.Config.AutoLSSCleaning {
		s.stoplssgc <- struct{}{}
		<-s.stoplssgc
	}

	if s.Config.AutoSwapper {
		s.stopswapper <- struct{}{}
		<-s.stopswapper
	}

	if s.Config.shouldPersist {
		s.lss.Close()
	}

	sbuf := dbInstances.MakeBuf()
	defer dbInstances.FreeBuf(sbuf)
	dbInstances.Delete(unsafe.Pointer(s), ComparePlasma, sbuf, &dbInstances.Stats)

	if s.useMemMgmt {
		close(s.smrChan)
		s.smrWg.Wait()
		s.destroyAllObjects()
	}
}

func ComparePlasma(a, b unsafe.Pointer) int {
	return int(uintptr(a)) - int(uintptr(b))
}

type Writer struct {
	*wCtx
}

// TODO: Refactor wCtx and Writer
type wCtx struct {
	*Plasma
	buf       *skiplist.ActionBuffer
	pgBuffers [][]byte
	slSts     *skiplist.Stats
	sts       *Stats
	dbIter    *skiplist.Iterator

	pgRdrFn PageReader

	pgAllocCtx *allocCtx

	reclaimList []reclaimObject
}

func (ctx *wCtx) freePages(pages []*pageDelta) {
	for _, pg := range pages {
		size := int64(computeMemUsed(pg, ctx.itemSize))
		ctx.sts.FreeSz += size
		if ctx.useMemMgmt {
			o := reclaimObject{typ: smrPage, size: uint32(size), ptr: unsafe.Pointer(pg)}
			ctx.reclaimList = append(ctx.reclaimList, o)
		}
	}
}

func (ctx *wCtx) SwapperContext() SwapperContext {
	return ctx.dbIter
}

func (s *Plasma) newWCtx() *wCtx {
	ctx := &wCtx{
		Plasma:     s,
		pgAllocCtx: new(allocCtx),
		buf:        s.Skiplist.MakeBuf(),
		slSts:      &s.Skiplist.Stats,
		sts:        new(Stats),
		pgBuffers:  make([][]byte, maxCtxBuffers),
	}

	ctx.dbIter = dbInstances.NewIterator(ComparePlasma, ctx.buf)
	ctx.pgRdrFn = func(offset LSSOffset) (Page, error) {
		return s.fetchPageFromLSS(offset, ctx)
	}

	return ctx
}

const (
	bufEncPage int = iota
	bufEncMeta
	bufEncSMO
	bufTempItem
)

func (ctx *wCtx) GetBuffer(id int) []byte {
	if ctx.pgBuffers[id] == nil {
		ctx.pgBuffers[id] = make([]byte, maxPageEncodedSize)
	}

	return ctx.pgBuffers[id]
}

func (s *Plasma) NewWriter() *Writer {

	w := &Writer{
		wCtx: s.newWCtx(),
	}

	s.Lock()
	defer s.Unlock()

	s.wlist = append(s.wlist, w)
	if s.useMemMgmt {
		s.smrWg.Add(1)
		go s.smrWorker(w.wCtx)
	}

	return w
}

func (s *Plasma) MemoryInUse() int64 {
	s.RLock()
	defer s.RUnlock()

	var memSz int64
	for _, w := range s.wlist {
		memSz += w.sts.AllocSz - w.sts.FreeSz
	}

	return memSz
}

func (s *Plasma) GetStats() Stats {
	var sts Stats

	s.RLock()
	defer s.RUnlock()

	sts.NumPages = int64(s.Skiplist.GetStats().NodeCount + 1)
	for _, w := range s.wlist {
		sts.Merge(w.sts)
	}
	sts.NumCachedPages = sts.NumPages - sts.NumPagesSwapOut + sts.NumPagesSwapIn
	sts.MemSz = sts.AllocSz - sts.FreeSz
	sts.MemSzIndex = sts.AllocSzIndex - sts.FreeSzIndex
	if s.shouldPersist {
		sts.BytesWritten = s.lss.BytesWritten()
		sts.LSSFrag, sts.LSSDataSize, sts.LSSUsedSpace = s.GetLSSInfo()
		sts.NumLSSCleanerReads = s.lssCleanerWriter.sts.NumLSSReads
		sts.LSSCleanerReadBytes = s.lssCleanerWriter.sts.LSSReadBytes
	}
	return sts
}

func (s *Plasma) LSSDataSize() int64 {
	var sz int64

	s.RLock()
	defer s.RUnlock()

	for _, w := range s.wlist {
		sz += w.sts.FlushDataSz
	}

	return sz
}

func (s *Plasma) indexPage(pid PageId, ctx *wCtx) {
	n := pid.(*skiplist.Node)
	if !s.Skiplist.Insert4(n, s.cmp, s.cmp, ctx.buf, n.Level(), false, ctx.slSts) {
		panic("duplicate index node")
	}

	ctx.sts.AllocSzIndex += int64(s.itemSize(n.Item()) + uintptr(n.Size()))
}

func (s *Plasma) unindexPage(pid PageId, ctx *wCtx) {
	n := pid.(*skiplist.Node)
	s.Skiplist.DeleteNode2(n, s.cmp, ctx.buf, ctx.slSts)
	size := int64(s.itemSize(n.Item()) + uintptr(n.Size()))
	ctx.sts.FreeSzIndex += size

	if s.useMemMgmt {
		o := reclaimObject{typ: smrPageId, size: uint32(size), ptr: unsafe.Pointer(n)}
		ctx.reclaimList = append(ctx.reclaimList, o)
	}
}

func (s *Plasma) tryPageRemoval(pid PageId, pg Page, ctx *wCtx) {
	n := pid.(*skiplist.Node)
	itm := n.Item()
	prev, _, found := s.Skiplist.Lookup(itm, s.cmp, ctx.buf, ctx.slSts)
	// Somebody completed the removal already
	if !found {
		return
	}

	pPid := PageId(prev)
	pPg, err := s.ReadPage(pPid, ctx.pgRdrFn, true, ctx)
	if err != nil {
		s.logError(fmt.Sprintf("tryPageRemove: err=%v", err))
		return
	}

	var pgBuf = ctx.GetBuffer(0)
	var metaBuf = ctx.GetBuffer(1)
	var fdSz, staleFdSz int

	pPg.Merge(pg)

	var offsets []LSSOffset
	var wbufs [][]byte
	var res LSSResource

	if s.shouldPersist {
		var numSegments int
		metaBuf = marshalPageSMO(pg, metaBuf)
		pgBuf, fdSz, staleFdSz, numSegments = pPg.Marshal(pgBuf, FullMarshal)

		sizes := []int{
			lssBlockTypeSize + len(metaBuf),
			lssBlockTypeSize + len(pgBuf),
		}

		offsets, wbufs, res = s.lss.ReserveSpaceMulti(sizes)

		writeLSSBlock(wbufs[0], lssPageRemove, metaBuf)

		writeLSSBlock(wbufs[1], lssPageData, pgBuf)
		pPg.AddFlushRecord(offsets[1], fdSz, numSegments)
	}

	if s.UpdateMapping(pPid, pPg, ctx) {
		s.unindexPage(pid, ctx)

		if s.shouldPersist {
			ctx.sts.FlushDataSz += int64(fdSz) - int64(staleFdSz)
			s.lss.FinalizeWrite(res)
		}

	} else if s.shouldPersist {
		discardLSSBlock(wbufs[0])
		discardLSSBlock(wbufs[1])
		s.lss.FinalizeWrite(res)
	}
}

func (s *Plasma) isStartPage(pid PageId) bool {
	return pid.(*skiplist.Node) == s.Skiplist.HeadNode()
}

func (s *Plasma) StartPageId() PageId {
	return s.Skiplist.HeadNode()
}

func (s *Plasma) EndPageId() PageId {
	return s.Skiplist.TailNode()
}

func (s *Plasma) trySMOs(pid PageId, pg Page, ctx *wCtx, doUpdate bool) bool {
	var updated bool

	if pg.NeedCompaction(s.Config.MaxDeltaChainLen) {
		staleFdSz := pg.Compact()
		if updated = s.UpdateMapping(pid, pg, ctx); updated {
			ctx.sts.Compacts++
			ctx.sts.FlushDataSz -= int64(staleFdSz)
		} else {
			ctx.sts.CompactConflicts++
		}
	} else if pg.NeedSplit(s.Config.MaxPageItems) {
		splitPid := s.AllocPageId(ctx)

		var fdSz, splitFdSz, staleFdSz, numSegments, numSegmentsSplit int
		var pgBuf = ctx.GetBuffer(0)
		var splitPgBuf = ctx.GetBuffer(1)

		newPg := pg.Split(splitPid)

		// Skip split, but compact
		if newPg == nil {
			s.FreePageId(splitPid, ctx)
			staleFdSz := pg.Compact()
			if updated = s.UpdateMapping(pid, pg, ctx); updated {
				ctx.sts.FlushDataSz -= int64(staleFdSz)
			}
			return updated
		}

		var offsets []LSSOffset
		var wbufs [][]byte
		var res LSSResource

		// Replace one page with two pages
		if s.shouldPersist {
			pgBuf, fdSz, staleFdSz, numSegments = pg.Marshal(pgBuf, s.Config.MaxPageLSSSegments)
			splitPgBuf, splitFdSz, _, numSegmentsSplit = newPg.Marshal(splitPgBuf, 1)

			sizes := []int{
				lssBlockTypeSize + len(pgBuf),
				lssBlockTypeSize + len(splitPgBuf),
			}

			offsets, wbufs, res = s.lss.ReserveSpaceMulti(sizes)

			typ := pgFlushLSSType(pg, numSegments)
			writeLSSBlock(wbufs[0], typ, pgBuf)
			pg.AddFlushRecord(offsets[0], fdSz, numSegments)

			writeLSSBlock(wbufs[1], lssPageData, splitPgBuf)
			newPg.AddFlushRecord(offsets[1], splitFdSz, numSegmentsSplit)
		}

		s.CreateMapping(splitPid, newPg, ctx)
		if updated = s.UpdateMapping(pid, pg, ctx); updated {
			s.indexPage(splitPid, ctx)
			ctx.sts.Splits++

			if s.shouldPersist {
				ctx.sts.FlushDataSz += int64(fdSz) + int64(splitFdSz) - int64(staleFdSz)
				s.lss.FinalizeWrite(res)
			}
		} else {
			ctx.sts.SplitConflicts++
			s.FreePageId(splitPid, ctx)

			if s.shouldPersist {
				discardLSSBlock(wbufs[0])
				discardLSSBlock(wbufs[1])
				s.lss.FinalizeWrite(res)
			}
		}
	} else if !s.isStartPage(pid) && pg.NeedMerge(s.Config.MinPageItems) {
		pg.Close()
		if updated = s.UpdateMapping(pid, pg, ctx); updated {
			s.tryPageRemoval(pid, pg, ctx)
			ctx.sts.Merges++
		} else {
			ctx.sts.MergeConflicts++
		}
	} else if doUpdate {
		updated = s.UpdateMapping(pid, pg, ctx)
	}

	return updated
}

func (s *Plasma) tryThrottleForMemory(ctx *wCtx) {
	if s.hasMemoryPressure {
		for s.TriggerSwapper(ctx.SwapperContext()) {
			time.Sleep(swapperWaitInterval)
		}
	}
}

func (s *Plasma) fetchPage(itm unsafe.Pointer, ctx *wCtx) (pid PageId, pg Page, err error) {
retry:
	if prev, curr, found := s.Skiplist.Lookup(itm, s.cmp, ctx.buf, ctx.slSts); found {
		pid = curr
	} else {
		pid = prev
	}

refresh:
	s.tryThrottleForMemory(ctx)

	if pg, err = s.ReadPage(pid, ctx.pgRdrFn, true, ctx); err != nil {
		return nil, nil, err
	}

	if !pg.InRange(itm) {
		pid = pg.Next()
		goto refresh
	}

	if pg.NeedRemoval() {
		s.tryPageRemoval(pid, pg, ctx)
		goto retry
	}

	s.updateCacheMeta(pid)

	return
}

func (w *Writer) Insert(itm unsafe.Pointer) error {
retry:
	pid, pg, err := w.fetchPage(itm, w.wCtx)
	if err != nil {
		return err
	}

	pg.Insert(itm)

	if !w.trySMOs(pid, pg, w.wCtx, true) {
		w.sts.InsertConflicts++
		goto retry
	}
	w.sts.BytesIncoming += int64(w.itemSize(itm))
	w.sts.Inserts++

	return nil
}

func (w *Writer) Delete(itm unsafe.Pointer) error {
retry:
	pid, pg, err := w.fetchPage(itm, w.wCtx)
	if err != nil {
		return err
	}

	pg.Delete(itm)

	if !w.trySMOs(pid, pg, w.wCtx, true) {
		w.sts.DeleteConflicts++
		goto retry
	}
	w.sts.BytesIncoming += int64(w.itemSize(itm))
	w.sts.Deletes++

	return nil
}

func (w *Writer) Lookup(itm unsafe.Pointer) (unsafe.Pointer, error) {
	pid, pg, err := w.fetchPage(itm, w.wCtx)
	if err != nil {
		return nil, err
	}

	ret := pg.Lookup(itm)
	w.trySMOs(pid, pg, w.wCtx, false)
	return ret, nil
}

func (s *Plasma) fetchPageFromLSS(baseOffset LSSOffset, ctx *wCtx) (*page, error) {
	pg := newPage(ctx, nil, nil).(*page)
	offset := baseOffset
	data := ctx.GetBuffer(bufEncPage)
	numSegments := 0
loop:
	for {
		l, err := s.lss.Read(offset, data)
		if err != nil {
			return nil, err
		}

		ctx.sts.NumLSSReads++
		ctx.sts.LSSReadBytes += int64(l)

		typ := getLSSBlockType(data)
		switch typ {
		case lssPageData, lssPageReloc, lssPageUpdate:
			currPgDelta := newPage(ctx, nil, nil).(*page)
			data := data[lssBlockTypeSize:l]
			nextOffset, hasChain := currPgDelta.unmarshalDelta(data, ctx)
			currPgDelta.AddFlushRecord(offset, len(data), 0)
			pg.Append(currPgDelta)
			offset = nextOffset
			numSegments++

			if !hasChain {
				break loop
			}
		default:
			panic(fmt.Sprintf("Invalid page data type %d", typ))
		}
	}

	if pg.head != nil {
		pg.SetNumSegments(numSegments)
		pg.head.rightSibling = pg.getPageId(pg.head.hiItm, ctx)
	}

	pg.prevHeadPtr = unsafe.Pointer(uintptr(uint64(baseOffset) | evictMask))

	return pg, nil
}

func (s *Plasma) logError(err string) {
	fmt.Printf("Plasma: (fatal error - %s)\n", err)
}

func (w *Writer) CompactAll() {
	callb := func(pid PageId, partn RangePartition) error {
		if pg, err := w.ReadPage(pid, nil, false, w.wCtx); err == nil {
			staleFdSz := pg.Compact()
			if updated := w.UpdateMapping(pid, pg, w.wCtx); updated {
				w.wCtx.sts.FlushDataSz -= int64(staleFdSz)
			}
		}
		return nil
	}

	w.PageVisitor(callb, 1)
}

func SetMemoryQuota(m int64) {
	atomic.StoreInt64(&memQuota, m)
}

func MemoryInUse() (sz int64) {
	buf := dbInstances.MakeBuf()
	defer dbInstances.FreeBuf(buf)

	ctx := dbInstances.NewIterator(ComparePlasma, buf)
	return MemoryInUse2(ctx)
}

func MemoryInUse2(ctx SwapperContext) (sz int64) {
	iter := (*skiplist.Iterator)(ctx)
	for iter.SeekFirst(); iter.Valid(); iter.Next() {
		db := (*Plasma)(iter.Get())
		sz += db.MemoryInUse()
	}

	return
}
