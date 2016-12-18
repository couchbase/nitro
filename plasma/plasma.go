package plasma

import (
	"fmt"
	"github.com/t3rm1n4l/nitro/skiplist"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

type PageReader func(offset lssOffset) (Page, error)

const maxCtxBuffers = 3

type Plasma struct {
	Config
	*skiplist.Skiplist
	*pageTable
	wlist                  []*Writer
	lss                    *lsStore
	lssCleanerWriter       *Writer
	persistWriters         []*Writer
	evictWriters           []*Writer
	stoplssgc, stopswapper chan struct{}
	sync.RWMutex

	// MVCC data structures
	mvcc         sync.RWMutex
	currSn       uint64
	numSnCreated int
	gcSn         uint64
	currSnapshot *Snapshot

	lastMaxSn      uint64
	minRPSn        uint64
	rpVersion      uint16
	recoveryPoints []*RecoveryPoint
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

	FlushDataSz int64
	MemSz       int64

	NumPagesSwapOut int64
	NumPagesSwapIn  int64
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

	s.FlushDataSz += o.FlushDataSz
	s.MemSz += o.MemSz

	s.NumPagesSwapOut += o.NumPagesSwapOut
	s.NumPagesSwapIn += o.NumPagesSwapIn
}

func (s Stats) String() string {
	return fmt.Sprintf("===== Stats =====\n"+
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
		"flushdata_size    = %d\n"+
		"memory_size       = %d\n"+
		"num_pages_swapout = %d\n"+
		"num_pages_swapin  = %d\n",
		s.Inserts-s.Deletes,
		s.Compacts, s.Splits, s.Merges,
		s.Inserts, s.Deletes, s.CompactConflicts,
		s.SplitConflicts, s.MergeConflicts,
		s.InsertConflicts, s.DeleteConflicts,
		s.SwapInConflicts, s.FlushDataSz,
		s.MemSz, s.NumPagesSwapOut,
		s.NumPagesSwapIn)
}

func New(cfg Config) (*Plasma, error) {
	var err error

	cfg = applyConfigDefaults(cfg)
	sl := skiplist.New()
	s := &Plasma{
		Config:   cfg,
		Skiplist: sl,
	}

	ptWr := s.NewWriter()

	var cfGetter, lfGetter FilterGetter
	if cfg.EnableShapshots {
		cfGetter = func() ItemFilter {
			var sn uint64
			gcSn := atomic.LoadUint64(&s.gcSn)
			rpSn := atomic.LoadUint64(&s.minRPSn)

			if rpSn > 0 && rpSn < gcSn {
				sn = rpSn
			} else {
				sn = gcSn
			}

			return &gcFilter{gcSn: sn}
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

	s.pageTable = newPageTable(sl, cfg.ItemSize, cfg.Compare, cfGetter, lfGetter, ptWr.wCtx.sts)

	pid := s.StartPageId()
	pg := newSeedPage()
	s.CreateMapping(pid, pg)

	if s.shouldPersist {
		s.lss, err = newLSStore(cfg.File, cfg.LSSLogSegmentSize, cfg.FlushBufferSize, 2)
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

		if cfg.AutoLSSCleaning {
			go s.lssCleanerDaemon()
		}

		if cfg.AutoSwapper {
			go s.swapperDaemon()
		}
	}

	return s, err
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
		s.updateRecoveryPoints(s.recoveryPoints, true)
	}
}

func (s *Plasma) doRecovery() error {
	pg := &page{
		storeCtx: s.storeCtx,
	}

	w := s.NewWriter()

	var rmPg *page
	var splitKey unsafe.Pointer
	doSplit := false
	doRmPage := false
	doMerge := false
	rmFdSz := 0

	buf := w.wCtx.GetBuffer(0)

	fn := func(offset lssOffset, bs []byte) (bool, error) {
		typ := getLSSBlockType(bs)
		switch typ {
		case lssDiscard:
		case lssPageSplit:
			doSplit = true
			splitKey = unmarshalPageSMO(pg, bs[lssBlockTypeSize:])
			break
		case lssRecoveryPoints:
			s.rpVersion, s.recoveryPoints = unmarshalRPs(bs[lssBlockTypeSize:])
		case lssMaxSn:
			s.currSn = decodeMaxSn(bs[lssBlockTypeSize:])
		case lssPageMerge:
			doRmPage = true
		case lssPageData, lssPageReloc:
			pg.Unmarshal(bs[lssBlockTypeSize:], w.wCtx)
			flushDataSz := len(bs)
			w.sts.FlushDataSz += int64(flushDataSz)

			var pid PageId
			if pg.low == skiplist.MinItem {
				pid = s.StartPageId()
			} else {
				pid = s.getPageId(pg.low, w.wCtx)
			}

			if pid == nil {
				pid = s.AllocPageId()
				s.CreateMapping(pid, pg)
				s.indexPage(pid, w.wCtx)
			} else {
				currPg, err := s.ReadPage(pid, w.wCtx.pgRdrFn, true)
				if err != nil {
					return false, err
				}
				// If same version, do prepend, otherwise replace.
				if currPg.GetVersion() == pg.GetVersion() || pg.IsEmpty() {
					pg.Append(currPg)
				} else {
					// Replace happens with a flushed page
					// Hence, no need to keep fdsize in basepage
					w.sts.FlushDataSz -= int64(currPg.GetFlushDataSize())
				}
			}

			if doSplit {
				splitPid := s.AllocPageId()
				newPg := pg.doSplit(splitKey, splitPid, -1)
				s.CreateMapping(splitPid, newPg)
				s.indexPage(splitPid, w.wCtx)
				w.wCtx.sts.Splits++
				doSplit = false
			} else if doRmPage {
				rmPg = new(page)
				*rmPg = *pg
				rmFdSz = flushDataSz
				doRmPage = false
				doMerge = true
				s.unindexPage(pid, w.wCtx)
			} else if doMerge {
				doMerge = false
				pg.Merge(rmPg)
				flushDataSz += rmFdSz
				w.wCtx.sts.Merges++
				rmPg = nil
			}

			pg.AddFlushRecord(offset, flushDataSz, false)
			s.CreateMapping(pid, pg)
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
		if lastPg != nil {
			lastPg.SetNext(pid)
		}

		pg, err := s.ReadPage(pid, w.pgRdrFn, true)
		lastPg = pg
		return err
	}

	s.PageVisitor(callb, 1)
	s.gcSn = s.currSn
	return err
}

func (s *Plasma) Close() {
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
}

type Writer struct {
	*Plasma
	*wCtx
}

type wCtx struct {
	buf       *skiplist.ActionBuffer
	pgBuffers [][]byte
	slSts     *skiplist.Stats
	sts       *Stats

	pgRdrFn PageReader
}

func (s *Plasma) newWCtx() *wCtx {
	ctx := &wCtx{
		buf:       s.Skiplist.MakeBuf(),
		slSts:     &s.Skiplist.Stats,
		sts:       new(Stats),
		pgBuffers: make([][]byte, maxCtxBuffers),
	}

	ctx.pgRdrFn = func(offset lssOffset) (Page, error) {
		return s.fetchPageFromLSS(offset, ctx)
	}

	return ctx
}

func (ctx *wCtx) GetBuffer(id int) []byte {
	if ctx.pgBuffers[id] == nil {
		ctx.pgBuffers[id] = make([]byte, maxPageEncodedSize)
	}

	return ctx.pgBuffers[id]
}

func (s *Plasma) NewWriter() *Writer {

	w := &Writer{
		Plasma: s,
		wCtx:   s.newWCtx(),
	}

	s.Lock()
	defer s.Unlock()

	s.wlist = append(s.wlist, w)
	return w
}

func (s *Plasma) GetStats() Stats {
	var sts Stats

	s.RLock()
	defer s.RUnlock()

	for _, w := range s.wlist {
		sts.Merge(w.sts)
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
}

func (s *Plasma) unindexPage(pid PageId, ctx *wCtx) {
	n := pid.(*skiplist.Node)
	s.Skiplist.DeleteNode(n, s.cmp, ctx.buf, ctx.slSts)
}

func (s *Plasma) tryPageRemoval(pid PageId, pg Page, ctx *wCtx) {
	n := pid.(*skiplist.Node)
	itm := n.Item()
	prev, _, found := s.Skiplist.Lookup(itm, s.cmp, ctx.buf, ctx.slSts)
	// Somebody else removed the node
	if !found {
		return
	}

	pPid := PageId(prev)
	pPg, err := s.ReadPage(pPid, ctx.pgRdrFn, true)
	if err != nil {
		s.logError(fmt.Sprintf("tryPageRemove: err=%v", err))
		return
	}

	var rmPgBuf = ctx.GetBuffer(0)
	var pgBuf = ctx.GetBuffer(1)
	var metaBuf = ctx.GetBuffer(2)
	var fdSz, rmFdSz int

	if s.shouldPersist {
		rmPgBuf, fdSz = pg.Marshal(rmPgBuf)
		pgBuf, rmFdSz = pPg.Marshal(pgBuf)
		metaBuf = marshalPageSMO(pg, metaBuf)
		// Merge flushDataSize info of dead page to parent
		fdSz += rmFdSz
	}

	pPg.Merge(pg)

	var offsets []lssOffset
	var wbufs [][]byte
	var res lssResource

	if s.shouldPersist {
		sizes := []int{
			lssBlockTypeSize + len(metaBuf),
			lssBlockTypeSize + len(rmPgBuf),
			lssBlockTypeSize + len(pgBuf),
		}

		offsets, wbufs, res = s.lss.ReserveSpaceMulti(sizes)
		pPg.AddFlushRecord(offsets[0], fdSz, false)

		writeLSSBlock(wbufs[0], lssPageMerge, metaBuf)
		writeLSSBlock(wbufs[1], lssPageData, rmPgBuf)
		writeLSSBlock(wbufs[2], lssPageData, pgBuf)
	}

	if s.UpdateMapping(pPid, pPg) {
		s.unindexPage(pid, ctx)

		if s.shouldPersist {
			ctx.sts.FlushDataSz += int64(fdSz)
			s.lss.FinalizeWrite(res)
		}

	} else if s.shouldPersist {
		discardLSSBlock(wbufs[0])
		discardLSSBlock(wbufs[1])
		discardLSSBlock(wbufs[2])
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
		updated = s.UpdateMapping(pid, pg)
		if updated {
			ctx.sts.Compacts++
			ctx.sts.FlushDataSz -= int64(staleFdSz)
		} else {
			ctx.sts.CompactConflicts++
		}
	} else if pg.NeedSplit(s.Config.MaxPageItems) {
		splitPid := s.AllocPageId()

		var fdSz int
		var pgBuf = ctx.GetBuffer(0)
		var splitMetaBuf = ctx.GetBuffer(1)

		if s.shouldPersist {
			pgBuf, fdSz = pg.Marshal(pgBuf)
		}

		newPg := pg.Split(splitPid)

		// Skip split, but compact
		if newPg == nil {
			s.FreePageId(splitPid)
			staleFdSz := pg.Compact()
			if updated = s.UpdateMapping(pid, pg); updated {
				ctx.sts.FlushDataSz -= int64(staleFdSz)
			}
			return updated
		}

		var offsets []lssOffset
		var wbufs [][]byte
		var res lssResource

		// Commit split information in lss
		if s.shouldPersist {
			splitMetaBuf = marshalPageSMO(newPg, splitMetaBuf)
			sizes := []int{
				lssBlockTypeSize + len(splitMetaBuf),
				lssBlockTypeSize + len(pgBuf),
			}

			offsets, wbufs, res = s.lss.ReserveSpaceMulti(sizes)
			pg.AddFlushRecord(offsets[0], fdSz, false)
			writeLSSBlock(wbufs[0], lssPageSplit, splitMetaBuf)
			writeLSSBlock(wbufs[1], lssPageData, pgBuf)
		}

		s.CreateMapping(splitPid, newPg)
		if updated = s.UpdateMapping(pid, pg); updated {
			s.indexPage(splitPid, ctx)
			ctx.sts.Splits++

			if s.shouldPersist {
				ctx.sts.FlushDataSz += int64(fdSz)
				s.lss.FinalizeWrite(res)
			}
		} else {
			ctx.sts.SplitConflicts++
			s.FreePageId(splitPid)

			if s.shouldPersist {
				discardLSSBlock(wbufs[0])
				discardLSSBlock(wbufs[1])
				s.lss.FinalizeWrite(res)
			}
		}

	} else if !s.isStartPage(pid) && pg.NeedMerge(s.Config.MinPageItems) {
		pg.Close()
		if updated = s.UpdateMapping(pid, pg); updated {
			s.tryPageRemoval(pid, pg, ctx)
			ctx.sts.Merges++
		} else {
			ctx.sts.MergeConflicts++
		}
	} else if doUpdate {
		updated = s.UpdateMapping(pid, pg)
	}

	return updated
}

func (s *Plasma) fetchPage(itm unsafe.Pointer, ctx *wCtx) (pid PageId, pg Page, err error) {
retry:
	if prev, curr, found := s.Skiplist.Lookup(itm, s.cmp, ctx.buf, ctx.slSts); found {
		pid = curr
	} else {
		pid = prev
	}

refresh:
	if pg, err = s.ReadPage(pid, ctx.pgRdrFn, true); err != nil {
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

func (s *Plasma) fetchPageFromLSS(baseOffset lssOffset, ctx *wCtx) (*page, error) {
	pg := &page{
		storeCtx: s.storeCtx,
	}

	offset := baseOffset
	data := ctx.GetBuffer(0)
loop:
	for {
		l, err := s.lss.Read(offset, data)
		if err != nil {
			return nil, err
		}

		typ := getLSSBlockType(data)
		switch typ {
		case lssPageSplit:
			splitKey := unmarshalPageSMO(pg, data[lssBlockTypeSize:])
			dataOffset := lssBlockEndOffset(offset, data[:l])
			if dataPg, err := s.fetchPageFromLSS(dataOffset, ctx); err == nil {
				dataPg.head = dataPg.newSplitPageDelta(splitKey, nil)
				dataPg.AddFlushRecord(offset, 0, false)
				pg.Append(dataPg)
			} else {
				return nil, err
			}
			break loop
		case lssPageMerge:
			mergeKey := unmarshalPageSMO(pg, data[lssBlockTypeSize:])
			rmPgDataOffset := lssBlockEndOffset(offset, data[:l])
			if rmPg, err := s.fetchPageFromLSS(rmPgDataOffset, ctx); err == nil {
				l, err := s.lss.Read(rmPgDataOffset, data)
				if err != nil {
					return nil, err
				}

				dataOffset := lssBlockEndOffset(rmPgDataOffset, data[:l])
				if dataPg, err := s.fetchPageFromLSS(dataOffset, ctx); err == nil {
					dataPg.head = dataPg.newMergePageDelta(mergeKey, rmPg.head)
					dataPg.AddFlushRecord(offset, 0, false)
					pg.Append(dataPg)
				} else {
					return nil, err
				}
			} else {
				return nil, err
			}
			break loop
		case lssPageData, lssPageReloc:
			currPgDelta := &page{
				storeCtx: s.storeCtx,
			}
			data := data[lssBlockTypeSize:l]
			nextOffset, hasChain := currPgDelta.unmarshalDelta(data, ctx)
			currPgDelta.AddFlushRecord(offset, len(data), false)
			pg.Append(currPgDelta)
			offset = nextOffset

			if !hasChain {
				break loop
			}
		default:
			panic(fmt.Sprintf("Invalid page data type %d", typ))
		}
	}

	if pg.head != nil {
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
		if pg, err := w.ReadPage(pid, nil, false); err == nil {
			staleFdSz := pg.Compact()
			if updated := w.UpdateMapping(pid, pg); updated {
				w.wCtx.sts.FlushDataSz -= int64(staleFdSz)
			}
		}
		return nil
	}

	w.PageVisitor(callb, 1)
}

func (s *Plasma) lookupIndex(key unsafe.Pointer, ctx *wCtx) PageId {
	if key == nil {
		return s.StartPageId()
	}

	if _, node, found := s.Skiplist.Lookup(key, s.cmp, ctx.buf, ctx.slSts); found {
		return PageId(node)
	}
	return nil
}
