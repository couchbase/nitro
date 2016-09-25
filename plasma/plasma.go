package plasma

import (
	"fmt"
	"github.com/t3rm1n4l/nitro/skiplist"
	"sync"
	"unsafe"
)

type Plasma struct {
	Config
	*skiplist.Skiplist
	*pageTable
	wlist []*Writer
	lss   *lsStore
	sync.RWMutex
}

type wCtx struct {
	buf                             *skiplist.ActionBuffer
	pgEncBuf1, pgEncBuf2, pgEncBuf3 []byte
	slSts                           *skiplist.Stats
	sts                             *Stats
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
		"delete_conflicts  = %d\n",
		s.Inserts-s.Deletes,
		s.Compacts, s.Splits, s.Merges,
		s.Inserts, s.Deletes, s.CompactConflicts,
		s.SplitConflicts, s.MergeConflicts,
		s.InsertConflicts, s.DeleteConflicts)
}

type Config struct {
	MaxDeltaChainLen int
	MaxPageItems     int
	MinPageItems     int
	Compare          skiplist.CompareFn
	ItemSize         ItemSizeFn

	MaxSize         int64
	File            string
	FlushBufferSize int
}

func New(cfg Config) (*Plasma, error) {
	sl := skiplist.New()
	s := &Plasma{
		Config:    cfg,
		Skiplist:  sl,
		pageTable: newPageTable(sl, cfg.ItemSize, cfg.Compare),
	}

	lss, err := newLSStore(cfg.File, cfg.MaxSize, cfg.FlushBufferSize, 2)
	s.lss = lss
	return s, err
}

func (s *Plasma) Close() {
	s.lss.Close()
}

type Writer struct {
	*Plasma
	*wCtx
}

func (s *Plasma) NewWriter() *Writer {
	w := &Writer{
		Plasma: s,
		wCtx: &wCtx{
			buf:       s.Skiplist.MakeBuf(),
			slSts:     &s.Skiplist.Stats,
			sts:       new(Stats),
			pgEncBuf1: make([]byte, maxPageEncodedSize),
			pgEncBuf2: make([]byte, maxPageEncodedSize),
			pgEncBuf3: make([]byte, maxPageEncodedSize),
		},
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

func (s *Plasma) indexPage(pid PageId, ctx *wCtx) {
	n := pid.(*skiplist.Node)
	s.Skiplist.Insert4(n, s.cmp, s.cmp, ctx.buf, n.Level(), false, ctx.slSts)
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
	pPg := s.ReadPage(pPid)

	rmPgBuf := ctx.pgEncBuf1
	rmPgBuf = pg.Marshal(rmPgBuf)

	pgBuf := ctx.pgEncBuf2
	pgBuf = pPg.Marshal(pgBuf)

	metaBuf := ctx.pgEncBuf3
	metaBuf = encodeMetaBlock(pg.(*page), metaBuf)

	pPg.Merge(pg)
	pgFlushOffset := pPg.(*page).addFlushDelta()

	sizes := []int{
		lssBlockTypeSize + len(metaBuf),
		lssBlockTypeSize + len(rmPgBuf),
		lssBlockTypeSize + len(pgBuf),
	}
	offsets, wbufs, res := s.lss.ReserveSpaceMulti(sizes)

	if s.UpdateMapping(pPid, pPg) {
		s.unindexPage(pid, ctx)

		writeLSSBlock(wbufs[0], lssPageMerge, metaBuf)
		writeLSSBlock(wbufs[1], lssPageData, rmPgBuf)
		writeLSSBlock(wbufs[2], lssPageData, pgBuf)
		*pgFlushOffset = offsets[0]
	} else {
		discardLSSBlock(wbufs[0])
		discardLSSBlock(wbufs[1])
		discardLSSBlock(wbufs[2])
	}

	s.lss.FinalizeWrite(res)
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
		pg.Compact()
		updated = s.UpdateMapping(pid, pg)
		if updated {
			ctx.sts.Compacts++
		} else {
			ctx.sts.CompactConflicts++
		}
	} else if pg.NeedSplit(s.Config.MaxPageItems) {
		splitPid := s.AllocPageId()

		pgBuf := ctx.pgEncBuf1
		pgBuf = pg.Marshal(pgBuf)
		newPg := pg.Split(splitPid)
		pgFlushOffset := pg.(*page).addFlushDelta()

		// Skip split
		if newPg == nil {
			s.FreePageId(splitPid)
			if doUpdate {
				if updated = s.UpdateMapping(pid, pg); updated {
					offset, wbuf, res := s.lss.ReserveSpace(lssBlockTypeSize + len(pgBuf))
					writeLSSBlock(wbuf, lssPageData, pgBuf)
					s.lss.FinalizeWrite(res)
					*pgFlushOffset = offset
				}
			}
			return updated
		}

		splitMetaBuf := ctx.pgEncBuf2
		splitMetaBuf = encodeMetaBlock(newPg.(*page), splitMetaBuf)

		// Commit split information in lss
		sizes := []int{
			lssBlockTypeSize + len(splitMetaBuf),
			lssBlockTypeSize + len(pgBuf),
		}
		offsets, wbufs, res := s.lss.ReserveSpaceMulti(sizes)

		if s.UpdateMapping(pid, pg) {
			s.CreateMapping(splitPid, newPg)
			s.indexPage(splitPid, ctx)
			ctx.sts.Splits++

			writeLSSBlock(wbufs[0], lssPageSplit, splitMetaBuf)
			writeLSSBlock(wbufs[1], lssPageData, pgBuf)

			*pgFlushOffset = offsets[0]
		} else {
			ctx.sts.SplitConflicts++
			s.FreePageId(splitPid)

			discardLSSBlock(wbufs[0])
			discardLSSBlock(wbufs[1])
		}
		s.lss.FinalizeWrite(res)
	} else if !s.isStartPage(pid) && pg.NeedMerge(s.Config.MinPageItems) {
		pg.Close()
		if s.UpdateMapping(pid, pg) {
			s.tryPageRemoval(pid, pg, ctx)
			ctx.sts.Merges++
			updated = true
		} else {
			ctx.sts.MergeConflicts++
		}
	} else if doUpdate {
		updated = s.UpdateMapping(pid, pg)
	}

	return updated
}

func (s *Plasma) fetchPage(itm unsafe.Pointer, ctx *wCtx) (pid PageId, pg Page) {
retry:
	if prev, curr, found := s.Skiplist.Lookup(itm, s.cmp, ctx.buf, ctx.slSts); found {
		pid = curr
	} else {
		pid = prev
	}

refresh:
	pg = s.ReadPage(pid)
	if !pg.InRange(itm) {
		pid = pg.(*page).head.rightSibling
		goto refresh
	}

	if pg.NeedRemoval() {
		s.tryPageRemoval(pid, pg, ctx)
		goto retry
	}

	return
}

func (w *Writer) Insert(itm unsafe.Pointer) {
retry:
	pid, pg := w.fetchPage(itm, w.wCtx)
	pg.Insert(itm)

	if !w.trySMOs(pid, pg, w.wCtx, true) {
		w.sts.InsertConflicts++
		goto retry
	}
	w.sts.Inserts++
}

func (w *Writer) Delete(itm unsafe.Pointer) {
retry:
	pid, pg := w.fetchPage(itm, w.wCtx)
	pg.Delete(itm)

	if !w.trySMOs(pid, pg, w.wCtx, true) {
		w.sts.DeleteConflicts++
		goto retry
	}
	w.sts.Deletes++
}

func (w *Writer) Lookup(itm unsafe.Pointer) unsafe.Pointer {
	pid, pg := w.fetchPage(itm, w.wCtx)
	ret := pg.Lookup(itm)
	w.trySMOs(pid, pg, w.wCtx, false)
	return ret
}
