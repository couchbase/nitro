package plasma

import (
	"github.com/t3rm1n4l/nitro/skiplist"
	"unsafe"
)

type Plasma struct {
	Config
	*skiplist.Skiplist
	*pageTable
}

type wCtx struct {
	buf *skiplist.ActionBuffer
	sts *skiplist.Stats
}

type Config struct {
	MaxDeltaChainLen int
	MaxPageItems     int
	MinPageItems     int
	Compare          skiplist.CompareFn
	ItemSize         ItemSizeFn
}

func New(cfg Config) *Plasma {
	sl := skiplist.New()
	s := &Plasma{
		Config:    cfg,
		Skiplist:  sl,
		pageTable: newPageTable(sl, cfg.ItemSize, cfg.Compare),
	}

	return s
}

type Writer struct {
	*Plasma
	*wCtx
}

func (s *Plasma) NewWriter() *Writer {
	return &Writer{
		Plasma: s,
		wCtx: &wCtx{
			buf: s.Skiplist.MakeBuf(),
			sts: &s.Skiplist.Stats,
		},
	}
	return nil
}

func (s *Plasma) indexPage(pid PageId, ctx *wCtx) {
	n := pid.(*skiplist.Node)
	s.Skiplist.Insert4(n, s.cmp, s.cmp, ctx.buf, n.Level(), false, ctx.sts)
}

func (s *Plasma) unindexPage(pid PageId, ctx *wCtx) {
	n := pid.(*skiplist.Node)
	s.Skiplist.DeleteNode(n, s.cmp, ctx.buf, ctx.sts)
}

func (s *Plasma) tryPageRemoval(pid PageId, pg Page, ctx *wCtx) {
	n := pid.(*skiplist.Node)
	itm := n.Item()
	prev, _, found := s.Skiplist.Lookup(itm, s.cmp, ctx.buf, ctx.sts)
	if !found {
		panic("should not happen")
	}
	pPid := PageId(prev)
	pPg := s.ReadPage(pPid)
	pPg.Merge(pg)
	if s.UpdateMapping(pPid, pPg) {
		s.unindexPage(pid, ctx)
	}
}

func (s *Plasma) isRootPage(pid PageId) bool {
	return pid.(*skiplist.Node) == s.Skiplist.HeadNode()
}

func (s *Plasma) trySMOs(pid PageId, pg Page, ctx *wCtx) {
	if pg.NeedCompaction(s.Config.MaxDeltaChainLen) {
		pg.Compact()
		s.UpdateMapping(pid, pg)
	} else if pg.NeedSplit(s.Config.MaxPageItems) {
		splitPid := s.AllocPageId()
		if newPg := pg.Split(splitPid); newPg == nil || !s.UpdateMapping(pid, pg) {
			s.FreePageId(splitPid)
		} else {
			s.CreateMapping(splitPid, newPg)
			s.indexPage(splitPid, ctx)
		}
	} else if !s.isRootPage(pid) && pg.NeedMerge(s.Config.MinPageItems) {
		pg.Close()
		if s.UpdateMapping(pid, pg) {
			s.tryPageRemoval(pid, pg, ctx)
		}
	}
}

func (s *Plasma) fetchPage(itm unsafe.Pointer, ctx *wCtx) (pid PageId, pg Page) {
retry:
	if prev, curr, found := s.Skiplist.Lookup(itm, s.cmp, ctx.buf, ctx.sts); found {
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
	if !w.UpdateMapping(pid, pg) {
		goto retry
	}

	w.trySMOs(pid, pg, w.wCtx)
}

func (w *Writer) Delete(itm unsafe.Pointer) {
retry:
	pid, pg := w.fetchPage(itm, w.wCtx)
	pg.Delete(itm)
	if !w.UpdateMapping(pid, pg) {
		goto retry
	}

	w.trySMOs(pid, pg, w.wCtx)
}

func (w *Writer) Lookup(itm unsafe.Pointer) unsafe.Pointer {
	pid, pg := w.fetchPage(itm, w.wCtx)
	ret := pg.Lookup(itm)
	w.trySMOs(pid, pg, w.wCtx)
	return ret
}
