package plasma

import (
	"github.com/couchbase/nitro/skiplist"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	swapperWorkChanBufSize = 40
	swapperWorkBatchSize   = 16
	swapperWaitInterval    = time.Microsecond * 10
)

type clockHandle struct {
	buf []byte
	pos unsafe.Pointer
	itr *skiplist.Iterator
}

func (s *Plasma) acquireClockHandle() *clockHandle {
	s.clockLock.Lock()
	return s.ch
}

func (s *Plasma) releaseClockHandle(h *clockHandle) {
	s.clockLock.Unlock()
}

func (s *Plasma) sweepClock(h *clockHandle) []PageId {
	pids := make([]PageId, 0, swapperWorkBatchSize)
	if h.pos == nil {
		pids = append(pids, s.StartPageId())
		h.itr.SeekFirst()
	} else {
		h.itr.Seek(h.pos)
	}

	for len(pids) < swapperWorkBatchSize && h.itr.Valid() {
		pid := h.itr.GetNode()
		pids = append(pids, pid)
		h.itr.Next()
	}

	if h.itr.Valid() {
		itm := h.itr.Get()
		sz := s.itemSize(itm)
		h.pos = unsafe.Pointer(&h.buf[0])
		memcopy(h.pos, itm, int(sz))
	} else {
		h.pos = nil
	}

	return pids
}

func (s *Plasma) tryEvictPages(ctx *wCtx) {
	sctx := ctx.SwapperContext()
	for s.TriggerSwapper(sctx) {
		h := s.acquireClockHandle()
		tok := ctx.BeginTx()
		pids := s.sweepClock(h)
		s.releaseClockHandle(h)
		for _, pid := range pids {
			if s.canEvict(pid) {
				s.Persist(pid, true, ctx)
			}
		}
		ctx.EndTx(tok)
	}
}

func (s *Plasma) swapperDaemon() {
	var wg sync.WaitGroup

	killch := make(chan struct{})
	s.ch = &clockHandle{
		buf: make([]byte, maxPageEncodedSize),
		itr: s.Skiplist.NewIterator2(s.cmp,
			s.Skiplist.MakeBuf()),
	}

	for i := 0; i < s.NumEvictorThreads; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sctx := s.evictWriters[i].SwapperContext()
			for {
				select {
				case <-killch:
					return
				default:
				}

				if s.TriggerSwapper(sctx) {
					s.tryEvictPages(s.evictWriters[i])
					s.trySMRObjects(s.evictWriters[i], swapperSMRInterval)
				} else {
					time.Sleep(swapperWaitInterval)
				}
			}
		}(i)
	}

	go func() {
		<-s.stopswapper
		close(killch)
	}()

	wg.Wait()

	s.stopswapper <- struct{}{}
}

type SwapperContext *skiplist.Iterator

func QuotaSwapper(ctx SwapperContext) bool {
	return MemoryInUse2(ctx) >= int64(float64(atomic.LoadInt64(&memQuota)))
}

func (s *Plasma) canEvict(pid PageId) bool {
	ok := true
	n := pid.(*skiplist.Node)
	ok = n.Cache == 0
	n.Cache = 0

	return ok
}

func (s *Plasma) updateCacheMeta(pid PageId) {
	pid.(*skiplist.Node).Cache = 1
}
