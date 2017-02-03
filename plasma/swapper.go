package plasma

import (
	"errors"
	"github.com/t3rm1n4l/nitro/skiplist"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	swapperWorkChanBufSize = 40
	swapperWorkBatchSize   = 4
	swapperWaitInterval    = time.Microsecond * 10
)

func (s *Plasma) runSwapperVisitor(workCh chan []PageId, killch chan struct{}) {
	numThreads := 1
	sweepBatch := make([][]PageId, s.NumEvictorThreads)

	if s.ClockLRUEviction {
		numThreads = 1
	} else {
		numThreads = s.NumEvictorThreads
	}

	random := make([]*rand.Rand, numThreads)
	for i, _ := range random {
		random[i] = rand.New(rand.NewSource(rand.Int63()))
	}

	callb := func(pid PageId, partn RangePartition) error {
		eligible := true
		if sweepBatch[partn.Shard] == nil {
			sweepBatch[partn.Shard] = make([]PageId, 0, swapperWorkBatchSize)
		}

		if !s.ClockLRUEviction {
			eligible = random[partn.Shard].Float32() <= 0.3
		}

		if eligible {
			sweepBatch[partn.Shard] = append(sweepBatch[partn.Shard], pid)
		}

		if len(sweepBatch[partn.Shard]) == cap(sweepBatch[partn.Shard]) {
			select {
			case workCh <- sweepBatch[partn.Shard]:
				sweepBatch[partn.Shard] = nil
			case <-killch:
				return errors.New("eviction visitor shutdown")
			}
		}
		return nil
	}

	for s.PageVisitor(callb, numThreads) == nil {
	}
}

func (s *Plasma) tryEvictPages(workCh chan []PageId, ctx *wCtx) {
	sctx := ctx.SwapperContext()
	for s.TriggerSwapper(sctx) {
		pids := <-workCh
		for _, pid := range pids {
			if s.canEvict(pid) {
				s.Persist(pid, true, ctx)
			}
		}
	}
}

func (s *Plasma) swapperDaemon() {
	var wg sync.WaitGroup
	workCh := make(chan []PageId, swapperWorkChanBufSize)
	killch := make(chan struct{})

	for i := 0; i < s.NumEvictorThreads; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sctx := s.evictWriters[i].wCtx.SwapperContext()
			for {
				select {
				case <-killch:
					return
				default:
				}
				if s.TriggerSwapper(sctx) && s.GetStats().NumCachedPages > 0 {
					s.tryEvictPages(workCh, s.evictWriters[i].wCtx)
				} else {
					time.Sleep(swapperWaitInterval)
				}
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.runSwapperVisitor(workCh, killch)
	}()

	go func() {
		<-s.stopswapper
		close(killch)
	}()

	wg.Wait()

	s.stopswapper <- struct{}{}
}

type SwapperContext *skiplist.Iterator

func QuotaSwapper(ctx SwapperContext) bool {
	return MemoryInUse2(ctx) >= int64(float64(atomic.LoadInt64(&memQuota))*0.7)
}

func (s *Plasma) canEvict(pid PageId) bool {
	ok := true
	if s.ClockLRUEviction {
		n := pid.(*skiplist.Node)
		ok = n.Cache == 0
		n.Cache = 0
	}

	return ok
}

func (s *Plasma) updateCacheMeta(pid PageId) {
	if s.ClockLRUEviction {
		pid.(*skiplist.Node).Cache = 1
	}
}
