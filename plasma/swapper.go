package plasma

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

const (
	swapperWorkChanBufSize = 40
	swapperWorkBatchSize   = 4
)

func (s *Plasma) runSwapperVisitor(workCh chan []PageId, killch chan struct{}) {
	numThreads := 1
	sweepBatch := make([][]PageId, s.NumEvictorThreads)
	numThreads = s.NumEvictorThreads

	random := make([]*rand.Rand, numThreads)
	for i, _ := range random {
		random[i] = rand.New(rand.NewSource(rand.Int63()))
	}

	callb := func(pid PageId, partn RangePartition) error {
		if sweepBatch[partn.Shard] == nil {
			sweepBatch[partn.Shard] = make([]PageId, 0, swapperWorkBatchSize)
		}

		eligible := random[partn.Shard].Float32() <= 0.3

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
	if s.TriggerSwapper != nil {
		for s.TriggerSwapper() {
			pids := <-workCh
			for _, pid := range pids {
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
			for {
				select {
				case <-killch:
					return
				default:
				}
				if s.TriggerSwapper() && s.GetStats().NumCachedPages > 0 {
					s.tryEvictPages(workCh, s.evictWriters[i].wCtx)
				} else {
					time.Sleep(time.Microsecond * 10)
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
