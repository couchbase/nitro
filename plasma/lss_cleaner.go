package plasma

import (
	"fmt"
	"sync/atomic"
	"time"
)

func (s *Plasma) tryPageRelocation(pid PageId, pg Page, buf []byte, ctx *wCtx) (bool, LSSOffset) {
	var ok bool

	// TODO: Avoid compact if the page does not have any swapout deltas
	staleFdSz := pg.Compact()
	bs, dataSz, staleSz, numSegments := pg.Marshal(buf, FullMarshal)
	offset, wbuf, res := s.lss.ReserveSpace(lssBlockTypeSize + len(bs))
	writeLSSBlock(wbuf, lssPageReloc, bs)

	pg.AddFlushRecord(offset, dataSz, numSegments)

	if ok = s.UpdateMapping(pid, pg, ctx); !ok {
		discardLSSBlock(wbuf)
		s.lss.FinalizeWrite(res)
		return false, 0
	}

	ctx.sts.Compacts++
	ctx.sts.FlushDataSz -= int64(staleFdSz)
	s.lss.FinalizeWrite(res)
	s.lssCleanerWriter.sts.FlushDataSz += int64(dataSz) - int64(staleSz)
	relocEnd := lssBlockEndOffset(offset, wbuf)
	s.trySMRObjects(ctx, lssCleanerSMRInterval)

	return true, relocEnd
}

func (s *Plasma) CleanLSS(proceed func() bool) error {
	var pg Page
	w := s.lssCleanerWriter
	relocBuf := w.GetBuffer(bufReloc)
	cleanerBuf := w.GetBuffer(bufCleaner)

	relocated := 0
	retries := 0
	skipped := 0

	callb := func(startOff, endOff LSSOffset, bs []byte) (cont bool, headOff LSSOffset, err error) {
		tok := w.BeginTx()
		defer w.EndTx(tok)

		typ := getLSSBlockType(bs)
		switch typ {
		case lssPageData, lssPageReloc:
			state, key := decodePageState(bs[lssBlockTypeSize:])
		retry:
			if pid := s.getPageId(key, w); pid != nil {
				if pg, err = s.ReadPage(pid, w.pgRdrFn, false, w); err != nil {
					return false, 0, err
				}

				if pg.NeedRemoval() {
					s.tryPageRemoval(pid, pg, w)
					goto retry
				}

				if pg.GetVersion() == state.GetVersion() || !pg.IsFlushed() {
					if ok, _ := s.tryPageRelocation(pid, pg, relocBuf, w); !ok {
						retries++
						goto retry
					}
					relocated++
				} else {
					allocs, _, _ := pg.GetMallocOps()
					s.discardDeltas(allocs)
					skipped++
				}
			}

			return proceed(), endOff, nil
		case lssRecoveryPoints:
			version, _ := unmarshalRPs(bs[lssBlockTypeSize:])
			s.mvcc.Lock()
			if s.rpVersion == version {
				s.updateRecoveryPoints(s.recoveryPoints)
			}
			s.mvcc.Unlock()
			return true, endOff, nil
		case lssDiscard, lssPageUpdate, lssPageRemove:
			return true, endOff, nil
		case lssMaxSn:
			maxSn := decodeMaxSn(bs[lssBlockTypeSize:])
			s.mvcc.Lock()
			if maxSn <= atomic.LoadUint64(&s.lastMaxSn) {
				s.updateMaxSn(atomic.LoadUint64(&s.currSn), true)
			}
			s.mvcc.Unlock()
		default:
			panic(fmt.Sprintf("unknown block typ %d", typ))
		}

		return true, endOff, nil
	}

	frag, ds, used := s.GetLSSInfo()
	start := s.lss.HeadOffset()
	end := s.lss.TailOffset()
	fmt.Printf("logCleaner: starting... frag %d, data: %d, used: %d log:(%d - %d)\n", frag, ds, used, start, end)
	err := s.lss.RunCleaner(callb, cleanerBuf)
	frag, ds, used = s.GetLSSInfo()
	start = s.lss.HeadOffset()
	end = s.lss.TailOffset()
	fmt.Printf("logCleaner: completed... frag %d, data: %d, used: %d, relocated: %d, retries: %d, skipped: %d log:(%d - %d)\n", frag, ds, used, relocated, retries, skipped, start, end)
	return err
}

func (s *Plasma) GetLSSInfo() (frag int, data int64, used int64) {
	frag = 0
	data = s.LSSDataSize()
	used = s.lss.UsedSpace()

	if used > 0 && data < used {
		frag = int((used - data) * 100 / used)
	}
	return
}

func (s *Plasma) lssCleanerDaemon() {
	shouldClean := func() bool {
		frag, _, _ := s.GetLSSInfo()
		return frag > s.Config.LSSCleanerThreshold
	}

loop:
	for {
		select {
		case <-s.stoplssgc:
			s.stoplssgc <- struct{}{}
			break loop
		default:
		}

		if shouldClean() {
			if err := s.CleanLSS(shouldClean); err != nil {
				fmt.Printf("logCleaner: failed (err=%v)\n", err)
			}
		}

		time.Sleep(time.Second)
	}
}
