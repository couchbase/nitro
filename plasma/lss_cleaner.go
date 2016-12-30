package plasma

import (
	"fmt"
	"sync/atomic"
	"time"
)

func (s *Plasma) tryPageRelocation(pid PageId, pg Page, buf []byte) (bool, lssOffset) {
	var ok bool
	bs, dataSz, staleSz := pg.MarshalFull(buf)
	offset, wbuf, res := s.lss.ReserveSpace(lssBlockTypeSize + len(bs))
	pg.AddFlushRecord(offset, dataSz, true)
	writeLSSBlock(wbuf, lssPageReloc, bs)

	if pg.IsInCache() {
		ok = s.UpdateMapping(pid, pg)
	} else {
		ok = s.EvictPage(pid, pg, offset)
	}

	if !ok {
		discardLSSBlock(wbuf)
		s.lss.FinalizeWrite(res)
		return false, 0
	}

	s.lss.FinalizeWrite(res)
	s.lssCleanerWriter.sts.FlushDataSz += int64(dataSz) - int64(staleSz)
	relocEnd := lssBlockEndOffset(offset, wbuf)
	return true, relocEnd
}

func (s *Plasma) CleanLSS(proceed func() bool) error {
	var pg Page
	w := s.lssCleanerWriter
	relocBuf := w.wCtx.GetBuffer(0)
	cleanerBuf := w.wCtx.GetBuffer(1)

	relocated := 0
	retries := 0
	skipped := 0

	callb := func(startOff, endOff lssOffset, bs []byte) (cont bool, headOff lssOffset, err error) {
		typ := getLSSBlockType(bs)
		switch typ {
		case lssPageData, lssPageReloc:
			state, key := decodePageState(bs[lssBlockTypeSize:])
		retry:
			if pid := s.getPageId(key, w.wCtx); pid != nil {
				if pg, err = s.ReadPage(pid, w.wCtx.pgRdrFn, false); err != nil {
					return false, 0, err
				}

				if pg.NeedRemoval() {
					s.tryPageRemoval(pid, pg, w.wCtx)
					goto retry
				}

				if pg.GetVersion() == state.GetVersion() || !pg.IsFlushed() {
					if ok, _ := s.tryPageRelocation(pid, pg, relocBuf); !ok {
						retries++
						goto retry
					}
					relocated++
				} else {
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
	start := s.lss.log.Head()
	end := s.lss.log.Tail()
	fmt.Printf("logCleaner: starting... frag %d, data: %d, used: %d log:(%d - %d)\n", frag, ds, used, start, end)
	err := s.lss.RunCleaner(callb, cleanerBuf)
	frag, ds, used = s.GetLSSInfo()
	start = s.lss.log.Head()
	end = s.lss.log.Tail()
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
