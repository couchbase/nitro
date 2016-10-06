package plasma

import (
	"fmt"
	"sync/atomic"
	"time"
)

func (s *Plasma) tryPageRelocation(pid PageId, pg Page, buf []byte) bool {
	bs, dataSz, staleSz := pg.MarshalFull(buf)
	offset, wbuf, res := s.lss.ReserveSpace(lssBlockTypeSize + len(bs))
	offsetPtr := pg.(*page).addFlushDelta(dataSz, true)
	if !s.UpdateMapping(pid, pg) {
		discardLSSBlock(wbuf)
		s.lss.FinalizeWrite(res)
		return false
	}

	writeLSSBlock(wbuf, lssPageReloc, bs)
	s.lss.FinalizeWrite(res)
	*offsetPtr = offset
	s.lsscw.sts.FlushDataSz += int64(dataSz) - int64(staleSz)
	return true
}

func (s *Plasma) cleanLSS(proceed func() bool) error {
	w := s.lsscw
	buf := w.wCtx.pgEncBuf1

	callb := func(_ lssOffset, bs []byte) bool {
		typ := getLSSBlockType(bs)
		if typ == lssPageData || typ == lssPageReloc {
			version, key := decodeBlockMeta(bs[lssBlockTypeSize:])
		retry:
			node, _, found := s.Skiplist.Lookup(key, s.cmp, w.wCtx.buf, w.wCtx.slSts)
			if found {
				pid := PageId(node)
				pg := s.ReadPage(pid).(*page)
				if pg.version == version {
					if !s.tryPageRelocation(pid, pg, buf) {
						goto retry
					}
				}
			}

			return proceed()
		} else if typ == lssPageSplit || typ == lssPageMerge {
			return true
		}

		return true
	}

	frag, ds, used := s.GetLSSInfo()
	start := atomic.LoadInt64(&s.lss.headOffset)
	end := atomic.LoadInt64(&s.lss.tailOffset)
	fmt.Printf("logCleaner: starting... frag %d, data: %d, used: %d log:(%d - %d)\n", frag, ds, used, start, end)
	err := s.lss.RunCleaner(callb, buf)
	frag, ds, used = s.GetLSSInfo()
	start = atomic.LoadInt64(&s.lss.headOffset)
	end = atomic.LoadInt64(&s.lss.tailOffset)
	fmt.Printf("logCleaner: completed... frag %d, data: %d, used: %d log:(%d - %d)\n", frag, ds, used, start, end)
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
			if err := s.cleanLSS(shouldClean); err != nil {
				fmt.Printf("logCleaner: failed (err=%v)\n", err)
			}
		}

		time.Sleep(time.Second)
	}
}
