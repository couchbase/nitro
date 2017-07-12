// Copyright (c) 2017 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package plasma

import (
	"fmt"
	"sync/atomic"
	"time"
)

func (s *Plasma) tryPageRelocation(pid PageId, pg Page, buf *Buffer, ctx *wCtx) (bool, LSSOffset) {
	var ok bool
	var staleCompactFdSz int

	// Do not compete with compactions ran by main writer threads
	// Log cleaner should compact only when deltalen exceeds threshold+2
	if pg.NeedCompaction(s.Config.MaxDeltaChainLen + 2) {
		staleCompactFdSz = pg.Compact()
	}

	bs, dataSz, staleSz, numSegments := pg.Marshal(buf, FullMarshal)
	offset, wbuf, res := s.lss.ReserveSpace(lssBlockTypeSize + len(bs))
	writeLSSBlock(wbuf, lssPageReloc, bs)

	pg.AddFlushRecord(offset, dataSz, numSegments)

	if ok = s.UpdateMapping(pid, pg, ctx); !ok {
		discardLSSBlock(wbuf)
		s.lss.FinalizeWrite(res)
		return false, 0
	}

	s.lss.FinalizeWrite(res)
	s.lssCleanerWriter.sts.FlushDataSz += int64(dataSz) - int64(staleSz) - int64(staleCompactFdSz)
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
		tok := w.BeginTxNonThrottle()
		defer w.EndTx(tok)

		w.sts.LSSReadBytes += int64(len(bs))
		w.sts.NumLSSReads += 1

		typ := getLSSBlockType(bs)
		switch typ {
		case lssPageData, lssPageReloc:
			data := bs[lssBlockTypeSize:]
			data = w.decompress(data, w.GetBuffer(bufDecompress))
			state, key := decodePageState(data)
		retry:
			if pid := s.getPageId(key, w); pid != nil {
				if pg, err = s.ReadPage(pid, false, w); err != nil {
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
	s.logInfo(fmt.Sprintf("logCleaner: starting... frag %d, data: %d, used: %d log:(%d - %d)", frag, ds, used, start, end))
	err := s.lss.RunCleaner(callb, cleanerBuf)
	frag, ds, used = s.GetLSSInfo()
	start = s.lss.HeadOffset()
	end = s.lss.TailOffset()
	s.logInfo(fmt.Sprintf("logCleaner: completed... frag %d, data: %d, used: %d, relocated: %d, retries: %d, skipped: %d log:(%d - %d)", frag, ds, used, relocated, retries, skipped, start, end))
	return err
}

func (s *Plasma) GetLSSInfo() (frag int, data int64, used int64) {
	frag = 0
	data = s.LSSDataSize()
	used = s.lss.UsedSpace()

	if used > 0 && data > 0 && data < used {
		frag = int((used - data) * 100 / used)
	}
	return
}

func (s *Plasma) TriggerLSSCleaner(minFrag int, minSize int64) bool {
	frag, _, used := s.GetLSSInfo()
	return frag > 0 && used > minSize && frag > minFrag
}

func (s *Plasma) lssCleanerDaemon() {
	shouldClean := func() bool {
		return s.TriggerLSSCleaner(s.Config.LSSCleanerThreshold, s.Config.LSSCleanerMinSize)
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
				s.logInfo(fmt.Sprintf("logCleaner: failed (err=%v)", err))
			}
		}

		time.Sleep(time.Second)
	}
}
