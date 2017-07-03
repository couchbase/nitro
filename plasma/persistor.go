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
	"encoding/binary"
	"unsafe"
)

var maxPageEncodedSize = 1024 * 4

type lssBlockType uint16

var lssBlockTypeSize = int(unsafe.Sizeof(*(new(lssBlockType))))

const (
	lssPageData lssBlockType = iota + 1
	lssPageReloc
	lssPageUpdate
	lssPageRemove
	lssRecoveryPoints
	lssMaxSn
	lssDiscard
)

func discardLSSBlock(wbuf []byte) {
	binary.BigEndian.PutUint16(wbuf[:lssBlockTypeSize], uint16(lssDiscard))
}

func writeLSSBlock(wbuf []byte, typ lssBlockType, bs []byte) {
	copy(wbuf[lssBlockTypeSize:], bs)
	binary.BigEndian.PutUint16(wbuf[:lssBlockTypeSize], uint16(typ))
}

func getLSSBlockType(bs []byte) lssBlockType {
	return lssBlockType(binary.BigEndian.Uint16(bs))
}

func (s *Plasma) Persist(pid PageId, evict bool, ctx *wCtx) Page {
	ctx.HoldLSS()
	defer ctx.UnHoldLSS()

	buf := ctx.GetBuffer(bufPersist)
retry:

	// Never read from lss
	pg, _ := s.ReadPage(pid, nil, false, ctx)
	if pg.NeedsFlush() {
		bs, dataSz, staleFdSz, numSegments := pg.Marshal(buf, s.Config.MaxPageLSSSegments)
		offset, wbuf, res := s.lss.ReserveSpace(lssBlockTypeSize + len(bs))
		typ := pgFlushLSSType(pg, numSegments)
		writeLSSBlock(wbuf, typ, bs)

		var ok bool
		if evict {
			pg.Evict(offset, numSegments)
		} else {
			pg.AddFlushRecord(offset, dataSz, numSegments)
		}

		if ok = s.UpdateMapping(pid, pg, ctx); ok {
			s.lss.FinalizeWrite(res)
			ctx.sts.FlushDataSz += int64(dataSz) - int64(staleFdSz)
		} else {
			discardLSSBlock(wbuf)
			s.lss.FinalizeWrite(res)
			goto retry
		}
	} else if evict && pg.IsEvictable() {
		offset, numSegs, _ := pg.GetFlushInfo()
		pg.Evict(offset, numSegs)
		if !s.UpdateMapping(pid, pg, ctx) {
			goto retry
		}
	}

	return pg
}

func (s *Plasma) PersistAll() {
	callb := func(pid PageId, partn RangePartition) error {
		s.Persist(pid, false, s.persistWriters[partn.Shard])
		return nil
	}

	s.PageVisitor(callb, s.NumPersistorThreads)
	s.lss.Sync(false)
}

func (s *Plasma) EvictAll() {
	callb := func(pid PageId, partn RangePartition) error {
		s.Persist(pid, true, s.evictWriters[partn.Shard])
		return nil
	}

	s.PageVisitor(callb, s.NumPersistorThreads)
}

func pgFlushLSSType(pg Page, numSegments int) lssBlockType {
	if numSegments > 0 {
		return lssPageUpdate
	}

	return lssPageData
}
