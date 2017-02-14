package plasma

import (
	"encoding/binary"
	"unsafe"
)

// TODO: Make failsafe buffer
var maxPageEncodedSize = 1024 * 1024 * 1

type lssBlockType uint16

var lssBlockTypeSize = int(unsafe.Sizeof(*(new(lssBlockType))))

const (
	lssPageData lssBlockType = iota
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
	buf := ctx.GetBuffer(0)
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
			ok = s.EvictPage(pid, pg, offset, ctx)
		} else {
			pg.AddFlushRecord(offset, dataSz, numSegments)
			ok = s.UpdateMapping(pid, pg, ctx)
		}

		if ok {
			s.lss.FinalizeWrite(res)
			ctx.sts.FlushDataSz += int64(dataSz) - int64(staleFdSz)
		} else {
			discardLSSBlock(wbuf)
			s.lss.FinalizeWrite(res)
			goto retry
		}
	} else if evict && pg.IsEvictable() {
		offset, _ := pg.GetLSSOffset()
		if !s.EvictPage(pid, pg, offset, ctx) {
			goto retry
		}
	}

	return pg
}

func (s *Plasma) PersistAll() {
	callb := func(pid PageId, partn RangePartition) error {
		s.Persist(pid, false, s.persistWriters[partn.Shard].wCtx)
		return nil
	}

	s.PageVisitor(callb, s.NumPersistorThreads)
	s.lss.Sync(false)
}

func (s *Plasma) EvictAll() {
	callb := func(pid PageId, partn RangePartition) error {
		s.Persist(pid, true, s.evictWriters[partn.Shard].wCtx)
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
