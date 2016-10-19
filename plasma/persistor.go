package plasma

import (
	"encoding/binary"
	"unsafe"
)

var maxPageEncodedSize = 1024 * 1024 * 10

type lssBlockType uint16

var lssBlockTypeSize = int(unsafe.Sizeof(*(new(lssBlockType))))

const (
	lssPageData lssBlockType = iota
	lssPageReloc
	lssPageSplit
	lssPageMerge
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

func (s *Plasma) Persist(pid PageId, evict bool, w *Writer) Page {
	buf := w.pgEncBuf1
retry:

	// Never read from lss
	pg, _ := s.ReadPage(pid, nil, false)
	if pg.NeedsFlush() {
		bs, dataSz := pg.Marshal(buf)
		offset, wbuf, res := s.lss.ReserveSpace(lssBlockTypeSize + len(bs))
		writeLSSBlock(wbuf, lssPageData, bs)

		var ok bool
		if evict {
			ok = s.EvictPage(pid, pg, offset)
		} else {
			pg.AddFlushRecord(offset, dataSz, false)
			ok = s.UpdateMapping(pid, pg)
		}

		if ok {
			s.lss.FinalizeWrite(res)
			w.sts.FlushDataSz += int64(dataSz)
		} else {
			discardLSSBlock(wbuf)
			s.lss.FinalizeWrite(res)
			goto retry
		}
	} else if evict && !pg.IsEmpty() {
		offset := pg.GetLSSOffset()
		if !s.EvictPage(pid, pg, offset) {
			goto retry
		}
	}

	return pg
}

func (s *Plasma) PersistAll() {
	pid := s.StartPageId()
	for pid != nil {
		pg := s.Persist(pid, false, s.pw)
		pid = pg.Next()
	}

	s.lss.Sync()
}

func (s *Plasma) EvictAll() {
	pid := s.StartPageId()
	for pid != nil {
		pg := s.Persist(pid, true, s.pw)
		pid = pg.Next()
	}
}
