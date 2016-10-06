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

func (s *Plasma) PersistAll() {
	buf := s.pw.pgEncBuf1

	pid := s.StartPageId()
	for pid != nil {
	retry:
		pg := s.ReadPage(pid).(*page)
		bs, dataSz := pg.Marshal(buf)
		offset, wbuf, res := s.lss.ReserveSpace(lssBlockTypeSize + len(bs))
		offsetPtr := pg.addFlushDelta(dataSz, false)
		if !s.UpdateMapping(pid, pg) {
			discardLSSBlock(wbuf)
			s.lss.FinalizeWrite(res)
			goto retry
		}

		writeLSSBlock(wbuf, lssPageData, bs)
		s.lss.FinalizeWrite(res)
		*offsetPtr = offset
		s.pw.sts.FlushDataSz += int64(dataSz)

		pid = pg.head.rightSibling
	}

	s.lss.Sync()
}
