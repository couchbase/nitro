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
	lssPageSplit
	lssPageMerge
	lssDiscard
)

func discardLSSBlock(wbuf []byte) {
	binary.BigEndian.PutUint16(wbuf[:lssBlockTypeSize], uint16(lssDiscard))
}

func writeLSSBlock(wbuf []byte, typ lssBlockType, bs []byte) {
	copy(wbuf[lssBlockTypeSize:], bs)
	if typ != lssPageData {
	}
}

func (s *Plasma) PersistAll() {
	buf := make([]byte, maxPageEncodedSize)

	pid := s.StartPageId()
	for pid != nil {
	retry:
		pg := s.ReadPage(pid).(*page)
		bs := pg.Marshal(buf)
		offset, wbuf, res := s.lss.ReserveSpace(lssBlockTypeSize + len(bs))
		offsetPtr := pg.addFlushDelta()
		if !s.UpdateMapping(pid, pg) {
			discardLSSBlock(wbuf)
			s.lss.FinalizeWrite(res)
			goto retry
		}

		copy(wbuf[lssBlockTypeSize:], bs)
		s.lss.FinalizeWrite(res)
		*offsetPtr = offset

		pid = pg.head.rightSibling
	}

	s.lss.Sync()
}
