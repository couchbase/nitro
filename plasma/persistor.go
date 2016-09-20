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
	lssPageAlloc
	lssPageDelete
	lssDiscard
)

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
			// discard lss space
			binary.BigEndian.PutUint16(wbuf[:lssBlockTypeSize], uint16(lssDiscard))
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
