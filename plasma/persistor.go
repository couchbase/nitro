package plasma

var maxPageEncodedSize = 1024 * 1024 * 10

func (s *Plasma) PersistAll() {
	buf := make([]byte, maxPageEncodedSize)

	pid := s.StartPageId()
	for pid != nil {
	retry:
		pg := s.ReadPage(pid).(*page)
		bs := pg.Marshal(buf)
		offset, wbuf, res := s.lss.ReserveSpace(len(bs))
		offsetPtr := pg.addFlushDelta()
		if !s.UpdateMapping(pid, pg) {
			// discard lss space
			s.lss.FinalizeWrite(res)
			goto retry
		}

		copy(wbuf, bs)
		s.lss.FinalizeWrite(res)
		*offsetPtr = offset

		pid = pg.head.rightSibling
	}

	s.lss.Sync()
}
