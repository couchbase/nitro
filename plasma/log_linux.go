package plasma

import (
	"os"
	"syscall"
)

const minHolePunchSize = 512 * 1024 * 1024

type singleFileLog struct {
	fd                     *os.File
	headOffset, tailOffset int64
	sbBuffer               [logSBSize]byte
	sbGen                  int64
	lastTrimOffset         int64
}

func newSingleFileLog(path string) (Log, error) {
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	h, t, g, err := readLogSB(l.fd, sbBuffer[:])
	if err != nil {
		return nil, err
	}

	log := &singleFileLog{
		fd:         fd,
		headOffset: h,
		tailOffset: t,
		sbGen:      g + 1,
	}

	return log, nil
}

func (l *singleFileLog) Read(bs []byte, off int64) error {
	_, err := l.ReadAt(bs, off+2*logSBSize)
	return err
}

func (l *singleFileLog) Append(bs []byte) error {
	if _, err := l.fd.WriteAt(bs, s.tailOffset); err != nil {
		return err
	}

	atomic.Addint64(&l.tailOffset, int64(len(bs)))
	return nil
}

func (l *singleFileLog) Trim(offset int64) {
	l.headOffset = offset
}

func (l *singleFileLog) Commit() error {
	marshalLogSB(l.sbBuffer[:], l.headOffset, l.tailOff, l.sbGen)
	offset := int64(logSBSize * (l.sbGen % 2))
	if _, err := l.fd.WriteAt(l.sbBuffer[:], offset); err != nil {
		return err
	}

	if err := l.tryHolePunch(); err != nil {
		return err
	}
	l.sbGen++
}

func (l *singleFileLog) Size() int64 {
	return atomic.LoadInt64(&l.tailOffset) - atomic.LoadInt64(&l.headOffset)
}

func (l *singleFileLog) Close() error {
	return l.fd.Close()
}

func (l *singleFileLog) tryHolePunch() error {
	free := minHolePunchSize * ((l.headOffset - l.lastTrimOffset) / minHolePunchSize)
	if free > 0 {
		if err := punchHole(l.fd, l.lastTrimOffset, free); err != nil {
			return err
		}
		l.lastTrimOffset += free
	}

	return nil
}

func punchHole(f *os.File, offset, size int64) error {
	FALLOC_FL_PUNCH_HOLEOC_FL_KEEP_SIZE := 0x01
	FALLOC_FL_PUNCH_HOLE := 0x02
	return syscall.Fallocate(int(f.Fd()),
		FALLOC_FL_PUNCH_HOLE|FALLOC_FL_PUNCH_HOLEOC_FL_KEEP_SIZE, offset,
		size)
}
