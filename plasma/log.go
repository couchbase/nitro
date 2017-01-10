package plasma

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"unsafe"
)

const (
	logSBSize  = 4096
	logVersion = 0
)

var segFileNameFormat = "log.%014d.data"
var segFilePattern = "log.*.data"
var segFileIdPattern = "log.%d.data"
var headerFileName = "header.data"
var ErrLogSuperBlockCorrupt = fmt.Errorf("Log superblock is corrupt")

type Log interface {
	Head() int64
	Tail() int64
	Read([]byte, int64) error
	Append([]byte) error
	Trim(offset int64)
	Commit() error
	Size() int64
	Close() error
}

type fileIndex struct {
	startOffset int64
	endOffset   int64
	index       []*os.File
	w           *os.File
}

type multiFilelog struct {
	sbBuffer [logSBSize]byte
	sbGen    int64
	sbFd     *os.File

	basePath    string
	segmentSize int64

	headOffset int64
	tailOffset int64

	index *fileIndex
}

func newLog(path string, segmentSize int64) (Log, error) {
	var sbBuffer [logSBSize]byte
	os.MkdirAll(path, 0755)
	headerFile := filepath.Join(path, headerFileName)
	fd, err := os.OpenFile(headerFile, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	h, t, g, err := readLogSB(fd, sbBuffer[:])
	if err != nil {
		return nil, err
	}

	log := &multiFilelog{
		segmentSize: segmentSize,
		sbBuffer:    sbBuffer,
		sbGen:       g + 1,
		sbFd:        fd,
		basePath:    path,
		headOffset:  h,
		tailOffset:  t,
	}

	if err := log.initIndex(); err != nil {
		return nil, err
	}

	return log, err
}

func (l *multiFilelog) initIndex() error {
	fi := new(fileIndex)
	files, _ := filepath.Glob(filepath.Join(l.basePath, segFilePattern))
	if len(files) > 0 {
		var startId, endId int64
		startFile := filepath.Base(files[0])
		endFile := filepath.Base(files[len(files)-1])
		fmt.Sscanf(startFile, segFileIdPattern, &startId)
		fmt.Sscanf(endFile, segFileIdPattern, &endId)
		fi.startOffset = startId * l.segmentSize
		fi.endOffset = endId*l.segmentSize + l.segmentSize
	}

	for i, f := range files {
		if fd, err := os.OpenFile(f, os.O_RDWR, 0755); err == nil {
			fi.index = append(fi.index, fd)
			if i == len(files)-1 {
				fi.w = fd
			}
		} else {
			for _, fd := range fi.index {
				fd.Close()
			}
			return err
		}
	}

	l.index = fi
	return nil
}

func (l *multiFilelog) Read(bs []byte, off int64) error {
	idx := l.getIndex()

retry:
	if off >= idx.endOffset {
		return fmt.Errorf("Log size is smaller than offset (%d < %d)", idx.endOffset, off)
	}

	if off < idx.startOffset {
		return fmt.Errorf("Log starts at offset %d, trying to read %d", idx.startOffset, off)
	}

	fdIdx := (off - idx.startOffset) / l.segmentSize
	fdOffset := off % l.segmentSize

	var residue []byte
	avail := l.segmentSize - fdOffset
	if avail < int64(len(bs)) {
		residue = bs[avail:]
		bs = bs[:avail]
	}

	if _, err := idx.index[fdIdx].ReadAt(bs, fdOffset); err != nil {
		return err
	}

	if len(residue) > 0 {
		off += int64(len(bs))
		bs = residue
		goto retry
	}

	return nil
}

func (l *multiFilelog) Head() int64 {
	return atomic.LoadInt64(&l.headOffset)
}

func (l *multiFilelog) Tail() int64 {
	return atomic.LoadInt64(&l.tailOffset)
}

func (l *multiFilelog) getIndex() *fileIndex {
	return (*fileIndex)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.index))))
}

func (l *multiFilelog) growLog() error {
	idx := l.getIndex()
	newFileId := (idx.endOffset + 1) / l.segmentSize
	file := filepath.Join(l.basePath, fmt.Sprintf(segFileNameFormat, newFileId))
	fd, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	idx.w.Sync()

	newIdx := *idx
	newIdx.index = append(newIdx.index, fd)
	newIdx.w = fd
	newIdx.endOffset += l.segmentSize

	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&l.index)), unsafe.Pointer(&newIdx))
	return nil
}

func (l *multiFilelog) Append(bs []byte) error {
	wsize := int64(len(bs))
	tail := l.tailOffset
retry:
	idx := l.getIndex()
	avail := idx.endOffset - tail
	bsSize := int64(len(bs))

	var residue []byte
	if bsSize > avail {
		residue = bs[avail:]
		bs = bs[:avail]
	}

	if wl := int64(len(bs)); wl > 0 {
		woffset := tail % l.segmentSize
		if _, err := idx.w.WriteAt(bs, woffset); err != nil {
			return err
		}

		tail += wl
	}

	if len(residue) > 0 {
		l.growLog()
		bs = residue
		goto retry
	}

	atomic.AddInt64(&l.tailOffset, wsize)
	return nil
}

func (l *multiFilelog) Trim(offset int64) {
	if offset > 0 {
		atomic.StoreInt64(&l.headOffset, offset)
	}
}

func (l *multiFilelog) doGCSegments() {
	idx := l.getIndex()
	free := (l.headOffset/l.segmentSize)*l.segmentSize - idx.startOffset
	if free > 0 {
		n := free / l.segmentSize
		toRemove := idx.index[:n]
		var rmList []string
		for _, fd := range toRemove {
			rmList = append(rmList, fd.Name())
			fd.Close()
		}
		toRetain := append([]*os.File(nil), idx.index[n:]...)

		newIdx := *idx
		newIdx.startOffset += free
		newIdx.index = toRetain

		// TODO: Make async cleanup
		func() {
			for _, f := range rmList {
				os.Remove(f)
			}
		}()

		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&l.index)), unsafe.Pointer(&newIdx))
	}
}

func (l *multiFilelog) Commit() error {
	idx := l.getIndex()
	if err := idx.w.Sync(); err != nil {
		return err
	}

	marshalLogSB(l.sbBuffer[:], l.Head(), l.Tail(), l.sbGen)
	offset := int64(logSBSize * (l.sbGen % 2))
	if _, err := l.sbFd.WriteAt(l.sbBuffer[:], offset); err != nil {
		return err
	}

	l.sbFd.Sync()
	l.sbGen++
	l.doGCSegments()
	return nil
}

func (l *multiFilelog) Size() int64 {
	return l.Tail() - l.Head()
}

func (l *multiFilelog) Close() error {
	idx := l.getIndex()
	for _, fd := range idx.index {
		fd.Close()
	}

	idx.index = nil
	return nil
}

func marshalLogSB(buf []byte, headOffset, tailOffset int64, gen int64) {
	woffset := 4
	binary.BigEndian.PutUint32(buf[woffset:woffset+4], uint32(logVersion))
	woffset += 4

	binary.BigEndian.PutUint64(buf[woffset:woffset+8], uint64(gen))
	woffset += 8

	binary.BigEndian.PutUint64(buf[woffset:woffset+8], uint64(headOffset))
	woffset += 8

	binary.BigEndian.PutUint64(buf[woffset:woffset+8], uint64(tailOffset))
	woffset += 8

	hash := crc32.ChecksumIEEE(buf[4:logSBSize])
	binary.BigEndian.PutUint32(buf[0:4], hash)
}

func unmarshalLogSB(buf []byte) (headOffset, tailOffset int64, gen int64, err error) {
	hash := binary.BigEndian.Uint32(buf[0:4])
	computedHash := crc32.ChecksumIEEE(buf[4:logSBSize])
	if hash != computedHash {
		err = ErrCorruptSuperBlock
		return
	}

	roffset := 8
	gen = int64(binary.BigEndian.Uint64(buf[roffset : roffset+8]))
	roffset += 8
	headOffset = int64(binary.BigEndian.Uint64(buf[roffset : roffset+8]))
	roffset += 8
	tailOffset = int64(binary.BigEndian.Uint64(buf[roffset : roffset+8]))
	roffset += 8
	return
}

func readLogSB(fd *os.File, buf []byte) (headOff, tailOff, gen int64, err error) {
	var hs, ts, gens [2]int64
	var errs [2]error

	if _, err = fd.ReadAt(buf, 0); err == io.EOF {
		return 0, 0, 0, nil
	} else if err != nil {
		return
	}

	hs[0], ts[0], gens[0], errs[0] = unmarshalLogSB(buf)

	if _, err = fd.ReadAt(buf, logSBSize); err == io.EOF {
		return hs[0], ts[0], gens[0], errs[0]
	} else if err != nil {
		return
	}

	hs[1], ts[1], gens[1], errs[1] = unmarshalLogSB(buf)

	var sbIndex int
	if errs[0] == nil && errs[1] == nil {
		if gens[0] < gens[1] {
			sbIndex = 1
		} else {
			sbIndex = 0
		}
	} else if errs[0] == nil {
		sbIndex = 0
	} else if errs[1] == nil {
		sbIndex = 1
	} else {
		err = ErrLogSuperBlockCorrupt
		return
	}

	return hs[sbIndex], ts[sbIndex], gens[sbIndex] + 1, nil
}
