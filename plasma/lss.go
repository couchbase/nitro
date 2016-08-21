package plasma

import (
	"encoding/binary"
	"errors"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

type lssOffset uint64
type lssResource interface{}
type lssCleanerCallback func(lssOffset, []byte) bool

type LSS interface {
	ReserveSpace(size int) (lssOffset, []byte, lssResource)
	FinalizeWrite(lssResource)
	Read(lssOffset, buf []byte) (int, error)

	RunCleaner(callb lssCleanerCallback, buf []byte) error
}

type lsStore struct {
	w          *os.File
	r          *os.File
	maxSize    int64
	headOffset int64
	tailOffset int64

	head *flushBuffer

	bufSize   int
	flushBufs []*flushBuffer
	currBuf   int32

	sync.Mutex
}

func newLSStore(file string, maxSize int64, bufSize int, nbufs int) (*lsStore, error) {
	var err error

	s := &lsStore{
		maxSize:   maxSize,
		bufSize:   bufSize,
		flushBufs: make([]*flushBuffer, nbufs),
	}

	if s.w, err = os.OpenFile(file, os.O_WRONLY|os.O_CREATE, 0755); err != nil {
		return nil, err
	}

	if s.r, err = os.Open(file); err != nil {
		s.w.Close()
		return nil, err
	}

	for i, _ := range s.flushBufs {
		s.flushBufs[i] = newFlushBuffer(bufSize, s.flush)
		s.flushBufs[i].Reset()
	}

	s.head = s.flushBufs[0]
	return s, nil
}

func (s *lsStore) UsedSpace() int64 {
	return s.tailOffset - s.headOffset
}

func (s *lsStore) AvailableSpace() int64 {
	return s.maxSize - s.UsedSpace()
}

func (s *lsStore) flush(fb *flushBuffer) {
	fpos := fb.StartOffset() % s.maxSize
	size := fb.EndOffset() - fb.StartOffset()
	for {
		if size <= s.AvailableSpace() {
			break
		}

		runtime.Gosched()
	}

	if fpos+size > s.maxSize {
		bs := fb.Bytes()
		tailSz := s.maxSize - fpos
		s.w.WriteAt(bs[tailSz:], 0)
		s.w.WriteAt(bs[:tailSz], fpos)
	} else {
		s.w.WriteAt(fb.Bytes(), fpos)
	}
	atomic.StoreInt64(&s.tailOffset, fb.EndOffset())
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&s.head)),
		unsafe.Pointer(fb.NextBuffer()))

	fb.Reset()
}

func (s *lsStore) ReserveSpace(size int) (lssOffset, []byte, lssResource) {
retry:
	id := atomic.LoadInt32(&s.currBuf)
	fbid := int(id) % len(s.flushBufs)
	fb := s.flushBufs[fbid]
	success, markedFull, offset, buf := fb.Alloc(size)
	if !success {
		if markedFull {
			nextId := id + 1
			nextFbid := int(nextId) % len(s.flushBufs)
			nextFb := s.flushBufs[nextFbid]
			for nextFb.IsFull() {
				runtime.Gosched()
			}

			nextFb.Init(fb, fb.EndOffset())

			if !atomic.CompareAndSwapInt32(&s.currBuf, id, nextId) {
				panic("should not happen")
			}

		} else {
			runtime.Gosched()
			goto retry
		}
	}

	return lssOffset(offset), buf, lssResource(fb)
}

func (s *lsStore) readBlock(off int64, buf []byte) error {
	fpos := off % s.maxSize

	if fpos+int64(len(buf)) > s.maxSize {
		tailSz := s.maxSize - fpos
		if _, err := s.r.ReadAt(buf[:tailSz], fpos); err != nil {
			return err
		}

		if _, err := s.r.ReadAt(buf[tailSz:], 0); err != nil {
			return err
		}
	} else {
		if _, err := s.r.ReadAt(buf, fpos); err != nil {
			return err
		}
	}

	return nil
}

func (s *lsStore) Read(lssOf lssOffset, buf []byte) (int, error) {
	offset := int64(lssOf)
retry:
	tail := atomic.LoadInt64(&s.tailOffset)

	// It's in the flush buffers
	if offset >= tail {
		id := atomic.LoadInt32(&s.currBuf)
		fbid := int(id) % len(s.flushBufs)
		end := s.flushBufs[fbid]
		start := s.head

		startOffset := start.StartOffset()
		endOffset := end.EndOffset()

		if startOffset < endOffset && offset >= startOffset && offset < endOffset {
		loop:
			for fb := start; fb != nil; fb = fb.NextBuffer() {
				if n, err := fb.Read(offset, buf); err == nil {
					return n, nil
				}

				if fb == end {
					break loop
				}
			}
		}
		goto retry
	}

	fpos := int64(offset) % s.maxSize
	if fpos+headerFBSize > s.maxSize {
		tailSz := s.maxSize - fpos
		if _, err := s.r.ReadAt(buf[:tailSz], fpos); err != nil {
			return 0, err
		}

		fpos = 0
		if _, err := s.r.ReadAt(buf[tailSz:headerFBSize], fpos); err != nil {
			return 0, err
		}
		fpos += headerFBSize - tailSz
	} else {
		if _, err := s.r.ReadAt(buf[:headerFBSize], fpos); err != nil {
			return 0, err
		}
		fpos += headerFBSize
	}

	if err := s.readBlock(offset, buf[:headerFBSize]); err != nil {
		return 0, err
	}

	l := int(binary.BigEndian.Uint32(buf[:headerFBSize]))
	err := s.readBlock(offset+headerFBSize, buf[:l])
	return l, err
}

func (s *lsStore) FinalizeWrite(res lssResource) {
	fb := res.(*flushBuffer)
	fb.Done()
}

func (s *lsStore) RunCleaner(callb lssCleanerCallback, buf []byte) error {
	s.Lock()
	defer s.Unlock()

	curr := atomic.LoadInt64(&s.headOffset)
	end := atomic.LoadInt64(&s.tailOffset)

	for {
		n, err := s.Read(lssOffset(curr), buf)
		if err != nil {
			return err
		}

		if !callb(lssOffset(curr), buf[:n]) {
			break
		}

		curr += int64(n + headerFBSize)
		atomic.StoreInt64(&s.headOffset, curr)
		if end == curr {
			break
		}
	}

	return nil
}

var errFBReadFailed = errors.New("flushBuffer read failed")

type flushCallback func(fb *flushBuffer)

const headerFBSize = 4

type flushBuffer struct {
	baseOffset int64
	b          []byte
	child      *flushBuffer
	state      uint64
	callb      flushCallback
}

func newFlushBuffer(sz int, callb flushCallback) *flushBuffer {
	return &flushBuffer{
		b:     make([]byte, sz),
		callb: callb,
	}
}

func (fb *flushBuffer) Bytes() []byte {
	_, _, offset := decodeState(fb.state)
	return fb.b[:offset]
}

func (fb *flushBuffer) StartOffset() int64 {
	return fb.baseOffset
}

func (fb *flushBuffer) EndOffset() int64 {
	_, _, offset := decodeState(fb.state)
	return fb.baseOffset + int64(offset)
}

func (fb *flushBuffer) NextBuffer() *flushBuffer {
	return fb.child
}

func (fb *flushBuffer) Read(off int64, buf []byte) (int, error) {
	base := fb.baseOffset
	start := int(off - base)
	_, _, offset := decodeState(fb.state)

	if base != fb.baseOffset || !(base <= off && off < base+int64(offset)) {
		return 0, errFBReadFailed
	}

	payloadOffset := start + headerFBSize
	l := int(binary.BigEndian.Uint32(fb.b[start:payloadOffset]))
	if payloadOffset+l > len(fb.b) {
		return 0, errFBReadFailed
	}

	copy(buf, fb.b[payloadOffset:payloadOffset+l])
	if base == fb.baseOffset {
		return l, nil
	}

	return 0, errFBReadFailed
}

func (fb *flushBuffer) Init(parent *flushBuffer, baseOffset int64) {
	fb.baseOffset = baseOffset
	// 1 writer rc for parent to enforce ordering of flush callback
	// 1 writer rc for parent to call flush callback if writers have already
	// terminated while initialization of next buffer.
	if parent != nil {
		fb.state = encodeState(false, 2, 0)
		parent.child = fb

		// If parent is already full and writers have completed operations,
		// this would trigger flush callback.
		parent.Done()
	}

}

func (fb *flushBuffer) Alloc(size int) (status bool, markedFull bool, off int64, buf []byte) {
retry:
	state := atomic.LoadUint64(&fb.state)
	isfull, nw, offset := decodeState(state)
	if isfull {
		return false, false, 0, nil
	}

	newOffset := headerFBSize + offset + size
	if newOffset > len(fb.b) {
		markedFull := true
		newState := encodeState(true, nw, offset)
		if !atomic.CompareAndSwapUint64(&fb.state, state, newState) {
			goto retry
		}
		return false, markedFull, 0, nil
	}

	newState := encodeState(false, nw+1, newOffset)
	if !atomic.CompareAndSwapUint64(&fb.state, state, newState) {
		goto retry
	}

	binary.BigEndian.PutUint32(fb.b[offset:offset+headerFBSize], uint32(size))
	return true, false, fb.baseOffset + int64(offset), fb.b[offset+headerFBSize : newOffset]
}

func (fb *flushBuffer) Done() {
retry:
	state := atomic.LoadUint64(&fb.state)
	isfull, nw, offset := decodeState(state)

	newState := encodeState(isfull, nw-1, offset)
	if !atomic.CompareAndSwapUint64(&fb.state, state, newState) {
		goto retry
	}

	if nw == 1 && isfull {
		fb.callb(fb)
	}

	if fb.child != nil {
		fb.child.Done()
	}
}

func (fb *flushBuffer) IsFull() bool {
	state := atomic.LoadUint64(&fb.state)
	isfull, _, _ := decodeState(state)
	return isfull
}

func (fb *flushBuffer) Reset() {
	fb.baseOffset = 0
	fb.state = encodeState(false, 1, 0)
	fb.child = nil
}

func decodeState(state uint64) (bool, int, int) {
	isfull := state&0x1 == 0x1           // 1 bit
	nwriters := int(state >> 1 & 0xffff) // 32 bit
	offset := int(state >> 33)           // remaining bits

	return isfull, nwriters, offset
}

func encodeState(isfull bool, nwriters int, offset int) uint64 {
	var isfullbits, nwritersbits, offsetbits uint64

	if isfull {
		isfullbits = 1
	}

	nwritersbits = uint64(nwriters) << 1
	offsetbits = uint64(offset) << 33

	state := isfullbits | nwritersbits | offsetbits
	return state
}
