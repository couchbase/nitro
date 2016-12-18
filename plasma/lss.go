package plasma

import (
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const lssVersion = 0
const headerSize = superBlockSize * 2
const superBlockSize = 4096
const lssReclaimBlockSize = 1024 * 1024 * 8

var ErrCorruptSuperBlock = errors.New("Superblock is corrupted")

type lssOffset uint64
type lssResource interface{}
type lssBlockCallback func(lssOffset, []byte) (bool, error)
type lssCleanerCallback func(start, end lssOffset, bs []byte) (cont bool, cleanOff lssOffset, err error)

type LSS interface {
	ReserveSpace(size int) (lssOffset, []byte, lssResource)
	ReserveSpaceMulti(sizes []int) ([]lssOffset, [][]byte, lssResource)
	FinalizeWrite(lssResource)
	TrimLog(lssOffset)
	Read(lssOffset, buf []byte) (int, error)
	Sync()

	RunCleaner(callb lssCleanerCallback, buf []byte) error
}

type lsStore struct {
	trimBatchSize int64

	startOffset int64

	cleanerTrimOffset lssOffset

	head *flushBuffer

	bufSize   int
	flushBufs []*flushBuffer
	currBuf   int32

	sbBuffer [superBlockSize]byte

	sync.Mutex

	path        string
	segmentSize int64
	log         Log
}

func newLSStore(path string, segSize int64, bufSize int, nbufs int) (*lsStore, error) {
	var err error

	s := &lsStore{
		path:          path,
		segmentSize:   segSize,
		bufSize:       bufSize,
		trimBatchSize: int64(bufSize),
		flushBufs:     make([]*flushBuffer, nbufs),
	}

	if s.log, err = newLog(path, segSize); err != nil {
		return nil, err
	}

	for i, _ := range s.flushBufs {
		s.flushBufs[i] = newFlushBuffer(bufSize, s.flush)
		s.flushBufs[i].Reset()
	}

	s.head = s.flushBufs[0]
	s.head.baseOffset = s.log.Tail()
	s.startOffset = s.log.Head()

	return s, nil
}

func (s *lsStore) Close() {
	s.log.Close()
}

func (s *lsStore) UsedSpace() int64 {
	return s.log.Size()
}

func (s *lsStore) flush(fb *flushBuffer) {
	for {
		err := s.log.Append(fb.Bytes())
		if err == nil {
			break
		}

		fmt.Printf("Plasma: (%s) Unable to write - err %v\n", s.path, err)
		time.Sleep(time.Second)
	}

	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&s.head)),
		unsafe.Pointer(fb.NextBuffer()))

	if trimOffset, doTrim := fb.GetTrimLogOffset(); doTrim {
		s.log.Trim(int64(trimOffset))
	}

	s.log.Commit()
	fb.Reset()
}

func (s *lsStore) initNextBuffer(id int32, fb *flushBuffer) {
	nextId := id + 1
	nextFbid := int(nextId) % len(s.flushBufs)
	nextFb := s.flushBufs[nextFbid]

	for !nextFb.IsReset() {
		runtime.Gosched()
	}

	nextFb.Init(fb, fb.EndOffset())

	if !atomic.CompareAndSwapInt32(&s.currBuf, id, nextId) {
		panic("should not happen")
	}
}

func (s *lsStore) TrimLog(off lssOffset) {
retry:
	id := atomic.LoadInt32(&s.currBuf)
	fbid := int(id) % len(s.flushBufs)
	fb := s.flushBufs[fbid]

	if !fb.SetTrimLogOffset(off) {
		goto retry
	}
}

func (s *lsStore) ReserveSpace(size int) (lssOffset, []byte, lssResource) {
	offs, bs, res := s.ReserveSpaceMulti([]int{size})
	return offs[0], bs[0], res
}

func (s *lsStore) ReserveSpaceMulti(sizes []int) ([]lssOffset, [][]byte, lssResource) {
retry:
	id := atomic.LoadInt32(&s.currBuf)
	fbid := int(id) % len(s.flushBufs)
	fb := s.flushBufs[fbid]
	success, markedFull, offsets, bufs := fb.Alloc(sizes)
	if !success {
		if markedFull {
			s.initNextBuffer(id, fb)
			goto retry
		}

		runtime.Gosched()
		goto retry
	}

	return offsets, bufs, lssResource(fb)
}

func (s *lsStore) Read(lssOf lssOffset, buf []byte) (int, error) {
	offset := int64(lssOf)
retry:
	tail := s.log.Tail()

	// It's in the flush buffers
	if offset >= tail {
		id := atomic.LoadInt32(&s.currBuf)
		fbid := int(id) % len(s.flushBufs)
		end := s.flushBufs[fbid]
		start := s.head

		startOffset := start.StartOffset()
		endOffset := end.EndOffset()

		if startOffset < endOffset && offset >= startOffset && offset < endOffset {
			for fb := start; fb != nil; fb = fb.NextBuffer() {
				if n, err := fb.Read(offset, buf); err == nil {
					return n, nil
				}
			}
		}
		goto retry
	}

	if err := s.log.Read(buf[:headerFBSize], offset); err != nil {
		return 0, err
	}

	l := int(binary.BigEndian.Uint32(buf[:headerFBSize]))
	err := s.log.Read(buf[:l], offset+headerFBSize)
	return l, err
}

func (s *lsStore) FinalizeWrite(res lssResource) {
	fb := res.(*flushBuffer)
	fb.Done()
}

func (s *lsStore) RunCleaner(callb lssCleanerCallback, buf []byte) error {
	s.Lock()
	defer s.Unlock()

	tailOff := s.log.Tail()
	startOff := s.startOffset

	fn := func(offset lssOffset, b []byte) (bool, error) {
		cont, cleanOff, err := callb(offset, lssBlockEndOffset(offset, b), b)
		if err != nil {
			return false, err
		}

		if int64(cleanOff-s.cleanerTrimOffset) >= s.trimBatchSize {
			s.TrimLog(cleanOff)
			s.cleanerTrimOffset = cleanOff
		}

		atomic.StoreInt64(&s.startOffset, int64(cleanOff))
		return cont, nil
	}

	return s.visitor(startOff, tailOff, fn, buf)
}

func (s *lsStore) Visitor(callb lssBlockCallback, buf []byte) error {
	return s.visitor(s.log.Head(), s.log.Tail(), callb, buf)
}

func (s *lsStore) visitor(start, end int64, callb lssBlockCallback, buf []byte) error {
	curr := start
	for curr < end {
		n, err := s.Read(lssOffset(curr), buf)
		if err != nil {
			return err
		}

		if cont, err := callb(lssOffset(curr), buf[:n]); err == nil && !cont {
			break
		} else if err != nil {
			return err
		}

		curr += int64(n + headerFBSize)
	}

	return nil
}

func (s *lsStore) Sync() {
retry:
	id := atomic.LoadInt32(&s.currBuf)
	fbid := int(id) % len(s.flushBufs)
	fb := s.flushBufs[fbid]

	var endOffset int64
	var closed bool

	if closed, endOffset = fb.TryClose(); !closed {
		runtime.Gosched()
		goto retry
	}

	s.initNextBuffer(id, fb)

	for {
		tailOffset := s.log.Tail()
		if tailOffset >= endOffset {
			break
		}
		runtime.Gosched()
	}
}

var errFBReadFailed = errors.New("flushBuffer read failed")

type flushCallback func(fb *flushBuffer)

const headerFBSize = 4

type flushBuffer struct {
	baseOffset int64
	state      uint64
	b          []byte
	child      *flushBuffer
	callb      flushCallback

	trimOffset lssOffset
}

func newFlushBuffer(sz int, callb flushCallback) *flushBuffer {
	return &flushBuffer{
		state: encodeState(false, 1, 0),
		b:     make([]byte, sz),
		callb: callb,
	}
}

func (fb *flushBuffer) GetTrimLogOffset() (lssOffset, bool) {
	return fb.trimOffset, fb.trimOffset > 0
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

func (fb *flushBuffer) TryClose() (markedFull bool, lssOff int64) {
	state := atomic.LoadUint64(&fb.state)
	closed, nw, offset := decodeState(state)
	newState := encodeState(true, nw+1, offset)
	if !closed && nw > 0 && atomic.CompareAndSwapUint64(&fb.state, state, newState) {
		lssOff = fb.EndOffset()
		fb.Done()
		return true, lssOff
	}

	return false, 0
}

func (fb *flushBuffer) NextBuffer() *flushBuffer {
	return fb.child
}

func (fb *flushBuffer) Read(off int64, buf []byte) (l int, err error) {
	state := atomic.LoadUint64(&fb.state)
	isfull, nw, offset := decodeState(state)
	newState := encodeState(isfull, nw+1, offset)

	if nw > 0 && atomic.CompareAndSwapUint64(&fb.state, state, newState) {
		start := fb.baseOffset
		end := start + int64(offset)

		if off >= start && off < end {
			payloadOffset := off - start
			dataOffset := payloadOffset + headerFBSize
			l = int(binary.BigEndian.Uint32(fb.b[payloadOffset:dataOffset]))
			copy(buf, fb.b[dataOffset:dataOffset+int64(l)])
		} else {
			err = errFBReadFailed
		}

		fb.Done()
	} else {
		err = errFBReadFailed
	}

	return
}

func (fb *flushBuffer) Init(parent *flushBuffer, baseOffset int64) {
	fb.baseOffset = baseOffset
	// 1 writer rc for parent to enforce ordering of flush callback
	// 1 writer rc for parent to call flush callback if writers have already
	// terminated while initialization of next buffer.
	if parent != nil {
		atomic.StoreUint64(&fb.state, encodeState(false, 2, 0))
		parent.child = fb

		// If parent is already full and writers have completed operations,
		// this would trigger flush callback.
		parent.Done()
	}
}

func (fb *flushBuffer) SetTrimLogOffset(off lssOffset) bool {
	state := atomic.LoadUint64(&fb.state)
	isfull, nw, offset := decodeState(state)

	newState := encodeState(isfull, nw+1, offset)
	if nw > 0 && atomic.CompareAndSwapUint64(&fb.state, state, newState) {
		fb.trimOffset = off
		fb.Done()
		return true
	}

	return false
}

func (fb *flushBuffer) Alloc(sizes []int) (status bool, markedFull bool, offs []lssOffset, bufs [][]byte) {
retry:
	state := atomic.LoadUint64(&fb.state)
	isfull, nw, offset := decodeState(state)
	if isfull {
		return false, false, nil, nil
	}

	size := 0
	for _, sz := range sizes {
		size += sz + headerFBSize
	}

	newOffset := offset + size
	if newOffset > len(fb.b) {
		markedFull := true
		newState := encodeState(true, nw, offset)
		if !atomic.CompareAndSwapUint64(&fb.state, state, newState) {
			goto retry
		}
		return false, markedFull, nil, nil
	}

	newState := encodeState(false, nw+1, newOffset)
	if !atomic.CompareAndSwapUint64(&fb.state, state, newState) {
		goto retry
	}

	bufs = make([][]byte, len(sizes))
	offs = make([]lssOffset, len(sizes))
	for i, bufOffset := 0, offset; i < len(sizes); i++ {
		binary.BigEndian.PutUint32(fb.b[bufOffset:bufOffset+headerFBSize], uint32(sizes[i]))
		bufs[i] = fb.b[bufOffset+headerFBSize : bufOffset+headerFBSize+sizes[i]]
		offs[i] = lssOffset(fb.baseOffset + int64(bufOffset))
		bufOffset += sizes[i] + headerFBSize
	}

	return true, false, offs, bufs
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
		if fb.child != nil {
			fb.child.Done()
		}
		fb.callb(fb)
	}
}

func (fb *flushBuffer) IsFull() bool {
	state := atomic.LoadUint64(&fb.state)
	isfull, _, _ := decodeState(state)
	return isfull
}

func (fb *flushBuffer) IsReset() bool {
	state := atomic.LoadUint64(&fb.state)
	return isResetState(state)
}

func (fb *flushBuffer) Reset() {
	fb.baseOffset = 0
	fb.child = nil
	state := resetState(atomic.LoadUint64(&fb.state))
	atomic.StoreUint64(&fb.state, state)
}

// State encoding
// [32 bit offset][14 bit void][16 bit nwriters][1 bit reset][1 bit full]
func decodeState(state uint64) (bool, int, int) {
	isfull := state&0x1 == 0x1           // 1 bit full
	nwriters := int(state >> 2 & 0xffff) // 16 bits
	offset := int(state >> 32)           // remaining bits

	return isfull, nwriters, offset
}

func encodeState(isfull bool, nwriters int, offset int) uint64 {
	var isfullbits, nwritersbits, offsetbits uint64

	if isfull {
		isfullbits = 1
	}

	nwritersbits = uint64(nwriters) << 2
	offsetbits = uint64(offset) << 32

	state := isfullbits | nwritersbits | offsetbits
	return state
}

func resetState(state uint64) uint64 {
	return state | 0x2
}

func isResetState(state uint64) bool {
	return state&0x2 > 0
}

func lssBlockEndOffset(off lssOffset, b []byte) lssOffset {
	return headerFBSize + off + lssOffset(len(b))
}
