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

type LSSOffset uint64
type LSSResource interface{}
type LSSBlockCallback func(LSSOffset, []byte) (bool, error)
type LSSCleanerCallback func(start, end LSSOffset, bs []byte) (cont bool, cleanOff LSSOffset, err error)

type LSS interface {
	ReserveSpace(size int) (LSSOffset, []byte, LSSResource)
	ReserveSpaceMulti(sizes []int) ([]LSSOffset, [][]byte, LSSResource)
	FinalizeWrite(LSSResource)
	TrimLog(LSSOffset)
	Read(LSSOffset, []byte) (int, error)
	Sync(bool)
	Visitor(callb LSSBlockCallback, buf []byte) error
	RunCleaner(callb LSSCleanerCallback, buf []byte) error
	BytesWritten() int64

	HeadOffset() LSSOffset
	TailOffset() LSSOffset
	UsedSpace() int64
	Close()
}

type lsStore struct {
	trimBatchSize int64

	startOffset int64

	cleanerTrimOffset LSSOffset

	head, tail unsafe.Pointer
	bufSize    int
	nbufs      int

	sbBuffer [superBlockSize]byte

	sync.Mutex

	path        string
	segmentSize int64

	lastCommitTS   time.Time
	commitDuration time.Duration
	trimOffset     LSSOffset
	log            Log

	bytesWritten int64
}

func (s *lsStore) HeadOffset() LSSOffset {
	return LSSOffset(s.log.Head())
}

func (s *lsStore) TailOffset() LSSOffset {
	return LSSOffset(s.log.Tail())
}

func (s *lsStore) BytesWritten() int64 {
	return s.bytesWritten
}

func NewLSStore(path string, segSize int64, bufSize int, nbufs int, commitDur time.Duration) (LSS, error) {
	var err error

	s := &lsStore{
		path:           path,
		segmentSize:    segSize,
		nbufs:          nbufs,
		bufSize:        bufSize,
		trimBatchSize:  int64(bufSize),
		commitDuration: commitDur,
	}

	if s.log, err = newLog(path, segSize, commitDur == 0); err != nil {
		return nil, err
	}

	head := newFlushBuffer(bufSize, s.flush)

	// Prepare circular linked buffers
	curr := head
	for i := 0; i < nbufs-1; i++ {
		nextFb := newFlushBuffer(bufSize, s.flush)
		curr.SetNext(nextFb)
		curr = nextFb
		curr.Reset()
	}

	curr.SetNext(head)

	head.baseOffset = s.log.Tail()
	s.startOffset = s.log.Head()

	s.head = unsafe.Pointer(head)
	s.tail = s.head

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
			s.bytesWritten += int64(len(fb.Bytes()))
			break
		}

		fmt.Printf("Plasma: (%s) Unable to write - err %v\n", s.path, err)
		time.Sleep(time.Second)
	}

	if trimOffset, doTrim := fb.GetTrimLogOffset(); doTrim {
		s.trimOffset = trimOffset
	}

	doCommit := fb.doCommit || time.Since(s.lastCommitTS) > s.commitDuration

	if doCommit {
		s.log.Trim(int64(s.trimOffset))
		s.log.Commit()
		s.lastCommitTS = time.Now()
	}

	nextFb := fb.NextBuffer()
	atomic.StorePointer(&s.head, unsafe.Pointer(nextFb))
}

func (s *lsStore) initNextBuffer(currFb *flushBuffer) {
	nextFb := currFb.NextBuffer()

	for !nextFb.IsReset() {
		runtime.Gosched()
	}

	atomic.StoreInt64(&nextFb.baseOffset, currFb.EndOffset())
	nextFb.seqno = currFb.seqno + 1

	// 1 writer rc for parent to enforce ordering of flush callback
	// 1 writer rc for the guy who closes the buffer
	atomic.StoreUint64(&nextFb.state, encodeState(false, 2, 0))

	if !atomic.CompareAndSwapPointer(&s.tail, unsafe.Pointer(currFb), unsafe.Pointer(nextFb)) {
		panic(fmt.Sprintf("fatal: tailSeqno:%d, currSeqno:%d", s.currBuf().seqno, currFb.seqno))
	}
}

func (s *lsStore) TrimLog(off LSSOffset) {
retry:
	fb := s.currBuf()
	if !fb.SetTrimLogOffset(off) {
		runtime.Gosched()
		goto retry
	}
}

func (s *lsStore) ReserveSpace(size int) (LSSOffset, []byte, LSSResource) {
	offs, bs, res := s.ReserveSpaceMulti([]int{size})
	return offs[0], bs[0], res
}

func (s *lsStore) currBuf() *flushBuffer {
	return (*flushBuffer)(atomic.LoadPointer(&s.tail))
}

func (s *lsStore) ReserveSpaceMulti(sizes []int) ([]LSSOffset, [][]byte, LSSResource) {
retry:
	fb := s.currBuf()
	success, markedFull, offsets, bufs := fb.Alloc(sizes)
	if !success {
		if markedFull {
			s.initNextBuffer(fb)
			fb.Done()
			goto retry
		}

		runtime.Gosched()
		goto retry
	}

	return offsets, bufs, LSSResource(fb)
}

func (s *lsStore) Read(lssOf LSSOffset, buf []byte) (int, error) {
	offset := int64(lssOf)
retry:
	tailOff := s.log.Tail()

	// It's in the flush buffers
	if offset >= tailOff {
		fb := (*flushBuffer)(s.head)
		for i := 0; i < s.nbufs; i++ {
			if n, err := fb.Read(offset, buf); err == nil {
				return n, nil
			}
			fb = fb.NextBuffer()
		}
		runtime.Gosched()
		goto retry
	}

	if err := s.log.Read(buf[:headerFBSize], offset); err != nil {
		return 0, err
	}

	l := int(binary.BigEndian.Uint32(buf[:headerFBSize]))
	err := s.log.Read(buf[:l], offset+headerFBSize)
	return l, err
}

func (s *lsStore) FinalizeWrite(res LSSResource) {
	fb := res.(*flushBuffer)
	fb.Done()
}

func (s *lsStore) RunCleaner(callb LSSCleanerCallback, buf []byte) error {
	s.Lock()
	defer s.Unlock()

	tailOff := s.log.Tail()
	startOff := s.startOffset

	fn := func(offset LSSOffset, b []byte) (bool, error) {
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

func (s *lsStore) Visitor(callb LSSBlockCallback, buf []byte) error {
	return s.visitor(s.log.Head(), s.log.Tail(), callb, buf)
}

func (s *lsStore) visitor(start, end int64, callb LSSBlockCallback, buf []byte) error {
	curr := start
	for curr < end {
		n, err := s.Read(LSSOffset(curr), buf)
		if err != nil {
			return err
		}

		if cont, err := callb(LSSOffset(curr), buf[:n]); err == nil && !cont {
			break
		} else if err != nil {
			return err
		}

		curr += int64(n + headerFBSize)
	}

	return nil
}

func (s *lsStore) Sync(commit bool) {
retry:
	fb := s.currBuf()

	var endOffset int64
	var closed bool

	if closed, endOffset = fb.TryClose(); !closed {
		runtime.Gosched()
		goto retry
	}

	s.initNextBuffer(fb)
	fb.doCommit = commit
	fb.Done()

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
	seqno      uint64
	baseOffset int64
	state      uint64
	b          []byte
	next       *flushBuffer
	callb      flushCallback

	doCommit bool

	trimOffset LSSOffset
}

func newFlushBuffer(sz int, callb flushCallback) *flushBuffer {
	return &flushBuffer{
		state: encodeState(false, 1, 0),
		b:     make([]byte, sz),
		callb: callb,
	}
}

func (fb *flushBuffer) GetTrimLogOffset() (LSSOffset, bool) {
	return fb.trimOffset, fb.trimOffset > 0
}

func (fb *flushBuffer) Bytes() []byte {
	_, _, _, offset := decodeState(fb.state)
	return fb.b[:offset]
}

func (fb *flushBuffer) StartOffset() int64 {
	return atomic.LoadInt64(&fb.baseOffset)
}

func (fb *flushBuffer) EndOffset() int64 {
	_, _, _, offset := decodeState(fb.state)
	return atomic.LoadInt64(&fb.baseOffset) + int64(offset)
}

func (fb *flushBuffer) TryClose() (markedFull bool, lssOff int64) {
	state := atomic.LoadUint64(&fb.state)
	isfull, reset, nw, offset := decodeState(state)
	newState := encodeState(true, nw, offset)
	if !isfull && !reset && atomic.CompareAndSwapUint64(&fb.state, state, newState) {
		lssOff = fb.EndOffset()
		return true, lssOff
	}

	return false, 0
}

func (fb *flushBuffer) NextBuffer() *flushBuffer {
	return fb.next
}

func (fb *flushBuffer) SetNext(nfb *flushBuffer) {
	fb.next = nfb
}

func (fb *flushBuffer) Read(off int64, buf []byte) (l int, err error) {
	state := atomic.LoadUint64(&fb.state)
	_, _, _, offset := decodeState(state)

	startOff := atomic.LoadInt64(&fb.baseOffset)
	endOff := startOff + int64(offset)

	if off >= startOff && off < endOff {
		payloadOffset := off - startOff
		dataOffset := payloadOffset + headerFBSize
		l = int(binary.BigEndian.Uint32(fb.b[payloadOffset:dataOffset]))
		copy(buf, fb.b[dataOffset:dataOffset+int64(l)])

		if startOff != atomic.LoadInt64(&fb.baseOffset) {
			err = errFBReadFailed
		}
	} else {
		err = errFBReadFailed
	}

	return
}

func (fb *flushBuffer) SetTrimLogOffset(off LSSOffset) bool {
	state := atomic.LoadUint64(&fb.state)
	isfull, reset, nw, offset := decodeState(state)

	newState := encodeState(isfull, nw+1, offset)
	if !reset && nw > 0 && atomic.CompareAndSwapUint64(&fb.state, state, newState) {
		fb.trimOffset = off
		fb.Done()
		return true
	}

	return false
}

func (fb *flushBuffer) Alloc(sizes []int) (status bool, markedFull bool, offs []LSSOffset, bufs [][]byte) {
retry:
	state := atomic.LoadUint64(&fb.state)
	isfull, reset, nw, offset := decodeState(state)

	if isfull || reset {
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
			runtime.Gosched()
			goto retry
		}
		return false, markedFull, nil, nil
	}

	newState := encodeState(false, nw+1, newOffset)
	if !atomic.CompareAndSwapUint64(&fb.state, state, newState) {
		goto retry
	}

	bufs = make([][]byte, len(sizes))
	offs = make([]LSSOffset, len(sizes))
	for i, bufOffset := 0, offset; i < len(sizes); i++ {
		binary.BigEndian.PutUint32(fb.b[bufOffset:bufOffset+headerFBSize], uint32(sizes[i]))
		bufs[i] = fb.b[bufOffset+headerFBSize : bufOffset+headerFBSize+sizes[i]]
		offs[i] = LSSOffset(fb.baseOffset + int64(bufOffset))
		bufOffset += sizes[i] + headerFBSize
	}

	return true, false, offs, bufs
}

func (fb *flushBuffer) Done() {
retry:
	state := atomic.LoadUint64(&fb.state)
	isfull, _, nw, offset := decodeState(state)

	newState := encodeState(isfull, nw-1, offset)
	if !atomic.CompareAndSwapUint64(&fb.state, state, newState) {
		goto retry
	}

	if nw == 1 && isfull {
		fb.callb(fb)
		fb.Reset()
		nextFb := fb.NextBuffer()
		nextFb.Done()
	}
}

func (fb *flushBuffer) IsFull() bool {
	state := atomic.LoadUint64(&fb.state)
	isfull, _, _, _ := decodeState(state)
	return isfull
}

func (fb *flushBuffer) IsReset() bool {
	state := atomic.LoadUint64(&fb.state)
	return isResetState(state)
}

func (fb *flushBuffer) Reset() {
	fb.doCommit = false
	fb.trimOffset = 0
	state := resetState(atomic.LoadUint64(&fb.state))
	atomic.StoreUint64(&fb.state, state)
}

// State encoding
// [32 bit offset][14 bit void][16 bit nwriters][1 bit reset][1 bit full]
func decodeState(state uint64) (bool, bool, int, int) {
	isfull := state&0x1 == 0x1           // 1 bit full
	reset := state&0x2 == 0x2            // 1 bit reset
	nwriters := int(state >> 2 & 0xffff) // 16 bits
	offset := int(state >> 32)           // remaining bits

	return isfull, reset, nwriters, offset
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

func lssBlockEndOffset(off LSSOffset, b []byte) LSSOffset {
	return headerFBSize + off + LSSOffset(len(b))
}
