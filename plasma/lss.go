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

const superBlockSize = 4096
const lssReclaimBlockSize = 1024 * 1024 * 8

type lssOffset int64
type lssResource interface{}
type lssBlockCallback func(lssOffset, []byte) bool
type lssCleanerCallback func(start, end lssOffset, bs []byte) (cont bool, cleanOff, relocEnd lssOffset)

type LSS interface {
	ReserveSpace(size int) (lssOffset, []byte, lssResource)
	ReserveSpaceMulti(sizes []int) ([]lssOffset, [][]byte, lssResource)
	FinalizeWrite(lssResource)
	Read(lssOffset, buf []byte) (int, error)
	Sync()

	RunCleaner(callb lssCleanerCallback, buf []byte) error
}

type lsStore struct {
	w          *os.File
	r          *os.File
	maxSize    int64
	headOffset int64
	tailOffset int64

	// Cleaner updates this pair during relocation
	cleanerHeadOffset, cleanerRelocEnd int64

	head *flushBuffer

	bufSize   int
	flushBufs []*flushBuffer
	currBuf   int32

	sbBuffer [superBlockSize]byte

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
	s.loadSuperBlock()
	s.head.baseOffset = s.tailOffset

	return s, nil
}

func (s *lsStore) Close() {
	s.Sync()
	s.updateSuperBlock()
	s.w.Close()
	s.r.Close()
}

func (s *lsStore) updateSuperBlock() {
	buf := s.sbBuffer[:]
	woffset := 0
	// version
	binary.BigEndian.PutUint32(buf[woffset:woffset+4], uint32(0))
	woffset += 4

	// headOffset
	binary.BigEndian.PutUint64(buf[woffset:woffset+8], uint64(atomic.LoadInt64(&s.headOffset)))
	woffset += 8

	// tailOffset
	binary.BigEndian.PutUint64(buf[woffset:woffset+8], uint64(atomic.LoadInt64(&s.tailOffset)))
	woffset += 8

	s.w.WriteAt(buf, 0)
}

func (s *lsStore) loadSuperBlock() {
	buf := s.sbBuffer[:]
	roffset := 4

	s.r.ReadAt(buf, 0)
	s.headOffset = int64(binary.BigEndian.Uint64(buf[roffset : roffset+8]))
	roffset += 8
	s.tailOffset = int64(binary.BigEndian.Uint64(buf[roffset : roffset+8]))
}

func (s *lsStore) UsedSpace() int64 {
	return atomic.LoadInt64(&s.tailOffset) - atomic.LoadInt64(&s.headOffset)
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
		s.w.WriteAt(bs[tailSz:], superBlockSize+0)
		s.w.WriteAt(bs[:tailSz], superBlockSize+fpos)
	} else {
		s.w.WriteAt(fb.Bytes(), superBlockSize+fpos)
	}

	atomic.StoreInt64(&s.tailOffset, fb.EndOffset())
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&s.head)),
		unsafe.Pointer(fb.NextBuffer()))

	// This may read old headOffset, newReloc - which is safe
	headOffset := atomic.LoadInt64(&s.cleanerHeadOffset)
	relocEndOffset := atomic.LoadInt64(&s.cleanerRelocEnd)
	if relocEndOffset > 0 && relocEndOffset <= fb.EndOffset() {
		atomic.StoreInt64(&s.headOffset, headOffset)
	}

	s.updateSuperBlock()
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

func (s *lsStore) readBlock(off int64, buf []byte) error {
	fpos := off % s.maxSize

	if fpos+int64(len(buf)) > s.maxSize {
		tailSz := s.maxSize - fpos
		if _, err := s.r.ReadAt(buf[:tailSz], superBlockSize+fpos); err != nil {
			return err
		}

		if _, err := s.r.ReadAt(buf[tailSz:], superBlockSize+0); err != nil {
			return err
		}
	} else {
		if _, err := s.r.ReadAt(buf, superBlockSize+fpos); err != nil {
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

	tailOff := atomic.LoadInt64(&s.tailOffset)

	fn := func(offset lssOffset, b []byte) bool {
		// The lss writer asynchronously updates headOffset when tail >= relocEnd
		cont, headOff, relocOff := callb(offset, lssBlockEndOffset(offset, b), b)

		// No relocation, hence update immediately
		if relocOff == 0 {
			atomic.StoreInt64(&s.headOffset, int64(headOff))
		} else {
			// We cannot set relocOffset to 0
			// It should be always monotonic to avoid race condition
			tailOff = int64(relocOff)
		}

		// It is safe for writer to update latest reloc, but old headOffset
		// But, new headOffset for old reloc is dangerous
		atomic.StoreInt64(&s.cleanerRelocEnd, int64(tailOff))
		atomic.StoreInt64(&s.cleanerHeadOffset, int64(headOff))

		return cont
	}

	return s.Visitor(fn, buf)
}

func (s *lsStore) Visitor(callb lssBlockCallback, buf []byte) error {
	curr := atomic.LoadInt64(&s.headOffset)
	end := atomic.LoadInt64(&s.tailOffset)
	for curr < end {
		n, err := s.Read(lssOffset(curr), buf)
		if err != nil {
			return err
		}

		if !callb(lssOffset(curr), buf[:n]) {
			break
		}

		curr += int64(n + headerFBSize)
	}

	return nil
}

func (s *lsStore) Sync() {
	id := atomic.LoadInt32(&s.currBuf)
	fbid := int(id) % len(s.flushBufs)
	fb := s.flushBufs[fbid]
	endOffset := fb.EndOffset()
	if fb.TryClose() {
		s.initNextBuffer(id, fb)
	}

	for {
		tailOffset := atomic.LoadInt64(&s.tailOffset)
		if tailOffset >= endOffset {
			break
		}
		runtime.Gosched()
	}

	s.w.Sync()
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
		state: encodeState(false, 1, 0),
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

func (fb *flushBuffer) TryClose() (markedFull bool) {
retry:
	state := atomic.LoadUint64(&fb.state)
	isfull, nw, offset := decodeState(state)

	if !isfull {
		newState := encodeState(true, nw, offset)
		if !atomic.CompareAndSwapUint64(&fb.state, state, newState) {
			goto retry
		}

		return true
	}

	return false
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
