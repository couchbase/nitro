package plasma

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
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
	w             *os.File
	r             *os.File
	trimBatchSize int64
	maxSize       int64
	headOffset    int64
	tailOffset    int64

	startOffset int64

	cleanerTrimOffset lssOffset

	// Generation number
	superBlockGen uint64

	head *flushBuffer

	bufSize   int
	flushBufs []*flushBuffer
	currBuf   int32

	sbBuffer [superBlockSize]byte

	sync.Mutex
}

func newLSStore(file string, maxSize int64, bufSize int, trimBatchSize int, nbufs int) (*lsStore, error) {
	var err error

	s := &lsStore{
		maxSize:       maxSize,
		bufSize:       bufSize,
		trimBatchSize: int64(trimBatchSize),
		flushBufs:     make([]*flushBuffer, nbufs),
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
	if err := s.loadSuperBlock(); err != nil {
		return nil, err
	}
	s.head.baseOffset = s.tailOffset

	return s, nil
}

func (s *lsStore) Close() {
	s.Sync()
	s.w.Close()
	s.r.Close()
}

func encodeSuperblock(buf []byte, headOffset, tailOffset int64, gen uint64) {
	woffset := 0
	binary.BigEndian.PutUint32(buf[woffset:woffset+4], uint32(lssVersion))
	woffset += 4

	binary.BigEndian.PutUint64(buf[woffset:woffset+8], uint64(gen))
	woffset += 8

	binary.BigEndian.PutUint64(buf[woffset:woffset+8], uint64(headOffset))
	woffset += 8

	binary.BigEndian.PutUint64(buf[woffset:woffset+8], uint64(tailOffset))
	woffset += 8

	hash := crc32.ChecksumIEEE(buf[:superBlockSize-4])
	binary.BigEndian.PutUint32(buf[superBlockSize-4:superBlockSize], hash)
}

func decodeSuperblock(buf []byte) (headOffset, tailOffset int64, gen uint64, err error) {
	roffset := 4
	gen = binary.BigEndian.Uint64(buf[roffset : roffset+8])
	roffset += 8
	headOffset = int64(binary.BigEndian.Uint64(buf[roffset : roffset+8]))
	roffset += 8
	tailOffset = int64(binary.BigEndian.Uint64(buf[roffset : roffset+8]))
	roffset += 8

	hash := binary.BigEndian.Uint32(buf[superBlockSize-4 : superBlockSize])
	computedHash := crc32.ChecksumIEEE(buf[:superBlockSize-4])
	if hash != computedHash {
		err = ErrCorruptSuperBlock
	}

	return
}

func (s *lsStore) updateSuperBlock(headOffset, tailOffset int64) {
	buf := s.sbBuffer[:]
	encodeSuperblock(buf, headOffset, tailOffset, s.superBlockGen)
	offset := int64(superBlockSize * (s.superBlockGen % 2))
	s.w.WriteAt(buf, offset)
	s.superBlockGen++
}

func (s *lsStore) loadSuperBlock() error {
	buf := s.sbBuffer[:]

	var headOffsets, tailOffsets [2]int64
	var gens [2]uint64
	var errs [2]error

	if _, err := s.r.ReadAt(buf, 0); err != nil && err != io.EOF {
		return err
	} else if err == nil {
		headOffsets[0], tailOffsets[0], gens[0], errs[0] = decodeSuperblock(buf)

		if _, err := s.r.ReadAt(buf, superBlockSize); err != nil && err != io.EOF {
			return err
		}
		headOffsets[1], tailOffsets[1], gens[1], errs[1] = decodeSuperblock(buf)

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
			return ErrCorruptSuperBlock
		}

		s.headOffset, s.tailOffset = headOffsets[sbIndex], tailOffsets[sbIndex]
		s.superBlockGen = gens[sbIndex] + 1

	}

	s.updateSuperBlock(s.headOffset, s.tailOffset)
	s.w.Sync()
	return nil
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
		s.w.WriteAt(bs[tailSz:], headerSize+0)
		s.w.WriteAt(bs[:tailSz], headerSize+fpos)
	} else {
		s.w.WriteAt(fb.Bytes(), headerSize+fpos)
	}

	tailOffset := fb.EndOffset()
	atomic.StoreInt64(&s.tailOffset, tailOffset)
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&s.head)),
		unsafe.Pointer(fb.NextBuffer()))

	trimOffset, doTrim := fb.GetTrimLogOffset()
	if doTrim {
		atomic.StoreInt64(&s.headOffset, int64(trimOffset))
	}

	s.updateSuperBlock(atomic.LoadInt64(&s.headOffset), tailOffset)
	s.w.Sync()

	if doTrim {
		// hole punch
	}
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

func (s *lsStore) readBlock(off int64, buf []byte) error {
	fpos := off % s.maxSize

	if fpos+int64(len(buf)) > s.maxSize {
		tailSz := s.maxSize - fpos
		if _, err := s.r.ReadAt(buf[:tailSz], headerSize+fpos); err != nil {
			return err
		}

		if _, err := s.r.ReadAt(buf[tailSz:], headerSize+0); err != nil {
			return err
		}
	} else {
		if _, err := s.r.ReadAt(buf, headerSize+fpos); err != nil {
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
			for fb := start; fb != nil; fb = fb.NextBuffer() {
				if n, err := fb.Read(offset, buf); err == nil {
					return n, nil
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

	fn := func(offset lssOffset, b []byte) (bool, error) {
		cont, headOff, err := callb(offset, lssBlockEndOffset(offset, b), b)
		if err != nil {
			return false, err
		}

		if int64(headOff-s.cleanerTrimOffset) >= s.trimBatchSize {
			s.TrimLog(headOff)
			s.cleanerTrimOffset = headOff
		}

		s.startOffset = int64(headOff)
		return cont, nil
	}

	return s.visitor(fn, buf)
}

func (s *lsStore) visitor(callb lssBlockCallback, buf []byte) error {
	curr := s.startOffset
	end := atomic.LoadInt64(&s.tailOffset)
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
		tailOffset := atomic.LoadInt64(&s.tailOffset)
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
