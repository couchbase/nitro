package plasma

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestLSSBasic(t *testing.T) {
	maxSize := int64(1024 * 1024 * 100)
	BufSize := 1024 * 1024
	nbuffers := 4

	os.Remove("test.data")
	lss, _ := newLSStore("test.data", maxSize, BufSize, nbuffers)

	n := 8000
	var offs []lssOffset
	bufread := make([]byte, 1024*1024)
	for i := 0; i < n; i++ {
		offset, buf, res := lss.ReserveSpace(1024)
		binary.BigEndian.PutUint64(buf[:8], uint64(i))
		lss.FinalizeWrite(res)
		offs = append(offs, offset)
		lss.Read(offs[i], bufread)
		got := int(binary.BigEndian.Uint64(bufread[:8]))
		if got != i {
			fmt.Printf("%d expected %d, got %d\n", offs[i], i, got)
		}
	}

	empty := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	for i := 0; i < n; i++ {
		lss.Read(offs[i], bufread)
		got := int(binary.BigEndian.Uint64(bufread[:8]))
		if got != i {
			t.Errorf("expected %d, got %d", i, got)
		}
		copy(bufread[:8], empty)
	}
}

func TestLSSCleaner(t *testing.T) {
	maxSize := int64(1024 * 1024 * 20)
	BufSize := 1024 * 1024
	nbuffers := 4

	os.Remove("test.data")
	lss, _ := newLSStore("test.data", maxSize, BufSize, nbuffers)

	n := 1000000
	freeSpace := int64(5 * 1024 * 1024)
	var lock sync.Mutex
	offs := make(map[int]lssOffset)

	go func() {
		buf := make([]byte, 1024*1024)
		for {
			if lss.AvailableSpace() < freeSpace {
				lss.RunCleaner(func(off lssOffset, bs []byte) bool {
					lock.Lock()
					got := int(binary.BigEndian.Uint64(bs[:8]))
					delete(offs, got)
					lock.Unlock()

					if lss.AvailableSpace() >= freeSpace {
						return false
					}

					return true
				}, buf)
			} else {
				time.Sleep(time.Second)
			}
		}
	}()

	bufread := make([]byte, 1024*1024)
	for i := 0; i < n; i++ {
		offset, buf, res := lss.ReserveSpace(1024)
		binary.BigEndian.PutUint64(buf[:8], uint64(i))
		lss.FinalizeWrite(res)
		lock.Lock()
		offs[i] = offset
		lock.Unlock()

		lss.Read(offset, bufread)
		got := int(binary.BigEndian.Uint64(bufread[:8]))
		if got != i {
			fmt.Printf("%d expected %d, got %d\n", offs[i], i, got)
		}
	}

	empty := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	for i, off := range offs {
		lss.Read(off, bufread)
		got := int(binary.BigEndian.Uint64(bufread[:8]))
		if got != i {
			t.Errorf("expected %d, got %d", i, got)
		}
		copy(bufread[:8], empty)
	}
}

func TestLSSSuperBlock(t *testing.T) {
	maxSize := int64(1024 * 1024 * 20)
	BufSize := 1024 * 1024
	nbuffers := 2
	freeSpace := int64(5 * 1024 * 1024)

	os.Remove("test.data")
	lss, _ := newLSStore("test.data", maxSize, BufSize, nbuffers)
	n := 100000

	go func() {
		buf := make([]byte, 1024*1024)
		for {
			if lss.AvailableSpace() < freeSpace {
				lss.RunCleaner(func(off lssOffset, bs []byte) bool {
					if lss.AvailableSpace() >= freeSpace {
						return false
					}

					return true
				}, buf)
			} else {
				time.Sleep(time.Second)
			}
		}
	}()

	for i := 0; i < n; i++ {
		_, buf, res := lss.ReserveSpace(1024)
		binary.BigEndian.PutUint64(buf[:8], uint64(i))
		lss.FinalizeWrite(res)
	}

	tail := lss.tailOffset
	head := lss.headOffset
	lss.Close()

	lss, _ = newLSStore("test.data", maxSize, BufSize, nbuffers)
	if tail != lss.tailOffset {
		t.Errorf("tail: expected %d, got %d", tail, lss.tailOffset)
	}

	if head != lss.headOffset {
		t.Errorf("head: expected %d, got %d", tail, lss.tailOffset)
	}
}
