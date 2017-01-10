package plasma

import (
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const segmentSize = 1024 * 1024 * 10

func TestLSSBasic(t *testing.T) {
	BufSize := 1024 * 1024
	nbuffers := 4

	os.RemoveAll("test.data")
	lss, err := newLSStore("test.data", segmentSize, BufSize, nbuffers, 0)
	if err != nil {
		panic(err)
	}

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

func TestLSSConcurrent(t *testing.T) {
	BufSize := 1024 * 1024
	nbuffers := 2

	var mu sync.Mutex
	m := make(map[lssOffset]int)

	os.RemoveAll("test.data")
	lss, _ := newLSStore("test.data", segmentSize, BufSize, nbuffers, 0)

	n := 10000
	var wg sync.WaitGroup
	for x := 0; x < 8; x++ {
		wg.Add(1)

		go func(x int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				offset, _, res := lss.ReserveSpace(1024)
				lss.FinalizeWrite(res)
				mu.Lock()
				if thr, ok := m[offset]; ok {
					t.Errorf("(%d lss offset %d was allocated earlier by thr %d", x, offset, thr)
				} else {
					m[offset] = x
				}
				mu.Unlock()
			}
		}(x)
	}

	wg.Wait()

}

func TestLSSCleaner(t *testing.T) {
	var wg sync.WaitGroup
	BufSize := 1024 * 1024
	nbuffers := 4

	os.RemoveAll("test.data")
	lss, _ := newLSStore("test.data", segmentSize, BufSize, nbuffers, 0)

	n := 1000000
	var lock sync.Mutex
	offs := make(map[int]lssOffset)

	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 1024*1024)
		cleaned := 0
		for cleaned < n/2 {
			lss.RunCleaner(func(off, endOff lssOffset, bs []byte) (bool, lssOffset, error) {
				lock.Lock()
				got := int(binary.BigEndian.Uint64(bs[:8]))
				delete(offs, got)
				lock.Unlock()
				cleaned++
				if cleaned == n/2 {
					return false, endOff, nil
				}
				return true, endOff, nil
			}, buf)
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

	lss.Sync(false)
	wg.Wait()

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
	var wg sync.WaitGroup
	BufSize := 1024 * 1024
	nbuffers := 2

	os.RemoveAll("test.data")
	lss, err := newLSStore("test.data", segmentSize, BufSize, nbuffers, 0)
	if err != nil {
		panic(err)
	}
	n := 100000

	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 1024*1024)
		cleaned := 0
		for cleaned < n/2 {
			lss.RunCleaner(func(off, endOff lssOffset, bs []byte) (bool, lssOffset, error) {
				cleaned++
				if cleaned < n/2 {
					return true, endOff, nil
				} else {
					return false, off, nil
				}
			}, buf)
		}
	}()

	for i := 0; i < n; i++ {
		_, buf, res := lss.ReserveSpace(1024)
		binary.BigEndian.PutUint64(buf[:8], uint64(i))
		lss.FinalizeWrite(res)
	}

	wg.Wait()
	lss.Sync(false)
	tail := lss.log.Tail()
	head := lss.log.Head()
	lss.Close()

	lss, err = newLSStore("test.data", segmentSize, BufSize, nbuffers, 0)
	if err != nil {
		panic(err)
	}

	if tail != lss.log.Tail() {
		t.Errorf("tail: expected %d, got %d", tail, lss.log.Tail())
	}

	if head != lss.log.Head() {
		t.Errorf("head: expected %d, got %d", head, lss.log.Head())
	}
}

func TestLSSPerf(t *testing.T) {
	var wg sync.WaitGroup

	os.RemoveAll("test.data")
	BufSize := 1024 * 1024
	nbuffers := 2
	segmentSize := int64(1024 * 1024 * 1024)
	lss, _ := newLSStore("test.data", segmentSize, BufSize, nbuffers, 0)

	var count int64
	n := runtime.GOMAXPROCS(0)
	limit := 1000000
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			tbuf := make([]byte, 512)

			defer wg.Done()
			for x := 0; x < limit; x++ {
				_, buf, res := lss.ReserveSpace(512)
				binary.BigEndian.PutUint64(tbuf[:8], uint64(id+i))
				copy(buf, tbuf)
				runtime.Gosched()
				lss.FinalizeWrite(res)
				atomic.AddInt64(&count, 1)
			}
		}(i)
	}

	closed := make(chan bool)
	go func() {
		var last int64
		for {
			select {
			case <-closed:
				return
			default:
			}

			time.Sleep(time.Second)
			c := atomic.LoadInt64(&count)
			fmt.Println(c - last)
			last = c
		}
	}()
	wg.Wait()

	close(closed)
	lss.Sync(false)
	lss.Close()

}
