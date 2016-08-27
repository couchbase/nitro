package plasma

import (
	"fmt"
	"github.com/t3rm1n4l/nitro/skiplist"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func newTestIntPlasmaStore() *Plasma {
	cfg := Config{
		MaxDeltaChainLen: 200,
		MaxPageItems:     400,
		MinPageItems:     25,
		Compare:          skiplist.CompareInt,
		ItemSize: func(unsafe.Pointer) uintptr {
			return unsafe.Sizeof(new(skiplist.IntKeyItem))
		},
		File:            "teststore.data",
		MaxSize:         1024 * 1024 * 1024 * 10,
		FlushBufferSize: 1024 * 1024,
	}

	s, err := New(cfg)
	if err != nil {
		panic(err)
	}

	return s
}

func TestPlasmaSimple(t *testing.T) {
	s := newTestIntPlasmaStore()
	w := s.NewWriter()
	for i := 0; i < 1000000; i++ {
		w.Insert(skiplist.NewIntKeyItem(i))
	}

	for i := 0; i < 1000000; i++ {
		itm := skiplist.NewIntKeyItem(i)
		got := w.Lookup(itm)
		if skiplist.CompareInt(itm, got) != 0 {
			t.Errorf("mismatch %d != %d", i, skiplist.IntFromItem(got))
		}
	}

	for i := 0; i < 800000; i++ {
		w.Delete(skiplist.NewIntKeyItem(i))
	}

	for i := 0; i < 1000000; i++ {
		itm := skiplist.NewIntKeyItem(i)
		got := w.Lookup(itm)
		if i < 800000 {
			if got != nil {
				t.Errorf("Expected missing %d", i)
			}
		} else {
			if skiplist.CompareInt(itm, got) != 0 {
				t.Errorf("Expected %d, got %d", i, skiplist.IntFromItem(got))
			}
		}
	}

	fmt.Println(s.GetStats())
}

func doInsert(w *Writer, wg *sync.WaitGroup, id, n int) {
	defer wg.Done()

	for i := 0; i < n; i++ {
		val := i + id*n
		itm := skiplist.NewIntKeyItem(val)
		w.Insert(itm)
	}
}

func doDelete(w *Writer, wg *sync.WaitGroup, id, n int) {
	defer wg.Done()

	for i := 0; i < n; i++ {
		val := i + id*n
		itm := skiplist.NewIntKeyItem(val)
		w.Delete(itm)
	}
}

func doLookup(w *Writer, wg *sync.WaitGroup, id, n int) {
	defer wg.Done()

	for i := 0; i < n; i++ {
		val := i + id*n
		itm := skiplist.NewIntKeyItem(val)
		if w.Lookup(itm) == nil {
			panic(i)
		}
	}
}

func TestPlasmaInsertPerf(t *testing.T) {
	var wg sync.WaitGroup

	numThreads := 4
	n := 10000000
	nPerThr := n / numThreads
	s := newTestIntPlasmaStore()
	total := numThreads * nPerThr

	t0 := time.Now()
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		w := s.NewWriter()
		go doInsert(w, &wg, i, nPerThr)
	}
	wg.Wait()

	dur := time.Since(t0)

	fmt.Println(s.GetStats())
	fmt.Printf("%d items insert took %v -> %v items/s\n", total, dur, float64(total)/float64(dur.Seconds()))
}

func TestPlasmaDeletePerf(t *testing.T) {
	var wg sync.WaitGroup

	numThreads := 4
	n := 10000000
	nPerThr := n / numThreads
	s := newTestIntPlasmaStore()
	total := numThreads * nPerThr

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		w := s.NewWriter()
		go doInsert(w, &wg, i, nPerThr)
	}
	wg.Wait()

	t0 := time.Now()
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		w := s.NewWriter()
		go doDelete(w, &wg, i, nPerThr)
	}
	wg.Wait()

	dur := time.Since(t0)

	fmt.Println(s.GetStats())
	fmt.Printf("%d items delete took %v -> %v items/s\n", total, dur, float64(total)/float64(dur.Seconds()))
}

func TestPlasmaLookupPerf(t *testing.T) {
	var wg sync.WaitGroup

	numThreads := 4
	n := 10000000
	nPerThr := n / numThreads
	s := newTestIntPlasmaStore()
	total := numThreads * nPerThr

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		w := s.NewWriter()
		go doInsert(w, &wg, i, nPerThr)
	}
	wg.Wait()

	t0 := time.Now()
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		w := s.NewWriter()
		go doLookup(w, &wg, i, nPerThr)
	}
	wg.Wait()

	dur := time.Since(t0)

	fmt.Println(s.GetStats())
	fmt.Printf("%d items lookup took %v -> %v items/s\n", total, dur, float64(total)/float64(dur.Seconds()))
}

func TestIteratorSimple(t *testing.T) {
	s := newTestIntPlasmaStore()
	w := s.NewWriter()
	for i := 0; i < 1000000; i++ {
		w.Insert(skiplist.NewIntKeyItem(i))
	}

	i := 0
	itr := s.NewIterator()
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		if v := skiplist.IntFromItem(itr.Get()); v != i {
			t.Errorf("expected %d, got %d", i, v)
		}

		i++
	}

	if i != 1000000 {
		t.Errorf("expected %d, got %d", i)
	}

}

func TestIteratorSeek(t *testing.T) {
	s := newTestIntPlasmaStore()
	w := s.NewWriter()
	for i := 0; i < 1000000; i++ {
		w.Insert(skiplist.NewIntKeyItem(i))
	}

	itr := s.NewIterator().(*Iterator)
	for i := 0; i < 1000000; i++ {
		itr.Seek(skiplist.NewIntKeyItem(i))
		if v := skiplist.IntFromItem(itr.Get()); v != i {
			t.Errorf("expected %d, got %d", i, v)
		}
	}
}

func TestPlasmaIteratorLookupPerf(t *testing.T) {
	var wg sync.WaitGroup

	numThreads := 8
	n := 10000000
	nPerThr := n / numThreads
	s := newTestIntPlasmaStore()
	total := numThreads * nPerThr

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		w := s.NewWriter()
		go doInsert(w, &wg, i, nPerThr)
	}
	wg.Wait()

	t0 := time.Now()
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, id, n int) {
			defer wg.Done()
			itr := s.NewIterator()

			for i := 0; i < n; i++ {
				val := i + id*n
				itm := skiplist.NewIntKeyItem(val)
				itr.Seek(itm)
			}
		}(&wg, i, nPerThr)
	}
	wg.Wait()

	dur := time.Since(t0)

	fmt.Println(s.GetStats())
	fmt.Printf("%d items iterator seek took %v -> %v items/s\n", total, dur, float64(total)/float64(dur.Seconds()))
}

func TestPlasmaPersistor(t *testing.T) {
	s := newTestIntPlasmaStore()
	w := s.NewWriter()
	for i := 0; i < 18000000; i++ {
		w.Insert(skiplist.NewIntKeyItem(i * 10))
	}

	t0 := time.Now()
	s.PersistAll()
	fmt.Println("took", time.Since(t0), s.lss.UsedSpace())
	for i := 0; i < 2000000; i++ {
		w.Insert(skiplist.NewIntKeyItem(5 + i*100))
	}
	t0 = time.Now()
	s.PersistAll()
	fmt.Println("took", time.Since(t0), s.lss.UsedSpace())
	for i := 0; i < 2000000; i++ {
		w.Insert(skiplist.NewIntKeyItem(6 + i*100))
	}
	t0 = time.Now()
	s.PersistAll()
	fmt.Println("took", time.Since(t0), s.lss.UsedSpace())
	fmt.Println(s.GetStats())
}
