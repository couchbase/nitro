package plasma

import (
	"fmt"
	"github.com/t3rm1n4l/nitro/skiplist"
	"os"
	"sync"
	"testing"
	"time"
	"unsafe"
)

var testCfg = Config{
	MaxDeltaChainLen: 200,
	MaxPageItems:     400,
	MinPageItems:     25,
	Compare:          skiplist.CompareInt,
	ItemSize: func(unsafe.Pointer) uintptr {
		return unsafe.Sizeof(new(skiplist.IntKeyItem))
	},
	File:                "teststore.data",
	MaxSize:             1024 * 1024 * 1024 * 5,
	FlushBufferSize:     1024 * 1024,
	LSSCleanerThreshold: 10,
	AutoLSSCleaning:     false,
}

func newTestIntPlasmaStore(cfg Config) *Plasma {

	s, err := New(cfg)
	if err != nil {
		panic(err)
	}

	return s
}

func TestPlasmaSimple(t *testing.T) {
	os.Remove("teststore.data")
	s := newTestIntPlasmaStore(testCfg)
	defer s.Close()

	w := s.NewWriter()
	for i := 0; i < 1000000; i++ {
		w.Insert(skiplist.NewIntKeyItem(i))
	}

	for i := 0; i < 1000000; i++ {
		itm := skiplist.NewIntKeyItem(i)
		got, _ := w.Lookup(itm)
		if skiplist.CompareInt(itm, got) != 0 {
			t.Errorf("mismatch %d != %d", i, skiplist.IntFromItem(got))
		}
	}

	for i := 0; i < 800000; i++ {
		w.Delete(skiplist.NewIntKeyItem(i))
	}

	for i := 0; i < 1000000; i++ {
		itm := skiplist.NewIntKeyItem(i)
		got, _ := w.Lookup(itm)
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

func doUpdate(w *Writer, wg *sync.WaitGroup, id, n int) {
	defer wg.Done()

	for i := 0; i < n; i++ {
		val := i + id*n
		itm := skiplist.NewIntKeyItem(val)
		w.Delete(itm)
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
		if x, _ := w.Lookup(itm); x == nil {
			panic(i)
		}
	}
}

func TestPlasmaInsertPerf(t *testing.T) {
	var wg sync.WaitGroup

	os.Remove("teststore.data")
	numThreads := 8
	n := 20000000
	nPerThr := n / numThreads
	s := newTestIntPlasmaStore(testCfg)
	defer s.Close()
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

	os.Remove("teststore.data")
	numThreads := 8
	n := 20000000
	nPerThr := n / numThreads
	s := newTestIntPlasmaStore(testCfg)
	defer s.Close()
	total := numThreads * nPerThr

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		w := s.NewWriter()
		go doInsert(w, &wg, i, nPerThr)
	}
	wg.Wait()
	fmt.Println(s.GetStats())

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

	os.Remove("teststore.data")
	numThreads := 8
	n := 20000000
	nPerThr := n / numThreads
	s := newTestIntPlasmaStore(testCfg)
	defer s.Close()
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
	os.Remove("teststore.data")
	s := newTestIntPlasmaStore(testCfg)
	defer s.Close()
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
	os.Remove("teststore.data")
	s := newTestIntPlasmaStore(testCfg)
	defer s.Close()
	w := s.NewWriter()
	for i := 0; i < 1000000; i++ {
		w.Insert(skiplist.NewIntKeyItem(i))
	}

	itr := s.NewIterator().(*Iterator)
	for i := 0; i < 1000000; i++ {
		itr.Seek(skiplist.NewIntKeyItem(i))
		if itr.Valid() {
			if v := skiplist.IntFromItem(itr.Get()); v != i {
				t.Errorf("expected %d, got %d", i, v)
			}
		} else {
			t.Errorf("%d not found", i)
		}
	}
}

func TestPlasmaIteratorLookupPerf(t *testing.T) {
	var wg sync.WaitGroup

	os.Remove("teststore.data")
	numThreads := 8
	n := 20000000
	nPerThr := n / numThreads
	s := newTestIntPlasmaStore(testCfg)
	defer s.Close()
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
	os.Remove("teststore.data")
	s := newTestIntPlasmaStore(testCfg)
	defer s.Close()
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

func TestPlasmaRecovery(t *testing.T) {
	os.Remove("teststore.data")
	s := newTestIntPlasmaStore(testCfg)
	defer s.Close()
	w := s.NewWriter()
	for i := 0; i < 130000; i++ {
		w.Insert(skiplist.NewIntKeyItem(i))
	}
	for i := 0; i < 100000; i++ {
		w.Delete(skiplist.NewIntKeyItem(i))
	}

	fmt.Println(s.GetStats())
	s.PersistAll()

	s.Close()
	s = newTestIntPlasmaStore(testCfg)
	w = s.NewWriter()
	fmt.Println(s.GetStats())

	fmt.Println(s.Skiplist.GetStats())

	for i := 0; i < 100000; i++ {
		itm := skiplist.NewIntKeyItem(i)
		got, _ := w.Lookup(itm)
		if got != nil {
			t.Errorf("expected nil %v", got)
		}
	}

	for i := 100000; i < 130000; i++ {
		itm := skiplist.NewIntKeyItem(i)
		got, _ := w.Lookup(itm)
		if got == nil {
			t.Errorf("mismatch %d != nil", i)
		} else if skiplist.CompareInt(itm, got) != 0 {
			t.Errorf("mismatch %d != %d", i, skiplist.IntFromItem(got))
		}
	}
}

func TestPlasmaLSSCleaner(t *testing.T) {
	os.Remove("teststore.data")
	cfg := testCfg
	cfg.LSSCleanerThreshold = 10
	s := newTestIntPlasmaStore(cfg)
	defer s.Close()

	w := s.NewWriter()

	for i := 0; i < 10000000; i++ {
		w.Insert(skiplist.NewIntKeyItem(i))
	}

	_, ds0, used0 := s.GetLSSInfo()
	s.PersistAll()
	fmt.Println(s.GetStats(), "\n")

	go func() {
		for {
			s.PersistAll()
			time.Sleep(time.Second * time.Duration(5))
		}
	}()

	go s.lssCleanerDaemon()
	for x := 0; x < 20; x++ {
		fmt.Println("Running iteration..", x)
		for i := 0; i < 1000000; i++ {
			itm := skiplist.NewIntKeyItem(i)
			w.Delete(itm)
			w.Insert(itm)
		}

		fmt.Println(s.GetStats())
	}

	time.Sleep(time.Second)
	frag, ds, used := s.GetLSSInfo()

	fmt.Printf("LSSInfo: frag:%d, ds:%d, used:%d\n", frag, ds, used)
	if used > used0*110/100 || ds > ds0*110/100 {
		t.Errorf("Expected better cleaning with frag ~ 10%")
	}
}

func TestPlasmaCleanerPerf(t *testing.T) {
	var wg sync.WaitGroup

	os.Remove("teststore.data")
	numThreads := 8
	n := 100000000
	nPerThr := n / numThreads
	cfg := testCfg
	cfg.LSSCleanerThreshold = 10
	cfg.AutoLSSCleaning = true
	s := newTestIntPlasmaStore(cfg)
	defer s.Close()
	total := numThreads * nPerThr

	t0 := time.Now()
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		w := s.NewWriter()
		go doInsert(w, &wg, i, nPerThr)
	}
	wg.Wait()

	dur := time.Since(t0)
	fmt.Printf("%d items insert took %v -> %v items/s\n", total, dur, float64(total)/float64(dur.Seconds()))
	s.PersistAll()

	t0 = time.Now()
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		w := s.NewWriter()
		go doUpdate(w, &wg, i, nPerThr)
	}
	wg.Wait()
	dur = time.Since(t0)
	fmt.Printf("%d items update took %v -> %v items/s\n", total, dur, float64(total)/float64(dur.Seconds()))

	fmt.Println(s.GetStats())

	frag, ds, used := s.GetLSSInfo()

	fmt.Printf("LSSInfo: frag:%d, ds:%d, used:%d\n", frag, ds, used)

}
