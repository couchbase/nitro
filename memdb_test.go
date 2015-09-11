package memdb

import "fmt"
import "os"
import "testing"
import "time"
import "math/rand"
import "sync"
import "runtime"
import "encoding/binary"

func TestInsert(t *testing.T) {
	db := New()
	w := db.NewWriter()
	for i := 0; i < 2000; i++ {
		w.Put(NewItem([]byte(fmt.Sprintf("%010d", i))))
	}

	for i := 1750; i < 2000; i++ {
		w.Delete(NewItem([]byte(fmt.Sprintf("%010d", i))))
	}
	snap := w.NewSnapshot()

	for i := 2000; i < 5000; i++ {
		w.Put(NewItem([]byte(fmt.Sprintf("%010d", i))))
	}

	_ = w.NewSnapshot()

	itr := w.NewIterator(snap)
	count := 0
	itr.SeekFirst()
	itr.Seek(NewItem([]byte(fmt.Sprintf("%010d", 1500))))
	for ; itr.Valid(); itr.Next() {
		expected := fmt.Sprintf("%010d", count+1500)
		got := string(itr.Get().data)
		count++
		if got != expected {
			t.Errorf("Expected %s, got %v", expected, got)
		}
	}

	if count != 250 {
		t.Errorf("Expected count = 250, got %v", count)
	}
}

func doInsert(db *MemDB, wg *sync.WaitGroup, n int, isRand bool, shouldSnap bool) {
	defer wg.Done()
	w := db.NewWriter()
	rnd := rand.New(rand.NewSource(int64(rand.Int())))
	for i := 0; i < n; i++ {
		var val int
		if isRand {
			val = rnd.Int()
		} else {
			val = i
		}
		if shouldSnap && i%100000 == 0 {
			s := w.NewSnapshot()
			s.Close()
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(val))
		// w.Put(NewItem([]byte(fmt.Sprintf("%025d", val))))
		w.Put(NewItem(buf))
	}
}

func TestInsertPerf(t *testing.T) {
	var wg sync.WaitGroup
	db := New()
	n := 1000000
	t0 := time.Now()
	total := n * runtime.GOMAXPROCS(0)
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go doInsert(db, &wg, n, true, true)
	}
	wg.Wait()

	dur := time.Since(t0)
	fmt.Printf("%d items took %v -> %v items/s snapshots_created %v live_snapshots %v\n",
		total, dur, float64(total)/float64(dur.Seconds()), db.getCurrSn(), len(db.GetSnapshots()))
}

func doGet(t *testing.T, db *MemDB, snap *Snapshot, wg *sync.WaitGroup, n int) {
	defer wg.Done()
	rnd := rand.New(rand.NewSource(int64(rand.Int())))

	buf := make([]byte, 8)
	itr := db.NewIterator(snap)
	for i := 0; i < n; i++ {
		val := rnd.Int() % n
		binary.BigEndian.PutUint64(buf, uint64(val))
		// itr.Seek(NewItem([]byte(fmt.Sprintf("%025d", val))))
		itr.Seek(NewItem(buf))
		if !itr.Valid() {
			t.Errorf("Expected to find %v", val)
		}
	}
}

func TestGetPerf(t *testing.T) {
	var wg sync.WaitGroup
	db := New()
	n := 1000000
	wg.Add(1)
	go doInsert(db, &wg, n, false, true)
	wg.Wait()
	snap := db.NewSnapshot()

	t0 := time.Now()
	total := n * runtime.GOMAXPROCS(0)
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go doGet(t, db, snap, &wg, n)
	}
	wg.Wait()
	dur := time.Since(t0)
	fmt.Printf("%d items took %v -> %v items/s\n", total, dur, float64(total)/float64(dur.Seconds()))
}

func CountItems(db *MemDB, snap *Snapshot) int {
	var count int
	itr := db.NewIterator(snap)
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		count++
	}
	itr.Close()
	return count
}

func TestLoadStoreDisk(t *testing.T) {
	os.RemoveAll("db.dump")
	var wg sync.WaitGroup
	cfg := DefaultConfig()
	db := NewWithConfig(cfg)
	n := 1000000
	t0 := time.Now()
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go doInsert(db, &wg, n/runtime.GOMAXPROCS(0), true, true)
	}
	wg.Wait()
	fmt.Printf("Inserting %v items took %v\n", n, time.Since(t0))
	snap := db.NewSnapshot()
	snap = db.NewSnapshot()
	fmt.Println(db.store.GetStats())

	t0 = time.Now()
	err := db.StoreToDisk("db.dump", snap, nil)
	if err != nil {
		t.Errorf("Expected no error. got=%v", err)
	}

	fmt.Printf("Storing to disk took %v\n", time.Since(t0))

	snap.Close()
	db = NewWithConfig(cfg)
	t0 = time.Now()
	snap, err = db.LoadFromDisk("db.dump", nil)
	if err != nil {
		t.Errorf("Expected no error. got=%v", err)
	}
	fmt.Printf("Loading from disk took %v\n", time.Since(t0))

	count := CountItems(db, snap)
	if count != n {
		t.Errorf("Expected %v, got %v", n, count)
	}
	fmt.Println(db.store.GetStats())
}

func TestDelete(t *testing.T) {
	expected := 10
	db := New()
	w := db.NewWriter()
	for i := 0; i < expected; i++ {
		w.Put(NewItem([]byte(fmt.Sprintf("%010d", i))))
	}

	snap1 := w.NewSnapshot()
	got := CountItems(db, snap1)
	if got != expected {
		t.Errorf("Expected 2000, got %d", got)
	}
	fmt.Println(db.store.GetStats())

	for i := 0; i < expected; i++ {
		w.Delete(NewItem([]byte(fmt.Sprintf("%010d", i))))
	}

	for i := 0; i < expected; i++ {
		w.Put(NewItem([]byte(fmt.Sprintf("%010d", i))))
	}
	snap2 := w.NewSnapshot()
	snap1.Close()
	snap3 := w.NewSnapshot()
	snap2.Close()
	time.Sleep(time.Second)

	got = CountItems(db, snap3)
	snap3.Close()

	if got != expected {
		t.Errorf("Expected %d, got %d", expected, got)
	}
	fmt.Println(db.store.GetStats())
}

func doReplace(wg *sync.WaitGroup, t *testing.T, w *Writer, start, end int) {
	defer wg.Done()

	for ; start < end; start++ {
		w.Delete(NewItem([]byte(fmt.Sprintf("%010d", start))))
		w.Put(NewItem([]byte(fmt.Sprintf("%010d", start))))
	}
}

func TestGCPerf(t *testing.T) {
	var wg sync.WaitGroup
	var last *Snapshot

	db := New()
	perW := 1000
	iterations := 1000
	nW := runtime.GOMAXPROCS(0)

	var ws []*Writer

	for i := 0; i < nW; i++ {
		ws = append(ws, db.NewWriter())
	}

	nc := 0
	for x := 0; x < iterations; x++ {
		for i := 0; i < nW; i++ {
			wg.Add(1)
			go doReplace(&wg, t, ws[i], i*perW, i*perW+perW)
		}
		wg.Wait()
		curr := db.NewSnapshot()
		if last != nil {
			last.Close()
		}

		last = curr
		nc += db.store.GetStats().NodeCount
	}

	db.NewSnapshot()
	last.Close()

	waits := 0
	for db.store.GetStats().NodeCount > nW*perW {
		time.Sleep(time.Millisecond)
		waits++
	}

	fmt.Printf("final_node_count = %v, average_live_node_count = %v, wait_time_for_collection = %vms\n", db.store.GetStats().NodeCount, nc/iterations, waits)
}
