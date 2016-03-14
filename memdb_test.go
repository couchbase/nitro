package memdb

import "fmt"
import "sync/atomic"
import "os"
import "testing"
import "time"
import "math/rand"
import "sync"
import "runtime"
import "encoding/binary"

//import "github.com/t3rm1n4l/memdb/mm"

var testConf Config

func init() {
	testConf = DefaultConfig()
	//testConf.UseMemoryMgmt(mm.Malloc, mm.Free)
}

func TestInsert(t *testing.T) {
	db := NewWithConfig(testConf)
	defer db.Close()

	w := db.NewWriter()
	for i := 0; i < 2000; i++ {
		w.Put([]byte(fmt.Sprintf("%010d", i)))
	}

	for i := 1750; i < 2000; i++ {
		w.Delete([]byte(fmt.Sprintf("%010d", i)))
	}
	snap, _ := w.NewSnapshot()

	for i := 2000; i < 5000; i++ {
		w.Put([]byte(fmt.Sprintf("%010d", i)))
	}

	w.NewSnapshot()

	count := 0
	itr := db.NewIterator(snap)
	itr.SeekFirst()
	itr.Seek([]byte(fmt.Sprintf("%010d", 1500)))
	for ; itr.Valid(); itr.Next() {
		expected := fmt.Sprintf("%010d", count+1500)
		got := string(itr.Get())
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
			s, _ := w.NewSnapshot()
			s.Close()
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(val))
		w.Put(buf)
	}
}

func TestInsertPerf(t *testing.T) {
	var wg sync.WaitGroup
	db := NewWithConfig(testConf)
	defer db.Close()
	n := 20000000 / runtime.GOMAXPROCS(0)
	t0 := time.Now()
	total := n * runtime.GOMAXPROCS(0)
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go doInsert(db, &wg, n, true, true)
	}
	wg.Wait()

	snap, _ := db.NewSnapshot()
	dur := time.Since(t0)
	VerifyCount(snap, n*runtime.GOMAXPROCS(0), t)
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
		itr.Seek(buf)
		if !itr.Valid() {
			t.Errorf("Expected to find %v", val)
		}
	}
}

func TestInsertDuplicates(t *testing.T) {
	db := NewWithConfig(testConf)
	defer db.Close()

	w := db.NewWriter()
	for i := 0; i < 2000; i++ {
		w.Put([]byte(fmt.Sprintf("%010d", i)))
	}

	w.NewSnapshot()

	// Duplicate
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("%010d", i)
		newNode := w.Put2([]byte(key))
		if newNode != nil {
			t.Errorf("Duplicate unexpected for %s", key)
		}
	}

	for i := 1500; i < 2000; i++ {
		w.Delete([]byte(fmt.Sprintf("%010d", i)))
	}
	w.NewSnapshot()

	for i := 1500; i < 5000; i++ {
		key := fmt.Sprintf("%010d", i)
		newNode := w.Put2([]byte(key))
		if newNode == nil {
			t.Errorf("Expected successful insert for %s", key)
		}
	}

	snap, _ := w.NewSnapshot()
	count := 0
	itr := db.NewIterator(snap)
	itr.SeekFirst()
	for ; itr.Valid(); itr.Next() {
		expected := fmt.Sprintf("%010d", count)
		got := string(itr.Get())
		count++
		if got != expected {
			t.Errorf("Expected %s, got %v", expected, got)
		}
	}

	if count != 5000 {
		t.Errorf("Expected count = 5000, got %v", count)
	}
}

func TestGetPerf(t *testing.T) {
	var wg sync.WaitGroup
	db := NewWithConfig(testConf)
	defer db.Close()
	n := 1000000
	wg.Add(1)
	go doInsert(db, &wg, n, false, true)
	wg.Wait()
	snap, _ := db.NewSnapshot()
	VerifyCount(snap, n, t)

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

func VerifyCount(snap *Snapshot, n int, t *testing.T) {

	if c := CountItems(snap); c != n {
		t.Errorf("Expected count %d, got %d", n, c)
	}
}

func CountItems(snap *Snapshot) int {
	var count int
	itr := snap.NewIterator()
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		count++
	}
	itr.Close()
	return count
}

func TestLoadStoreDisk(t *testing.T) {
	os.RemoveAll("db.dump")
	var wg sync.WaitGroup
	db := NewWithConfig(testConf)
	defer db.Close()
	n := 1000000
	t0 := time.Now()
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go doInsert(db, &wg, n/runtime.GOMAXPROCS(0), true, true)
	}
	wg.Wait()
	fmt.Printf("Inserting %v items took %v\n", n, time.Since(t0))
	snap, _ := db.NewSnapshot()
	snap, _ = db.NewSnapshot()
	fmt.Println(db.DumpStats())

	t0 = time.Now()
	err := db.StoreToDisk("db.dump", snap, 8, nil)
	if err != nil {
		t.Errorf("Expected no error. got=%v", err)
	}

	fmt.Printf("Storing to disk took %v\n", time.Since(t0))

	snap.Close()
	db = NewWithConfig(testConf)
	defer db.Close()
	t0 = time.Now()
	snap, err = db.LoadFromDisk("db.dump", 8, nil)
	if err != nil {
		t.Errorf("Expected no error. got=%v", err)
	}
	fmt.Printf("Loading from disk took %v\n", time.Since(t0))

	count := CountItems(snap)
	if count != n {
		t.Errorf("Expected %v, got %v", n, count)
	}

	count = int(snap.Count())
	if count != n {
		t.Errorf("Count mismatch on snapshot. Expected %d, got %d", n, count)
	}
	fmt.Println(db.DumpStats())
}

func TestStoreDiskShutdown(t *testing.T) {
	os.RemoveAll("db.dump")
	var wg sync.WaitGroup
	db := NewWithConfig(testConf)
	n := 1000000
	t0 := time.Now()
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go doInsert(db, &wg, n/runtime.GOMAXPROCS(0), true, true)
	}
	wg.Wait()
	fmt.Printf("Inserting %v items took %v\n", n, time.Since(t0))
	snap, _ := db.NewSnapshot()
	snap, _ = db.NewSnapshot()
	fmt.Println(db.DumpStats())

	t0 = time.Now()
	errch := make(chan error)
	go func() {
		errch <- db.StoreToDisk("db.dump", snap, 8, nil)
	}()

	db.Close()

	if err := <-errch; err != ErrShutdown {
		t.Errorf("Expected ErrShutdown. got=%v", err)
	}
}

func TestDelete(t *testing.T) {
	expected := 10
	db := NewWithConfig(testConf)
	defer db.Close()
	w := db.NewWriter()
	for i := 0; i < expected; i++ {
		w.Put([]byte(fmt.Sprintf("%010d", i)))
	}

	snap1, _ := w.NewSnapshot()
	got := CountItems(snap1)
	if got != expected {
		t.Errorf("Expected 2000, got %d", got)
	}
	fmt.Println(db.DumpStats())

	for i := 0; i < expected; i++ {
		w.Delete([]byte(fmt.Sprintf("%010d", i)))
	}

	for i := 0; i < expected; i++ {
		w.Put([]byte(fmt.Sprintf("%010d", i)))
	}
	snap2, _ := w.NewSnapshot()
	snap1.Close()
	snap3, _ := w.NewSnapshot()
	snap2.Close()
	time.Sleep(time.Second)

	got = CountItems(snap3)
	snap3.Close()

	if got != expected {
		t.Errorf("Expected %d, got %d", expected, got)
	}
	fmt.Println(db.DumpStats())
}

func doReplace(wg *sync.WaitGroup, t *testing.T, w *Writer, start, end int) {
	defer wg.Done()

	for ; start < end; start++ {
		w.Delete([]byte(fmt.Sprintf("%010d", start)))
		w.Put([]byte(fmt.Sprintf("%010d", start)))
	}
}

func TestGCPerf(t *testing.T) {
	var wg sync.WaitGroup
	var last *Snapshot

	db := NewWithConfig(testConf)
	defer db.Close()
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
		curr, _ := db.NewSnapshot()
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

func TestMemoryInUse(t *testing.T) {
	db := NewWithConfig(testConf)
	defer db.Close()

	dumpStats := func() {
		fmt.Printf("ItemsCount: %v, MemoryInUse: %v, NodesCount: %v\n", db.ItemsCount(), MemoryInUse(), db.store.GetStats().NodeCount)
	}
	w := db.NewWriter()
	for i := 0; i < 5000; i++ {
		w.Put([]byte(fmt.Sprintf("%010d", i)))
	}
	snap1, _ := w.NewSnapshot()

	dumpStats()

	for i := 0; i < 5000; i++ {
		w.Delete([]byte(fmt.Sprintf("%010d", i)))
	}

	snap1.Close()
	snap2, _ := w.NewSnapshot()
	w.NewSnapshot()
	snap2.Close()
	time.Sleep(time.Second)
	dumpStats()
}

func TestFullScan(t *testing.T) {
	var wg sync.WaitGroup
	db := NewWithConfig(testConf)
	defer db.Close()
	n := 1000000
	t0 := time.Now()
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go doInsert(db, &wg, n/runtime.GOMAXPROCS(0), true, true)
	}
	wg.Wait()
	fmt.Printf("Inserting %v items took %v\n", n, time.Since(t0))
	snap, _ := db.NewSnapshot()
	VerifyCount(snap, n, t)
	fmt.Println(db.DumpStats())

	t0 = time.Now()
	c := CountItems(snap)
	fmt.Printf("Full iteration of %d items took %v\n", c, time.Since(t0))
}

func TestVisitor(t *testing.T) {
	const shards = 32
	const concurrency = 8
	const n = 1000000

	var wg sync.WaitGroup
	db := NewWithConfig(testConf)
	defer db.Close()
	expectedSum := int64((n - 1) * (n / 2))

	wg.Add(1)
	doInsert(db, &wg, n, false, false)
	snap, _ := db.NewSnapshot()
	fmt.Println(db.DumpStats())

	var counts [shards]int64
	var startEndRange [shards][2]uint64
	var sum int64

	callb := func(itm *Item, shard int) error {
		v := binary.BigEndian.Uint64(itm.Bytes())
		atomic.AddInt64(&sum, int64(v))
		atomic.AddInt64(&counts[shard], 1)

		if shard > 0 && startEndRange[shard][0] == 0 {
			startEndRange[shard][0] = v
		} else {
			if startEndRange[shard][1] > v {
				t.Errorf("shard-%d validation of sort order %d > %d", shard, startEndRange[shard][1], v)
			}
			startEndRange[shard][1] = v
		}

		return nil
	}

	total := 0
	t0 := time.Now()
	db.Visitor(snap, callb, shards, concurrency)
	dur := time.Since(t0)
	fmt.Printf("Took %v to iterate %v items, %v items/s\n", dur, n, float32(n)/float32(dur.Seconds()))

	for i, v := range counts {
		fmt.Printf("shard - %d count = %d, range: %d-%d\n", i, v, startEndRange[i][0], startEndRange[i][1])
		total += int(v)
	}

	if total != n {
		t.Errorf("Expected count %d, received %d", n, total)
	}

	if expectedSum != sum {
		t.Errorf("Expected sum %d, received %d", expectedSum, sum)
	}
}

func TestVisitorError(t *testing.T) {
	const n = 100000
	var wg sync.WaitGroup
	db := NewWithConfig(testConf)
	defer db.Close()

	wg.Add(1)
	doInsert(db, &wg, n, false, false)
	snap, _ := db.NewSnapshot()

	errVisitor := fmt.Errorf("visitor failed")
	callb := func(itm *Item, shard int) error {
		v := binary.BigEndian.Uint64(itm.Bytes())
		if v == 90000 {
			return errVisitor
		}
		return nil
	}

	if db.Visitor(snap, callb, 4, 4) != errVisitor {
		t.Errorf("Expected error")
	}
}

func doUpdate(db *MemDB, wg *sync.WaitGroup, w *Writer, start, end int, version int) {
	defer wg.Done()
	for ; start < end; start++ {
		oldval := uint64(start) + uint64(version-1)*10000000
		val := uint64(start) + uint64(version)*10000000
		buf1 := make([]byte, 8)
		binary.BigEndian.PutUint64(buf1, uint64(val))
		buf2 := make([]byte, 8)
		binary.BigEndian.PutUint64(buf2, uint64(oldval))
		if version > 1 {
			if !w.Delete(buf2) {
				panic("delete failed")
			}
		}
		w.Put(buf1)
	}
}

func TestLoadDeltaStoreDisk(t *testing.T) {
	os.RemoveAll("db.dump")
	conf := DefaultConfig()
	conf.UseDeltaInterleaving()
	db := NewWithConfig(conf)

	var writers []*Writer
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		writers = append(writers, db.NewWriter())
	}

	n := 1000000
	chunk := n / runtime.GOMAXPROCS(0)
	version := 0

	doMutate := func() *Snapshot {
		var wg sync.WaitGroup
		version++
		for i := 0; i < runtime.GOMAXPROCS(0); i++ {
			wg.Add(1)
			start := i * chunk
			end := start + chunk
			go doUpdate(db, &wg, writers[i], start, end, version)
		}
		wg.Wait()

		snap, _ := db.NewSnapshot()
		return snap
	}

	var snap *Snapshot
	for x := 0; x < 2; x++ {
		if snap != nil {
			snap.Close()
		}
		snap = doMutate()
	}

	waiter := make(chan bool)
	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() {
		defer wg2.Done()

		var snap *Snapshot
		for x := 0; x < 10; x++ {
			if snap != nil {
				snap.Close()
			}

			snap = doMutate()
			if x == 0 {
				close(waiter)
			}
		}

		snap.Close()
		count := db.gcsnapshots.GetStats().NodeCount

		for count > 5 {
			time.Sleep(time.Second)
			count = db.gcsnapshots.GetStats().NodeCount
		}
	}()

	callb := func(itm *ItemEntry) {
		<-waiter
	}

	t0 := time.Now()
	err := db.StoreToDisk("db.dump", snap, 8, callb)
	if err != nil {
		t.Errorf("Expected no error. got=%v", err)
	}

	fmt.Printf("Storing to disk took %v\n", time.Since(t0))

	wg2.Wait()
	db.Close()

	db = NewWithConfig(conf)
	defer db.Close()
	t0 = time.Now()
	snap, err = db.LoadFromDisk("db.dump", 8, nil)
	if err != nil {
		t.Errorf("Expected no error. got=%v", err)
	}
	fmt.Printf("Loading from disk took %v\n", time.Since(t0))

	count := CountItems(snap)
	if count != n {
		t.Errorf("Expected %v, got %v", n, count)
	}

	count = int(snap.Count())
	if count != n {
		t.Errorf("Count mismatch on snapshot. Expected %d, got %d", n, count)
	}

	itr := snap.NewIterator()
	i := 0
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		itm := itr.Get()
		val := binary.BigEndian.Uint64(itm)
		exp := uint64(i) + uint64(2)*10000000

		if val != exp {
			t.Errorf("expected %d, got %d", exp, val)
		}
		i++
	}
	itr.Close()

	fmt.Println(db.DumpStats())
	fmt.Println("Restored", db.DeltaRestored)
	fmt.Println("RestoredFailed", db.DeltaRestoreFailed)
}
