package skiplist

import "testing"
import "fmt"
import "math/rand"
import "runtime"
import "sync"
import "time"

func TestInsert(t *testing.T) {
	s := New()
	cmp := CompareBytes
	buf := s.MakeBuf()
	defer s.FreeBuf(buf)

	for i := 0; i < 2000; i++ {
		s.Insert(NewByteKeyItem([]byte(fmt.Sprintf("%010d", i))), cmp, buf)
	}

	for i := 1750; i < 2000; i++ {
		s.Delete(NewByteKeyItem([]byte(fmt.Sprintf("%010d", i))), cmp, buf)
	}

	itr := s.NewIterator(cmp, buf)
	count := 0
	itr.SeekFirst()
	itr.Seek(NewByteKeyItem([]byte(fmt.Sprintf("%010d", 1500))))
	for ; itr.Valid(); itr.Next() {
		expected := fmt.Sprintf("%010d", count+1500)
		got := string(*(itr.Get().(*byteKeyItem)))
		count++
		if got != expected {
			t.Errorf("Expected %s, got %v", expected, got)
		}
	}

	if count != 250 {
		t.Errorf("Expected count = 250, got %v", count)
	}
}

func doInsert(sl *Skiplist, wg *sync.WaitGroup, n int, isRand bool) {
	defer wg.Done()
	buf := sl.MakeBuf()
	defer sl.FreeBuf(buf)

	cmp := CompareInt
	rnd := rand.New(rand.NewSource(int64(rand.Int())))
	for i := 0; i < n; i++ {
		var val int
		if isRand {
			val = rnd.Int()
		} else {
			val = i
		}

		itm := intKeyItem(val)
		sl.Insert2(&itm, cmp, buf, rnd.Float32)
	}
}

func doGet(sl *Skiplist, wg *sync.WaitGroup, n int) {
	defer wg.Done()
	rnd := rand.New(rand.NewSource(int64(rand.Int())))
	cmp := CompareInt
	buf := sl.MakeBuf()
	defer sl.FreeBuf(buf)

	itr := sl.NewIterator(cmp, buf)
	for i := 0; i < n; i++ {
		val := rnd.Int() % n
		itm := intKeyItem(val)
		itr.Seek(&itm)
	}

}

func TestInsertPerf(t *testing.T) {
	var wg sync.WaitGroup
	sl := New()
	n := 1000000
	t0 := time.Now()
	total := n * runtime.GOMAXPROCS(0)
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go doInsert(sl, &wg, n, true)
	}
	wg.Wait()

	dur := time.Since(t0)

	fmt.Printf("%d items took %v -> %v items/s conflicts %v\n", total, dur, float64(total)/float64(dur.Seconds()), sl.GetStats().InsertConflicts)
}

func TestGetPerf(t *testing.T) {
	var wg sync.WaitGroup
	sl := New()
	n := 1000000
	wg.Add(1)
	go doInsert(sl, &wg, n, false)
	wg.Wait()

	t0 := time.Now()
	total := n * runtime.GOMAXPROCS(0)
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go doGet(sl, &wg, n)
	}
	wg.Wait()
	dur := time.Since(t0)
	fmt.Printf("%d items took %v -> %v items/s\n", total, dur, float64(total)/float64(dur.Seconds()))

}
