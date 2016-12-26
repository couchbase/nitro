package plasma

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
	"unsafe"
)

var testSnCfg = Config{
	MaxDeltaChainLen: 200,
	MaxPageItems:     400,
	MinPageItems:     25,
	Compare:          cmpItem,
	ItemSize: func(itm unsafe.Pointer) uintptr {
		return uintptr((*item)(itm).Size())
	},
	File:                "teststore.data",
	FlushBufferSize:     1024 * 1024,
	LSSCleanerThreshold: 10,
	AutoLSSCleaning:     true,
	AutoSwapper:         true,
	EnableShapshots:     true,
	MaxMemoryUsage:      5 * 1024 * 1024 * 1024,
}

func TestMVCCSimple(t *testing.T) {
	os.RemoveAll("teststore.data")
	s := newTestIntPlasmaStore(testSnCfg)
	defer s.Close()

	w := s.NewWriter()
	for i := 0; i < 10000; i++ {
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i)), []byte(fmt.Sprintf("val-%10d", i)))
	}

	count := 0
	snap1 := s.NewSnapshot()

	for i := 0; i < 9990; i++ {
		w.DeleteKV([]byte(fmt.Sprintf("key-%10d", i)))
	}

	k := []byte(fmt.Sprintf("key-%10d", 10))
	w.InsertKV(k, []byte("newval"))

	snap2 := s.NewSnapshot()

	itr2 := snap2.NewIterator()
	for itr2.SeekFirst(); itr2.Valid(); itr2.Next() {
		count++
	}

	if count != 11 {
		t.Errorf("Expected 10, got %d", count)
	}

	itr2.Seek(k)
	if string(itr2.Value()) != "newval" {
		t.Errorf("expected newval, got %s %v", string(itr2.Value()), itr2.Valid())
	}

	count = 0
	itr1 := snap1.NewIterator()
	for itr1.SeekFirst(); itr1.Valid(); itr1.Next() {
		count++
	}

	if count != 10000 {
		t.Errorf("Expected 10000, got %d", count)
	}
}

func TestMVCCLookup(t *testing.T) {
	os.RemoveAll("teststore.data")
	s := newTestIntPlasmaStore(testSnCfg)
	defer s.Close()

	w := s.NewWriter()
	for i := 0; i < 10000; i++ {
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i)), []byte(fmt.Sprintf("val-%10d", i)))
	}

	k := []byte(fmt.Sprintf("key-%10d", 1000))
	w.InsertKV(k, []byte(fmt.Sprintf("%d", 1)))
	w.InsertKV(k, []byte(fmt.Sprintf("%d", 2)))
	w.InsertKV(k, []byte(fmt.Sprintf("%d", 3)))
	w.InsertKV(k, []byte(fmt.Sprintf("%d", 4)))
	w.InsertKV(k, []byte(fmt.Sprintf("%d", 5)))

	v, _ := w.LookupKV(k)
	if string(v) != "5" {
		t.Errorf("Expected 5, got %s", v)
	}

	w.CompactAll()

	v, _ = w.LookupKV(k)
	if string(v) != "5" {
		t.Errorf("Expected 5, got %s", v)
	}

	iv, err := w.LookupKV([]byte("invalid"))
	if err != ErrItemNotFound && iv == nil {
		t.Errorf("Expected err, got %v %v", err, iv)
	}
}

func TestMVCCGarbageCollection(t *testing.T) {
	os.RemoveAll("teststore.data")
	s := newTestIntPlasmaStore(testSnCfg)
	defer s.Close()

	w := s.NewWriter()
	for i := 0; i < 1000; i++ {
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i)), []byte("sn1"))
	}

	snap1 := s.NewSnapshot()

	for i := 0; i < 1000; i++ {
		w.DeleteKV([]byte(fmt.Sprintf("key-%10d", i)))
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i)), []byte("sn2"))
	}

	snap2 := s.NewSnapshot()

	for i := 0; i < 1000; i++ {
		w.DeleteKV([]byte(fmt.Sprintf("key-%10d", i)))
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i)), []byte("sn3"))
	}

	snap3 := s.NewSnapshot()

	for i := 0; i < 1000; i++ {
		w.DeleteKV([]byte(fmt.Sprintf("key-%10d", i)))
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i)), []byte("sn4"))
	}

	snap4 := s.NewSnapshot()

	itr := s.NewIterator()

	count := func() int {
		count := 0
		for itr.SeekFirst(); itr.Valid(); itr.Next() {
			count++
		}
		return count
	}

	if c := count(); c != 7000 {
		t.Errorf("Expected 7000, got %d", c)
	}

	snap2.Close()
	w.CompactAll()

	if c := count(); c != 7000 {
		t.Errorf("Expected 7000, got %d", c)
	}

	snap1.Close()
	w.CompactAll()

	if c := count(); c != 3000 {
		t.Errorf("Expected 3000, got %d", c)
	}

	snap4.Close()
	w.CompactAll()

	if c := count(); c != 3000 {
		t.Errorf("Expected 3000, got %d", c)
	}

	snap3.Close()
	w.CompactAll()

	if c := count(); c != 1000 {
		t.Errorf("Expected 5000, got %d", c)
	}
}

func doInsertMVCC(w *Writer, wg *sync.WaitGroup, id, n int) {
	defer wg.Done()

	buf := make([]byte, 8)

	for i := 0; i < n; i++ {
		val := i + id*n
		binary.BigEndian.PutUint64(buf, uint64(val))
		w.InsertKV(buf, nil)
	}
}

func doUpdateMVCC(w *Writer, wg *sync.WaitGroup, id, n int, itern int) {
	defer wg.Done()

	kbuf := make([]byte, 8)
	vbuf := make([]byte, 8)

	for i := 0; i < n; i++ {
		val := i + id*n
		binary.BigEndian.PutUint64(kbuf, uint64(val))
		binary.BigEndian.PutUint64(vbuf, uint64(itern))
		w.DeleteKV(kbuf)
		w.InsertKV(kbuf, vbuf)
	}
}

func SkipTestPlasmaMVCCPerf(t *testing.T) {
	var wg sync.WaitGroup

	os.RemoveAll("teststore.data")
	numThreads := 8
	n := 20000000
	iterations := 5
	nPerThr := n / numThreads
	s := newTestIntPlasmaStore(testSnCfg)
	defer s.Close()
	total := numThreads * nPerThr

	t0 := time.Now()
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		w := s.NewWriter()
		go doInsertMVCC(w, &wg, i, nPerThr)
	}
	wg.Wait()

	dur := time.Since(t0)

	fmt.Println(s.GetStats())
	fmt.Printf("%d items insert took %v -> %v items/s\n", total, dur, float64(total)/float64(dur.Seconds()))

	s.NewSnapshot().Close()

	for x := 0; x < iterations; x++ {
		fmt.Println("Starting update iteration ", x)
		t0 := time.Now()
		for i := 0; i < numThreads; i++ {
			wg.Add(1)
			w := s.NewWriter()
			go doUpdateMVCC(w, &wg, i, nPerThr, x)
		}
		wg.Wait()

		dur := time.Since(t0)

		s.NewSnapshot().Close()
		fmt.Println(s.GetStats())
		fmt.Printf("%d items update took %v -> %v items/s\n", total, dur, float64(total)/float64(dur.Seconds()))
	}

}

func TestPlasmaRecoveryPoint(t *testing.T) {
	os.RemoveAll("teststore.data")
	s := newTestIntPlasmaStore(testSnCfg)

	w := s.NewWriter()
	for i := 1; i < 100000; i++ {
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i)), []byte(fmt.Sprintf("val-%10d", i)))
		if i%1000 == 0 {
			snap := s.NewSnapshot()
			if i%10000 == 0 {
				snap.Open()
				s.CreateRecoveryPoint(snap, []byte(fmt.Sprint(i)))
			}
			snap.Close()
		}
	}

	rpts := s.GetRecoveryPoints()
	l := len(rpts)
	for i := 0; i < l-4; i++ {
		s.RemoveRecoveryPoint(rpts[i])
	}

	fmt.Printf("(1) Recovery points gcSn:%d, minRPSn:%v\n", s.gcSn, s.rpSns)
	for _, rpt := range s.GetRecoveryPoints() {
		fmt.Printf("recovery_point sn:%d meta:%s\n", rpt.sn, string(rpt.meta))
	}

	s.PersistAll()
	s.Close()

	fmt.Println("Reopening database...")
	s = newTestIntPlasmaStore(testSnCfg)

	fmt.Printf("(2) Recovery points gcSn:%d, minRPSn:%v\n", s.gcSn, s.rpSns)
	for _, rpt := range s.GetRecoveryPoints() {
		fmt.Printf("recovery_point sn:%d meta:%s\n", rpt.sn, string(rpt.meta))
	}

	rpts = s.GetRecoveryPoints()
	rb := rpts[2]
	snap, _ := s.Rollback(rb)
	fmt.Println("Rollbacked to", string(rb.meta))

	itr := snap.NewIterator()
	count := 0
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		count++
	}

	var expected1 int
	fmt.Sscan(string(rb.meta), &expected1)
	if count != expected1 {
		t.Errorf("Expected %d, got %d", expected1, count)
	}

	w = s.NewWriter()
	for i := 1; i < 100000; i++ {
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i+100000)), []byte(fmt.Sprintf("val-%10d", i+100000)))
		if i%1000 == 0 {
			snap := s.NewSnapshot()
			if i%10000 == 0 {
				snap.Open()
				s.CreateRecoveryPoint(snap, []byte(fmt.Sprint(i+100000)))
			}
			snap.Close()
		}
	}

	s.Close()

	fmt.Println("Reopening database...")
	s = newTestIntPlasmaStore(testSnCfg)

	w = s.NewWriter()
	fmt.Printf("(3) Recovery points gcSn:%d, minRPSn:%v\n", s.gcSn, s.rpSns)
	for _, rpt := range s.GetRecoveryPoints() {
		fmt.Printf("recovery_point sn:%d meta:%s\n", rpt.sn, string(rpt.meta))
	}

	rpts = s.GetRecoveryPoints()
	rb = rpts[8]
	snap, _ = s.Rollback(rb)
	fmt.Println("Rollbacked to", string(rb.meta))

	itr = snap.NewIterator()
	count = 0
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		count++
	}

	var expected2 int
	fmt.Sscan(string(rb.meta), &expected2)
	expected2 = expected2 - 100000 + expected1
	if count != expected2 {
		t.Errorf("Expected %d, got %d", expected2, count)
	}

	v, err := w.LookupKV([]byte(fmt.Sprintf("key-%10d", 120000)))
	if err != nil || string(v) != fmt.Sprintf("val-%10d", 120000) {
		t.Errorf("invalid response %v %s", err, string(v))
	}

	_, err = w.LookupKV([]byte(fmt.Sprintf("key-%10d", 90000)))
	if err != ErrItemNotFound {
		t.Errorf("Expected not found")
	}
}

func TestMVCCIntervalGC(t *testing.T) {
	os.RemoveAll("teststore.data")
	s := newTestIntPlasmaStore(testSnCfg)
	defer s.Close()

	n := 1000

	w := s.NewWriter()
	for i := 0; i < n; i++ {
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i)), []byte("sn1"))
	}

	snap1 := s.NewSnapshot()

	for i := 0; i < n; i++ {
		w.DeleteKV([]byte(fmt.Sprintf("key-%10d", i)))
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i)), []byte("sn2"))
	}

	snap2 := s.NewSnapshot()

	for i := 0; i < n; i++ {
		w.DeleteKV([]byte(fmt.Sprintf("key-%10d", i)))
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i)), []byte("sn3"))
	}

	snap3 := s.NewSnapshot()
	snap3.Open()
	s.CreateRecoveryPoint(snap3, []byte(fmt.Sprint("rp1")))

	for i := 0; i < n; i++ {
		w.DeleteKV([]byte(fmt.Sprintf("key-%10d", i)))
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i)), []byte("sn4"))
	}

	snap4 := s.NewSnapshot()

	for i := 0; i < n; i++ {
		w.DeleteKV([]byte(fmt.Sprintf("key-%10d", i)))
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i)), []byte("sn5"))
	}

	snap5 := s.NewSnapshot()

	for i := 0; i < n; i++ {
		w.DeleteKV([]byte(fmt.Sprintf("key-%10d", i)))
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i)), []byte("sn6"))
	}

	// Insert and delete in same snapshot
	for i := n; i < n+100; i++ {
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i)), []byte("sn6"))
		w.DeleteKV([]byte(fmt.Sprintf("key-%10d", i)))
	}

	snap6 := s.NewSnapshot()
	snap6.Open()
	s.CreateRecoveryPoint(snap6, []byte(fmt.Sprint("rp2")))

	for i := 0; i < n; i++ {
		w.DeleteKV([]byte(fmt.Sprintf("key-%10d", i)))
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i)), []byte("sn7"))
	}

	snap7 := s.NewSnapshot()

	for i := 0; i < n; i++ {
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i)), []byte("sn8"))
		w.DeleteKV([]byte(fmt.Sprintf("key-%10d", i)))
	}

	itr := s.NewIterator()
	count := func() int {
		count := 0
		for itr.SeekFirst(); itr.Valid(); itr.Next() {
			count++
		}
		return count
	}

	w.CompactAll()
	if c := count(); c != 13000 {
		t.Errorf("Expected 13000, got %d", c)
	}

	snap1.Close()
	w.CompactAll()
	if c := count(); c != 11000 {
		t.Errorf("Expected 13000, got %d", c)
	}

	snap2.Close()
	w.CompactAll()
	if c := count(); c != 9000 {
		t.Errorf("Expected 13000, got %d", c)
	}

	snap3.Close()
	w.CompactAll()
	if c := count(); c != 9000 {
		t.Errorf("Expected 13000, got %d", c)
	}

	snap4.Close()
	w.CompactAll()
	if c := count(); c != 7000 {
		t.Errorf("Expected 13000, got %d", c)
	}

	snap5.Close()
	w.CompactAll()
	if c := count(); c != 5000 {
		t.Errorf("Expected 13000, got %d", c)
	}

	snap6.Close()
	w.CompactAll()
	if c := count(); c != 5000 {
		t.Errorf("Expected 13000, got %d", c)
	}

	snap7.Close()
	w.CompactAll()
	if c := count(); c != 5000 {
		t.Errorf("Expected 13000, got %d", c)
	}
}
