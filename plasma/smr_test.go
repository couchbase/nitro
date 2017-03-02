package plasma

import (
	"fmt"
	"github.com/couchbase/nitro/mm"
	"os"
	"sync"
	"testing"
	"time"
)

func (w *testWriter) trySnapshot() {
	if w.snapshotInterval > 0 && w.numOps%w.snapshotInterval == 0 {
		w.snCh <- true
		<-w.snCh
	}
}

func TestSMRSimple(t *testing.T) {
	os.RemoveAll("teststore.data")

	cfg := testSnCfg
	cfg.UseMemoryMgmt = true
	cfg.AutoSwapper = false
	s := newTestIntPlasmaStore(cfg)

	w := s.NewWriter()
	for i := 0; i < 800; i++ {
		token := w.BeginTx()
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i)), []byte(fmt.Sprintf("val-%10d", i)))
		w.EndTx(token)
	}

	s.NewSnapshot().Close()

	for i := 0; i < 800; i++ {
		token := w.BeginTx()
		w.DeleteKV([]byte(fmt.Sprintf("key-%10d", i)))
		w.EndTx(token)
	}

	s.NewSnapshot().Close()

	time.Sleep(time.Second)
	fmt.Println(s.GetStats())
	w.CompactAll()
	s.PersistAll()
	s.Close()

	a, b := mm.GetAllocStats()
	if a-b != 0 {
		t.Errorf("Found memory leak of %d allocs", a-b)
	}

	fmt.Println("Reopening database...")
	s = newTestIntPlasmaStore(cfg)
	s.Close()

	a, b = mm.GetAllocStats()
	if a-b != 0 {
		t.Errorf("Found memory leak of %d allocs", a-b)
	}

}

func TestSMRConcurrent(t *testing.T) {
	defer SetMemoryQuota(maxMemoryQuota)
	os.RemoveAll("teststore.data")

	SetMemoryQuota(5 * 1024 * 1024)
	var wg sync.WaitGroup
	numThreads := 8
	n := 1000000
	iterations := 5
	nPerThr := n / numThreads
	cfg := testSnCfg
	cfg.UseMemoryMgmt = true
	cfg.AutoSwapper = true
	s := newTestIntPlasmaStore(cfg)

	total := numThreads * nPerThr

	t0 := time.Now()
	ws := make([]*testWriter, numThreads)

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		ws[i] = newTestWriter(s.NewWriter())
		ws[i].snCh = make(chan bool)
		ws[i].snapshotInterval = 10000
		go doInsertMVCC(ws[i], &wg, i, nPerThr)
	}

	stopch := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopch:
				return
			default:
				for i := 0; i < numThreads; i++ {
					<-ws[i].snCh
				}
				sn := s.NewSnapshot()
				sn.Close()
				for i := 0; i < numThreads; i++ {
					ws[i].snCh <- true
				}
			}
		}
	}()

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
			go doUpdateMVCC(ws[i], &wg, i, nPerThr, x)
		}
		wg.Wait()

		dur := time.Since(t0)

		s.NewSnapshot().Close()
		fmt.Println(s.GetStats())
		fmt.Printf("%d items update took %v -> %v items/s\n", total, dur, float64(total)/float64(dur.Seconds()))
	}

	close(stopch)

	s.NewSnapshot().Close()
	s.Close()

	a, b := mm.GetAllocStats()
	if a-b != 0 {
		t.Errorf("Found memory leak of %d allocs", a-b)
	}

	fmt.Println("Reopening db....")
	s = newTestIntPlasmaStore(cfg)
	s.Close()

	a, b = mm.GetAllocStats()
	if a-b != 0 {
		t.Errorf("Found memory leak of %d allocs", a-b)
	}
}
