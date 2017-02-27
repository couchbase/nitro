package plasma

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

type testWriter struct {
	*Writer
	numOps           int
	snapshotInterval int
	snCh             chan bool
}

func newTestWriter(w *Writer) *testWriter {
	return &testWriter{Writer: w}
}

func TestPlasmaGreaterThanMemoryPerf(t *testing.T) {
	defer SetMemoryQuota(maxMemoryQuota)
	var wg sync.WaitGroup

	os.RemoveAll("teststore.data")
	numThreads := 16
	n := 1000000000
	iterations := 5
	nPerThr := n / numThreads
	cfg := testSnCfg
	cfg.UseMemoryMgmt = true
	cfg.AutoSwapper = true
	s := newTestIntPlasmaStore(cfg)
	defer s.Close()
	SetMemoryQuota(400 * 1024 * 1024)
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
		last := 0
		now := 0
		for {
			now = 0
			select {
			case <-stopch:
			default:
			}

			for _, w := range ws {
				now += w.numOps
			}

			fmt.Println("--------------------")
			fmt.Printf("Throughput %d items/s\n", now-last)
			fmt.Println(s.GetStats())
			time.Sleep(time.Second)
			last = now
		}
	}()

	go func() {
		for {
			select {
			case <-stopch:
				return
			default:
				for i := 0; i < numThreads; i++ {
					<-ws[i].snCh
				}
				s.NewSnapshot().Close()
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

}

func TestGMRecovery(t *testing.T) {
	defer SetMemoryQuota(maxMemoryQuota)

	SetMemoryQuota(5 * 1024 * 1024)
	os.RemoveAll("teststore.data")
	cfg := testSnCfg
	cfg.UseMemoryMgmt = true
	cfg.AutoSwapper = true
	s := newTestIntPlasmaStore(cfg)
	w := newTestWriter(s.NewWriter())

	n := 1000000
	for i := 0; i < n; i++ {
		w.InsertKV([]byte(fmt.Sprintf("key-%10d", i)), []byte(fmt.Sprintf("val-%10d", i)))
		w.numOps++
		if i%1000 == 0 {
			snap := s.NewSnapshot()
			snap.Close()
		}
	}

	snap := s.NewSnapshot()
	snap.Open()
	s.CreateRecoveryPoint(snap, []byte("rp"))

	for _, rpt := range s.GetRecoveryPoints() {
		fmt.Printf("recovery_point sn:%d meta:%s\n", rpt.sn, string(rpt.meta))
	}

	s.Close()

	fmt.Println("Reopening database...")
	s = newTestIntPlasmaStore(cfg)

	rpts := s.GetRecoveryPoints()
	rb := rpts[0]
	snap, _ = s.Rollback(rb)
	fmt.Println("Rollbacked to", string(rb.meta))

	fmt.Println(s.GetStats())
	itr := snap.NewIterator()
	count := 0
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		count++
	}

	if count != n {
		t.Errorf("Expected %d, got %d", n, count)
	}
	s.Close()
}
