package plasma

import (
	"fmt"
	"github.com/couchbase/nitro/skiplist"
	"os"
	"sort"
	"sync"
	"testing"
)

func TestPlasmaPageVisitor(t *testing.T) {
	os.RemoveAll("teststore.data")
	s := newTestIntPlasmaStore(testCfg)
	defer s.Close()

	concurr := 16
	var w []*Writer
	for i := 0; i < concurr; i++ {
		w = append(w, s.NewWriter())
	}

	for i := 0; i < 1000000; i++ {
		w[0].Insert(skiplist.NewIntKeyItem(i))
	}

	var pidKeys []int
	var gotKeys []int
	var mu sync.Mutex

	counts := make([]int, concurr)

	for pid := s.StartPageId(); pid != s.EndPageId(); pid = NextPid(pid) {
		if pid == s.StartPageId() {
			pidKeys = append(pidKeys, 0)
		} else {

			pg, _ := s.ReadPage(pid, nil, false, w[0].wCtx)
			pidKeys = append(pidKeys, skiplist.IntFromItem(pg.MinItem()))
		}
	}

	callb := func(pid PageId, partn RangePartition) error {
		pg, _ := s.ReadPage(pid, nil, false, w[partn.Shard].wCtx)
		mu.Lock()
		defer mu.Unlock()

		if pg.MinItem() == skiplist.MinItem {
			gotKeys = append(gotKeys, 0)
		} else {
			gotKeys = append(gotKeys, skiplist.IntFromItem(pg.MinItem()))
		}

		counts[partn.Shard]++

		return nil
	}

	s.PageVisitor(callb, concurr)

	sort.Ints(gotKeys)
	if len(gotKeys) != len(pidKeys) {
		t.Errorf("Expected %d, got %d", len(pidKeys), len(gotKeys))
	}

	for i, k := range pidKeys {
		if k != gotKeys[i] {
			t.Errorf("Mismatch %v != %v", pidKeys[i], gotKeys[i])
		}
	}

	fmt.Println("Paritition counts", counts)
}
