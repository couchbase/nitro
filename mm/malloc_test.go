// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package mm

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func TestJeMalloc(t *testing.T) {
	Malloc(100 * 1024 * 1024)
	fmt.Println("size:", Size())
	fmt.Println(Stats())

	stsJsonStr := StatsJson()
	unmarshaledSts := new(map[string]interface{})

	if err := json.Unmarshal([]byte(stsJsonStr), unmarshaledSts); err != nil {
		t.Errorf("Failed to unmarshal json stats: %v", err)
	}

	buf, err := json.MarshalIndent(unmarshaledSts, "", "    ")
	if err != nil {
		t.Errorf("Failed to marshal again: %v", err)
	}
	fmt.Println(string(buf))
}

func TestJeMallocSizeAt(t *testing.T) {
	p := Malloc(89)
	if sz := SizeAt(p); sz != 96 {
		t.Errorf("Expected sizeclass 96, but got %d", sz)
	}
}

func TestJeMallocProf(t *testing.T) {
	profPath := "TestProf.prof"

	if runtime.GOOS != "linux" {
		return
	}

	if err := os.Remove(profPath); err != nil && !os.IsNotExist(err) {
		t.Errorf("Could not remove old profile: err[%v]", err)
		return
	}

	if err := ProfActivate(); err != nil {
		t.Error(err)
		return
	}

	defer func() {
		if err := ProfDeactivate(); err != nil {
			t.Error(err)
		}
	}()

	if err := ProfDump(profPath); err != nil {
		t.Error(err)
		return
	}
}

func TestJeMallocArenaCreate(t *testing.T) {
	n := NArenas()
	if CreateArenas() == 0 {
		n2 := NArenas()
		t.Log(n, n2)
		if n >= n2 {
			t.Error("error")
		}
	}
}

// run this test separately as arena stats can get polluted
func TestJeMallocArenaStatsFromZero(t *testing.T) {
	n := 100000
	nThreads := runtime.GOMAXPROCS(0) * 2 // 2 * G
	var wg sync.WaitGroup

	dArenas := NArenas()

	// check user arena configuration
	if err := CreateArenas(); err != 0 {
		return
	}

	defer func() {
		fmt.Println(Stats())
	}()

	ptrs := make([][]unsafe.Pointer, nThreads)

	// arena stats are not updated immediately (thread based)
	doWait := func() {
		for i := 0; i < 1000; i++ {
			AllocSize()
			time.Sleep(time.Millisecond * 10)
		}
	}

	doAllocs := func(useX bool) {
		// check thread assignment
		t0 := time.Now()
		for i := 0; i < nThreads; i++ {
			wg.Add(1)
			ptrs[i] = make([]unsafe.Pointer, 0)
			go func(k int) {
				defer wg.Done()
				sz := 1024
				for j := 0; j < n; j++ {
					if useX {
						ptrs[k] = append(ptrs[k], MallocArena(sz))
					} else {
						ptrs[k] = append(ptrs[k], Malloc(sz))
					}
				}
			}(i)
		}
		wg.Wait()
		t.Logf("doAllocs Malloc(%v): %v", useX, time.Since(t0))
		doWait()
	}

	doFree := func() {
		t0 := time.Now()
		for i := 0; i < nThreads; i++ {
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				for j := 0; j < len(ptrs[k]); j++ {
					Free(ptrs[k][j])
				}
				ptrs[k] = nil
			}(i)
		}
		wg.Wait()
		t.Logf("doFree Free:%v", time.Since(t0))
		doWait()
	}

	// check default allocations
	doAllocs(false)

	t.Run("auto_arena_alloc_stats", func(t *testing.T) {
		t.Log("Auto Arena Resident", SizeAuto(), "Alloc", AllocSizeAuto(), "Active", ActiveSizeAuto())

		if SizeAuto() == 0 || AllocSizeAuto() == 0 {
			t.Error("error")
			return
		}

		if SizeAuto() < SizeUser() {
			t.Error("error")
			return
		}

		if AllocSizeUser() > 0 {
			t.Error("error")
			return
		}

		// no active pages for user arenas yet
		if ActiveSizeUser() > 0 {
			t.Error("error")
			return
		}

		// merged stat should always be ge than user arena stats
		if AllocSize() < AllocSizeAuto() {
			t.Error("error")
			return
		}
	})

	doFree()

	t.Run("auto_arena_free_stats", func(t *testing.T) {
		for i := dArenas; i < NArenas(); i++ {
			if ArenaStats(i, "small.ndalloc")+ArenaStats(i, "large.ndalloc") > 0 {
				t.Error("error")
				return
			}
		}
	})

	// check user arena allocations
	doAllocs(true)

	m := uint64(0)
	t.Run("user_arena_alloc_stats", func(t *testing.T) {
		t.Log("User Resident", SizeUser(), "Alloc", AllocSizeUser(), "Active", ActiveSizeUser())

		if SizeUser() == 0 || AllocSizeUser() == 0 || ActiveSizeUser() == 0 {
			t.Error("error")
			return
		}
		m = AllocSizeUser()

		if Size() < SizeUser() {
			t.Error("error")
			return
		}

		for i := dArenas; i < NArenas(); i++ {
			if ArenaStats(i, "small.nmalloc") + ArenaStats(i, "large.nmalloc") == 0 {
				t.Error("error")
				return
			}
		}
	})

	doFree()

	t.Run("user_arena_free_stats", func(t *testing.T) {
		if Size() < SizeUser() {
			t.Error("error")
			return
		}

		if m <= AllocSizeUser() {
			t.Error("error")
			return
		}

		for i := dArenas; i < NArenas(); i++ {
			if ArenaStats(i, "small.nmalloc") < ArenaStats(i, "large.nmalloc") {
				t.Error("error")
				return
			}
		}
	})

	t.Run("purge_arena", func(t *testing.T) {
		m, mx, mg := Size(), SizeUser(), SizeAuto()

		t0 := time.Now()
		FreeOSMemoryUser()
		fmt.Println("User Arena Purge Duration:", time.Since(t0))
		doWait()

		// merged resident should drop
		if Size() >= m {
			t.Error("error")
			return
		}
		// user arena resident should drop
		if SizeUser() >= mx {
			t.Error("error")
			return
		}

		// auto arena purged
		t0 = time.Now()
		FreeOSMemory()
		fmt.Println("Auto Arena Purge Duration", time.Since(t0))
		doWait()

		if SizeAuto() >= mg {
			t.Error("error")
			return
		}
	})

	t.Run("decay_statistics", func(t *testing.T) {
		l1 := ArenaStats(0, "dirty_decay_ms")
		l2 := ArenaStats(0, "muzzy_decay_ms")
		for i := 0; i < NArenas(); i++ {
			ms := ArenaStats(i, "dirty_decay_ms")
			if ms > l1 {
				t.Error("error", ms, l1)
			}
			ms = ArenaStats(i, "muzzy_decay_ms")
			if ms > l2 {
				t.Error("error", ms, l2)
			}
			t.Log("DirtyPurged:", ArenaStats(i, "dirty_purged"))
		}
	})
}

// test dedicated arena is not used for large allocations for user arenas
func TestJeMallocArenaLarge(t *testing.T) {
	nThreads := runtime.GOMAXPROCS(0)
	var wg sync.WaitGroup

	defer func() {
		fmt.Println(Stats())
	}()

	if err := CreateArenas(); err != 0 && err != -1 {
		t.Error("error", err)
		return
	}

	ptrs := make([][]unsafe.Pointer, nThreads)

	sz := 10 * 1024 * 1024
	iters := 1000
	doAllocs := func() {
		for i := 0; i < nThreads; i++ {
			wg.Add(1)
			ptrs[i] = make([]unsafe.Pointer, 0)
			go func(k int) {
				defer wg.Done()
				for j := 0; j < iters; j++ {
					ptrs[k] = append(ptrs[k], MallocArena(sz))
				}
			}(i)
		}
		wg.Wait()
	}

	doFree := func() {
		for i := 0; i < nThreads; i++ {
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				for j := 0; j < len(ptrs[k]); j++ {
					Free(ptrs[k][j])
				}
				ptrs[k] = nil
			}(i)
		}
		wg.Wait()
	}

	for i := 0; i < 5; i++ {
		doAllocs()
		t.Log("User Arena Resident", SizeUser(), AllocSizeUser(), (sz * iters * nThreads))
		doFree()
		FreeOSMemoryUser()
		t.Log("User Arena Resident", SizeUser(), AllocSizeUser(), (sz * iters * nThreads))
	}
}
