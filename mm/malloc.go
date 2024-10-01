// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package mm

/*
#include "malloc.h"
#include <string.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

var (
	// Debug enables debug stats
	Debug = true
	mu    sync.Mutex

	statSize     *C.char
	statNregs    *C.char
	statCurregs  *C.char
	statCurslabs *C.char
)

var stats struct {
	allocs uint64
	frees  uint64
}

func init() {
	statSize = C.CString(C.MM_STAT_SIZE)
	statNregs = C.CString(C.MM_STAT_NREGS)
	statCurregs = C.CString(C.MM_STAT_CURREGS)
	statCurslabs = C.CString(C.MM_STAT_CURSLABS)
}

// user arenas (note: narenas in je_malloc_conf are auto/default arenas)
func CreateArenas() int {
	mu.Lock()
	defer mu.Unlock()

	return int(C.mm_create_arenas())
}

// Malloc implements C like memory allocator
func Malloc(l int) unsafe.Pointer {
	if Debug {
		atomic.AddUint64(&stats.allocs, 1)
	}
	return C.mm_malloc(C.size_t(l))
}

func MallocArena(l int) unsafe.Pointer {
	if Debug {
		atomic.AddUint64(&stats.allocs, 1)
	}
	return C.mm_malloc_user_arena(C.size_t(l), 0)
}

// key is mainly for windows where we do not do thread based arena assignment
func MallocArenaByKey(l int, key uint16) unsafe.Pointer {
	if Debug {
		atomic.AddUint64(&stats.allocs, 1)
	}
	return C.mm_malloc_user_arena(C.size_t(l), C.ushort(key))
}

// Free implements C like memory deallocator
func Free(p unsafe.Pointer) {
	if Debug {
		atomic.AddUint64(&stats.frees, 1)
	}
	C.mm_free(p)
}

// SizeAt returns real allocated size from an allocated pointer
func SizeAt(p unsafe.Pointer) int {
	return int(C.mm_sizeat(p))
}

// Stats returns allocator statistics
// Returns jemalloc stats
func Stats() string {
	mu.Lock()
	defer mu.Unlock()

	buf := C.mm_stats()
	s := "---- Stats ----\n"
	if Debug {
		s += fmt.Sprintf("Mallocs = %d\n"+
			"Frees   = %d\n", stats.allocs, stats.frees)
	}

	if buf != nil {
		s += C.GoString(buf)
		C.free(unsafe.Pointer(buf))
	}

	return s
}

func ArenaStats(i int, str string) uint64 {
	buf := C.CString(str)
	if buf == nil {
		return 0
	}
	defer C.free(unsafe.Pointer(buf))

	return uint64(C.mm_arenas_i_stat(C.uint(i), buf))
}

func NArenas() int {
	return int(C.mm_narenas())
}

func NArenasUser() int {
	return int(C.mm_user_narenas())
}

type JemallocBinStats struct {
	FragPercent uint64
	Resident    uint64
}

func computeBinFrag(curregs, curslabs, nregs uint64) uint64 {
	if curslabs <= 0 || nregs <= 0 {
		return 0
	}

	return 100 - ((100 * curregs) / (curslabs * nregs))
}

func computeBinResident(curslabs, nregs, size uint64) uint64 {
	return curslabs * nregs * size
}

func getBinsStats() map[string]JemallocBinStats {
	nbins := uint64(C.mm_arenas_nbins())
	bs := make(map[string]JemallocBinStats)

	for i := uint64(0); i < nbins; i++ {
		binInd := C.uint(i)
		size := uint64(C.mm_arenas_bin_i_stat(binInd, statSize))
		nregs := uint64(C.mm_arenas_bin_i_stat(binInd, statNregs))
		curregs := uint64(C.mm_stats_arenas_merged_bins_j_stat(binInd, statCurregs))
		curslabs := uint64(C.mm_stats_arenas_merged_bins_j_stat(binInd, statCurslabs))

		sts := JemallocBinStats{}
		sts.FragPercent = computeBinFrag(curregs, curslabs, nregs)
		sts.Resident = computeBinResident(curslabs, nregs, size)

		bs[fmt.Sprintf("bin_%d", size)] = sts
	}

	return bs
}

func StatsJson() string {
	mu.Lock()
	defer mu.Unlock()

	buf := C.mm_stats_json()

	s := ""
	if buf != nil {
		s += C.GoString(buf)
		C.free(unsafe.Pointer(buf))
	}

	// Unmarshal json and add derived stats to it
	stsJson := make(map[string]interface{})
	err := json.Unmarshal([]byte(s), &stsJson)
	if err != nil {
		return s
	}
	stsJson["bin_stats"] = getBinsStats()

	data, err := json.Marshal(stsJson)
	if err != nil {
		return s
	}

	return string(data)
}

// Size returns total resident size merged across all arenas
func Size() uint64 {
	return uint64(C.mm_size())
}

func SizeUser() uint64 {
	return uint64(C.mm_size_user_arena())
}

func SizeAuto() uint64 {
	return uint64(C.mm_size_auto_arena())
}

func AllocSize() uint64 {
	return uint64(C.mm_alloc_size())
}

func AllocSizeUser() uint64 {
	return uint64(C.mm_alloc_size_user_arena())
}

func AllocSizeAuto() uint64 {
	return uint64(C.mm_alloc_size_auto_arena())
}

func DirtySize() uint64 {
	return uint64(C.mm_dirty_size())
}

func DirtySizeUser() uint64 {
	return uint64(C.mm_dirty_size_user_arena())
}

func DirtySizeAuto() uint64 {
	return uint64(C.mm_dirty_size_auto_arena())
}

func ActiveSize() uint64 {
	return uint64(C.mm_active_size())
}

func ActiveSizeUser() uint64 {
	return uint64(C.mm_active_size_user_arena())
}

func ActiveSizeAuto() uint64 {
	return uint64(C.mm_active_size_auto_arena())
}

func GetAllocStats() (uint64, uint64) {
	return atomic.LoadUint64(&stats.allocs), atomic.LoadUint64(&stats.frees)
}

// FreeOSMemory forces jemalloc to scrub memory and release back to OS
func FreeOSMemory() error {
	errCode := int(C.mm_free2os())
	if errCode != 0 {
		return fmt.Errorf("status: %d", errCode)
	}

	return nil
}

// does parallel scrub of user arenas memory and release back to OS
func FreeOSMemoryUser() error {
	var err error
	var wg sync.WaitGroup
	for i := 0; i < NArenasUser(); i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			errCode := int(C.mm_free2os_user_arena(C.uint(id)))
			if errCode != 0 && err == nil {
				err = fmt.Errorf("status: %d", errCode)
			}
		}(i)
	}

	wg.Wait()
	return err
}

func ProfActivate() error {
	if errCode := int(C.mm_prof_activate()); errCode != 0 {
		return fmt.Errorf("Error during jemalloc profile activate. err = [%v]",
			C.GoString(C.strerror(C.int(errCode))))
	}

	return nil
}

func ProfDeactivate() error {
	if errCode := int(C.mm_prof_deactivate()); errCode != 0 {
		return fmt.Errorf("Error during jemalloc profile deactivate. err = [%v]",
			C.GoString(C.strerror(C.int(errCode))))
	}

	return nil
}

func ProfDump(filePath string) error {
	filePathAsCString := C.CString(filePath)
	defer C.free(unsafe.Pointer(filePathAsCString))

	if errCode := int(C.mm_prof_dump(filePathAsCString)); errCode != 0 {
		return fmt.Errorf("Error during jemalloc profile dump. err = [%v]",
			C.GoString(C.strerror(C.int(errCode))))
	}

	return nil
}
