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
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

var (
	// Debug enables debug stats
	Debug = true
	mu    sync.Mutex
)

var stats struct {
	allocs uint64
	frees  uint64
}

// Malloc implements C like memory allocator
func Malloc(l int) unsafe.Pointer {
	if Debug {
		atomic.AddUint64(&stats.allocs, 1)
	}
	return C.mm_malloc(C.size_t(l))
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

func StatsJson() string {
	mu.Lock()
	defer mu.Unlock()

	buf := C.mm_stats_json()

	s := ""
	if buf != nil {
		s += C.GoString(buf)
		C.free(unsafe.Pointer(buf))
	}

	return s
}

// Size returns total size allocated by mm allocator
func Size() uint64 {
	return uint64(C.mm_size())
}

func AllocSize() uint64 {
	return uint64(C.mm_alloc_size())
}

func DirtySize() uint64 {
	return uint64(C.mm_dirty_size())
}

func GetAllocStats() (uint64, uint64) {
	return stats.allocs, stats.frees
}

// FreeOSMemory forces jemalloc to scrub memory and release back to OS
func FreeOSMemory() error {
	errCode := int(C.mm_free2os())
	if errCode != 0 {
		return fmt.Errorf("status: %d", errCode)
	}

	return nil
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
