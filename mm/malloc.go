// Copyright (c) 2016 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package mm

/*
#include "malloc.h"
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

// Stats returns allocator statistics
// Returns jemalloc stats
func Stats() string {
	mu.Lock()
	defer mu.Unlock()

	buf := C.mm_stats()
	s := "==== Stats ====\n"
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

// Size returns total size allocated by mm allocator
func Size() uint64 {
	return uint64(C.mm_size())
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
