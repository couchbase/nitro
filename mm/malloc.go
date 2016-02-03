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
	Debug bool = true
	mu    sync.Mutex
)

var stats struct {
	allocs uint64
	frees  uint64
}

func Malloc(l int) unsafe.Pointer {
	if Debug {
		atomic.AddUint64(&stats.allocs, 1)
	}
	return C.mm_malloc(C.size_t(l))
}

func Free(p unsafe.Pointer) {
	if Debug {
		atomic.AddUint64(&stats.frees, 1)
	}
	C.mm_free(p)
}

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

func Size() uint64 {
	return uint64(C.mm_size())
}

func FreeOSMemory() error {
	errCode := int(C.mm_free2os())
	if errCode != 0 {
		return fmt.Errorf("status: %d", errCode)
	}

	return nil
}
