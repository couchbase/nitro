package mm

// #include <stdlib.h>
import "C"

import (
	"unsafe"
)

func Malloc(l int) unsafe.Pointer {
	return C.malloc(C.size_t(l))
}

func Free(p unsafe.Pointer) {
	C.free(p)
}
