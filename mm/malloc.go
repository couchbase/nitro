package mm

/*
#cgo LDFLAGS: -ljemalloc

#include <stdlib.h>
#include <jemalloc/jemalloc.h>

typedef struct {
	char *buf;
	int offset;
	int size;
} stats_buf;

void writecb(void *ref, const char *s) {
	stats_buf *buf = (stats_buf *)(ref);
	int len;
	len = strlen(s);
	if (buf->offset + len >= buf->size) {
		buf->size *=2;
		buf->buf = realloc(buf->buf, buf->size);
	}
	strncpy(buf->buf + buf->offset, s, len);
	buf->offset += len;
}

char * doStats()  {
	stats_buf buf;
	buf.size = 1024;
	buf.buf = malloc(buf.size);
	buf.offset = 0;
	je_malloc_stats_print(writecb, &buf, NULL);
	buf.buf[buf.offset] = 0;
	return buf.buf;
}

*/
import "C"

import (
	"fmt"
	"sync/atomic"
	"unsafe"
)

var Debug bool = true

var stats struct {
	allocs uint64
	frees  uint64
}

func Malloc(l int) unsafe.Pointer {
	if Debug {
		atomic.AddUint64(&stats.allocs, 1)
	}
	return C.je_malloc(C.size_t(l))
}

func Free(p unsafe.Pointer) {
	if Debug {
		atomic.AddUint64(&stats.frees, 1)
	}
	C.je_free(p)
}

func Stats() string {
	buf := C.doStats()
	s := "==== Stats ====\n"
	if Debug {
		s += fmt.Sprintf("Mallocs = %d\n"+
			"Frees   = %d\n", stats.allocs, stats.frees)
	}

	s += C.GoString(buf)
	C.free(unsafe.Pointer(buf))

	return s
}
