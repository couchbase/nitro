package mm

/*
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
	"unsafe"
)

func Malloc(l int) unsafe.Pointer {
	return C.je_malloc(C.size_t(l))
}

func Free(p unsafe.Pointer) {
	C.je_free(p)
}

func Stats() string {
	buf := C.doStats()
	s := C.GoString(buf)
	C.free(unsafe.Pointer(buf))
	return s
}
