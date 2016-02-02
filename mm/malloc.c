#include "malloc.h"
#include <stdio.h>
#include <string.h>

#ifdef JEMALLOC
#include <jemalloc/jemalloc.h>

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


char *doStats()  {
	stats_buf buf;
	buf.size = 1024;
	buf.buf = malloc(buf.size);
	buf.offset = 0;
	je_malloc_stats_print(writecb, &buf, NULL);
	buf.buf[buf.offset] = 0;
	return buf.buf;
}

#endif

void *mm_malloc(size_t sz) {
#ifdef JEMALLOC
    return je_malloc(sz);
#else
    return malloc(sz);
#endif
}

void mm_free(void *p) {
#ifdef JEMALLOC
    return je_free(p);
#else
    return free(p);
#endif
}

char *mm_stats() {
#ifdef JEMALLOC
    return doStats();
#else
    return NULL;
#endif
}

size_t mm_size() {
    size_t resident, sz;
    sz = sizeof(size_t);
#ifdef JEMALLOC
    je_mallctl("stats.resident", &resident, &sz, NULL, 0);
    return resident;
#else
    return 0;
#endif
}

int mm_free2os() {
#ifdef JEMALLOC
	char buf[100];
	unsigned int narenas;
	size_t len = sizeof(narenas);
	je_mallctl("arenas.narenas", &narenas, &len, NULL, 0);
	sprintf(buf, "arena.%u.purge", narenas);
	return je_mallctl(buf, NULL, NULL, NULL, 0);
#endif
	return 0;
}
