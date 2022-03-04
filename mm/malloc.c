// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.
#include "malloc.h"
#include <stdio.h>
#include <string.h>

#ifdef JEMALLOC
#include <jemalloc/jemalloc.h>

const char* je_malloc_conf = "narenas:2";

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
    return je_calloc(1, sz);
#else
    return calloc(1, sz);
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

size_t mm_sizeat(void *p) {
#ifdef JEMALLOC
    return je_sallocx(p, 0);
#else
    return 0;
#endif
}

size_t mm_size() {
    size_t resident, sz;
    sz = sizeof(size_t);
#ifdef JEMALLOC
    // Force stats cache flush
    uint64_t epoch = 1;
    sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);

    je_mallctl("stats.resident", &resident, &sz, NULL, 0);
    return resident;
#else
    return 0;
#endif
}

size_t mm_alloc_size() {
    size_t allocated, sz;
    sz = sizeof(size_t);
#ifdef JEMALLOC
    // Force stats cache flush
    uint64_t epoch = 1;
    sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);

    je_mallctl("stats.allocated", &allocated, &sz, NULL, 0);
    return allocated;
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
