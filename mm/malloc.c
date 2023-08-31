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
#include <errno.h>

#ifdef JEMALLOC
#include <jemalloc/jemalloc.h>

const char* je_malloc_conf = "narenas:2"

// Enable profiling, but keep it deactivated. Profiling is supported only on linux.
#ifdef __linux__
        ",prof:true,prof_active:false"
#endif
    ;

// writecb is callback passed to jemalloc used to process a chunk of
// stats text. It is in charge of making sure that the buffer is
// sufficiently sized.
void writecb(void *ref, const char *s) {
	stats_buf *buf = (stats_buf *)(ref);
	int len;
	len = strlen(s);
	if (buf->offset + len >= buf->size) {
		// Buffer is too small, resize it to fit at least len and string terminator
		buf->size += len + 2;
		buf->buf = realloc(buf->buf, buf->size);
	}
	strncpy(buf->buf + buf->offset, s, len);
	buf->offset += len;
}

// doStats returns a string with jemalloc stats.
// Caller is responsible to call free on the string buffer.
char *doStats(char *opts)  {
	stats_buf buf;
	buf.size = 1024;
	buf.buf = malloc(buf.size);
	buf.offset = 0;
	je_malloc_stats_print(writecb, &buf, opts);
	buf.buf[buf.offset] = 0;
	return buf.buf;
}

#endif

// mm_arenas_nbins returns the stat nbins which is the
// number of bin size classes.
unsigned int mm_arenas_nbins() {
#ifdef JEMALLOC
    unsigned int nbins = 0;
    size_t sz = sizeof(unsigned int);
    je_mallctl("arenas.nbins", &nbins, &sz, NULL, 0);

    return nbins;
#else
    return 0;
#endif
}

// mm_arenas_bin_i_stat returns the value of the stat `stat` which is
// something valid which can be used in arenas.bin.<i>.<stat>.
// Should be used only when value is expected to be a size_t.
size_t mm_arenas_bin_i_stat(unsigned int i, const char *stat) {
#ifdef JEMALLOC
    size_t stat_val = 0;
    size_t sz = sizeof(size_t);
    char ctl[128];
    snprintf(ctl, 128, "arenas.bin.%d.%s", i, stat);
    je_mallctl(ctl, &stat_val, &sz, NULL, 0);

    return stat_val;
#else
    return 0;
#endif
}

// mm_stats_arenas_merged_bins_j_stat returns the value of the stat `stat` merged across arenas
// The `stat` should be something valid which can be used in stats.arenas.<i>.bins.<j>.<stat>.
// Should be used only when value is expected to be a size_t.
size_t mm_stats_arenas_merged_bins_j_stat(unsigned int j, const char *stat) {
#ifdef JEMALLOC
    size_t stat_val = 0;
    size_t sz = sizeof(size_t);
    char ctl[128];
    snprintf(ctl, 128, "stats.arenas.%d.bins.%d.%s", MALLCTL_ARENAS_ALL, j, stat);
    je_mallctl(ctl, &stat_val, &sz, NULL, 0);

    return stat_val;
#else
    return 0;
#endif
}

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
    return doStats(NULL);
#else
    return NULL;
#endif
}

char *mm_stats_json() {
#ifdef JEMALLOC
    return doStats("J");
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

size_t mm_dirty_size() {
#ifdef JEMALLOC
    // Force stats cache flush
    uint64_t epoch = 1;
    size_t sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);

    // Just enough to hold "stats.arenas.%d.pdirty" formatted with max uint64
    char ctl[42];
    snprintf(ctl, 42, "stats.arenas.%d.pdirty", MALLCTL_ARENAS_ALL);

    // Get page size
    size_t pageSize = 0;
    sz = sizeof(size_t);
    je_mallctl("arenas.page", &pageSize, &sz, NULL, 0);

    // Get number of dirty pages
    size_t pdirty = 0;
    je_mallctl(ctl, &pdirty, &sz, NULL, 0);

    // Return number of dirty bytes
    return pdirty * pageSize;
#else
    return 0;
#endif
}

size_t mm_active_size() {
    size_t active, sz;
    sz = sizeof(size_t);
#ifdef JEMALLOC
    // Force stats cache flush
    uint64_t epoch = 1;
    sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);

    je_mallctl("stats.active", &active, &sz, NULL, 0);
    return active;
#else
    return 0;
#endif
}


int mm_free2os() {
#ifdef JEMALLOC
	char buf[100];
	sprintf(buf, "arena.%u.purge", MALLCTL_ARENAS_ALL);
	return je_mallctl(buf, NULL, NULL, NULL, 0);
#endif
	return 0;
}

int mm_prof_activate() {
#ifdef JEMALLOC
    bool active = true;
    return je_mallctl("prof.active", NULL, NULL, &active, sizeof(active));
#endif
    return ENOTSUP;
}

int mm_prof_deactivate() {
#ifdef JEMALLOC
    bool active = false;
    return je_mallctl("prof.active", NULL, NULL, &active, sizeof(active));
#endif
    return ENOTSUP;
}

int mm_prof_dump(char* filePath) {
#ifdef JEMALLOC
    return je_mallctl("prof.dump", NULL, NULL, &filePath, sizeof(const char *));
#endif
    return ENOTSUP;
}
