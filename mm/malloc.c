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

// a) do not disable tcache, indexer may run into thread limit exhaustion
// b) windows do not honor the config string see MB-63068
const char* je_malloc_conf = "narenas:2"

// Enable profiling, but keep it deactivated. Profiling is supported only on
// linux.
#ifdef __linux__
        ",prof:true,prof_active:false"
#endif
        ;

// number of user arenas
#define MAX_USER_ARENAS 2U
static unsigned int user_arenas[MAX_USER_ARENAS];
static unsigned int user_arenas_init;

#if defined(__linux__) || defined(__APPLE__)
# include <stdatomic.h> // C11

static atomic_uint counter = 0;
static __thread unsigned int tsd = 0;

// key is ignored for thread based assignment
static inline unsigned int assign_arena(unsigned short key) {
    if (tsd == 0) {
        unsigned int x = 1;
        tsd = (unsigned int)(atomic_fetch_add(&counter, x)) + 1;
    }
    return user_arenas[tsd % MAX_USER_ARENAS];
}
#else
// on windows C11 based thread local specifier has portability issues
// https://github.com/golang/go/issues/20982
static inline unsigned int assign_arena(unsigned short key) {
    return user_arenas[key % MAX_USER_ARENAS];
}
#endif

// not thread safe
static void reset_user_arena_info() {
#ifdef JEMALLOC
    for (unsigned int i = 0; i < MAX_USER_ARENAS; i++) {
        if (user_arenas[i] > 0) {
            user_arenas[i] = 0;
        }
    }
    user_arenas_init = 0;
#endif
}

static int is_auto_arena(unsigned int arena) {
#ifdef JEMALLOC
    if (user_arenas_init > 0) {
        for (unsigned int i = 0; i < MAX_USER_ARENAS; i++) {
            if ((user_arenas[i] > 0) && (user_arenas[i] == arena)) {
                return 0;
            }
        }
    }
    return 1;
#else
    return 1;
#endif
}

// writecb is callback passed to jemalloc used to process a chunk of
// stats text. It is in charge of making sure that the buffer is
// sufficiently sized.
void writecb(void* ref, const char* s) {
    stats_buf* buf = (stats_buf*)(ref);
    int len;
    len = strlen(s);
    if (buf->offset + len >= buf->size) {
        // Buffer is too small, resize it to fit at least len and string
        // terminator
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

static int mm_create_arena(unsigned int* arena) {
#ifdef JEMALLOC
    if (arena == NULL) {
        return -EINVAL;
    }
    size_t sz = sizeof(unsigned);
    return je_mallctl("arenas.create", (void*)arena, &sz, NULL, 0);
#else
    return -ENOTSUP;
#endif
}

// not thread safe
int mm_create_arenas() {
#ifdef JEMALLOC
    if (user_arenas_init > 0) {
        return -1;
    }

    for (unsigned int i = 0; i < MAX_USER_ARENAS; i++) {
        int ret = mm_create_arena(&user_arenas[i]);
        if (ret != 0) {
            reset_user_arena_info();
            return ret;
        }
    }
    user_arenas_init = 1;
    return 0;
#else
    return -ENOTSUP;
#endif
}

// count may remain same even after arena destroy
unsigned int mm_narenas() {
#ifdef JEMALLOC
    unsigned int narenas = 0;
    size_t sz = sizeof(unsigned);
    int ret = je_mallctl("arenas.narenas", &narenas, &sz, NULL, 0);
    if (ret == 0) {
        return narenas;
    }
    return 0;
#else
    return 0;
#endif
}

unsigned int mm_user_narenas() {
#ifdef JEMALLOC
    if (user_arenas_init == 0) {
        return 0;
    }

    return MAX_USER_ARENAS;
#else
    return 0;
#endif
}

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

// Should be used only when value is expected to be a size_t.
size_t mm_arenas_i_stat(unsigned int i, const char* stat) {
#ifdef JEMALLOC
    if (stat == NULL) {
        return 0;
    }
    size_t stat_val = 0;
    size_t sz = sizeof(size_t);
    char ctl[128];
    snprintf(ctl, 128, "stats.arenas.%u.%s", i, stat);
    je_mallctl(ctl, &stat_val, &sz, NULL, 0);

    return stat_val;
#else
    return 0;
#endif
}

// mm_arenas_bin_i_stat returns the value of the stat `stat` which is
// something valid which can be used in arenas.bin.<i>.<stat>.
// Should be used only when value is expected to be a size_t.
size_t mm_arenas_bin_i_stat(unsigned int i, const char *stat) {
#ifdef JEMALLOC
    if (stat == NULL) {
        return 0;
    }
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
    if (stat == NULL) {
        return 0;
    }
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

void* mm_malloc(size_t sz) {
#ifdef JEMALLOC
    return je_calloc(1, sz);
#else
    return calloc(1, sz);
#endif
}

void* mm_malloc_user_arena(size_t sz, unsigned short key) {
#ifdef JEMALLOC
    unsigned int arena = assign_arena(key);
    return je_mallocx(sz, MALLOCX_ARENA(arena) | MALLOCX_ZERO);
#else
    return calloc(1, sz);
#endif
}

// jemalloc uses radix tree to identify the associated extent
// a) free first releases to tcache bins
// b) on full (CACHE_BIN_NCACHED_MAX) or GC, entries are flushed
//    to the respective extents for use by other threads
// c) tcache can have pointers from extents of arenas other than one associated
void mm_free(void *p) {
#ifdef JEMALLOC
    je_free(p);
#else
    free(p);
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

// merged stat
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

// dedicated arena for oversized allocations is not required for user arenas.
// see opt.oversize_threshold
size_t mm_size_user_arena() {
#ifdef JEMALLOC
    if (user_arenas_init == 0) {
        return 0;
    }

    char ctl[128];
    uint64_t epoch = 1;
    size_t sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);

    size_t resident = 0;
    sz = sizeof(resident);
    for (unsigned int i = 0; i < MAX_USER_ARENAS; i++) {
        size_t res = 0;
        snprintf(ctl, 128, "stats.arenas.%u.resident", user_arenas[i]);
        (void)je_mallctl(ctl, &res, &sz, NULL, 0);
        resident += res;
    }
    return resident;
#else
    return 0;
#endif
}

size_t mm_size_auto_arena() {
#ifdef JEMALLOC
    char ctl[128];
    uint64_t epoch = 1;
    size_t sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);

    size_t resident = 0;
    sz = sizeof(resident);
    unsigned int n = mm_narenas();
    for (unsigned int i = 0; i < n; i++) {
        if (is_auto_arena(i)) {
            size_t res = 0;
            snprintf(ctl, 128, "stats.arenas.%u.resident", i);
            (void)je_mallctl(ctl, &res, &sz, NULL, 0);
            resident += res;
        }
    }
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

size_t mm_alloc_size_user_arena() {
#ifdef JEMALLOC
    if (user_arenas_init == 0) {
        return 0;
    }

    char ctl[128];
    uint64_t epoch = 1;
    size_t sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);

    size_t allocated = 0;
    sz = sizeof(allocated);
    for (unsigned int i = 0; i < MAX_USER_ARENAS; i++) {
        size_t alloc = 0;
        snprintf(ctl, 128, "stats.arenas.%u.small.allocated", user_arenas[i]);
        je_mallctl(ctl, &alloc, &sz, NULL, 0);
        allocated += alloc;
        snprintf(ctl, 128, "stats.arenas.%u.large.allocated", user_arenas[i]);
        alloc = 0;
        je_mallctl(ctl, &alloc, &sz, NULL, 0);
        allocated += alloc;
    }
    return allocated;
#else
    return 0;
#endif
}

size_t mm_alloc_size_auto_arena() {
#ifdef JEMALLOC
    char ctl[128];
    uint64_t epoch = 1;
    size_t sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);

    size_t allocated = 0;
    sz = sizeof(allocated);
    unsigned int n = mm_narenas();
    for (unsigned int i = 0; i < n; i++) {
        if (is_auto_arena(i)) {
            size_t alloc = 0;
            snprintf(ctl, 128, "stats.arenas.%u.small.allocated", i);
            je_mallctl(ctl, &alloc, &sz, NULL, 0);
            allocated += alloc;
            snprintf(ctl, 128, "stats.arenas.%u.large.allocated", i);
            alloc = 0;
            je_mallctl(ctl, &alloc, &sz, NULL, 0);
            allocated += alloc;
        }
    }
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

// dirty memory for user arenas
size_t mm_dirty_size_user_arena() {
#ifdef JEMALLOC
    if (user_arenas_init == 0) {
        return 0;
    }

    char ctl[128];
    uint64_t epoch = 1;
    size_t sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);

    // Get page size
    size_t pageSize = 0;
    sz = sizeof(pageSize);
    je_mallctl("arenas.page", &pageSize, &sz, NULL, 0);

    size_t pdirty = 0;
    for (unsigned int i = 0; i < MAX_USER_ARENAS; i++) {
        size_t pd = 0;
        snprintf(ctl, 128, "stats.arenas.%u.pdirty", user_arenas[i]);
        je_mallctl(ctl, &pd, &sz, NULL, 0);
        pdirty += pd;
    }
    // Return number of dirty bytes
    return pdirty * pageSize;
#else
    return 0;
#endif
}

size_t mm_dirty_size_auto_arena() {
#ifdef JEMALLOC
    char ctl[128];
    uint64_t epoch = 1;
    size_t sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);

    // Get page size
    size_t pageSize = 0;
    sz = sizeof(pageSize);
    je_mallctl("arenas.page", &pageSize, &sz, NULL, 0);

    size_t pdirty = 0;
    unsigned int n = mm_narenas();
    for (unsigned int i = 0; i < n; i++) {
        if (is_auto_arena(i)) {
            size_t pd = 0;
            snprintf(ctl, 128, "stats.arenas.%u.pdirty", i);
            je_mallctl(ctl, &pd, &sz, NULL, 0);
            pdirty += pd;
        }
    }
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

size_t mm_active_size_user_arena() {
#ifdef JEMALLOC
    if (user_arenas_init == 0) {
        return 0;
    }

    char ctl[128];
    uint64_t epoch = 1;
    size_t sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);

    // Get page size
    size_t pageSize = 0;
    sz = sizeof(pageSize);
    je_mallctl("arenas.page", &pageSize, &sz, NULL, 0);

    size_t pactive = 0;
    for (unsigned int i = 0; i < MAX_USER_ARENAS; i++) {
        size_t pactv = 0;
        snprintf(ctl, 128, "stats.arenas.%u.pactive", user_arenas[i]);
        je_mallctl(ctl, &pactv, &sz, NULL, 0);
        pactive += pactv;
    }
    return pactive * pageSize;
#else
    return 0;
#endif
}

size_t mm_active_size_auto_arena() {
#ifdef JEMALLOC
    char ctl[128];
    uint64_t epoch = 1;
    size_t sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);

    // Get page size
    size_t pageSize = 0;
    sz = sizeof(size_t);
    je_mallctl("arenas.page", &pageSize, &sz, NULL, 0);

    size_t pactive = 0;
    unsigned int n = mm_narenas();
    for (unsigned int i = 0; i < n; i++) {
        if (is_auto_arena(i)) {
            size_t pactv = 0;
            snprintf(ctl, 128, "stats.arenas.%u.pactive", i);
            je_mallctl(ctl, &pactv, &sz, NULL, 0);
            pactive += pactv;
        }
    }
    return pactive * pageSize;
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

int mm_free2os_user_arena(unsigned int idx) {
#ifdef JEMALLOC
    if ((user_arenas_init == 0) || (idx >= MAX_USER_ARENAS)) {
        return 0;
    }

    char ctl[100];
    snprintf(ctl, 100, "arena.%u.purge", user_arenas[idx]);
    return je_mallctl(ctl, NULL, NULL, NULL, 0);
#endif
    return 0;
}

int mm_prof_activate() {
#if defined(JEMALLOC) && defined(__linux__)
    bool active = true;
    return je_mallctl("prof.active", NULL, NULL, &active, sizeof(active));
#endif
    return ENOTSUP;
}

int mm_prof_deactivate() {
#if defined(JEMALLOC) && defined(__linux__)
    bool active = false;
    return je_mallctl("prof.active", NULL, NULL, &active, sizeof(active));
#endif
    return ENOTSUP;
}

int mm_prof_dump(char* filePath) {
#if defined(JEMALLOC) && defined(__linux__)
    return je_mallctl("prof.dump", NULL, NULL, &filePath, sizeof(const char *));
#endif
    return ENOTSUP;
}
