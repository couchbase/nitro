// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.
#ifndef MALLOC_MM_H
#define MALLOC_MM_H

#include <stdlib.h>

typedef struct {
	char *buf;
	int offset;
	int size;
} stats_buf;


void *mm_malloc(size_t);

void mm_free(void *);

char *mm_stats();

size_t mm_sizeat(void *);

size_t mm_size();

size_t mm_alloc_size();

size_t mm_dirty_size();

int mm_free2os();

int mm_prof_activate();

int mm_prof_deactivate();

int mm_prof_dump(char* filePath);

#endif
