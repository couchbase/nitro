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

size_t mm_size();

#endif
