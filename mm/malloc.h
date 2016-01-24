#include <stdlib.h>

typedef struct {
	char *buf;
	int offset;
	int size;
} stats_buf;


void *mm_malloc(size_t);
void mm_free(void *);
char *mm_stats();

