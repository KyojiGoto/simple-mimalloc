#include <stdlib.h>
#include "memlib.h"

void *mm_malloc(size_t sz)
{
	(void)sz; /* Avoid warning about unused variable */
	return NULL;
}

void mm_free(void *ptr)
{
	(void)ptr; /* Avoid warning about unused variable */
}


int mm_init(void)
{
	return 0;
}

