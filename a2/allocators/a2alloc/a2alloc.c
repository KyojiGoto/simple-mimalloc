#include <stdlib.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include "memlib.h"
#include <assert.h>
#include <stdbool.h>
#define _GNU_SOURCE
#include <sched.h>
#include <pthread.h>
#include <sys/types.h>
#include <stdatomic.h>
#include <math.h>

// NOTE: copied from kheap
#define PAGE_SIZE 4096
#if defined(__GNUC__)
#if __LP64__
#define PAGE_FRAME 0xfffffffffffff000
#else
#define PAGE_FRAME 0xfffff000
#endif /* __X86_64__ */
#endif /* __GNUC__ */

#define SMALLEST_SUBPAGE_SIZE 8
#define LARGEST_SUBPAGE_SIZE 4096

typedef ptrdiff_t vaddr_t;
////////////////////////////////////////

// DESIGN DEVIATIONS/INTERPRETATIONS/ASSUMPTIONS
// 1: pages direct goes from 8 to 512
// 2: multiple of 8 block sizes are a subset of the nearest power of 2 block size in pages (eg, 24 is in 32's list in pages)
// 3: first page area is smaller than the rest to fit metadata from pages and the segment
// 4: pages have a next field, that way we dont have to allocate memory for linked lists
// 5: zero indexing instead of indexing from 1 in the paper
// 6: linked lists of pages/segments are not explicitly allocated, the structs themselves contain next pointers if they need linked list functionality
// 7: in_use bool
// 8: doubly linked lists for pages (prev and next ptrs): needed for freeing, so that heap->pages does not get broken

typedef uint8_t *address;

#define NUM_PAGES 18 // 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, >524288
// static const size_t pages_sizes[NUM_PAGES] = {8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288};

#define NUM_DIRECT_PAGES 128
#define NUM_TOTAL_SIZES 137
static const size_t all_sizes[NUM_TOTAL_SIZES] = {8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88, 96, 104, 112, 120, 128, 136, 144, 152, 160, 168, 176, 184, 192, 200, 208, 216, 224, 232, 240, 248, 256, 264, 272, 280, 288, 296, 304, 312, 320, 328, 336, 344, 352, 360, 368, 376, 384, 392, 400, 408, 416, 424, 432, 440, 448, 456, 464, 472, 480, 488, 496, 504, 512, 520, 528, 536, 544, 552, 560, 568, 576, 584, 592, 600, 608, 616, 624, 632, 640, 648, 656, 664, 672, 680, 688, 696, 704, 712, 720, 728, 736, 744, 752, 760, 768, 776, 784, 792, 800, 808, 816, 824, 832, 840, 848, 856, 864, 872, 880, 888, 896, 904, 912, 920, 928, 936, 944, 952, 960, 968, 976, 984, 992, 1000, 1008, 1016, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288};

#define CACHESIZE 128 /* Should cover most machines. */ // NOTE: copied from Ex2
#define MB 1048576										// 2^20 bytes
#define KB 1024											// 2^10 bytes

#define MALLOC_SMALL_THRESHOLD 1024
#define MALLOC_LARGE_THRESHOLD (512 * KB)

#define SEGMENT_SIZE (4 * MB)
#define NUM_PAGES_SMALL_SEGMENT 64

#define SMALL_PAGE_SIZE (64 * KB)
#define LARGE_PAGE_SIZE SEGMENT_SIZE

#define NEXT_ADDRESS (address)(dseg_hi + 1) // this is the address we would get if we could call mem_sbrk(0);

#define MEM_LIMIT (256 * MB)
#define MAX_NUM_SEGMENTS (MEM_LIMIT / SEGMENT_SIZE) // max number of segments possible (assuming we start at an aligned address)
uint8_t segment_bitmap[MAX_NUM_SEGMENTS];			// TODO: this size is overkill
// pthread_mutex_t lock;
pthread_mutex_t segment_bitmap_lock = PTHREAD_MUTEX_INITIALIZER;

size_t num_segments_capacity = MAX_NUM_SEGMENTS; // max number of segments in our instance
size_t num_segments_allocated = 0;
size_t num_segments_free = 0;

address first_segment_address = NULL; // our "0 index" for our bitmap

struct block_t
{
	struct block_t *next;
};

struct page
{
	struct block_t *free;				 // pointer to first free block
	struct block_t *local_free;			 // pointer to first deferred free by owner thread
	struct block_t *_Atomic thread_free; // atomic pointer to first deferred free by non-owner thread
	address page_area;					 // pointer to start of page area

	size_t num_used; // number of blocks used (used for freeing pages)

	atomic_size_t num_thread_freed; // Number of blocks freed by other threads (used for freeing pages)
	size_t total_num_blocks;		// total number of blocks

	address capacity; // end of last usable block
	address reserved; // end of page area (such that entire segment is 4MB-aligned)
	// DEBUG INFO
	size_t block_size; // size_class

	bool in_use; // for sanity check/debugging, false if is free, true if being used

	struct page *next, *prev; // for free page linked list and for pages (in heap) linked list, DO NOT USE ANYWHERE ELSE (will mess up free/pages)
} typedef page;

enum page_kind_enum
{
	SMALL, // 64KB 8-1024 bytes
	LARGE, // objects under 512KB, 1 large page that spans whole segment
	HUGE,  // objects over 512KB
};

/// Use segment alignment to get the pointer to ptr's "parent" segment
#define get_segment(ptr) (struct segment *)((uintptr_t)(ptr) >> 22 << 22)

struct segment
{
	size_t cpu_id;				   // owner CPUID
	uint32_t page_shift;		   // for small pages this is 16 (= 64KiB), while for large and huge pages it is 22 (= 4MiB) such that the index is always zero in those cases (as there is just one page)
	enum page_kind_enum page_kind; // page kind based on blocksize
	size_t total_num_pages;		   //  1 page if kind= large or huge, 64 equal-sized pages if small
	// TODO figure out if we want to store segment metadata independent of page
	// since small pages are 64KiB and 64 per segment.
	size_t num_used_pages; // sum of used + free should = total_num_pages
	size_t num_free_pages;
	page *free_pages;					 // only relevant for small pages
	page pages[NUM_PAGES_SMALL_SEGMENT]; // pointer to array of page metadata (can be size 1)

	bool in_use; // for sanity check/debugging

	size_t num_contiguous_segments; // only for page_kind = HUGE, if object is >4MB.
	struct segment *next, *prev;	// pointer to next segment for small_segment_refs in thread_heap
									// } __attribute__((aligned(SEGMENT_SIZE))) /*NOTE: this only works on gcc*/ typedef segment;
} /*NOTE: this only works on gcc*/ typedef segment;

struct thread_heap
{
	bool init; // 8
	// 8, 16, 24... -> 1024, step 8, start at index 1 to conform to bit shifting logic
	struct page *pages_direct[NUM_DIRECT_PAGES]; // 8

	// 2^3 -> 2^ 19
	struct page *pages[NUM_PAGES]; //
	size_t cpu_id;				   // CPU number
	// page *small_page_refs;		// freelist of unallocated small page refs
	struct segment *small_segment_refs; // linked list of freed segments that can be written to
	// check before allocating new segment with mem_sbrk
	// TODO fix padding
	uint8_t padding[CACHESIZE - 40]; // ensure that false sharing does not occur for this, TODO
} typedef thread_heap;

#define NUM_CPUS 267
// pointers to thread-local heaps
thread_heap *tlb;

// void init_thread_heap(size_t id)
// {
// }

// thread_heap *get_heap(size_t id)
// {
// }

size_t get_cpuid()
{
	static __thread int id = -1;
	if (id == -1)
		id = sched_getcpu();
	// assert(id == sched_getcpu());
	assert(id >= 0);
	assert(id < NUM_CPUS);
	return (size_t)id;
}

bool static inline segment_in_use(size_t index)
{
	return (bool)(segment_bitmap[index / 8] & (1 << (index % 8)));
}

void static inline atomic_push(struct block_t *_Atomic *list, struct block_t *block)
{
	// set block to be the head of the list
	//  first, make block->next point to the rest of the list
	block->next = *list;
	// when CAS fails (list != &block->next): block->next is set to list, loop again
	// when CAS succeeds (block->next == list),
	// set value pointed to by list to block (make block head)
	// therefore, when the while terminates, block is head of list,
	// and rest of list is pointed to by block
	while (!atomic_compare_exchange_strong(list, &block->next, &block))
		;
}

void static inline set_segment_in_use(size_t index, bool in_use)
{
	if (in_use)
		segment_bitmap[index / 8] |= (1) << (index % 8);
	else
		segment_bitmap[index / 8] ^= (1) << (index % 8);
}

size_t static inline segment_address_to_index(segment *segment)
{
	return ((uint64_t)segment - (uint64_t)first_segment_address) / SEGMENT_SIZE;
}

segment static inline *index_to_segment_address(size_t index)
{
	return (segment *)(index * SEGMENT_SIZE + first_segment_address);
}

// int64_t static inline best_fit_index(size_t size, const size_t *sizes_array, size_t len)
// {
// 	// ceil div to get number of blocks needed, then multiply by block size
// 	// TODO: change this to use a static array of predefined pages_sizes
// 	unsigned i;
// 	for (i = 0; i < len; i++)
// 	{
// 		if (size <= sizes_array[i])
// 		{
// 			return i;
// 		}
// 	}
// 	return -1;
// }

// gets the appropriate index for the block size that fits <size> in our all_sizes array
// NOTE: all items up until and including 1024, have indexes that align with pages_direct's indexes
//		so, this can be used to get an index for that
//  size_t static inline nearest_block_size_index(size_t size) {
//  	//ceil div to get number of blocks needed, then multiply by block size
//  	//TODO: change this to use a static array of predefined pages_sizes
//  	unsigned i;
//  	for (i = 0; i < NUM_TOTAL_SIZES; i++)
//  	{
//  		if (size <= all_sizes[i])
//  		{
//  			return i;
//  		}
//  	}
//  }

/// NOTE: when using this, you must check if the index is within NUM_DIRECT_PAGES
#define pages_direct_index(size) ((((size) + 7) >> 3) - 1)

// get index into all_sizes
size_t static inline nearest_block_size_index(size)
{
	if (size <= MALLOC_SMALL_THRESHOLD)
		return pages_direct_index(size);
	if (size <= MALLOC_LARGE_THRESHOLD)
		return nearest_block_size_index(MALLOC_SMALL_THRESHOLD) + ceill(log2(size)) - ceill(log2(MALLOC_SMALL_THRESHOLD));
	return nearest_block_size_index(MALLOC_LARGE_THRESHOLD) + 1; // NOTE: this index is outside of all_sizes, meant for error checking
}

// NOTE: in pages (the linked list) the list for 32 block size has to be able to hold 24 block size pages, otherwise we would not be able
//		to have 24 block sized pages in pages direct (more than 1 at least)
size_t static inline nearest_block_size(size_t size)
{
	if (size > MALLOC_LARGE_THRESHOLD)
		return size; // HUGE pages have custom block sizes, the pages only contain one block
	return all_sizes[nearest_block_size_index(size)];
}

// TODO make this deterministic, getting size class should just be 2 calculations
/// gets the appropriate index for pages
// size_t static inline size_class(size_t sz)
// {
// 	unsigned i;
// 	for (i = 0; i < NSIZES; i++)
// 	{
// 		if (sz <= pages_sizes[i])
// 		{
// 			return i;
// 		}
// 	}

/// get index into heap->pages
size_t static inline size_class(size_t size)
{
	if (size < 8)
		return 0;
	long double index = ceill(log2(size)) - 3;		  // 2^3 == 8, 8 is our first element, each next element goes up by a power of 2
	return index < NUM_PAGES ? index : NUM_PAGES - 1; // last element is reserved for sizes past a certain point
}

// 	printf("Subpage allocator cannot handle allocation of size %lu\n",
// 		   (unsigned long)sz);
// 	exit(1);

// 	// keep compiler happy
// 	return 0;
// }

/// for debugging linked lists, TODO: remove this
bool linked_list_contains(struct page *list, struct page *pg)
{
	for (struct page *page = list; page != NULL; page = page->next)
	{
		if (page == pg)
			return true;
	}
	return false;
}

void page_collect(page *page)
{
	page->free = page->local_free; // move the local num_free_pages list
	page->local_free = NULL;
	// move the thread num_free_pages list atomically
	struct block_t *tfree = atomic_exchange(&page->thread_free, NULL);
	if (tfree == NULL)
		return;
	// append freelist to thread freelist
	struct block_t *tail = tfree;
	while (tail->next != NULL)
		tail = tail->next;
	tail->next = page->free;
	page->free = tfree; // head of thread freelist is now head of free list
}

enum page_kind_enum get_page_kind(size_t size)
{
	if (size <= MALLOC_SMALL_THRESHOLD)
		return SMALL;
	if (size <= MALLOC_LARGE_THRESHOLD)
		return LARGE;
	return HUGE;
}

void create_free_blocks(struct page *page)
{
	// // debug
	// struct block_t *first_block = (struct block_t *)page->page_area;
	// size_t allocated_blocks = 1;
	// size_t num_blocks = page->total_num_blocks;
	// struct block_t *curr_block = first_block;
	// for (allocated_blocks; allocated_blocks < num_blocks; allocated_blocks++)
	// {
	// 	// next block should be block_size + next pointer away from current block
	// 	// TODO assert that pointer to every block + blocksize <= capacity
	// 	struct block_t *next_block = (struct block_t *)((address)curr_block + page->block_size);
	// 	next_block->next = NULL;
	// 	curr_block->next = next_block;
	// 	curr_block = next_block;

	// 	// DEBUG
	// 	assert(page->capacity > (address)next_block);

	// }
	// return first_block;
	size_t page_area_offset = (size_t)page->page_area;
	for (int i = 0; i < page->total_num_blocks - 1; i++)
	{
		struct block_t *curr = ((struct block_t *)(page_area_offset + page->block_size * i));
		struct block_t *next = ((struct block_t *)(page_area_offset + page->block_size * (i + 1)));
		curr->next = next;
		next->next = NULL;

		assert(page->capacity > (address)curr);
		assert(page->capacity > (address)next);
	}
	page->free = (struct block_t *)page->page_area;
}

// modify segment, creating free pages inside it and updating metadata to reflect change.
void create_free_pages(struct segment *segment)
{
	// page *first_page = (page *)&segment->pages[0];
	// page *curr_page = first_page;
	// size_t allocated_pages = 1;
	// for (allocated_pages = 0; allocated_pages < segment->total_num_pages; allocated_pages++)
	// {
	// 	page *next_page = (page *) ((address) curr_page + sizeof(struct page));
	// 	next_page->next = NULL;
	// 	curr_page->next = next_page;
	// 	curr_page = next_page;
	// 	assert((address) segment + SEGMENT_SIZE > next_page);
	// }

	// return first_page;
	segment->pages[0].next = NULL;
	segment->pages[0].prev = NULL;

	for (int i = 0; i < segment->total_num_pages - 1; i++)
	{
		segment->pages[i].next = &segment->pages[i + 1];
		segment->pages[i + 1].next = NULL;

		segment->pages[i + 1].prev = &segment->pages[i];
	}

	segment->free_pages = &segment->pages[0];
}

void set_segment_metadata(thread_heap *heap, size_t size, size_t num_contiguous_segments_required, segment *new_seg); // TODO: move the definiton here and remove this

segment *malloc_huge_segment(thread_heap *heap, size_t size)
{
	segment *new_seg = NULL;

	size_t num_contiguous_segments_required = (size + sizeof(struct segment) + SEGMENT_SIZE - 1) / SEGMENT_SIZE; // ceil division by segment size

	pthread_mutex_lock(&segment_bitmap_lock);

	// we need to find consecutive segment sized chunks of memory
	// if we don't have enough we need to sbrk the remaining amount needed
	size_t num_contiguous = 0;
	size_t chunk_start_index = 0; // the segment index of our huge segment

	if (num_segments_free > 0)
	{
		for (size_t i = 0; i < num_segments_allocated; i++)
		{
			if (segment_in_use(i))
			{
				// we ran into a non free segment chunk of memory, we have to start over
				num_contiguous = 0;
				chunk_start_index = i + 1;
			}
			else
			{
				num_contiguous++;
			}
		}
	}
	else
	{
		chunk_start_index = num_segments_allocated;
	}

	assert(chunk_start_index < num_segments_capacity);
	new_seg = index_to_segment_address(chunk_start_index); // this is gonna be the start of our huge segment

	// reserve existing contiguous chunk
	for (size_t i = chunk_start_index; i < chunk_start_index + num_contiguous; i++)
	{
		assert(segment_in_use(i) == false);
		set_segment_in_use(i, true);
		num_segments_free--;
	}

	size_t num_segments_to_allocate = num_contiguous_segments_required - num_contiguous;

	if (num_segments_to_allocate > 0)
	{
		mem_sbrk(SEGMENT_SIZE * num_segments_to_allocate); //"extend" our segment if needed, by the remaining amount we need
		num_segments_allocated += num_segments_to_allocate;
		assert(num_segments_allocated <= num_segments_capacity);

		for (size_t i = chunk_start_index + num_contiguous; i < chunk_start_index + num_contiguous_segments_required; i++)
		{
			set_segment_in_use(i, true);
		}
	}
	pthread_mutex_unlock(&segment_bitmap_lock);
	set_segment_metadata(heap, size, num_contiguous_segments_required, new_seg);

	return new_seg;
}
// TODO: Add reclaiming of unused segment
segment *malloc_segment(thread_heap *heap, size_t size)
{
	if (size > MALLOC_LARGE_THRESHOLD)
		return malloc_huge_segment(heap, size);

	segment *new_seg = NULL;

	// check for segment with uninitialized page
	if (get_page_kind(size) == SMALL) // only applies to small segs, in-use large/huge segs will never have a free page
	{
		for (struct segment *seg = heap->small_segment_refs; seg != NULL; seg = seg->next)
		{
			if (seg->free_pages != NULL)
				return seg;
		}
	}

	pthread_mutex_lock(&segment_bitmap_lock);
	if (new_seg == NULL && num_segments_free > 0)
	{
		// TODO: if its a huge page check for contiguous blocks (with appropriate num of blocks)
		for (size_t i = 0; i < num_segments_allocated; i++)
		{
			if (!segment_in_use(i))
			{
				new_seg = index_to_segment_address(i);
				num_segments_free--;
				set_segment_in_use(i, true);
			}
		}
	}

	if (new_seg == NULL)
	{
		assert(num_segments_allocated < num_segments_capacity);
		new_seg = mem_sbrk(SEGMENT_SIZE);
		assert(new_seg != NULL);
		set_segment_in_use(num_segments_allocated, true);
		num_segments_allocated++;
		assert(num_segments_allocated <= num_segments_capacity);
	}
	pthread_mutex_unlock(&segment_bitmap_lock);

	set_segment_metadata(heap, size, 1, new_seg);

	return new_seg;
}

void set_segment_metadata(thread_heap *heap, size_t size, size_t num_contiguous_segments_required, segment *new_seg)
{
	assert(get_segment(new_seg) == new_seg);		// TODO: the assertion
	assert((uintptr_t)new_seg % SEGMENT_SIZE == 0); // ensure address is aligned
	enum page_kind_enum page_kind = get_page_kind(size);
	// assert(page_kind != HUGE);
	new_seg->cpu_id = heap->cpu_id;
	// (From paper)...
	// we can calculate the page index by taking the difference and shifting by the
	// segment page_shift: for small pages this is 16 (= 64KiB), while for large and
	// huge pages it is 22 (= 4MiB) such that the index is always zero in those cases
	// (as there is just one page)
	new_seg->page_shift = page_kind == SMALL ? 16 : 22;
	new_seg->page_kind = page_kind;
	new_seg->total_num_pages = page_kind == SMALL ? NUM_PAGES_SMALL_SEGMENT : 1;
	new_seg->num_used_pages = 0;
	new_seg->num_free_pages = page_kind == SMALL ? NUM_PAGES_SMALL_SEGMENT : 1; // 64 pages in small, 1 in others
	new_seg->next = NULL;
	new_seg->prev = NULL;
	new_seg->num_contiguous_segments = num_contiguous_segments_required;
	size_t segment_size = SEGMENT_SIZE * num_contiguous_segments_required;

	// pointer to start of pages array
	// new_seg->free_pages = create_free_pages(new_seg);
	create_free_pages(new_seg);

	// TODO: DETERMINE PAGE AREA POINTER BASED ON SIZEOF METADATA
	// for all pages, multiply page metadata by # num pages and add size of segment metadata
	// set the page area ptr to this value
	// make sure it doesnt have some weird alignment
	// make sure the area_ptr + #num_pages * area_size <= segment_ptr + 4mb, make sure stuff is within segment
	page *page = &new_seg->pages[0];
	page->page_area = (address)((uint64_t)new_seg + sizeof(struct segment)); // first page area starts after metadata for segment
	page->reserved = (address)(page_kind == SMALL
								   ? (uint64_t)new_seg + SMALL_PAGE_SIZE
								   : (uint64_t)new_seg + segment_size);
	assert(page->reserved - (address)new_seg <= segment_size);

	if (page_kind == SMALL)
	{
		for (int i = 1; i < new_seg->total_num_pages; i++)
		{
			page = &new_seg->pages[i];
			page->page_area = (address)((uint64_t)new_seg + SMALL_PAGE_SIZE * i);
			page->reserved = (address)((uint64_t)page->page_area + SMALL_PAGE_SIZE);

			assert(page->reserved - (address)new_seg <= segment_size);
		}
		assert((uint64_t)new_seg->pages[0].reserved - (uint64_t)new_seg->pages[0].page_area < (uint64_t)new_seg->pages[1].reserved - (uint64_t)new_seg->pages[1].page_area);
	}

	// Add segment to small_segment_refs in heap
	if (new_seg->page_kind == SMALL)
	{
		// HERE
		segment *head = heap->small_segment_refs;
		new_seg->next = head;
		if (head != NULL)
			head->prev = new_seg;
		heap->small_segment_refs = new_seg;
	}
}

// removes page node from doubly linked list
void remove_page_node(page *node)
{
	page *prev, *next;
	prev = node->prev;
	next = node->next;

	node->prev = NULL;
	node->next = NULL;

	if (prev != NULL)
		prev->next = next;
	if (next != NULL)
		next->prev = prev;
}

// removes page node from doubly linked list
void remove_segment_node(segment *node)
{
	segment *prev, *next;
	prev = node->prev;
	next = node->next;

	node->prev = NULL;
	node->next = NULL;

	if (prev != NULL)
		prev->next = next;
	if (next != NULL)
		next->prev = prev;
}

// create page
page *malloc_page(thread_heap *heap, size_t size)
{

	// check to see if there is a num_free_pages page in segment we want to go to.
	// there is space for num_free_pages page....
	segment *segment = malloc_segment(heap, size);

	// assume we have valid page pointer to create
	page *head = segment->free_pages;
	segment->free_pages = head->next;

	// head->next = NULL;
	remove_page_node(head);
	struct page *page_to_use = head;
	assert(!linked_list_contains(segment->free_pages, page_to_use)); // TODO: REMOVE, it is expensive
	assert(segment->free_pages != page_to_use);						 // should our page should be removed from free list
	assert(segment->free_pages == NULL || segment->free_pages->prev == NULL);
	assert(page_to_use->next == NULL);
	assert(page_to_use->prev == NULL);

	segment->num_free_pages--;
	segment->num_used_pages++;

	// // DEBUG INFO
	page_to_use->in_use = true;
	page_to_use->block_size = nearest_block_size(size); // TODO: make an array of ALL block pages_sizes possible
	page_to_use->num_used = 0;
	page_to_use->local_free = NULL;

	// atomic
	atomic_store(&page_to_use->thread_free, NULL);
	atomic_store(&page_to_use->num_thread_freed, 0); // Number of blocks freed by other threads

	// logic for getting number of blocks
	// each block_t takes up sizeof(block_t) + size_class(size) = blocksize
	// capacity needs to align with segment size (4 MB)
	// sizeof(segment) + (sizeof(page) + capacity * block_size) * num_pages <= 4MB
	// alternatively if segment metadata is NOT part of the page
	// (sizeof(page) + capacity * block_size) * num_pages <= 4MB
	// capacity <= (((4MB - sizeof(segment))/num_pages) - sizeof(page))/block_size
	// segment->page_kind

	uint64_t available_space = (uint64_t)page_to_use->reserved - (uint64_t)page_to_use->page_area;
	page_to_use->total_num_blocks = segment->page_kind == HUGE ? 1 : available_space / page_to_use->block_size; // 1 big block for huge pages (NOTE: there is room for improvement here, we set huge pages to have 1 block, so that blocks always have an address within the segment's aligned address, otherwise we have to set up datastructures to figure out where the segment actually starts)
	// huge pages only ever have 1 block.

	// These segments are 4MiB
	// (or larger for huge objects that are over 512KiB), and start with the segment- and
	// page meta data, followed by the actual pages where the first page is shortened
	// by the size of the meta data plus a guard page

	size_t useable_space = page_to_use->total_num_blocks * page_to_use->block_size;
	page_to_use->capacity = (address)((uint64_t)page_to_use->page_area + useable_space);

	// debug
	// debug, turn off via compile flag or something later
	// page->capacity =
	assert(page_to_use->capacity <= page_to_use->reserved);
	assert(segment->page_kind == HUGE || page_to_use->capacity - (address)segment <= SEGMENT_SIZE);

	// set up block_t freelist
	// page-> free is start of freelist
	// page->free = create_free_blocks(page);
	create_free_blocks(page_to_use);
	assert((address)page_to_use->free == (address)page_to_use->page_area);
	assert((address)page_to_use->free < page_to_use->capacity);

	return page_to_use;
}

void segment_free(struct segment *segment)
{
	// TODO: HUGE PAGES/SEGMENTS
	size_t index = segment_address_to_index(segment);
	// TODO remove or put behind IFNDEF
	pthread_mutex_lock(&segment_bitmap_lock);

	assert(segment->num_contiguous_segments <= MAX_NUM_SEGMENTS);
	assert(index + segment->num_contiguous_segments <= num_segments_allocated); // make sure indexes we access are within legal bounds, ie should not be freeing things outside of our sbrk'd mem

	for (int i = index; i < index + segment->num_contiguous_segments; i++)
	{
		assert(segment_in_use(i) == true); // should not be freeing a segment that is free
		set_segment_in_use(i, false);
		num_segments_free++;
	}

	// update local heap metadata
	size_t id = get_cpuid();
	thread_heap *heap = &tlb[id];
	assert(id == heap->cpu_id && heap->init);
	if (heap->small_segment_refs == segment)
		heap->small_segment_refs = segment->next;
	remove_segment_node(segment);

	pthread_mutex_unlock(&segment_bitmap_lock);
}

void page_free(struct page *page)
{
	page->in_use = false;

	struct segment *segment = get_segment(page);
	assert(!linked_list_contains(segment->free_pages, page)); // TODO: REMOVE, it is expensive

	assert((size_t)segment % SEGMENT_SIZE == 0);

	// update heap data
	thread_heap *heap = &tlb[get_cpuid()];
	assert(heap->init == true);
	struct page **size_class_list = &heap->pages[size_class(page->block_size)];
	// assert(!linked_list_contains(*size_class_list, page));//TODO: REMOVE, it is expensive
	assert(segment->free_pages != *size_class_list);
	// TODO: ensure this is proper logic
	if (*size_class_list != NULL && *size_class_list == page)
		*size_class_list = page->next;
	assert(*size_class_list == NULL || segment->free_pages != *size_class_list);
	size_t pages_direct_idx = pages_direct_index(page->block_size);
	if (pages_direct_idx < NUM_DIRECT_PAGES && heap->pages_direct[pages_direct_idx] == page)
	{
		heap->pages_direct[pages_direct_idx] = NULL;
	}
	remove_page_node(page); // remove from heap->pages

	// update free list
	struct page *head = segment->free_pages;
	assert(head != page);
	assert(page->next != page);
	assert(page->prev != page);
	page->next = head;
	if (head != NULL)
		head->prev = page;
	assert(page->next != page);
	assert(page->prev != page);
	assert(!linked_list_contains(segment->free_pages, page)); // TODO: REMOVE, it is expensive
	segment->free_pages = page;
	segment->num_free_pages++;
	segment->num_used_pages--;
	assert(segment->total_num_pages == segment->num_free_pages + segment->num_used_pages);

	if (segment->num_used_pages == 0)
	{
		segment_free(segment);
	}
}

void *mm_malloc(size_t sz);

// TODO: rotate linked list to optimize walking through the list
void *malloc_generic(thread_heap *heap, size_t size)
{
	// breakpoint here to make sure the block size and page indices are correct
	size_t block_size = nearest_block_size(size);
	int64_t pages_direct_idx = pages_direct_index(size);
	int pages_idx = size_class(size);

	assert(heap->pages[pages_idx] == NULL || heap->pages[pages_idx]->prev == NULL);
	for (struct page *page = heap->pages[pages_idx]; page != NULL; page = page->next)
	{
		assert(page->next != page);
		assert(page->prev != page);

		// only collect page if no block references available. 
		if(page->free == NULL){
			page_collect(page);
		}

		if (page->num_used - atomic_load(&page->num_thread_freed) == 0)
		{					 // objects currently used - objects freed by other threads = 0
			page_free(page); // add page to segment's free_pages
		}
		else if (page->free != NULL && page->block_size == block_size)
		{

			if (pages_direct_idx < NUM_DIRECT_PAGES)
			{ // update pages_direct entry if block size is appropriate
				heap->pages_direct[pages_direct_idx] = page;
			}
			// TODO: When a page is found with free space, the page list is also rotated at that point so that a next search starts from that point. <- optimization

			return mm_malloc(size);
		}
	}

	// page/segment alloc path.
	struct page *page = malloc_page(heap, size); // TODO: change page num field
	assert(page->next == NULL);
	assert(page->prev == NULL);
	struct block_t *block = page->free;
	page->free = block->next;
	block->next = NULL;
	page->num_used++;

	// update pointers
	assert(page->next != page);
	assert(page->prev != page);

	struct page *old_head = heap->pages[pages_idx];
	assert(!linked_list_contains(old_head, page)); // TODO: REMOVE, it is expensive
	page->next = old_head;
	if (old_head != NULL)
		old_head->prev = page;
	assert(page->next != page);
	assert(page->prev != page);
	heap->pages[pages_idx] = page;
	if (pages_direct_idx < NUM_DIRECT_PAGES)
	{ // update pages_direct entry if block size is appropriate
		heap->pages_direct[pages_direct_idx] = page;
	}

	return block;
}

void *malloc_large(thread_heap *heap, size_t size)
{
	size_t block_size = nearest_block_size(size);
	for (struct page *page = heap->pages[size_class(size)]; page != NULL; page = page->next)
	{
		if (page->free != NULL && page->block_size == block_size)
		{
			struct block_t *block = page->free;
			page->free = block->next;
			page->num_used++;
			assert(page->num_used <= page->total_num_blocks);
			return block;
		}
	}
	return malloc_generic(heap, size);
}

void *malloc_huge(thread_heap *heap, size_t size)
{
	return malloc_large(heap, size);
}

void *malloc_small(thread_heap *heap, size_t size)
{
	// breakpoint here to check the index of direct page
	// assert(size <)
	size_t page_index = pages_direct_index(size);
	assert(page_index < NUM_DIRECT_PAGES);
	struct page *page = heap->pages_direct[page_index];
	// if the page exists, make sure it's block size is correct
	assert(page == NULL || page->block_size == (page_index + 1) * 8);

	// similarly, if the page exists but page->free is NULL
	assert(page == NULL || page->free != NULL || page->free == NULL && page->num_used > 5);
	if (page == NULL || page->free == NULL)
	{
		return malloc_generic(heap, size);
	}

	struct block_t *block = page->free;
	page->free = block->next;
	page->num_used++;
	assert(page->num_used <= page->total_num_blocks);

	return block;
}

void *mm_malloc(size_t sz)
{
	if (sz == 0)
		return NULL;
	// return malloc(sz);
	// get CPU ID via syscall
	size_t cpu_id = get_cpuid();

	// if local thread heap is not initialized
	if (!tlb[cpu_id].init)
	{
		tlb[cpu_id].init = true;
		memset(tlb[cpu_id].pages_direct, 0, sizeof(page *) * NUM_DIRECT_PAGES);
		memset(tlb[cpu_id].pages, 0, sizeof(page *) * NUM_PAGES);
		tlb[cpu_id].cpu_id = cpu_id;
		tlb[cpu_id].small_segment_refs = NULL;
		// tlb[cpu_id].small_page_refs = NULL;
	}

	// breakpoint here to check tlb metadata
	if (sz <= MALLOC_SMALL_THRESHOLD)
	{
		return malloc_small(&tlb[cpu_id], sz);
	}
	else if (sz <= MALLOC_LARGE_THRESHOLD)
	{
		return malloc_large(&tlb[cpu_id], sz);
	}
	// assert(false);
	return malloc_huge(&tlb[cpu_id], sz);

	// return NULL;
}

void mm_free(void *ptr)
{

	(void)ptr; /* Avoid warning about unused variable */
	// free(ptr);
	if (ptr == NULL)
		return;
	struct segment *segment = get_segment(ptr);
	assert(segment != ptr);
	assert((size_t)segment % SEGMENT_SIZE == 0);
	if (segment == NULL)
	{
		return;
	}

	size_t page_index = ((uintptr_t)ptr - (uintptr_t)segment) >> segment->page_shift;
	assert((segment->page_kind != SMALL && page_index == 0) || page_index < NUM_PAGES_SMALL_SEGMENT); // TODO: adjust for small pages or remove if not easy to do that

	page *page = &segment->pages[page_index];
	assert(!linked_list_contains(segment->free_pages, page)); // TODO: REMOVE, it is expensive
	assert(page != segment->free_pages);

	struct block_t *block = (struct block_t *)ptr;

	size_t id = get_cpuid();
	assert(tlb[id].init == true);
	if (id == segment->cpu_id)
	{ // local free
		block->next = page->local_free;
		page->local_free = block;
		page->num_used--;
		if (page->num_used - atomic_load(&page->num_thread_freed) == 0)
			page_free(page);
	}
	else
	{ // non-local free TODO: finish this, make fields that need to be atomic actually atomic
		atomic_push(&page->thread_free, block);
		atomic_fetch_add(&page->num_thread_freed, 1);
	}
}

// int mm_init(void)
// {
// 	if (dseg_lo == NULL && dseg_hi == NULL)
// 	{
// 		int init = mem_init();
// 		address starting_point = NEXT_ADDRESS;
// 		uint64_t alignment_space =  SEGMENT_SIZE - (uint64_t)starting_point % SEGMENT_SIZE;

// 		//if we don't have enough for our local heaps
// 		if (alignment_space < sizeof(thread_heap) * NUM_CPUS) {
// 			alignment_space += SEGMENT_SIZE;
// 		}

// 		thread_heap *heap = mem_sbrk(alignment_space);
// 		tlb = &heap;

// 		assert((uint64_t)(NEXT_ADDRESS) % SEGMENT_SIZE == 0);

// 		for (int i = 0; i < NUM_CPUS; i++)
// 		{
// 			tlb[i].init = 0;
// 		}

// 		first_segment_address = NEXT_ADDRESS;

// 		memset(segment_bitmap, 0, sizeof(uint8_t) * MAX_NUM_SEGMENTS);

// 		return init;
// 	}
// 	return 0;
// }

int mm_init(void)
{

	if (dseg_lo == NULL && dseg_hi == NULL)
	{
		int init = mem_init();
		address starting_point = NEXT_ADDRESS;
		uint64_t alignment_space = SEGMENT_SIZE - (uint64_t)starting_point % SEGMENT_SIZE;
		// pointer to a pointer of the first heap

		// if we don't have enough for our local heaps
		while (alignment_space < sizeof(thread_heap) * NUM_CPUS)
		{
			alignment_space += SEGMENT_SIZE;
		}

		tlb = mem_sbrk(alignment_space);

		assert((uint64_t)(NEXT_ADDRESS) % SEGMENT_SIZE == 0);

		for (int i = 0; i < NUM_CPUS; i++)
		{
			tlb[i].init = false;
		}

		first_segment_address = NEXT_ADDRESS;
		pthread_mutex_lock(&segment_bitmap_lock);
		num_segments_capacity = (MEM_LIMIT - ((uint64_t)first_segment_address - (uint64_t)starting_point)) / SEGMENT_SIZE;

		memset(segment_bitmap, 0, sizeof(uint8_t) * MAX_NUM_SEGMENTS);
		pthread_mutex_unlock(&segment_bitmap_lock);

		return init;
	}

	return 0;
}
