#ifndef FREELIST_NEW_H
#define FREELIST_NEW_H

#include "my_headers.h"
#include "slab_new.h"

//struct freelist;
//struct freeblock_entry;
//struct freelist_entry_new;
struct freelist_entry_new {
   uint64_t slab_idx;
   //struct freelist_entry_new *prev;
   struct freelist_entry_new *next;
};

struct freeblock_entry {
   struct freelist_entry_new *free_slots;
   uint16_t size; // number of free slots in the block
   //struct freeblock_entry *prev;
   struct freeblock_entry *next;
   uint32_t blk_id;
};

struct freelist {
   struct freeblock_entry **freeblock_array;
   struct freeblock_entry *sorted_free_block_head;
   struct freeblock_entry *sorted_free_block_tail;
   uint64_t num_blocks;
   uint64_t num_free_blocks;
};

struct freelist* init_free_list(int num_blocks); 
void add_item_in_free_list_new(struct slab_new *s, size_t idx); 
void sort_items_in_free_list(struct slab_new *s);
int get_free_item_idx_new(struct slab_new *s);
void delete_freelist(struct slab_new *s);
void print_freeblock_array(struct slab_new *s);
void print_sorted_free_blocks(struct slab_new *s);

#endif
