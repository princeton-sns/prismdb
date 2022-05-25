#ifndef FREELIST_NEW_H
#define FREELIST_NEW_H

#include "my_headers.h"
#include "slab_new.h"

struct freelist_entry_new;
// void add_item_in_free_list(struct slab *s, size_t idx, struct item_metadata *item);
// void add_son_in_freelist(struct slab *s, size_t idx, struct item_metadata *item);
// void get_free_item_idx(struct slab_callback *cb);

// void add_item_in_free_list_recovery(struct slab *s, size_t idx, struct item_metadata *item);
// void rebuild_free_list(struct slab *s);
// void print_free_list(struct slab *s, int depth, struct item_metadata *item);
void add_item_in_free_list_new(struct slab_new *s, size_t idx);
int get_free_item_idx_new(struct slab_new *s); 
// Helper function to print the free list slab index values
void print_freelist_slab_idx(struct slab_new *s);
#endif
