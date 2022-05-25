/*
 * Freelist implementation
 */

#include "freelist_new.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

//struct freelist {
//   struct freeblock_entry **freeblock_array;
//   struct freeblock_entry *sorted_free_block_head;
//   uint64_t num_blocks;
//   uint64_t num_free_blocks;
//};
//
//struct freeblock_entry {
//   struct freelist_entry_new *free_slots;
//   uint16_t size; // number of free slots in the block
//   //struct freeblock_entry *prev;
//   struct freeblock_entry *next;
//   uint32_t blk_id;
//};
//
//struct freelist_entry_new {
//   uint64_t slab_idx;
//   //struct freelist_entry_new *prev;
//   struct freelist_entry_new *next;
//};

struct freelist* init_free_list(int num_blocks) {
   struct freelist *free_list;
   free_list = (struct freelist *)malloc(1 * sizeof(struct freelist));
   if (free_list == NULL) {
      fprintf(stderr, "init malloc freelist failed!\n");
   }
   
   struct freeblock_entry **freeblock_array;
   freeblock_array = (struct freeblock_entry **)malloc(num_blocks * sizeof(struct freeblock_entry *));
   if (freeblock_array == NULL) {
      fprintf(stderr, "init malloc freeblock array failed!\n");
   }
   for (int i = 0; i < num_blocks; ++i) {
      freeblock_array[i] = NULL;
   }
   
   free_list->freeblock_array = freeblock_array;
   free_list->sorted_free_block_head = NULL;
   free_list->sorted_free_block_tail = NULL;
   free_list->num_free_blocks = 0; 
   free_list->num_blocks = num_blocks;
   return free_list;
}

// Note that the only time new free slots are added is by the end of a migration, or when a item changes size
void add_item_in_free_list_new(struct slab_new *s, size_t idx) {
   //fprintf(stderr, "FREELIST: add_item_in_free_list_new begins idx=%d slabsize %d\n", idx, s->item_size);
   struct freelist *free_list = s->free_list;
   // insert the free slot entry to freeblock_array
   int num_slots_per_page = PAGE_SIZE / s->item_size;
   int block_idx = idx / num_slots_per_page;

   // allocate free block on demand 
   if (free_list->freeblock_array[block_idx] == NULL) {
      //fprintf(stderr, "allocate for block %d\n", block_idx);
      free_list->freeblock_array[block_idx] = (struct freeblock_entry *)malloc(1*sizeof(struct freeblock_entry));
      free_list->freeblock_array[block_idx]->free_slots = NULL;
      free_list->freeblock_array[block_idx]->size = 0;
      //free_list->freeblock_array[block_idx]->prev = NULL;
      free_list->freeblock_array[block_idx]->next = NULL;
      free_list->freeblock_array[block_idx]->blk_id = block_idx;

      if (free_list->sorted_free_block_tail == NULL) { 
        free_list->sorted_free_block_head = free_list->freeblock_array[block_idx];
        free_list->sorted_free_block_tail = free_list->freeblock_array[block_idx];
      } else {
        free_list->sorted_free_block_tail->next = free_list->freeblock_array[block_idx];
        free_list->sorted_free_block_tail = free_list->sorted_free_block_tail->next;
      }
        
   }
   struct freeblock_entry *curr_block = free_list->freeblock_array[block_idx];

   struct freelist_entry_new *new_entry;
   new_entry = calloc(1, sizeof(*new_entry));
   new_entry->slab_idx = idx;
   new_entry->next = NULL;

   struct freelist_entry_new *curr_free_slots_head = curr_block->free_slots;
   if (curr_free_slots_head == NULL) {
      //new_entry->prev = NULL;
      curr_block->free_slots = new_entry;
      free_list->num_free_blocks++;
   } else {
      while (curr_free_slots_head->next != NULL) {
         curr_free_slots_head = curr_free_slots_head->next;
      }
      curr_free_slots_head->next = new_entry;
   }
   curr_block->size++;
   s->nb_free_items++;
   //fprintf(stderr, "FREELIST: add_item_in_free_list_new completes idx=%d slabsize %d\n", idx, s->item_size);
}

// sort on size in descending order
int cmpfunc (const void * a, const void * b) {
    int a1 = (*(struct freeblock_entry **)a)->size;
    int b1 = (*(struct freeblock_entry **)b)->size;
    return (b1 - a1);
}

// sorted the free blocks in descending order based on the size 
// (aka the number of free slots within the block)
void sort_items_in_free_list(struct slab_new *s) {
   //fprintf(stderr, "FREELIST: sort_items_in_free_list begins\n");
   struct freelist *free_list = s->free_list;
   if (free_list->num_free_blocks < 1) {
      return;
   }
   //assert (free_list->num_free_blocks > 0 && "freelist num_free_blocks is 0!\n");
   // Pass 1: scan over all blocks to include all non-empty free blocks
   fprintf(stderr, "begin malloc to_sort with size %d\n", free_list->num_free_blocks);
   struct freeblock_entry **to_sort = (struct freeblock_entry **)malloc((free_list->num_free_blocks)*sizeof(struct freeblock_entry *));
   if (to_sort == NULL) {
      fprintf(stderr, "ERROR: malloc to_sort array fails!\n");
      return;
   }

   int j = 0;
   for (int i = 0; i < free_list->num_blocks; ++i) {
      if (free_list->freeblock_array[i] == NULL) {
         continue;
      }
      if (free_list->freeblock_array[i]->size < 1) {
         // delay the de-allocation of empty free blocks to the migration stage
         //fprintf(stderr, "ERROR: free the block during sorting free blocks, should not happen!\n");
         free(free_list->freeblock_array[i]); 
         continue;
      }
      to_sort[j] = free_list->freeblock_array[i];
      j++;
   }
   assert(j == free_list->num_free_blocks && "end of pass 1, j != num_free_blocks");
   //fprintf(stderr, "end of pass 1: num_free_blocks is %d and j is %d, j should be 1 greater\n", free_list->num_free_blocks, j);

   // Pass 2: sort all non-empty blocks in descending order 
   qsort(to_sort, free_list->num_free_blocks, sizeof(struct freeblock_entry **), cmpfunc);
   // sorted_free_block_head points to the block with largest number of free slots
   // and update the next pointer of non-empty free blocks
   free_list->sorted_free_block_head = to_sort[0];
   int i = 0;
   for (; i < (free_list->num_free_blocks - 1); ++i) {
      to_sort[i]->next = to_sort[i+1];
   }
   to_sort[i]->next = NULL;
   free_list->sorted_free_block_tail = to_sort[i];
   assert( (i+1) == free_list->num_free_blocks && "end of pass 2, i+1 != num_free_blocks");
   //fprintf(stderr, "end of pass 2: num_free_blocks is %d and i is %d\n, they should be equal", free_list->num_free_blocks, i);

   // de-allocate to_sort array 
   free(to_sort);
   //fprintf(stderr, "FREELIST: sort_items_in_free_list completes\n");
}


int get_free_item_idx_new(struct slab_new *s) {
   //fprintf(stderr, "FREELIST: get_free_item_idx_new begins, slabsize = %d\n", s->item_size);
   if (s->nb_free_items < 1) {
       return -1;
   }

   struct freelist *free_list = s->free_list;
   struct freeblock_entry *curr_block = free_list->sorted_free_block_head;
   // fprintf(stderr, "FREELIST: block %d has %d free slots\n", curr_block->blk_id, curr_block->size);
   struct freelist_entry_new *old_entry = curr_block->free_slots;
   uint64_t slab_idx = old_entry->slab_idx;

   curr_block->free_slots = old_entry->next;
   free(old_entry);
   curr_block->size--;

   if (curr_block->size < 1) {
      // current free block is completely consumed, move on to the next free block
      //fprintf(stderr, "FREELIST: block %d has zero free slots, de-allocate it\n", curr_block->blk_id);
      free_list->sorted_free_block_head = curr_block->next;
      if (free_list->sorted_free_block_tail == curr_block) {
        free_list->sorted_free_block_tail = NULL;
      }
      free_list->freeblock_array[curr_block->blk_id] = NULL;
      free(curr_block);
      free_list->num_free_blocks--;
   }

   s->nb_free_items--;
   //fprintf(stderr, "FREELIST: get_free_item_idx_new completes, slabsize = %d\n", s->item_size);
   return (int) slab_idx;
}

void delete_freelist(struct slab_new *s) {
   struct freelist *free_list = s->free_list;
   for (int i = 0; i < free_list->num_blocks; ++i) {
      struct freeblock_entry *free_block = free_list->freeblock_array[i];
      if (free_block == NULL) {
         continue;
      }

      struct freelist_entry_new *free_entry = free_block->free_slots;
      while (free_entry != NULL) {
         struct freelist_entry_new *prev_entry = free_entry;
         free_entry = free_entry->next;
         free(prev_entry);
      }
      free(free_block);
   }
   free(free_list->freeblock_array);
   free(free_list);
}

// debug functions 
void print_freeblock_array(struct slab_new *s) {
   struct freelist *free_list = s->free_list;
   uint64_t num_free_blocks = 0;
   for (int i = 0; i < free_list->num_blocks; ++i) {
      if (free_list->freeblock_array[i] == NULL) {
         fprintf(stderr, "block %d: NULL\n", i);
         continue;
      }
      fprintf(stderr, "block %d: size %hu free_slots ", i, free_list->freeblock_array[i]->size);
      struct freelist_entry_new *curr_entry = free_list->freeblock_array[i]->free_slots;
      if (curr_entry == NULL) {
         fprintf(stderr, "NULL\n");
      } else {
         while (curr_entry != NULL) {
            fprintf(stderr, "%lu ", curr_entry->slab_idx);
            curr_entry = curr_entry->next;
         }
         fprintf(stderr, "\n");
         num_free_blocks++;
      }
   }
   if (num_free_blocks != free_list->num_free_blocks) {
      fprintf(stderr, "ERROR: number of free blocks does not match, get %lu, should %lu\n", num_free_blocks, free_list->num_free_blocks);
   }
   fprintf(stderr, "print_freeblock_array SUCCESS!\n");
}

void print_sorted_free_blocks(struct slab_new *s) {
   struct freelist *free_list = s->free_list;
   uint64_t num_free_blocks = 0;
   uint64_t num_free_slots = 0;
   struct freeblock_entry *curr_block = free_list->sorted_free_block_head;
   while (curr_block != NULL) {
      fprintf(stderr, "block %u size %hu free_slots ", curr_block->blk_id, curr_block->size);
      if (curr_block->size == 0) {
         if (curr_block->free_slots != NULL) {
            fprintf(stderr, "ERROR: zero size but non-empty free slots!\n");
            break;
         }
      }
      struct freelist_entry_new *curr_entry = curr_block->free_slots;
      uint16_t num_entries = 0;
      while (curr_entry != NULL) {
         fprintf(stderr, "%lu ", curr_entry->slab_idx);
         curr_entry = curr_entry->next;
         num_entries++;
      }
      fprintf(stderr, "\n");
      if (num_entries != curr_block->size) {
         fprintf(stderr, "ERROR: size of free block != number of free entries, get %hu, should be %hu\n", num_entries, curr_block->size);
         break;
      }
      num_free_slots += num_entries;
      num_free_blocks++;
      curr_block = curr_block->next;
   }
   if (num_free_blocks != free_list->num_free_blocks) {
      fprintf(stderr, "ERROR: number of free blocks does not match, get %lu, should be %lu\n", num_free_blocks, free_list->num_free_blocks);
   }
   if (num_free_slots != s->nb_free_items) {
      fprintf(stderr, "ERROR: number of free slots does not match, get %lu, should be %lu\n", num_free_slots, s->nb_free_items);
   }
    
   fprintf(stderr, "print_sorted_free_blocks SUCCESS! slabsize %d has %d free slots\n", s->item_size, s->nb_free_items);
}
