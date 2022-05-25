#ifndef SLAB_NEW_H
#define SLAB_NEW_H 1

#include "indexes/btree.h"
#include "indexes/memory-item.h"

//#define USE_O_DIRECT

#ifdef USE_O_DIRECT
  #define PAGE_SIZE (512LU)
#else
  #define PAGE_SIZE (4LU*1024LU)
#endif

struct kv_pair {
   size_t key_size;
   size_t val_size;
   char *buf;
};

struct op_result {
   int slab_id;
   uint64_t slab_idx;
   int success;
};

struct item_metadata {
   size_t rdt;
   size_t key_size;
   size_t value_size;
   // key
   // value
};

struct slab_context_new {
   size_t worker_id __attribute__((aligned(64)));        // ID
   struct slab_new **slabs;                                  // Files managed by this worker
   uint64_t rdt;                                         // Latest timestamp
   int nb_slabs;
};

//struct slab_new;

/* Header of a slab -- shouldn't contain any pointer as it is persisted on disk. */
struct slab_new {
   struct slab_context_new *ctx;

   size_t item_size;
   size_t nb_items;   // Number of non freed items
   size_t last_item;  // Total number of items, including freed
   size_t nb_max_items;

   int fd;
   size_t size_on_disk;

   size_t nb_free_items, nb_free_items_in_memory;
   struct freelist *free_list; // FREELIST
   //struct freelist_entry_new *freed_items, *freed_items_tail;
   btree_t *freed_items_recovery, *freed_items_pointed_to;
};


// Helper functions
int get_slab_id_new(struct slab_context_new *ctx, size_t item_size);
off_t item_page_num_new(struct slab_new *s, size_t idx);
//void * get_disk_page(struct slab_new *s, uint64_t idx);
off_t item_in_page_offset_new(struct slab_new *s, size_t idx);
//void print_myitem(char *item, size_t key, size_t value);
size_t decode_size(char* buffer);


//char *create_item(char *key, char *value);
struct slab_new* create_slab_new(struct slab_context_new *ctx, int worker_id, size_t item_size);
void delete_all_slabs(struct slab_context_new *ctx); //FREELIST 
int close_slab_fds(struct slab_context_new *ctx);
struct slab_new* resize_slab_new(struct slab_new *s);
void *read_item_new(struct slab_new *s, size_t idx);
bool read_item_val(struct slab_context_new *ctx, struct index_entry *e, char *buf);

// populate res with a pair of pointers to key array and value array respectively
// Note: need to free two char arrays afterwards!
void read_item_key_val(struct slab_context_new *ctx, struct index_entry *e, struct kv_pair *res);

int remove_item_sync(struct slab_context_new *ctx, struct index_entry *e, bool load_phase_);
int update_freelist(struct slab_context_new *ctx, struct index_entry *e);
void sort_all_slab_freelist(struct slab_context_new *ctx); //FREELIST
// Helper function to print the free list slab index values
void print_freelist(struct slab_context_new *ctx);
void add_item_sync(struct slab_context_new *ctx, char *item, size_t item_size, struct op_result *res, bool load_phase_);
void update_item_sync(struct index_entry *e, struct slab_context_new *ctx, char *item, size_t item_size, struct op_result *res, bool load_phase_);
void insert_item_at_idx(struct slab_new *slab, char *item, size_t item_size, size_t idx, struct op_result *res, bool load_phase_);


#endif
