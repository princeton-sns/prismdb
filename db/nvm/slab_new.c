#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

// #include <stdio.h>
// #include <stdlib.h>
// #include <stdint.h>
// #include <unistd.h>
// #include <sys/types.h>
// #include <sys/stat.h>
// #include <fcntl.h>
// #include <sys/mman.h>
// #include <assert.h>
// #include <string.h>
// #include <sys/time.h>
// #include <pthread.h>
// #include <signal.h>
// #include <sys/syscall.h>

// #include "options.h"
// #include <errno.h>
#include "slab_new.h"

//#include <fcntl.h>

// JIANAN: TODO: write ioengine_new.h for synchronous IOs
// #include "ioengine.h"
// #include "stats.h"
// #include "freelist.h"
#include "freelist_new.h"
// #include "slabworker_new.h" // JIANAN: not sure if we need to rewrite slabworker or if it is relevant
// #include "utils.h"
//#include "items.h"


#ifdef USE_O_DIRECT
int use_o_direct = 1;
#else
int use_o_direct = 0;
#endif

size_t decode_size(char* buf) {
   uint8_t *buffer = (uint8_t*) buf;
   if (sizeof(size_t) == 4) {
      return (uint32_t)(buffer[0]) |
         (uint32_t)((buffer[1]) << 8) |
         (uint32_t)((buffer[2]) << 16) |
         (uint32_t)((buffer[3]) << 24);

   } else if (sizeof(size_t) == 8) {
      return (uint64_t)(buffer[0]) |
         (uint64_t)((buffer[1]) << 8) |
         (uint64_t)((buffer[2]) << 16) |
         (uint64_t)((buffer[3]) << 24) |
         (uint64_t)((buffer[4]) << 32) |
         (uint64_t)((buffer[5]) << 40) |
         (uint64_t)((buffer[6]) << 48) |
         (uint64_t)((buffer[7]) << 56);
   }
}

/*
 * Synchronous read item
 */
 void *read_item_new(struct slab_new *s, size_t idx) {
   size_t page_num = item_page_num_new(s, idx);
   char *disk_page;
   if (use_o_direct) {
     disk_page = memalign(PAGE_SIZE, PAGE_SIZE);
   } else {
     disk_page = (char *) malloc(PAGE_SIZE);
   }

   off_t offset = page_num*PAGE_SIZE;
   int r = pread(s->fd, disk_page, PAGE_SIZE, offset);
   if(r != PAGE_SIZE)
      printf(stderr,"pread failed! Read %d instead of %lu (offset %lu)\n", r, PAGE_SIZE, offset);
      //disk_data = aligned_alloc(PAGE_SIZE, PAGE_SIZE);
    return &disk_page[item_in_page_offset_new(s, idx)];
 }


// TODO: kv size cannot go beyond a block size 4KB
bool read_item_val(struct slab_context_new *ctx, struct index_entry *e, char *buf) {
   // 1. Get the disk page data
   struct slab_new *cur_slab = ctx->slabs[e->slab];
   size_t page_num = item_page_num_new(cur_slab, e->slab_idx);
   #ifdef USE_O_DIRECT
   char disk_page[PAGE_SIZE] __attribute__((aligned(PAGE_SIZE)));
   #else
   char disk_page[PAGE_SIZE];
   #endif

   //char disk_page[PAGE_SIZE];
   off_t page_offset = page_num*PAGE_SIZE;
   int r = pread(cur_slab->fd, disk_page, PAGE_SIZE, page_offset);
   if(r != PAGE_SIZE)
      printf(stderr,"pread failed! Read %d instead of %lu (offset %lu)\n", r, PAGE_SIZE, page_offset);

   // 2. Read the key and value from the disk page
   off_t pg_offset = item_in_page_offset_new(cur_slab, e->slab_idx);
   size_t offset = pg_offset + sizeof(size_t);

   size_t key_size = decode_size(&disk_page[offset]);
   if (key_size == -1){
     return false;
   }

   offset = offset + sizeof(size_t);
   size_t val_size = decode_size(&disk_page[offset]);
   offset = pg_offset + sizeof(struct item_metadata) + key_size;
   memcpy(buf, &disk_page[offset], val_size);
   return true;
}


// Assume the caller has already memory-allocate res
// This function reads the key and value from Optane, using fields from index_entry
// And stores the results in res
void read_item_key_val(struct slab_context_new *ctx, struct index_entry *e, struct kv_pair *res) {
   // 1. Get the disk page data
   struct slab_new *cur_slab = ctx->slabs[e->slab];
   size_t page_num = item_page_num_new(cur_slab, e->slab_idx);
   #ifdef USE_O_DIRECT
   char disk_page[PAGE_SIZE] __attribute__((aligned(PAGE_SIZE)));
   #else
   char disk_page[PAGE_SIZE];
   #endif
   //char disk_page[PAGE_SIZE];
   off_t page_offset = page_num*PAGE_SIZE;
   int r = pread(cur_slab->fd, disk_page, PAGE_SIZE, page_offset);
   if(r != PAGE_SIZE)
      printf(stderr,"pread failed! Read %d instead of %lu (offset %lu)\n", r, PAGE_SIZE, page_offset);
   //fprintf(stderr, "after reading disk page\n");
   off_t pg_offset = item_in_page_offset_new(cur_slab, e->slab_idx);
   //fprintf(stderr, "after reading page offset\n");

   size_t offset = pg_offset + sizeof(size_t);
   char key_size_char[sizeof(size_t)];
   memcpy(&key_size_char, &disk_page[offset], sizeof(size_t));
   size_t key_size = decode_size(&key_size_char);
   //fprintf(stderr, "after decode key, key size %zu\n", key_size);

   offset = offset + sizeof(size_t);
   char val_size_char[sizeof(size_t)];
   memcpy(&val_size_char, &disk_page[offset], sizeof(size_t));
   size_t val_size = decode_size(&val_size_char);

   //fprintf(stderr, "after decode key, key size %zu, val_size %zu\n", key_size, val_size);

   // TODO: HACK: Key size is -1 for deleted keys, value size is 0
   if (key_size == -1){
     res->key_size = -1;
     fprintf(stderr, "after decode key, key size %d, val_size %zu\n", key_size, val_size);
     key_size = sizeof(uint64_t);
   }
   else {
    res->key_size = key_size;
   }
   res->val_size = val_size;

   offset = pg_offset + sizeof(struct item_metadata);
   memcpy(res->buf, &disk_page[offset], key_size);

   offset = pg_offset + sizeof(struct item_metadata) + key_size;
   //fprintf(stderr, "page offset %zu, key size %zu, value size %zu\n", pg_offset, key_size, val_size);
   memcpy(&res->buf[key_size], &disk_page[offset], val_size);
   //fprintf(stderr, "after copying value to buf\n");
}


int get_slab_id_new(struct slab_context_new *ctx, size_t cur_item_size) {
   // int num_slabs = sizeof(ctx->slabs) / sizeof(ctx->slabs[0]);
   for(int i = 0; i < ctx->nb_slabs; i++) {
      if(cur_item_size <= ctx->slabs[i]->item_size) {
         //fprintf(stderr, "get slab %zu, slab item size %zu\n", i, ctx->slabs[i]->item_size);
         return i;
      }
   }
   fprintf(stderr, "item is too big: cur_item_size %d ctx->slabs[0]->item_size %d\n", cur_item_size, ctx->slabs[0]->item_size);
   return -1;
}

off_t item_page_num_new(struct slab_new *s, size_t idx) {
    size_t items_per_page = PAGE_SIZE/s->item_size;
    return idx / items_per_page;
 }


//void * get_disk_page(struct slab_new *s, uint64_t idx) {
//   size_t page_num = item_page_num_new(s, idx);
//
//   char *disk_page = (char *) malloc(PAGE_SIZE);
//   off_t offset = page_num*PAGE_SIZE;
//
//   int r = pread(s->fd, disk_page, PAGE_SIZE, offset);
//   if(r != PAGE_SIZE)
//      printf(stderr,"pread failed! Read %d instead of %lu (offset %lu)\n", r, PAGE_SIZE, offset);
//   return disk_page;
//}

off_t item_in_page_offset_new(struct slab_new *s, size_t idx) {
   size_t items_per_page = PAGE_SIZE/s->item_size;
   return (idx % items_per_page)*s->item_size;
}

//void print_myitem(char *item, size_t key, size_t value) {
//   fprintf(stderr, "%s\n", "print_item");
//   fprintf(stderr, " %s: ", "meta");
//   size_t meta = sizeof(struct item_metadata);
//
//   // JIANAN: TODO
//   for (int i = 0; i < 3; i++) {
//      fprintf(stderr, "%zu ", item[i*sizeof(size_t)]);
//   }
//   fprintf(stderr, "\n %s: ", "key");
//   for (int i = 0; i < key; i++) {
//      fprintf(stderr, "%c", item[meta+i]);
//   }
//   fprintf(stderr, "\n %s: ", "value");
//   for (int i = 0; i < value; i++) {
//      fprintf(stderr, "%c", item[meta+key+i]);
//   }
//   fprintf(stderr, "\nDone print_item \n ");
//}

//char *create_item(char *key, char *value) {
//   size_t key_sz = strlen(key);
//   size_t val_sz = strlen(value);
//   size_t item_size = key_sz + val_sz + sizeof(struct item_metadata);
//   //fprintf(stderr, "key size %zu, value size %zu, meta size %zu, item size %zu \n", key_sz, val_sz, sizeof(struct item_metadata), item_size);
//
//   char *item = malloc(item_size);
//   if (item == NULL) {
//     fprintf(stderr, "create_item: malloc failed\n");
//   }
//
//   struct item_metadata *meta = (struct item_metadata *)item;
//   meta->key_size = key_sz;
//   meta->value_size = val_sz;
//
//   char *item_key = &item[sizeof(*meta)];
//   char *item_value = &item[sizeof(*meta) + meta->key_size];
//   strncpy(item_key, key, meta->key_size);
//   strncpy(item_value, value, meta->value_size);
//
//   print_myitem(item, key_sz, val_sz);
//   return item;
//}

/*
 * Create a slab: a file that only contains items of a given size.
 * @callback is a callback that will be called on all previously existing items of the slab if it is restored from disk.
 */
struct slab_new* create_slab_new(struct slab_context_new *ctx, int slab_worker_id, size_t item_size) {
   //fprintf(stderr, "create_slab_new start \n");

   struct stat sb;
   char path[512];
   struct slab_new *s = calloc(1, sizeof(*s));

   size_t disk = 0;

   sprintf(path, PATH, disk, slab_worker_id, 0LU, item_size);
   // JIANAN: TODO
   // If file is opened with O_DIRECT flag, for synchronous IO, flush + fsync?
   // JIANAN: use kernel page cache
   if (use_o_direct) {
   //s->fd = open(path,  O_RDWR | O_CREAT | O_DIRECT | O_SYNC, 0777);
    s->fd = open(path,  O_RDWR | O_CREAT | O_DIRECT, 0777);
   } else {
    s->fd = open(path,  O_RDWR | O_CREAT, 0777);
   }
   if(s->fd == -1)
      fprintf(stderr, "Cannot allocate slab %s, error code: %d, error msg: %s\n", path, errno, strerror(errno));

   fstat(s->fd, &sb);
   s->size_on_disk = sb.st_size;
   if(s->size_on_disk < 2*PAGE_SIZE) {
      // JIANAN: fallocate only supported on xfs, ext4, btrfs, tmpfs, gfs
      int allocateRes = fallocate(s->fd, 0, 0, 2*PAGE_SIZE);
      s->size_on_disk = 2*PAGE_SIZE;
      //fprintf(stderr, "fallocate returns %d, fd %d\n", allocateRes, s->fd);
   }

   size_t nb_items_per_page = PAGE_SIZE / item_size;
   s->nb_max_items = s->size_on_disk / PAGE_SIZE * nb_items_per_page;
   s->nb_items = 0;
   s->item_size = item_size;
   s->nb_free_items = 0;
   s->last_item = 0;
   s->ctx = ctx;
   fprintf(stderr, "before init_free_list\n");
   struct freelist *tmp_freelist = init_free_list(3000000);
   s->free_list = tmp_freelist;
   //s->free_list = init_free_list(3000000); //FREELIST : hardcode 3M pages
   fprintf(stderr, "after init_free_list free_list %X\n", s->free_list);

   // Read the first page and rebuild the index if the file contains data
   //fprintf(stderr, "before reading meta \n");
   struct item_metadata *meta = read_item_new(s, 0);
   //fprintf(stderr, "after reading meta\n");
   // JIANAN: TODO: ignore this case first
//    if(meta->key_size != 0) { // if the key_size is not 0 then then file has been written before
//       callback->slab = s;
//       rebuild_index(slab_worker_id, s, callback);
//    }

   //fprintf(stderr, "create_slab_new end \n");
   return s;
}

//FREELIST
void delete_all_slabs(struct slab_context_new *ctx) {
  for (int i = 0; i < ctx->nb_slabs; i++) {
    fprintf(stderr, "slab %d, before delete_freelist\n", i);
    delete_freelist(ctx->slabs[i]);
    fprintf(stderr, "slab %d, after delete_freelist\n", i);
    free(ctx->slabs[i]);
    fprintf(stderr, "slab %d, after freeing slab_new struct\n", i);
  }
}

int close_slab_fds(struct slab_context_new *ctx) {
   int res = 0;
   int num_slabs = sizeof(ctx->slabs) / sizeof(ctx->slabs[0]);
   for(int i = 0; i < num_slabs; i++) {
      int c = close(ctx->slabs[i]->fd);
      if (c == -1) {
         res = -1;
      }
   }
   return res;
}

/*
 * Double the size of a slab on disk
 */
struct slab_new* resize_slab_new(struct slab_new *s) {
   if(s->size_on_disk < 10000000000LU) {
      s->size_on_disk *= 2;
      if(fallocate(s->fd, 0, 0, s->size_on_disk))
         fprintf(stderr, "Cannot resize slab (item size %lu) new size %lu\n", s->item_size, s->size_on_disk);
      s->nb_max_items *= 2;
   } else {
      size_t nb_items_per_page = PAGE_SIZE / s->item_size;
      s->size_on_disk += 10000000000LU;
      if(fallocate(s->fd, 0, 0, s->size_on_disk))
         fprintf(stderr, "Cannot resize slab (item size %lu) new size %lu\n", s->item_size, s->size_on_disk);
      s->nb_max_items = s->size_on_disk / PAGE_SIZE * nb_items_per_page;
   }
   return s;
}

void add_item_sync(struct slab_context_new *ctx, char *item, size_t item_size, struct op_result *res, bool load_phase_) {
   // Step 0: find the slab file based on item size
   int slab_id = get_slab_id_new(ctx, item_size);
   struct slab_new *slab = ctx->slabs[slab_id];
   res->slab_id = slab_id;

   //fprintf(stderr, "FREELIST: add_item_sync\n");
   // Step 1: find if the slab's free list has any free slot
   int free_idx = get_free_item_idx_new(slab); // TODO: implement get_frree_item_idx

   // Step 2: find the correct offset location to insert the record
   size_t idx;
   if (free_idx > -1) {
      //fprintf(stderr, "FREELIST: get from freelist: idx = %d\n", free_idx);
      idx = (size_t) free_idx;
   } else {
      // if no free slot, add to the end of slab file
      // resize the slab if reaching its full capacity
      if (slab->last_item >= slab->nb_max_items) {
         resize_slab_new(slab);
      }
      idx = slab->last_item;
      //fprintf(stderr, "FREELIST: freelist is full, append to the end of file: idx = %d\n", idx);
      slab->last_item++;
   }

   // Step 3: insert the record at the correct location and update result
   //fprintf(stderr, "insert at slab %d at offset %zu\n", slab_id, idx);
   res->slab_idx = (uint64_t) idx;
   insert_item_at_idx(slab, item, item_size, idx, res, load_phase_);
   //fprintf(stderr, "FREELIST: after insert_item_at_idx\n");
   return;
}

void insert_item_at_idx(struct slab_new *slab, char *item, size_t item_size, size_t idx, struct op_result *res, bool load_phase_) {

   // 1. Get the disk page data
   size_t page_num = item_page_num_new(slab, idx);
   #ifdef USE_O_DIRECT
   char page[PAGE_SIZE] __attribute__((aligned(PAGE_SIZE)));
   #else
   char page[PAGE_SIZE];
   #endif
   //char page[PAGE_SIZE];
   off_t page_offset = page_num*PAGE_SIZE;
   int r = pread(slab->fd, page, PAGE_SIZE, page_offset);
   if(r != PAGE_SIZE)
      printf(stderr,"pread failed! Read %d instead of %lu (offset %lu)\n", r, PAGE_SIZE, page_offset);
   //fprintf(stderr, "%s\n", "get disk page");

   // get the byte offset within the page
   off_t pg_offset = item_in_page_offset_new(slab, idx);
   //fprintf(stderr, "get page byte offset %lld\n", pg_offset);

   // update the disk page
   memcpy(&page[pg_offset], item, item_size);

   // write back to the file
   // TODO: buffer wrrites
   //fprintf(stderr, "pwrite starts, fd %d, PAGE_SIZE %d, offset %d\n", slab->fd, PAGE_SIZE, page_num*PAGE_SIZE);
   int res_pwrite = pwrite(slab->fd, page, PAGE_SIZE, page_num*PAGE_SIZE);
   if (res_pwrite == -1) {
      fprintf(stderr, "pwrite fails, error code: %d, error msg: %s\n", errno, strerror(errno));
      res->success = -1;
      return;
   }
   //fprintf(stderr, "%s\n", "pwrite");

   // flush and sync
   // JIANAN: TODO: double check this, do we need fflush() or O_SYNC flag when opening this file?
   if (!load_phase_){
     int res_sync = fdatasync(slab->fd);
     if (res_sync == -1) {
         fprintf(stderr, "fdatasync fails, error code: %d, error msg: %s\n", errno, strerror(errno));
         res->success = -1;
         return;
       }
     //fprintf(stderr, "%s\n", "fdatasync done");
   }

   res->success = 0;
   //fprintf(stderr, "insert_item_at_index returns\n");
   return;
}


void update_item_sync(struct index_entry *e, struct slab_context_new *ctx, char *item, size_t item_size, struct op_result *res, bool load_phase_) {
   //fprintf(stderr, "Update\n");
   int new_slab_id = get_slab_id_new(ctx, item_size);
   struct slab_new *new_slab = ctx->slabs[new_slab_id];
   struct slab_new *old_slab = ctx->slabs[e->slab];

   // if item does not change size
   // do an in-place update with new content
   if (new_slab == old_slab) {
      res->slab_id = new_slab_id;
      res->slab_idx = e->slab_idx;
      //fprintf(stderr, "in place insert at slab %d at offset %zu\n", new_slab_id, e->slab_idx);
      insert_item_at_idx(old_slab, item, item_size, e->slab_idx, res, load_phase_);

   } else {
      //fprintf(stderr, "new insert at slab %d at offset %zu\n", new_slab_id, e->slab_idx);
      // remove item from the old slab + idx
      int delete_success = remove_item_sync(ctx, e, load_phase_);
      if (delete_success == -1) {
         fprintf(stderr, "update: failed deleting old record \n");
         // JIANAN: TODO maybe exit here?
      }
      // add it to the new slab + idx
      add_item_sync(ctx, item, item_size, res, load_phase_);

   }
   return;
}

int remove_item_sync(struct slab_context_new *ctx, struct index_entry *e, bool load_phase_) {
   // Step 1: update the record on disk with a tombstone message
   // A tombstone message has a key size of value -1 in the metadata
   char item[e->item_size];
   struct item_metadata *meta = (struct item_metadata *)item;
   // JIANAN: might need to update timestamp?
   // meta->rdt = get_rdt(s->ctx);
   //meta->rdt = 0xC0FEC0FEC0FEC0FE;

   // TODO: set correct timestamp for recovery
   //meta->rdt = e->ts;
   meta->key_size = -1;
   meta->value_size = 0;

   struct op_result res = {0, e->slab_idx, -1}; // first field is irrelevant here
   struct slab_new *cur_slab = ctx->slabs[e->slab];
   //fprintf(stderr, "before calling insert_item_at_idx where idx = %zu\n", e->slab_idx);
   insert_item_at_idx(cur_slab, &item, e->item_size, e->slab_idx, &res, load_phase_);
   //fprintf(stderr, "remove item at offset %zu, result %d\n", e->slab_idx, res.success);

   // Step 2: update the freelist
   cur_slab->nb_items--;

   add_item_in_free_list_new(cur_slab, e->slab_idx);
   return res.success;
}



int update_freelist(struct slab_context_new *ctx, struct index_entry *e){
   // Step 1: update the freelist
   struct slab_new *cur_slab = ctx->slabs[e->slab];
   cur_slab->nb_items--;
   add_item_in_free_list_new(cur_slab, e->slab_idx);
	 //fprintf(stderr, "freelist called\n");
   return 0;
}

//FREELIST
void sort_all_slab_freelist(struct slab_context_new *ctx) {
  for (int i = 0; i < ctx->nb_slabs; i++) {
    sort_items_in_free_list(ctx->slabs[i]);
  }
}

// Helper function to print the free list slab index values
void print_freelist(struct slab_context_new *ctx) {
  struct slab_new *cur_slab = NULL;
  for(int i=0; i<ctx->nb_slabs; i++){
    cur_slab = ctx->slabs[i];
    //print_freeblock_array(cur_slab);
    print_sorted_free_blocks(cur_slab);
  }
}

