#ifndef MEM_ITEM_H
#define MEM_ITEM_H

#include "../my_headers.h"
#include <stddef.h>
#include <stdint.h>

struct slab;
struct slab_new;


// // JIANAN
// struct index_entry_new { // This index entry could be made much smaller by, e.g., have 64b for [slab_size, slab_idx] it is then easy to do size -> slab* given a slab context
//    struct slab_new *slab;
//    size_t slab_idx;
// };

// struct index_entry { // This index entry could be made much smaller by, e.g., have 64b for [slab_size, slab_idx] it is then easy to do size -> slab* given a slab context
//    union {
//       struct slab *slab;
//       void *page;
//    };
//    union {
//       size_t slab_idx;
//       void *lru;
//    };
// };

// key: a, b, c
// ts: 1000, 99, 1001
// p ts = 1002

/*struct index_entry { // This index entry could be made much smaller by, e.g., have 64b for [slab_size, slab_idx] it is then easy to do size -> slab* given a slab context
    struct slab_new *slab;
    size_t slab_idx;
    size_t item_size;
    uint64_t ts;
};*/

//#pragma pack(8)
//struct index_entry { // This index entry could be made smaller. under_migration field can be a single bit.
//    struct slab_new *slab;
//    size_t slab_idx;
//    uint16_t item_size;
//    uint16_t under_migration;
//};

#pragma pack(8)
struct index_entry { // This index entry could be made smaller. under_migration field can be a single bit.
    size_t slab_idx;
    uint32_t slab; // represent the index of slab file in the partition context's slab context list
    uint16_t item_size;
};

// make a smaller memory footprint of index_entry, 4 byte-aligned
// index_entry takes 12 bytes
//#pragma pack(8)
/*struct index_entry {
    struct slab_new *slab; // 8 bytes
    uint16_t item_size; // 2 bytes
    unsigned int slab_idx : 7; // 7 bits
    unsigned int under_migration : 1; // 1 bit
};*/

struct index_scan {
   uint64_t *hashes;
   struct index_entry *entries;
   size_t nb_entries;
};

typedef struct index_entry index_entry_t;


#endif
