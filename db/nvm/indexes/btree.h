#ifndef BTREE_H
#define BTREE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include "memory-item.h"

typedef void btree_t;

// Helper Functions
uint64_t decode_size64(unsigned char* buffer);
void encode_size64(char* dst, uint64_t value);
// bool isPopular(uint64_t k, void *pop_table, uint32_t pop_rank);

btree_t *btree_create();
int btree_find(btree_t *t, unsigned char*k, size_t len, struct index_entry *e);
void btree_delete(btree_t *t, unsigned char*k, size_t len, bool keyIsUint64);
void btree_insert(btree_t *t, unsigned char*k, size_t len, struct index_entry *e);
struct index_scan btree_find_n(btree_t *t, unsigned char* k, size_t len, size_t n);

void btree_forall_keys(btree_t *t, void (*cb)(uint64_t h, void *data), void *data);
void btree_free(btree_t *t);
int btree_find_between_count(btree_t *t, uint64_t start, uint64_t end, void *pop_table, uint32_t pop_rank, void* clock_cache_ptr, float pop_threshold);
int btree_find_between_count_uniq_pages(btree_t *t, uint64_t start, uint64_t end, void *pop_table, uint32_t pop_rank, void* clock_cache_ptr, float pop_threshold, int *slabsizes);
uint64_t btree_find_between_sum_popvals(btree_t *t, uint64_t start, uint64_t end, void* clock_cache_ptr, float pop_threshold);
float btree_find_between_sum_inv_popvals(btree_t *t, uint64_t start, uint64_t end, void* clock_cache_ptr, float pop_threshold);
float btree_find_between_avg_popval(btree_t *t, uint64_t start, uint64_t end, void* clock_cache_ptr, float pop_threshold);
struct index_scan btree_find_between(btree_t *t, uint64_t* prev_mig_key, uint64_t start, uint64_t end, void *pop_table, uint32_t pop_rank, void* clock_cache_ptr, float pop_threshold);
struct index_scan btree_find_n_bytes(btree_t *t, uint64_t start, uint32_t max_size);
struct index_scan btree_find_n_bytes_rr(btree_t *t, uint64_t* prev_mig_key, uint32_t max_size, void *pop_table, uint32_t pop_rank, bool key_logging, bool use_popularity, void* clock_cache_ptr, float pop_threshold);
// Migration Metric 2
//struct index_scan btree_find_n_bytes_rr_metric2(p_ctx->index, &p_ctx->prev_migration_key, (uint32_t) (0.8*(float)maxSstFileSizeBytes), &pop_table_, popRank, false, false, (void*)(p_ctx->pop_cache_ptr), popThreshold, (void*) &updated_bucket_info, bucket_sz);

struct index_scan btree_find_between_metric2(btree_t *t, uint64_t* prev_mig_key, uint64_t start, uint64_t end, void *pop_table, uint32_t pop_rank, void* clock_cache_ptr, float pop_threshold, void* updated_bucket_info, uint64_t bucket_sz);

void btree_find_between_count_tuple(btree_t *t, uint64_t start, uint64_t end, void *pop_table, uint32_t pop_rank, void* clock_cache_ptr, float pop_threshold, uint64_t sst_fn, uint64_t sst_size, void *version_ptr, void* val_tuple);

int btree_find_between_total_count(btree_t *t, uint64_t start, uint64_t end);

uint64_t btree_find_closest(btree_t *t, uint64_t k);
void btree_print_all_keys(btree_t *t);
uint64_t btree_get_min(btree_t *t);
uint64_t btree_get_max(btree_t *t);
uint64_t  btree_get_size(btree_t *t);
uint64_t  btree_get_size_in_bytes(btree_t *t);


uint64_t btree_find_between_new(btree_t *t, uint64_t* prev_mig_key, uint64_t start, uint64_t end, void *pop_table, uint32_t pop_rank, void* clock_cache_ptr, float pop_threshold, void* migration_keys, void* migration_keys_prefix);

#ifdef __cplusplus
}
#endif

#endif
