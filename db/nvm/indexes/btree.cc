#include "cpp-btree/btree_map.h"
#include "btree.h"
#include <map>
#include <thread>

#include "../../db/db_impl.h"
#include "../../db/version_set.h"

using namespace std;
using namespace btree;

extern "C"
{

   // Helper Functions
   uint64_t decode_size64(unsigned char* buf){
      uint8_t* buffer = (uint8_t *) buf;
      return (uint64_t)(buffer[7]) |
         (uint64_t)((buffer[6]) << 8) |
         (uint64_t)((buffer[5]) << 16) |
         (uint64_t)((buffer[4]) << 24) |
         (uint64_t)((buffer[3]) << 32) |
         (uint64_t)((buffer[2]) << 40) |
         (uint64_t)((buffer[1]) << 48) |
         (uint64_t)((buffer[0]) << 56);
   }

  void encode_size64(char* dst, uint64_t value) {
    uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);
    buffer[7] = static_cast<uint8_t>(value);
    buffer[6] = static_cast<uint8_t>(value >> 8);
    buffer[5] = static_cast<uint8_t>(value >> 16);
    buffer[4] = static_cast<uint8_t>(value >> 24);
    buffer[3] = static_cast<uint8_t>(value >> 32);
    buffer[2] = static_cast<uint8_t>(value >> 40);
    buffer[1] = static_cast<uint8_t>(value >> 48);
    buffer[0] = static_cast<uint8_t>(value >> 56);
  }
   bool isPopular(uint64_t k, std::map<uint64_t, uint64_t> *pop_table, uint32_t pop_rank) {
      std::map<uint64_t, uint64_t>::iterator i = pop_table->find(k);
      if (i != pop_table->end()) {
         // found
         if (i->second < pop_rank) {
            //fprintf(stderr, "isPopular key %llu found+popular pop_rank %llu\n", i->second, pop_rank);
            return true;
         }
         // below the popularity threshold, view as unpopular
	 //fprintf(stderr, "isPopular key %llu found+unpopular pop_rank %llu\n", i->second, pop_rank);
         return false;
      }
      // not found, view as unpopular
      //fprintf(stderr, "isPopular key %llu notfound pop_rank %llu\n", k, pop_rank);
      return false;
   }


   btree_t *btree_create() {
      btree_map<uint64_t, struct index_entry> *b = new btree_map<uint64_t, struct index_entry>();
      return b;
   }

   int btree_find(btree_t *t, unsigned char* k, size_t len, struct index_entry *e) {
      //uint64_t hash = *(uint64_t*)k;
      uint64_t hash = decode_size64(k); // TODO: make it work for keys > 8bytes
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      auto i = b->find(hash);
      if(i != b->end()) {
         *e = i->second;
         return 1;
      } else {
         return 0;
      }
   }

   void btree_delete(btree_t *t, unsigned char*k, size_t len, bool keyIsUint64) {
      uint64_t hash;
      if (keyIsUint64){
        hash = *(uint64_t*)k;
      }
      else {
        hash = decode_size64(k); // TODO: make it work for keys > 8bytes
      }
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      b->erase(hash);
   }

   void btree_insert(btree_t *t, unsigned char*k, size_t len, struct index_entry *e) {
      //uint64_t hash = *(uint64_t*)k;
      uint64_t hash = decode_size64(k); // TODO: make it work for keys > 8bytes
      //fprintf(stderr, "btree insert key %llu\n", hash);
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      b->insert(make_pair(hash, *e));
   }

   // JIANAN
   // return all keys between (start, end) in the trree
   struct index_scan btree_find_between(btree_t *t, uint64_t* prev_mig_key, uint64_t start, uint64_t end, void *pop_table, uint32_t pop_rank, void* clock_cache_ptr, float pop_threshold) {
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      fprintf(stderr, "DBG: %X btree_find_between starts with prev mig key %llu, start %llu, end %llu\n", std::this_thread::get_id(), *prev_mig_key, start, end);
      int n = 0;
      auto i = b->find_closest(start);
      while (i != b->end() && i->first <= end) {
         n++;
         i++;
      }
      //fprintf(stderr, "first while loop done, n = %d\n", n);

      uint64_t skipped_keys = 0;

      std::map<uint64_t, uint64_t> *my_pop_table = static_cast<std::map<uint64_t, uint64_t> * >(pop_table);
      struct index_scan res;
      res.hashes = (uint64_t*) malloc(n*sizeof(*res.hashes)); // worst case of number of matching records
      res.entries = (struct index_entry*) malloc(n*sizeof(*res.entries));
      res.nb_entries = 0;

      //fprintf(stderr, "malloc done \n");
      // HACK: btree_find_between should not return more than a total of unpop keys whose size goes beyond a sst file
      uint32_t max_retsize_bytes = (uint32_t)(0.8*64*(2<<19));
      uint32_t cur_size_bytes = 0;

      auto j = b->find_closest(start);
      while (n > 0) {
        // HACK
        if (cur_size_bytes > max_retsize_bytes) {
          fprintf(stderr, "DBG: %X btree_find_between has size in bytes %lu, exceeds max sst size %lu\n", std::this_thread::get_id(), cur_size_bytes, max_retsize_bytes);
          break;
        }

         // only add unpopular keys
	 if (clock_cache_ptr != NULL) {
           if (! (static_cast<leveldb::ClockCache*>(clock_cache_ptr))->IsClockPopular(j->first, pop_threshold)) {
              res.hashes[res.nb_entries] = j->first;
              res.entries[res.nb_entries] = j->second;
              res.nb_entries++;
              // HACK
              cur_size_bytes = cur_size_bytes + j->second.item_size - 24; 
           }
           else {
              skipped_keys++;
              //fprintf(stderr, "skip popular key %llu\n", j->first);
           }
	 }
	 else {
           //if(j == b->end()){
           //  fprintf(stderr, "ERROR: n is %llu j is %llu\n", n, j);
	   //  fprintf(stderr, "ERROR: n is %llu j is %llu\n", n, j->first);
	   //}
           if (!isPopular(j->first, my_pop_table, pop_rank)) {
              res.hashes[res.nb_entries] = j->first;
              res.entries[res.nb_entries] = j->second;
              res.nb_entries++;
              // HACK
              cur_size_bytes = cur_size_bytes + j->second.item_size - 24; 
           }
           else {
              skipped_keys++;
              //fprintf(stderr, "skip popular key %llu\n", j->first);
           }
         }
         j++;
         n--;
      }
      //fprintf(stderr, "sceond while loop done, n = %d\n", n);

      // Update prev_mig_key, which serves as the round-robin pointer to the btree
      if ( j == b->end()){
         // move the pointer back to the beginning of the btree
         *prev_mig_key = -1;
      } else {
         if (res.nb_entries < 1) {
            // if found 0 unpopular key, move the pointer to the end of the search
            *prev_mig_key = j->first;
         } else {
            *prev_mig_key = res.hashes[res.nb_entries-1];
         }
      }

      fprintf(stderr, "DBG: %X btree_find_between returns %zu items, skipped %llu keys\n", std::this_thread::get_id(), res.nb_entries, skipped_keys);
      return res;
   }

   struct index_scan btree_find_between_metric2(btree_t *t, uint64_t* prev_mig_key, uint64_t start, uint64_t end, void *pop_table, uint32_t pop_rank, void* clock_cache_ptr, float pop_threshold, void* updated_bucket_info, uint64_t bucket_sz) {
      fprintf(stderr, "DBG: btree_find_between_metric2 starts with prev mig key %llu, start %llu, end %llu\n", *prev_mig_key, start, end);
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      int n = 0;
      auto i = b->find_closest(start);
      while (i != b->end() && i->first <= end) {
         n++;
         i++;
      }

      //fprintf(stderr, "first while loop done, n = %d\n", n);
      uint64_t skipped_keys = 0;

      std::map<uint64_t, uint64_t> *my_pop_table = static_cast<std::map<uint64_t, uint64_t> * >(pop_table);
      struct index_scan res;
      res.hashes = (uint64_t*) malloc(n*sizeof(*res.hashes)); // worst case of number of matching records
      res.entries = (struct index_entry*) malloc(n*sizeof(*res.entries));
      res.nb_entries = 0;

      auto j = b->find_closest(start);
      int cur_bid = j->first / bucket_sz;
      uint64_t total_mig_keys = 0;
      uint64_t pop_keys = 0;
      uint64_t max_key = j->first;

      std::map<uint64_t, std::pair<uint64_t, uint64_t> > *my_bucket_info = static_cast<std::map<uint64_t, std::pair<uint64_t, uint64_t> > * >(updated_bucket_info);

      while (n > 0) {
         if (j->first >= cur_bid * bucket_sz + bucket_sz) {
            my_bucket_info->insert({cur_bid, std::make_pair(total_mig_keys, pop_keys)});
            //fprintf(stderr, "DEBUG: btree_metric2 bucket %d has %llu total_mig_key\n", cur_bid, total_mig_keys);
            //static_cast<leveldb::DBImpl::Bucket*>(bucket_lst)[cur_bid].num_total_keys -= total_mig_keys;
            //static_cast<leveldb::DBImpl::Bucket*>(bucket_lst)[cur_bid].num_pop_keys = pop_keys; // TODO: not accurate here
            //fprintf(stderr, "DEBUG: bucket %d now has %llu total keys, %llu pop keys\n", cur_bid, static_cast<leveldb::DBImpl::Bucket*>(bucket_lst)[cur_bid].num_total_keys, pop_keys);
            total_mig_keys = 0;
            pop_keys = 0;
            cur_bid++;
         }
         // only add unpopular keys
	      if (clock_cache_ptr != NULL) {
           if (! (static_cast<leveldb::ClockCache*>(clock_cache_ptr))->IsClockPopular(j->first, pop_threshold)) {
              res.hashes[res.nb_entries] = j->first;
              res.entries[res.nb_entries] = j->second;
              res.nb_entries++;
              total_mig_keys++;
           }
           else {
              pop_keys++;
              skipped_keys++;
           }
	      }
	      else {
            //if(j == b->end()){
            //  fprintf(stderr, "ERROR: n is %llu j is %llu\n", n, j);
	         //  fprintf(stderr, "ERROR: n is %llu j is %llu\n", n, j->first);
	         //}
           if (!isPopular(j->first, my_pop_table, pop_rank)) {
              res.hashes[res.nb_entries] = j->first;
              res.entries[res.nb_entries] = j->second;
              res.nb_entries++;
              total_mig_keys++;
           }
           else {
              pop_keys++;
              skipped_keys++;
           }
         }
         max_key = j->first;
         j++;
         n--;
      }
      int end_bid = max_key / bucket_sz;
      if (end_bid >= cur_bid) {
        my_bucket_info->insert({cur_bid, std::make_pair(total_mig_keys, pop_keys)});
        //fprintf(stderr, "DEBUG: btree_metric2 bucket %d has %llu total_mig_key\n", cur_bid, total_mig_keys);
      }

      // Update prev_mig_key, which serves as the round-robin pointer to the btree
      if ( j == b->end()){
         // move the pointer back to the beginning of the btree
         *prev_mig_key = -1;
      } else {
         if (res.nb_entries < 1) {
            // if found 0 unpopular key, move the pointer to the end of the search
            *prev_mig_key = j->first;
         } else {
            *prev_mig_key = res.hashes[res.nb_entries-1];
         }
      }

      fprintf(stderr, "DBG: btree_find_between_metric2 returns %zu items, skipped %llu keys\n", res.nb_entries, skipped_keys);
      return res;
   }


  uint64_t btree_find_between_new(btree_t *t, uint64_t* prev_mig_key, uint64_t start, uint64_t end, void *pop_table, uint32_t pop_rank, void* clock_cache_ptr, float pop_threshold, void* migration_keys, void* migration_keys_prefix) {
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      //fprintf(stderr, "btree_find_between starts with prev mig key %llu, start %llu, end %llu\n", *prev_mig_key, start, end);
      int n = 0;
      auto i = b->find_closest(start);
      while (i != b->end() && i->first <= end) {
         n++;
         i++;
      }
      fprintf(stderr, "first while loop done, n = %d\n", n);

      uint64_t skipped_keys = 0;

      struct index_scan res;
      std::map<uint64_t, uint64_t> *my_pop_table = static_cast<std::map<uint64_t, uint64_t> * >(pop_table);
      std::vector<struct index_entry> *entries = static_cast<std::vector<struct index_entry> *>(migration_keys);
      std::vector<uint64_t> *hashes = static_cast<std::vector<uint64_t> *>(migration_keys_prefix);
      entries = (std::vector<struct index_entry> *) malloc(n*sizeof(*res.entries));
      hashes = (std::vector<uint64_t> *) malloc(n*sizeof(*res.hashes));
      uint64_t count = 0;

      fprintf(stderr, "malloc done \n");

      auto j = b->find_closest(start);
      while (n > 0) {
         // only add unpopular keys
	 if (clock_cache_ptr != NULL) {
           if (! (static_cast<leveldb::ClockCache*>(clock_cache_ptr))->IsClockPopular(j->first, pop_threshold)) {
	      hashes->push_back(j->first);
	      entries->push_back(j->second);
	      count++;
           }
           else {
              skipped_keys++;
              //fprintf(stderr, "skip popular key %llu\n", j->first);
           }
	 }
	 else {
           //if(j == b->end()){
           //  fprintf(stderr, "ERROR: n is %llu j is %llu\n", n, j);
	   //  fprintf(stderr, "ERROR: n is %llu j is %llu\n", n, j->first);
	   //}
           if (!isPopular(j->first, my_pop_table, pop_rank)) {
	      fprintf(stderr, "check 1 before\n");
	      hashes->push_back(j->first);
	      entries->push_back(j->second);
	      count++;
	      fprintf(stderr, "check 1\n");
           }
           else {
              skipped_keys++;
              //fprintf(stderr, "skip popular key %llu\n", j->first);
           }
         }
         j++;
         n--;
      }
      fprintf(stderr, "sceond while loop done, n = %d\n", n);

      // Update prev_mig_key, which serves as the round-robin pointer to the btree
      if ( j == b->end()){
         // move the pointer back to the beginning of the btree
         *prev_mig_key = -1;
      } else {
         if (count < 1) {
            // if found 0 unpopular key, move the pointer to the end of the search
            *prev_mig_key = j->first;
         } else {
            *prev_mig_key = hashes->at(count-1);
         }
      }

      fprintf(stderr, "btree_find_between returns %zu items, skipped %llu keys\n", count, skipped_keys);
      return count;
   }

	// JIANAN
  // returns the count of number of keys that fall between a given range
  int btree_find_between_count(btree_t *t, uint64_t start, uint64_t end, void *pop_table, uint32_t pop_rank, void* clock_cache_ptr, float pop_threshold) {
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      std::map<uint64_t, uint64_t> *my_pop_table = static_cast<std::map<uint64_t, uint64_t> * >(pop_table);

      int n = 0;
      //fprintf(stderr, "before start %llu end %llu bend %llu\n", start, end, (--(b->end()))->first);
      auto i = b->find_closest(start);
      //fprintf(stderr, "after start %llu end %llu closest %llu bend %llu\n", start, end, i->first, (--(b->end()))->first);
      while (i != b->end() && i->first <= end) {
         if (clock_cache_ptr != NULL) {
	   //leveldb::clockcache* a = static_cast<leveldb::clockcache*>(clock_cache_ptr);
	   //if (! a->isclockpopular(i->first, pop_threshold)) {
           if (! (static_cast<leveldb::ClockCache*>(clock_cache_ptr))->IsClockPopular(i->first, pop_threshold)) {
             n++;
           }
         }
         else {
           if (!isPopular(i->first, my_pop_table, pop_rank)) {
             n++;
           }
         }
         i++;
      }
      return n;
   }

  int btree_find_between_count_uniq_pages(btree_t *t, uint64_t start, uint64_t end, void *pop_table, uint32_t pop_rank, void* clock_cache_ptr, float pop_threshold, int *slabsizes) {
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      std::map<uint64_t, uint64_t> *my_pop_table = static_cast<std::map<uint64_t, uint64_t> * >(pop_table);
      std::set<int> uniq_pages;

      int n = 0;
      //fprintf(stderr, "before start %llu end %llu bend %llu\n", start, end, (--(b->end()))->first);
      auto i = b->find_closest(start);
      //fprintf(stderr, "after start %llu end %llu closest %llu bend %llu\n", start, end, i->first, (--(b->end()))->first);
      while (i != b->end() && i->first <= end) {
         if (clock_cache_ptr != NULL) {
	   //leveldb::clockcache* a = static_cast<leveldb::clockcache*>(clock_cache_ptr);
	   //if (! a->isclockpopular(i->first, pop_threshold)) {
           if (! (static_cast<leveldb::ClockCache*>(clock_cache_ptr))->IsClockPopular(i->first, pop_threshold)) {
             // for this unpopular key, find its page index within the slab file
             int num_slots_per_page = 4096 / (slabsizes[i->second.slab]);
             int slab_page_idx = i->second.slab_idx / num_slots_per_page;
             uniq_pages.insert(slab_page_idx);
           }
         }
         else {
           if (!isPopular(i->first, my_pop_table, pop_rank)) {
             // for this unpopular key, find its page index within the slab file
             int num_slots_per_page = 4096 / (slabsizes[i->second.slab]);
             int slab_page_idx = i->second.slab_idx / num_slots_per_page;
             uniq_pages.insert(slab_page_idx);
           }
         }
         i++;
      }
      fprintf(stderr, "%X\t number of unique pages is %d\n", std::this_thread::get_id(), uniq_pages.size());
      return uniq_pages.size();
   }

  // Return the total number of keys within [start, end]
  int btree_find_between_total_count(btree_t *t, uint64_t start, uint64_t end) {
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      int n = 0;
      auto i = b->find_closest(start);
      while (i != b->end() && i->first <= end) {
         n++;
         i++;
      }
      return n;
  }

  void btree_find_between_count_tuple(btree_t *t, uint64_t start, uint64_t end, void *pop_table, uint32_t pop_rank, void* clock_cache_ptr, float pop_threshold, uint64_t sst_fn, uint64_t sst_size, void* version_ptr, void* val_tuple) {
      //fprintf(stderr, "btree_find_between_count_tuple starts with start %llu, end %llu\n", start, end);
      std::tuple<uint64_t, uint64_t, uint64_t> *my_val_tuple = static_cast<std::tuple<uint64_t, uint64_t, uint64_t> * >(val_tuple);
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      std::map<uint64_t, uint64_t> *my_pop_table = static_cast<std::map<uint64_t, uint64_t> * >(pop_table);

      int total_keys = 0;
      int pop_keys = 0;
      // overlap_keys is the number of unpopular keys that also exist in the SST file
      int overlap_keys = 0;
      //fprintf(stderr, "BEFORE start %llu end %llu bend %llu\n", start, end, (--(b->end()))->first);
      auto i = b->find_closest(start);
      //fprintf(stderr, "AFTER start %llu end %llu closest %llu bend %llu\n", start, end, i->first, (--(b->end()))->first);
      leveldb::ReadOptions options;
      options.fill_cache = false;
      while (i != b->end() && i->first <= end) {
         // check if the key is popular
         if (clock_cache_ptr != NULL) {
         //leveldb::ClockCache* a = static_cast<leveldb::ClockCache*>(clock_cache_ptr);
         //if (! a->IsClockPopular(i->first, pop_threshold)) {
           if ((static_cast<leveldb::ClockCache*>(clock_cache_ptr))->IsClockPopular(i->first, pop_threshold)) {
             pop_keys++;
           } else {
              // check if the key exists in the bloom filter or not
              if ((static_cast<leveldb::Version*>(version_ptr))->CheckBloomFilter(options, sst_fn, sst_size, i->first)) {
                overlap_keys++;
              }
          }
         }
         else {
           if (isPopular(i->first, my_pop_table, pop_rank)) {
             pop_keys++;
           } else {
               // check if the key exists in the bloom filter or not
              //fprintf(stderr, "DEBUG: check bloom filter file number %llu  file size %llu for key %llu\n", sst_fn, sst_size, i->first);
              if ((static_cast<leveldb::Version*>(version_ptr))->CheckBloomFilter(options, sst_fn, sst_size, i->first)) {
              //if (true) {
                //fprintf(stderr, "DEBUG: done check bloom filter file number %llu for key %llu\n", sst_fn, i->first);
                overlap_keys++;
              }
           }
         }

         i++;
         total_keys++;
      }
      std::get<0>(*my_val_tuple) = total_keys;
      std::get<1>(*my_val_tuple) = pop_keys;
      std::get<2>(*my_val_tuple) = overlap_keys;
      //fprintf(stderr, "DEBUG: btree_count_val_tuple, total_keys is %llu, pop_keys is %llu, overlap keys is %llu\n", total_keys, pop_keys, overlap_keys);
  }

  // JIANAN
  // Returns the sum of clock value of all popular keys between [start, end]
  uint64_t btree_find_between_sum_popvals(btree_t *t, uint64_t start, uint64_t end, void* clock_cache_ptr, float pop_threshold) {
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      uint64_t sum = 0;
      int num_pop_keys = 0;
      if (clock_cache_ptr == NULL) { return sum; }
 
      auto i = b->find_closest(start);
      while (i != b->end() && i->first <= end) {
        //// keyinfo = (is_popular, clock_value)
        std::pair<bool, int> keyinfo = (static_cast<leveldb::ClockCache*>(clock_cache_ptr))->GetKeyInfo(i->first, pop_threshold);
        if (keyinfo.first) { // if the key is popular
          num_pop_keys++;
          sum += keyinfo.second; // add its clock value
         }
        i++;
      }
      fprintf(stderr, "DBG: %X btree_find_between_sum_popvals has %llu pop keys, sum of pop vals is %llu\n", std::this_thread::get_id(), num_pop_keys, sum);
      return sum;
  }

  // JIANAN
  // Returns the sum of the inverse of clock value of all keys between [start, end]
  float btree_find_between_sum_inv_popvals(btree_t *t, uint64_t start, uint64_t end, void* clock_cache_ptr, float pop_threshold) {
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      float sum = 0;
      if (clock_cache_ptr == NULL) { return sum; }
      int count = 0; 
      auto i = b->find_closest(start);
      while (i != b->end() && i->first <= end) {
        int clock_value  = (static_cast<leveldb::ClockCache*>(clock_cache_ptr))->Lookup(i->first);
        if (clock_value < 1) {
          sum += 1;
        } else {
          sum += (float) (1 / clock_value);
        }
        count++;
        i++;
      }
      fprintf(stderr, "DBG: %X btree_find_between_sum_inv_popvals has %d total keys, sum of inversed pop vals is %.5f\n", std::this_thread::get_id(), count, sum);

      return sum;
  }
  // JIANAN
  // Returns the averaged clock value of all keys between [start, end]
  float btree_find_between_avg_popval(btree_t *t, uint64_t start, uint64_t end, void* clock_cache_ptr, float pop_threshold) {
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      float sum = 0;
      if (clock_cache_ptr == NULL) { return sum; }
      int count = 0; 
      auto i = b->find_closest(start);
      while (i != b->end() && i->first <= end) {
        int clock_value  = (static_cast<leveldb::ClockCache*>(clock_cache_ptr))->Lookup(i->first);
        if (clock_value > 0) {
          sum += clock_value;
        }
        count++;
        i++;
      }
      float avg_pop = (float) sum / count;
      fprintf(stderr, "DBG: %X btree_find_between_sum_inv_popvals has %d total keys, averaged key popularity is %.5f\n", std::this_thread::get_id(), count, avg_pop);

      return avg_pop;
  }
   // JIANAN
   // return neighbors of the start key until the total size in bytes exceed max_size
   struct index_scan btree_find_n_bytes(btree_t *t, uint64_t start, uint32_t max_size) {
      //fprintf(stderr, "btree_find_n_bytes starts with start key %llu\n", start);
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);

      //fprintf(stderr, "size of btree %zu\n", b->size());
      auto i = b->begin();
      uint32_t cur_size = 0;
      int n = 0;

      while (cur_size < max_size) {
         if (i == b->end()) {
            break;
         }
         cur_size = cur_size + i->second.item_size;
         i++;
         n++;
      }

      struct index_scan res;
      res.hashes = (uint64_t*) malloc(n*sizeof(*res.hashes));
      res.entries = (struct index_entry*) malloc(n*sizeof(*res.entries));
      res.nb_entries = 0;

      auto j = b->begin();
      while (n > 0) {
         res.hashes[res.nb_entries] = j->first;
         res.entries[res.nb_entries] = j->second;
         res.nb_entries++;
         j++;
         n--;
      }
      //fprintf(stderr, "btree_find_n_bytes returns %zu keys\n", res.nb_entries);
      return res;

      // int n = 300000; // preallocate with max size, assuming no more than 300,000 matching items
      // struct index_scan res;
      // res.hashes = (uint64_t*) malloc(n*sizeof(*res.hashes));
      // res.entries = (struct index_entry*) malloc(n*sizeof(*res.entries));
      // fprintf(stderr, "done malloc\n");
      // res.nb_entries = 0;

      // auto i = b->find_closest(start); // i scans to the left
      // fprintf(stderr, "find i\n");
      // auto j = b->find_closest(start); // j scans to the right
      // fprintf(stderr, "find j\n");
      // j.increment();

      // fprintf(stderr, "before scanning left and right neighbors\n");
      // uint32_t cur_size = 0;
      // while (cur_size < max_size) {
      //    if (i == b->begin() && j == b->end()) {
      //       break;
      //    }

      //    if (i != b->begin()) {
      //       res.hashes[res.nb_entries] = i->first;
      //       res.entries[res.nb_entries] = i->second;
      //       res.nb_entries++;
      //       cur_size = cur_size + i->second.item_size;
      //       i--;
      //    }

      //    if (j != b->end()) {
      //       res.hashes[res.nb_entries] = j->first;
      //       res.entries[res.nb_entries] = j->second;
      //       res.nb_entries++;
      //       cur_size = cur_size + j->second.item_size;
      //       j++;
      //    }
      // }
      // return &res;
   }

   // round-robin version of btree_find_n_bytes
   struct index_scan btree_find_n_bytes_rr(btree_t *t, uint64_t* prev_mig_key, uint32_t max_size, void *pop_table, uint32_t pop_rank, bool key_logging, bool use_popularity, void* clock_cache_ptr, float pop_threshold) {
      fprintf(stderr, "DBG: %X btree_find_n_bytes_rr starts with prev mig key %llu\n", std::this_thread::get_id(), *prev_mig_key);
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      //fprintf(stderr, "size of btree %zu\n", b->size());
      auto i = b->begin();
      uint64_t smallest_key = i->first;
      //fprintf(stderr, "smallest is %llu\n", i->first);

      // TODO: can be a problem with hashed keys
      if (*prev_mig_key != -1) {
         i = b->find_closest(*prev_mig_key);
         //fprintf(stderr, "find_closest seeks to %llu\n", i->first);
      }
      //else{
      //   fprintf(stderr, "NOT EXPECTED OTHER THAN INIT\n");
      //}

      // TODO: assumption is 64MB sst file with 150 byte keys
      int n = 450000; // preallocate with max size, assuming no more than 450,000 matching items
      int curr_n = 0;
      struct index_scan res;
      res.hashes = (uint64_t*) malloc(n*sizeof(*res.hashes));
      res.entries = (struct index_entry*) malloc(n*sizeof(*res.entries));
      //fprintf(stderr, "done malloc\n");
      res.nb_entries = 0;
      uint32_t cur_size = 0;
      std::map<uint64_t, uint64_t> *my_pop_table = static_cast<std::map<uint64_t, uint64_t> * >(pop_table);
      uint64_t skipped_keys = 0;
      while (cur_size < max_size) {
         if (i == b->end()) {
            break;
         }
         if (curr_n == n) {
            break;
         }

         if (!use_popularity) {
            res.hashes[res.nb_entries] = i->first;
            res.entries[res.nb_entries] = i->second;
            res.nb_entries++;
            cur_size = cur_size + i->second.item_size; // item size is kv pair + key size + value size + timestamp (i.e. 24 bytes metadata)
            curr_n++;
            //fprintf(stderr, "cur size %lu max_size %lu\n", cur_size, max_size);
         } else {
           // add unpopular keys
           if (clock_cache_ptr != NULL) {
             if (! (static_cast<leveldb::ClockCache*>(clock_cache_ptr))->IsClockPopular(i->first, pop_threshold)) {
                //fprintf(stderr, "Select key %llu\n", i->first);
                res.hashes[res.nb_entries] = i->first;
                res.entries[res.nb_entries] = i->second;
                res.nb_entries++;
                cur_size = cur_size + i->second.item_size; // item size is kv pair + key size + value size + timestamp (i.e. 24 bytes metadata)
                curr_n++;
                //fprintf(stderr, "cur size %lu max_size %lu\n", cur_size, max_size);
             } else {
                skipped_keys++;
                if (key_logging) {
                  fprintf(stderr, "%X skip popular key %llu\n", std::this_thread::get_id(), i->first);
                }
             }
           }
           else {
             if (!isPopular(i->first, my_pop_table, pop_rank)) {
                //fprintf(stderr, "Select key %llu\n", i->first);
                res.hashes[res.nb_entries] = i->first;
                res.entries[res.nb_entries] = i->second;
                res.nb_entries++;
                cur_size = cur_size + i->second.item_size; // item size is kv pair + key size + value size + timestamp (i.e. 24 bytes metadata)
                curr_n++;
                //fprintf(stderr, "cur size %lu max_size %lu\n", cur_size, max_size);
             } else {
                skipped_keys++;
                if (key_logging) {
                  fprintf(stderr, "%X skip popular key %llu\n", std::this_thread::get_id(), i->first);
                }
             }
           }
         }
         i++;
      }

      if ( i == b->end()){
         *prev_mig_key = -1;
      } else {
         *prev_mig_key = res.hashes[res.nb_entries-1];
      }
      fprintf(stderr, "DBG: %X btree_find_n_bytes_rr returns %zu keys, skips %llu keys, prev key is %llu\n", std::this_thread::get_id(), res.nb_entries, skipped_keys, *prev_mig_key);
      return res;
   }

   struct index_scan btree_find_n(btree_t *t, unsigned char* k, size_t len, size_t n) {
      struct index_scan res;
      res.hashes = (uint64_t*) malloc(n*sizeof(*res.hashes));
      res.entries = (struct index_entry*) malloc(n*sizeof(*res.entries));
      res.nb_entries = 0;

      //uint64_t hash = *(uint64_t*)k;
      uint64_t hash = decode_size64(k); // TODO: make it work for keys > 8bytes
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      auto i = b->find_closest(hash);
      while(i != b->end() && res.nb_entries < n) {
         res.hashes[res.nb_entries] = i->first;
         res.entries[res.nb_entries] = i->second;
         res.nb_entries++;
         i++;
      }

      return res;
   }

   uint64_t btree_find_closest(btree_t *t, uint64_t k){
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      auto e = b->end();
      //fprintf(stderr, "btree find closest min %llu max %llu k %llu\n", b->begin()->first, (--e)->first, k);
      auto i = b->find_closest(k);
      //fprintf(stderr, "returning i %llu\n", i->first);
      return i->first;
   }

   uint64_t btree_get_min(btree_t *t){
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      return b->begin()->first;
   }

   uint64_t btree_get_max(btree_t *t){
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);

      auto it = b->end();
      //uint64_t max_k = it->first;
      it--;
      uint64_t last_k = it->first;
      fprintf(stderr, "DBG: %X btree_get_max %llu\n", std::this_thread::get_id(), last_k); 
      return last_k;
   }

   void btree_print_all_keys(btree_t *t) {
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      auto i = b->begin();
      while(i != b->end()) {
         fprintf(stderr, "BTREE key %llu\n", i->first);
         i++;
      }
      return;
   }

   void btree_forall_keys(btree_t *t, void (*cb)(uint64_t h, void *data), void *data) {
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      auto i = b->begin();
      while(i != b->end()) {
         cb(i->first, data);
         i++;
      }
      return;
   }

   uint64_t btree_get_size(btree_t *t) {
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      return b->size();
   }

   uint64_t  btree_get_size_in_bytes(btree_t *t) {
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      return b->bytes_used();
   }



   void btree_free(btree_t *t) {
      btree_map<uint64_t, struct index_entry> *b = static_cast< btree_map<uint64_t, struct index_entry> * >(t);
      delete b;
   }


}
