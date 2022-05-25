// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <atomic>
#include <deque>
#include <set>
#include <string>
#include <thread>
#include <map>

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"

// PRISMDB
#include "db/version_edit.h"
#include <fstream>

// JIANAN: Optane
extern "C" {
  #include "nvm/indexes/btree.h"
  // #include "nvm/indexes/memory-item-new.h"
  #include "nvm/indexes/memory-item.h"
  #include "nvm/slab_new.h"
}
#include <mutex>

#include "tbb/concurrent_hash_map.h"

#include "nvm/indexes/cpp-btree/btree_map.h"
#include "nvm/indexes/cpp-btree/btree_container.h"

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

#define BUCKET_SZ 64000

typedef struct Bucket {
  // TODO: not sure if atomic vars are needed, if done within the critical section
  uint64_t num_total_keys;
  uint8_t* pop_bitmap;
  uint64_t num_pop_keys;
  float overlap_ratio;
  std::atomic_flag in_use = ATOMIC_FLAG_INIT;
} Bucket;

// Key of hash map. We store hash value with the key for convenience.
struct ClockCacheKey {
  Slice key;
  uint32_t hash_value;

  ClockCacheKey() = default;

  ClockCacheKey(const Slice& k, uint32_t h) {
    key = k;
    hash_value = h;
  }

  static bool equal(const ClockCacheKey& a, const ClockCacheKey& b) {
    return a.hash_value == b.hash_value && a.key == b.key;
  }

  static size_t hash(const ClockCacheKey& a) {
    return static_cast<size_t>(a.hash_value);
  }
};

// Compare function provided for tbb library
/*struct HashClockCompare {
    static size_t hash( const std::string& x ) {
        const void * key = x.c_str();
        int len = x.size();
        uint32_t seed = 0xFEEDC0FE;

        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        const unsigned int m = 0x5bd1e995;
        const int r = 24;

        // Initialize the hash to a 'random' value
        unsigned int h = seed ^ len;

        // Mix 4 bytes at a time into the hash
        const unsigned char * data = (const unsigned char *)key;

        while(len >= 4)
        {
                unsigned int k = *(unsigned int *)data;
                k *= m;
                k ^= k >> r;
                k *= m;
                h *= m;
                h ^= k;
                data += 4;
                len -= 4;
        }

        // Handle the last few bytes of the input array
        switch(len)
        {
          case 3: h ^= data[2] << 16;
          case 2: h ^= data[1] << 8;
          case 1: h ^= data[0];
            h *= m;
        };

        // Do a few final mixes of the hash to ensure the last few
        h ^= h >> 13;
        h *= m;
        h ^= h >> 15;
        return h;
    }
    //! True if strings are equal
    static bool equal( const std::string& x, const std::string& y ) {
        return x==y;
    }
};*/

struct HashClockCompare {
    static size_t hash( const uint64_t x ) {
        std::string x_str = std::to_string(x);
        const void * key = x_str.c_str();
        int len = x_str.size();
        uint32_t seed = 0xFEEDC0FE;

        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        const unsigned int m = 0x5bd1e995;
        const int r = 24;

        // Initialize the hash to a 'random' value
        unsigned int h = seed ^ len;

        // Mix 4 bytes at a time into the hash
        const unsigned char * data = (const unsigned char *)key;

        while(len >= 4)
        {
                unsigned int k = *(unsigned int *)data;
                k *= m;
                k ^= k >> r;
                k *= m;
                h *= m;
                h ^= k;
                data += 4;
                len -= 4;
        }
        // Handle the last few bytes of the input array
        switch(len)
        {
          case 3: h ^= data[2] << 16;
          case 2: h ^= data[1] << 8;
          case 1: h ^= data[0];
            h *= m;
        };

        // Do a few final mixes of the hash to ensure the last few
        h ^= h >> 13;
        h *= m;
        h ^= h >> 15;
        return h;
    }
    //! True if strings are equal
    static bool equal( const uint64_t x, const uint64_t y ) {
        return x==y;
    }
};


// Maximum value that the clock entry can have
#define CLOCK_BITS_MAX_VALUE 3

// A single shard of clock based popularity cache, does not use mutex by default
class ClockCache {
 public:
  ClockCache(size_t capacity, Bucket *bl, int migration_metric);
  ~ClockCache();

  // Separate from constructor so caller can easily make an array of ClockCache
  void SetCapacity(size_t capacity) {
    capacity_ = capacity;
    fprintf(stderr, "CLOCK: initialized clock cache with capacity=%d bytes\n", capacity_);
  }


  // Like Cache methods, but with an extra "hash" parameter.
  //void Insert(const std::string& key);
  //int8_t Lookup(const std::string& key);
  void Insert(const uint64_t key);
  int8_t Lookup(const uint64_t key);
  size_t TotalCharge() const {
    return usage_;
  }

  // HACK: on_optane bit is always set to 1 since we track get() for keys
  // on optane and qlc. MarkOptaneBit is currently NOT called from migration code.
  //void MarkOptaneBit(const std::string& key, bool set_optane_bit);
  void MarkOptaneBit(const uint64_t key, bool set_optane_bit);

  // btree.c uses this function to check if a key is popular
  bool IsClockPopular(uint64_t key, float pop_threshold);

  std::pair<bool, int> GetKeyInfo(uint64_t key, float pop_threshold);

  void GenClockProbDist(float pop_threshold);

  float clk_prob_dist[CLOCK_BITS_MAX_VALUE+1];

  void EvictBucketPopKeys(uint64_t k);

  bool AreClockValuesNonZero();

 private:

  void EvictIfCacheFull();
  void PrintClockCacheValueHist(const std::string& msg);

  // Initialized before use.
  size_t capacity_;

  // neeeded for migration metric
  Bucket *bucket_list_;

  int migration_metric_;

  size_t usage_;

  //typedef tbb::concurrent_hash_map<std::string, uint8_t, HashClockCompare> HashTableClock;
  typedef tbb::concurrent_hash_map<uint64_t, uint8_t, HashClockCompare> HashTableClock;
  HashTableClock table_clock_;
  HashTableClock::const_iterator table_clock_iter_;

  // ASH: Array stores histogram of clock bit values in table_clock_
  uint32_t clock_cache_value_hist_[CLOCK_BITS_MAX_VALUE+1];
};

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);

  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  ~DBImpl() override;

  // Implementations of the DB interface
  Status Put(const WriteOptions&, const Slice& key,
             const Slice& value) override;
  Status Delete(const WriteOptions&, const Slice& key) override;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;
  Status Scan(const ReadOptions& options, const Slice& start_key,
                      const uint64_t scan_size,
                      std::vector<std::pair<Slice, Slice>>* results) override;
  void SetMigrationBitmap(int pid, uint64_t k, unsigned int under_mig);
  unsigned int GetFromMigrationBitmap(int pid, uint64_t k);
  void ResetMigrationBitmap(int pid);
  Status PutImpl(const WriteOptions& opt, const Slice& key, const Slice& value, bool called_from_migration=false);
  Status DeleteImpl(const WriteOptions& opt, const Slice& key);
  Iterator* NewIterator(const ReadOptions&) override;
  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;
  bool GetProperty(const Slice& property, std::string* value) override;
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;
  void CompactRange(const Slice* begin, const Slice* end) override;

  // create two methods to explicitly output and reset migration stats
  void ReportMigrationStats() override;
  void ResetMigrationStats() override;
  void SetDbMode(bool load) override;
  void SetCorrectBucketTotalKeys() override;
  void SetPopFile(const char* pop_file) override;


  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;  // null means beginning of key range
    const InternalKey* end;    // null means end of key range
    InternalKey tmp_storage;   // Used to keep track of compaction progress
  };

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }

    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;
  };

  enum MigrationReason{
    MIG_REASON_OPTANE_SIZE=0,
    MIG_REASON_UPSERT,
    MIG_REASON_INVALID
  };

  const char* pop_file_ = nullptr;
  // pop_table currently works only for 8-byte keys
  // TODO:
  std::map<uint64_t, uint64_t> pop_table_;
  // TODO: use rank instead
  uint32_t popRank = 0;
  float popThreshold = 0.7;
  // popularity clock cache size in bytes
  uint32_t popCacheSize = 2200000;
  bool load_phase_ = true;
  uint64_t numKeys = 10000000;
  uint64_t numWriteKeys = numKeys; // For twitter
  uint64_t numPartitions = 8;
  uint64_t maxDbSizeBytes = 0;
  uint32_t maxKeySizeBytes = 8;
  uint32_t maxKVSizeBytes = 1024;
  float optaneThreshold = 0.23;
  uint64_t maxSstFileSizeBytes = 64*(2<<19); // size of sst files
  uint32_t minSstFileMigThreshold = 0;
  typedef struct PartitionContext {
    uint64_t last_upsert_fn = 0;
    uint8_t pid; // partition id
    uint64_t sequenceNum = 0;
    //int num_warmup_migrations = 50; // For Twitter
    int num_warmup_migrations = 0; // For YCSB
    uint8_t* under_migration_bitmap; // protected by partition lock
    std::mutex mtx;
    btree_t* index;
    slab_context_new* slabContext;
    port::Mutex mutex;
    port::CondVar background_work_finished_signal;
    bool background_compaction_scheduled;
    // Set of table files to protect from deletion because they are
    // part of ongoing compactions.
    std::set<uint64_t> pending_outputs;
    std::map<uint64_t, std::pair<uint64_t,uint64_t>> files; // maps from sst file number to sst file smallest and largest
    std::map<uint64_t, uint64_t> file_sizes; // maps from sst file number to sst file sizes
    // overall size of this partition on optane. if size of one entry is 200bytes
    // FOR ANALYSIS ONLY, TODO: optimize this
    std::map<uint64_t, uint64_t> file_entries; // maps sst file number to number of entries in that file
    // and assigned slab size has size 256 bytes, then 256bytes will be used for counting total size
    uint64_t size_in_bytes=0;
    uint64_t prev_migration_key = -1;
    uint64_t max_optane_usage = 0;
    uint64_t num_put_reqs = 0;

    // migration thresholds
    float ratelimit_threshold = 0.98; //0.98; //1.0;
    float migration_upper_bound = 0.98; //0.98; //1.0;
    float read_dominated_threshold = 0.95;
    float read_ratio_improv_threshold = 0.05;
    uint64_t upsert_delay_threshold = 100000; // in terms of get operations
    uint64_t read_ratio_tracking_freq = 100000; // in terms of get operations, should always be less that upsert_delay_threshold
    uint64_t clock_warmup_ops = read_ratio_tracking_freq;
    uint64_t stop_upsert_trigger = 31250000; // stops upserts at 250M ops

    // popularity cache
    ClockCache* pop_cache_ptr = NULL;

    // for read popularity debugging
    uint64_t optane_reads = 0;
    uint64_t qlc_reads = 0;
    MigrationReason mig_reason = MIG_REASON_OPTANE_SIZE;
    uint64_t upsert_delay = 0; // in terms of get operations
    float prev_optane_read_percent = 0.0;

    // JIANAN: add Optane-related structure
    int num_puts = 0;
    int num_upsert_puts = 0;
    int num_updates = 0;
    float put_time = 0;
    float put_copy_array = 0;
    float put_acquire_lock = 0;
    float put_find_index_time = 0;
    float insert_optane_time = 0;
    float update_optane_time = 0;
    float put_delete_index_time = 0;
    float put_insert_index_time = 0;

    int num_optane_gets = 0;
    int num_qlc_gets = 0;
    float get_optane_time = 0;
    float get_qlc_time = 0;
    float get_acquire_optane_lock = 0;
    float get_find_optane_index = 0;
    float get_read_optane = 0;
    float get_acquire_qlc_lock = 0;
    float get_read_qlc = 0;

    int migrationId = 0;
    int num_mig_keys = 0;
    float mig_backgroundcall = 0;
    float mig_select = 0;
    float mig_select_lock = 0;
    float mig_select_btree = 0;
    float mig_select_copy = 0;
    float mig_pick = 0;
    float mig_pick_lock = 0;
    float mig_compaction = 0;
    float mig_compaction_lock = 0;
    float mig_compaction_read_optane = 0;
    float mig_compaction_read_qlc = 0;
    float mig_compaction_write_qlc = 0;
    float mig_remove = 0;
    float mig_remove_lock = 0;

  }PartitionContext;
  PartitionContext* partitions;
  //static PartitionContext partitions[numPartitions]; // hard-code for now

  uint64_t bucket_sz;
  // for approx migration metric, per bucket info
  //typedef struct Bucket {
  //  // TODO: not sure if atomic vars are needed, if done within the critical section
  //  uint64_t num_total_keys;
  //  uint8_t* pop_bitmap;
  //  uint64_t num_pop_keys;
  //  float overlap_ratio;
  //} Bucket;
  Bucket *BucketList;

  void initBuckets(uint64_t size);
  void EvictBucketPopKeys(uint64_t k);
  void UpdateBucketNumKeys(std::map<uint64_t, std::pair<uint64_t, uint64_t>>& updated_bucket_info);
  void UpdateBucketOverlapRatios(float new_s, int start_bid, int end_bid);

  void setPartitionNum(int size) {numPartitions = size;}
  void setKeyNum(uint64_t size) { numKeys = size; }
  void setMaxDbSize() {
    maxDbSizeBytes = numWriteKeys*maxKVSizeBytes;
    //minSstFileMigThreshold = maxDbSizeBytes/maxSstFileSizeBytes;
    }
  void setPopRank() {
    popRank = static_cast<uint32_t>(popThreshold * numKeys);
  }
  void initPartitions(void);
  int getPartition(uint64_t k);
  void CheckAndTriggerUpserts(PartitionContext* p_ctx);
  // JIANAN: end here

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit, bool* save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void RemoveObsoleteFiles(PartitionContext* p_ctx = nullptr) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  WriteBatch* BuildBatchGroup(Writer** last_writer)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void RecordBackgroundError(const Status& s);

  void MaybeScheduleCompaction(uint8_t pid=0, MigrationReason reason=MIG_REASON_OPTANE_SIZE) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void* db, uint8_t pid);
  void BackgroundCall(PartitionContext* p_ctx);
  void BackgroundCompaction(PartitionContext* p_ctx) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupCompaction(PartitionContext* p_ctx, CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWork(PartitionContext* p_ctx, CompactionState* compact, std::vector<index_entry>& migration_keys, std::vector<uint64_t>& migration_keys_prefix, int start_bid, int end_bid, std::vector<std::pair<Slice, Slice>>& upsert_keys);
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status OpenCompactionOutputFile(PartitionContext* p_ctx, CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  // PRISMDB
  // Check if key is present in optane index. If so, set the "value" argument.
  // Return value: kOk if key is found, else kNotFound
  Status GetObjectFromOptane(const ReadOptions& options, const Slice& key,
                   std::string* value);
  // Function selects the keys that need to be migrated from optane to flash and
  // sorts them (low to high). Optionally, it also sets the overlapping sst files
  void SelectMigrationKeys(PartitionContext* p_ctx, std::vector<index_entry>& migration_keys,
		  std::vector<uint64_t>& migration_keys_prefix, std::vector<FileMetaData*>& overlapping_sst_files);
  // This function returns a vector of all SST file Metadata pointers
  std::vector<FileMetaData*> GetSSTFileMetaData();

  // JIANAN:
  // This function takes the starrt_key
  // finds the SST file that overlaps with start_key
  // sets the smallest and largest keys (converted to uint64_t) within this SST file in overlap argument
  // return -1 if no overlapping sst file was found
  int getOverlapSSTBounds(PartitionContext* p_ctx, std::vector<FileMetaData*>& overlapping_sst_files, uint64_t start_key, std::pair<uint64_t, uint64_t>& overlap);
  int getOverlapSSTBoundsOpt(PartitionContext* p_ctx, std::vector<FileMetaData*>& overlapping_sst_files, uint64_t start_key, std::pair<uint64_t, uint64_t>& overlap, int num_files=1);
  int findBestSSTOverlap(PartitionContext* p_ctx, std::pair<uint64_t, uint64_t>& overlap, int num_files);

  void findRandomSSTRanges(PartitionContext* p_ctx, std::vector<std::pair<uint64_t, uint64_t> >& bounds_lst, int range_num, int range_size);

  //void findSSTRanges(PartitionContext* p_ctx, std::vector<std::pair<uint64_t, uint64_t> >& bounds_lst, int migration_policy, int range_num);
  void findSSTRanges(PartitionContext* p_ctx, std::vector<std::pair<uint64_t, uint64_t> >& bounds_lst, std::vector<uint64_t>& sst_fns, int migration_policy, int range_num);

  void findSSTRanges_Twitter(PartitionContext* p_ctx, std::vector<std::pair<uint64_t, uint64_t> >& bounds_lst, std::vector<uint64_t>& sst_fns, int migration_policy, int range_num);

  //void selectBestRange(PartitionContext* p_ctx, std::vector<std::pair<uint64_t, uint64_t> >& bounds_lst, std::pair<uint64_t, uint64_t>& final_bound, int migration_metric);
  void selectBestRange(PartitionContext* p_ctx, std::vector<std::pair<uint64_t, uint64_t> >& bounds_lst, std::vector<uint64_t>& sst_fns, std::pair<uint64_t, uint64_t>& final_bound, int migration_metric);

  float find_precise_M(PartitionContext* p_ctx, uint64_t min_key, uint64_t max_key, uint64_t sst_fn, uint64_t sst_size);
  float find_precise_cost(PartitionContext* p_ctx, uint64_t min_key, uint64_t max_key, uint64_t sst_fn, uint64_t sst_size);
  float find_approx_M(uint64_t min_key, uint64_t max_key, uint64_t sst_size);

  float find_precise_M_new(PartitionContext* p_ctx, uint64_t min_key, uint64_t max_key, uint64_t sst_fn, uint64_t sst_size);
  float find_precise_cost_benefit(PartitionContext* p_ctx, uint64_t min_key, uint64_t max_key, uint64_t sst_fn, uint64_t sst_size); 

  float find_precise_free_density(PartitionContext* p_ctx, uint64_t min_key, uint64_t max_key, uint64_t sst_fn, uint64_t sst_size);
  // Prints sst file numbers
  void PrintSstFiles(bool lock=false);

  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;  // options_.comparator == &internal_comparator_
  const bool owns_info_log_;
  const bool owns_cache_;
  const std::string dbname_;

  // P2: if keys are not bulk loaded into the database, set this flag to false
  const bool is_twitter_ = false; // true
  uint32_t slab_size;
  uint32_t nb_slabs = 1; //5

  // table_cache_ provides its own synchronization
  TableCache* const table_cache_;

  // Lock over the persistent DB state.  Non-null iff successfully acquired.
  FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  std::atomic<bool> shutting_down_;
  port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
  MemTable* mem_;
  MemTable* imm_ GUARDED_BY(mutex_);  // Memtable being compacted
  std::atomic<bool> has_imm_;         // So bg thread can detect non-null imm_
  WritableFile* logfile_;
  uint64_t logfile_number_ GUARDED_BY(mutex_);
  log::Writer* log_;
  uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.

  // Queue of writers.
  std::deque<Writer*> writers_ GUARDED_BY(mutex_);
  WriteBatch* tmp_batch_ GUARDED_BY(mutex_);

  SnapshotList snapshots_ GUARDED_BY(mutex_);

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

  // Has a background compaction been scheduled or is running?
  bool background_compaction_scheduled_ GUARDED_BY(mutex_);

  ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);

  VersionSet* const versions_ GUARDED_BY(mutex_);

  // Have we encountered a background error in paranoid mode?
  Status bg_error_ GUARDED_BY(mutex_);

  CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
