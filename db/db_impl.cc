// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

#include <random>

// JIANAN
extern "C" {
  #include "nvm/in-memory-index-btree.h"
  // #include "optane/headers.h"
  // #include "optane/workload-common.h"
}

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;
};

struct DBImpl::CompactionState {
  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}

  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  // P2 HACK: if block_cache is nullptr, do not create one
  //if (result.block_cache == nullptr) {
  //  result.block_cache = NewLRUCache(8 << 20);
  //}
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

// JIANAN: predefine number of worker thread to be 2
int nb_disks = 1;
int nb_workers_per_disk = 2;

//size_t slab_sizes[] = {1024}; // size of item
//size_t nb_slabs = sizeof(slab_sizes)/sizeof(*slab_sizes);
///////////////////////////////////////

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(false),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      has_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_)) {}

// JIANAN
void DBImpl::ReportMigrationStats() {
  for (int i = 0; i < numPartitions; i++) {
    fprintf(stderr, "\nPartition %d\n", i);
    fprintf(stderr, "\nPutImpl Statistics\n");
    fprintf(stderr, "Num of Puts: %d\n", partitions[i].num_puts);
    fprintf(stderr, "Num of Inserts %d; Num of Updates: %d\n", (partitions[i].num_puts - partitions[i].num_updates),partitions[i].num_updates);
    fprintf(stderr, "Average time for Put(): %.f ns\n", (partitions[i].put_time / (float) partitions[i].num_puts));
    fprintf(stderr, "Average time for converting kv format: %.f ns\n", (partitions[i].put_copy_array / (float) partitions[i].num_puts));
    fprintf(stderr, "Average time for acquiring partition lock: %.f ns\n", (partitions[i].put_acquire_lock / (float) partitions[i].num_puts));
    fprintf(stderr, "Average time for btree_find: %.f ns\n", (partitions[i].put_find_index_time / (float) partitions[i].num_puts));
    fprintf(stderr, "Average time for insert_item_sync: %.f ns\n", (partitions[i].insert_optane_time / (float) (partitions[i].num_puts - partitions[i].num_updates)));
    fprintf(stderr, "Average time for update_item_sync: %.f ns\n", (partitions[i].update_optane_time / (float) partitions[i].num_updates));
    fprintf(stderr, "Average time for btree_delete: %.f ns\n", (partitions[i].put_delete_index_time / (float) partitions[i].num_updates));
    fprintf(stderr, "Average time for btree_insert: %.f ns\n", (partitions[i].put_insert_index_time / (float) partitions[i].num_puts));

    fprintf(stderr, "\nGet Statistics\n");
    fprintf(stderr, "Num of Gets: %d\n", partitions[i].num_optane_gets + partitions[i].num_qlc_gets);
    fprintf(stderr, "Num of optane: %d; Num of qlc: %d\n", partitions[i].num_optane_gets, partitions[i].num_qlc_gets);
    fprintf(stderr, "Average time for Get() from optane: %.f ns\n", (partitions[i].get_optane_time / (float) partitions[i].num_optane_gets));
    fprintf(stderr, "  Average time for acquiring optane lock: %.f ns\n", (partitions[i].get_acquire_optane_lock / (float) partitions[i].num_optane_gets));
    fprintf(stderr, "  Average time for btree_find: %.f ns\n", (partitions[i].get_find_optane_index / (float) partitions[i].num_optane_gets));
    fprintf(stderr, "  Average time for read_item_key_val: %.f ns\n", (partitions[i].get_read_optane / (float) partitions[i].num_optane_gets));
    fprintf(stderr, "Average time for Get() from qlc: %.f ns\n", (partitions[i].get_qlc_time / (float) partitions[i].num_qlc_gets));
    fprintf(stderr, "  Average time for acquiring qlc lock: %.f ns\n", (partitions[i].get_acquire_qlc_lock / (float) partitions[i].num_qlc_gets));
    fprintf(stderr, "  Average time for reading from qlc: %.f ns\n", (partitions[i].get_read_qlc / (float) partitions[i].num_qlc_gets));

    fprintf(stderr, "\nMigration Statistics\n");
    fprintf(stderr, "Num of Migrations: %d\n", partitions[i].migrationId);
    fprintf(stderr, "Average num keys for Migration: %.f \n", (partitions[i].num_mig_keys / (float) partitions[i].migrationId));
    fprintf(stderr, "Average time for BackgroundCall: %.f ns\n", (partitions[i].mig_backgroundcall / (float) partitions[i].migrationId));
    fprintf(stderr, "   Selecting migration keys: %.f ns\n", (partitions[i].mig_select / (float) partitions[i].migrationId));
    fprintf(stderr, "       Acquire partition lock: %.f ns\n", (partitions[i].mig_select_lock / (float) partitions[i].migrationId));
    fprintf(stderr, "       Select keys from btree: %.f ns\n", (partitions[i].mig_select_btree / (float) partitions[i].migrationId));
    fprintf(stderr, "       Copy keys to the arrays: %.f ns\n", (partitions[i].mig_select_copy / (float) partitions[i].migrationId));
    fprintf(stderr, "   Picking dummy sst migration file: %.f ns\n", (partitions[i].mig_pick/ (float) partitions[i].migrationId));
    fprintf(stderr, "       Acquire lsm lock: %.f ns\n", (partitions[i].mig_pick_lock / (float) partitions[i].migrationId));
    fprintf(stderr, "   Doing compaction: %.f ns\n", (partitions[i].mig_compaction / (float) partitions[i].migrationId));
    fprintf(stderr, "       Acquire lsm lock: %.f ns\n", (partitions[i].mig_compaction_lock / (float) partitions[i].migrationId));
    fprintf(stderr, "       Read from optane: %.f ns\n", (partitions[i].mig_compaction_read_optane / (float) partitions[i].migrationId));
    fprintf(stderr, "       Read from qlc: %.f ns\n", (partitions[i].mig_compaction_read_qlc / (float) partitions[i].migrationId));
    fprintf(stderr, "       Write to qlc: %.f ns\n", (partitions[i].mig_compaction_write_qlc / (float) partitions[i].migrationId));
    fprintf(stderr, "   Removing from optane and index: %.f ns\n", (partitions[i].mig_remove / (float) partitions[i].migrationId));
    fprintf(stderr, "       Acquire partition lock: %.f ns\n", (partitions[i].mig_remove_lock / (float) partitions[i].migrationId));


    fprintf(stderr, "\nbtree size in keys: %llu; btree size in KB: %llu\n", btree_get_size(partitions[i].index), (btree_get_size_in_bytes(partitions[i].index)/1024));
  }
  fprintf(stderr, "\nhash table size in KB: %llu\n", ((sizeof(uint64_t)*2*pop_table_.size() + sizeof(pop_table_))/1024));

  ////FREELIST: debug prints
  //for (int i = 0; i < numPartitions; i++) {
  //  print_freelist(partitions[i].slabContext);
  //}
}

void DBImpl::ResetMigrationStats() {
  for (int i = 0; i < numPartitions; i++) {
    partitions[i].num_puts = 0;
    partitions[i].num_upsert_puts = 0;
    partitions[i].num_updates = 0;
    partitions[i].put_time = 0;
    partitions[i].put_copy_array = 0;
    partitions[i].put_acquire_lock = 0;
    partitions[i].put_find_index_time = 0;
    partitions[i].insert_optane_time = 0;
    partitions[i].update_optane_time = 0;
    partitions[i].put_delete_index_time = 0;
    partitions[i].put_insert_index_time = 0;

    partitions[i].num_optane_gets = 0;
    partitions[i].num_qlc_gets = 0;
    partitions[i].get_optane_time = 0;
    partitions[i].get_qlc_time = 0;
    partitions[i].get_acquire_optane_lock = 0;
    partitions[i].get_find_optane_index = 0;
    partitions[i].get_read_optane = 0;
    partitions[i].get_acquire_qlc_lock = 0;
    partitions[i].get_read_qlc = 0;

    partitions[i].migrationId = 0;
    partitions[i].num_mig_keys = 0;
    partitions[i].mig_backgroundcall = 0;
    partitions[i].mig_select = 0;
    partitions[i].mig_select_lock = 0;
    partitions[i].mig_select_btree = 0;
    partitions[i].mig_select_copy = 0;
    partitions[i].mig_pick = 0;
    partitions[i].mig_pick_lock = 0;
    partitions[i].mig_compaction = 0;
    partitions[i].mig_compaction_lock = 0;
    partitions[i].mig_compaction_read_optane = 0;
    partitions[i].mig_compaction_read_qlc = 0;
    partitions[i].mig_compaction_write_qlc = 0;
    partitions[i].mig_remove = 0;
    partitions[i].mig_remove_lock = 0;
  }
}

void DBImpl::SetDbMode(bool load) {
  load_phase_ = load;
  fprintf(stderr, "Set load phase %d\n", load);
}

void DBImpl::SetPopFile(const char* pop_file) {

  if (popThreshold == 0){
    fprintf(stderr, "db_impl skip pop_file, popThreshold is 0\n");
    return;
  }
  pop_file_ = pop_file;
  fprintf(stderr, "db_impl pop_file: %s\n", pop_file_);
  int i = 0;
  if (pop_file_ != nullptr) {
    std::ifstream infile(pop_file_);
    uint64_t count, k;
    fprintf(stderr, "start reading popularity file\n");
    while (infile >> count >> k){
      //pop_table_[k] = count;
      pop_table_[k] = i;
      i++;
      //fprintf(stderr, "key: %llu, count: %llu\n", k, count);
    }
    fprintf(stderr, "size of pop_table: %zu\n", pop_table_.size());
  }
}


DBImpl::~DBImpl() {
  // Wait for background work to finish.
  mutex_.Lock();
  shutting_down_.store(true, std::memory_order_release);
  mutex_.Unlock();
  // while (background_compaction_scheduled_) {
  //   background_work_finished_signal_.Wait();
  // }
  for (int i = 0; i < numPartitions; i++) {
    while(partitions[i].background_compaction_scheduled) {
      partitions[i].background_work_finished_signal.Wait();
    }
  }
  //mutex_.Unlock();

  // JIANAN: free allocated memory for Optane
  for (int i = 0; i < numPartitions; i++) {
    // TODO: free slab structures
    close_slab_fds(partitions[i].slabContext); // close all open file descriptors for slab files
    //FREELIST : de-allocate slab_new struct and its freelist struct
    fprintf(stderr, "before delete_all_slabs\n");
    delete_all_slabs(partitions[i].slabContext);
    fprintf(stderr, "after delete_all_slabs\n");
    delete [] partitions[i].slabContext->slabs;
    delete partitions[i].slabContext;
    delete partitions[i].pop_cache_ptr;
    delete [] partitions[i].under_migration_bitmap;
  }
  delete [] partitions;

  if (options_.migration_metric == 2) {
    int num_buckets = numKeys/bucket_sz + 1 ;
    for (int i = 0; i < num_buckets; i++) {
      delete [] BucketList[i].pop_bitmap;
    }
    delete [] BucketList;
  }
  fprintf(stderr, "delete allocated Optane objects\n");
  // close all file descriptors

  // JIANAN: TODO: free memory for index tree and slabs file, check if KVell code already provides this

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

// JIANAN
void DBImpl::initPartitions(void) {
  // JIANAN: test with a key space of size 10: [0, 9]
  fprintf(stderr, "key space: %d\n", numKeys);

  partitions = new PartitionContext[numPartitions];
  for (int i = 0; i < numPartitions; i++) {
    // Step 1: init btree index
    partitions[i].index = btree_create();
    partitions[i].pid = (uint8_t)i;
    fprintf(stderr, "init btree done\n");

    if (is_twitter_) {
      //partitions[i].num_warmup_migrations = 1; // was 50 for 328M keys
      partitions[i].num_warmup_migrations = 0;
    }
    // Create under_migration bitmap
    partitions[i].under_migration_bitmap = new uint8_t[(numKeys/numPartitions)/(sizeof(uint8_t)*8)];

    // Step 2: init slab context
    slab_context_new *ctx = new slab_context_new;
    ctx->nb_slabs = nb_slabs;
    ctx->slabs = new slab_new*[nb_slabs]; // slabs points to an array of slab_new pointers
    for (int j = 0; j < nb_slabs; j++) {
      //fprintf(stderr, "init partition %d slab %d whose size is %zu\n", i, j, slab_sizes[j]);
      //ctx->slabs[j] = create_slab_new(partitions[i].slabContext, i, slab_sizes[j]);
      if (is_twitter_) {
        if (j==0){
          // cluster 39 config 100, 200, 300, 400, 600
	  // cluster 51 config 300, 400, 500, 600, 1000
          //ctx->slabs[j] = create_slab_new(partitions[i].slabContext, i, 64); // read dominated trace cluster 19 (was 256)
          ctx->slabs[j] = create_slab_new(partitions[i].slabContext, i, 300); // read dominated trace cluster 19 (was 256)
        }
	else if (j==1){
          ctx->slabs[j] = create_slab_new(partitions[i].slabContext, i, 400); // read dominated trace cluster 19 (was 256)
        }
	else if (j==2){
          ctx->slabs[j] = create_slab_new(partitions[i].slabContext, i, 500); // read dominated trace cluster 19 (was 256)
        }
	else if (j==3){
          ctx->slabs[j] = create_slab_new(partitions[i].slabContext, i, 600); // read dominated trace cluster 19 (was 256)
        }
	else{
          ctx->slabs[j] = create_slab_new(partitions[i].slabContext, i, 1000); // write dominated trace cluster 39
        }
      } else {
        //ctx->slabs[j] = create_slab_new(partitions[i].slabContext, i, 1024);
        if (j == 0) {
          ctx->slabs[j] = create_slab_new(partitions[i].slabContext, i, 1024);
        }  
        //if (j == 0) {
        //  ctx->slabs[j] = create_slab_new(partitions[i].slabContext, i, 800);
        //} else if (j == 1) {
        //  ctx->slabs[j] = create_slab_new(partitions[i].slabContext, i, 1200);
        //}  else if (j == 2) {
        //  ctx->slabs[j] = create_slab_new(partitions[i].slabContext, i, 1500);
        //}
      }
    }
    partitions[i].slabContext = ctx;
    fprintf(stderr, "init slabs done\n");

    partitions[i].background_compaction_scheduled = false;
    partitions[i].background_work_finished_signal.SetMutex(&partitions[i].mutex);

    if (popCacheSize > 0){
      // per partition clock capacity is in bytes, +1 for the 1 byte of hash table entry
      if (options_.migration_metric == 2) {
        partitions[i].pop_cache_ptr = new ClockCache((popCacheSize*(maxKeySizeBytes+1))/numPartitions, BucketList, options_.migration_metric);
      } else {
        partitions[i].pop_cache_ptr = new ClockCache((popCacheSize*(maxKeySizeBytes+1))/numPartitions,nullptr, options_.migration_metric);
      }
      fprintf(stderr, "DEBUG: pop_cache_ptr=%X\n", partitions[i].pop_cache_ptr);
      for (int j=0; j<=CLOCK_BITS_MAX_VALUE; j++){
        partitions[i].pop_cache_ptr->clk_prob_dist[j] = -1;
      }
    }
  }
}

// size is in terms of keys (not bytes)
void DBImpl::initBuckets(uint64_t size) {
  if (options_.migration_metric == 2) {
    bucket_sz = size;

    int num_buckets = numKeys/bucket_sz + 1 ;
    BucketList = new Bucket[num_buckets];

    // key space is [0, numKeys)
    for (int i = 0; i < num_buckets; i++) {
      BucketList[i].num_total_keys = 0;
      BucketList[i].num_pop_keys = 0;
      BucketList[i].overlap_ratio = 0;
      BucketList[i].pop_bitmap = new uint8_t[bucket_sz/(sizeof(uint8_t)*8)];
      for (int j=0; j<bucket_sz/(sizeof(uint8_t)*8); j++){
        BucketList[i].pop_bitmap[j] = 0;
      }
    }

    fprintf(stderr, "init buckets done, bucket size %llu \n", bucket_sz);
  }
}



Status DBImpl::NewDB() {

  // JIANAN: start
  fprintf(stderr, "DBImpl::NewDB() called\n");
  //DBImpl::setPartitionNum(4);
  //DBImpl::setKeyNum(8000000);

  //fprintf(stderr, "done with setkeynum\n");
  DBImpl::setMaxDbSize();
  if (is_twitter_) {
    // HACK: for cluster 19
    //maxDbSizeBytes = 35*1e9;
    // HACK: for cluster 39
    //maxDbSizeBytes = 100*1e9;
    // HACK: for cluster 51
    maxDbSizeBytes = 800*1e6;
  }
  DBImpl::setPopRank(); // update popRank
  // set bucket size
  // TODO: make bucket size configurable via options
  DBImpl::initBuckets(BUCKET_SZ);
  DBImpl::initPartitions();
  maxSstFileSizeBytes = static_cast<uint32_t>(options_.max_file_size);

  // JIANAN: end

  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->RemoveFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::RemoveObsoleteFiles(PartitionContext* p_ctx) {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  //std::set<uint64_t> live = pending_outputs_;
  std::set<uint64_t> live = p_ctx->pending_outputs;
  /*fprintf(stderr, "%X\tPending outputs size %d\n", std::this_thread::get_id(), p_ctx->pending_outputs.size());
  for (auto f : live){
    fprintf(stderr, "%X\tRemoveObsolete pending file %llu\n", std::this_thread::get_id(), f);
  }*/
  versions_->AddLiveFiles(&live);

  /*fprintf(stderr, "Live size %d\n", live.size());
  for (auto f : live){
    fprintf(stderr, "%X\tRemoveObsolete live file %llu\n", std::this_thread::get_id(), f);
  }*/

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  for (std::string& filename : filenames) {
    //fprintf(stderr, "%X\tRemoveObsolete file %s\n", std::this_thread::get_id(), filename.c_str());
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          if (p_ctx->files.find(number) != p_ctx->files.end()){
            keep = (live.find(number) != live.end());
            //fprintf(stderr, "%X\tRemoveObsolete keep %d\n", std::this_thread::get_id(), keep);
	          if (!keep) {
              p_ctx->files.erase(number);
              p_ctx->file_entries.erase(number);
            }
          }
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        files_to_delete.push_back(std::move(filename));
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
            static_cast<unsigned long long>(number));
      }
    }
    p_ctx->pending_outputs.clear();
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
  mutex_.Unlock(); //ASHL
  for (const std::string& filename : files_to_delete) {
    //fprintf(stderr, "%X\tRemoving file %s\n", std::this_thread::get_id(), filename.c_str());
    env_->RemoveFile(dbname_ + "/" + filename);
  }
  mutex_.Lock(); //ASHL
}

Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      Log(options_.info_log, "Creating DB %s since it was missing.",
          dbname_.c_str());
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.store(false, std::memory_order_release);
    RemoveObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

void DBImpl::PrintSstFiles(bool lock) {
  if (lock){
    mutex_.Lock(); //ASHL
  }
  std::vector<FileMetaData*> files = GetSSTFileMetaData();
  fprintf(stderr, "%X\tSST FILES:\n", std::this_thread::get_id());
  for (auto f : files) {
    fprintf(stderr, "%X\t %llu\n", std::this_thread::get_id(), f->number);
  }
  if (lock){
    mutex_.Unlock(); //ASHL
  }
}

// JIANAN
// This function finds the SST file that overlaps with start_key
// returns the smallest and largest keys (converted to uint64_t) within this SST file
// Question: is it ever possible that overlapped SST file is more than one, if so, return a list of pairs
int DBImpl::getOverlapSSTBounds(PartitionContext* p_ctx, std::vector<FileMetaData*>& overlapping_sst_files, uint64_t start_key, std::pair<uint64_t, uint64_t>& overlap) {
  using namespace std::chrono;
  auto begin = high_resolution_clock::now();
  int ret = -1;
  mutex_.Lock(); // ASHL
  //fprintf(stderr, "%X\tPROFILING: getOverlapSSTBounds time %llu\n", std::this_thread::get_id(), duration_cast<nanoseconds>(high_resolution_clock::now() - begin).count());
  std::vector<FileMetaData*> files = DBImpl::GetSSTFileMetaData();
  for (auto it = files.begin(); it != files.end(); ++it) {
    if (p_ctx->files.find((*it)->number) == p_ctx->files.end()){
      continue;
    }
    InternalKey smallest = (*it)->smallest;
    InternalKey largest = (*it)->largest;
    //fprintf(stderr, "SST file smallest %s largest %s\n", smallest.user_key().ToString(true).c_str(), largest.user_key().ToString(true).c_str());

    // convert InternalKey to a uint64_t key
    uint64_t min = decode_size64((unsigned char*)smallest.user_key().data());
    uint64_t max = decode_size64((unsigned char*)largest.user_key().data());
    //fprintf(stderr, "SST file min %llu max %llu\n", min, max);

    if (start_key >= min && start_key <= max) {
      overlapping_sst_files.push_back(*it);
      overlap.first = min;
      overlap.second = max;
      ret = 0;
      break;
    }
  }
  mutex_.Unlock(); //ASHL
  //fprintf(stderr, "%X\tPROFILING: getOverlapSSTBounds time %llu\n", std::this_thread::get_id(), duration_cast<nanoseconds>(high_resolution_clock::now() - begin).count());
  return ret;
}

struct comp
{
    template<typename T>
    bool operator()(const T& l, const T& r) const
    {
        return l.second.first < r.second.first;
    }
};

  // TODO: add support for returning more than 1 file, and some metric
  int DBImpl::findBestSSTOverlap(PartitionContext* p_ctx, std::pair<uint64_t, uint64_t>& overlap, int num_files) {
    int ret=-1;
    fprintf(stderr, "%X\tFindBestSSTOverlap num_files %llu\n", std::this_thread::get_id(), p_ctx->files.size());
    // sort all current SST files by the value of their first element
    std::set<std::pair<uint64_t,std::pair<uint64_t,uint64_t>>, comp> sortedset(p_ctx->files.begin(), p_ctx->files.end());

    /*fprintf(stderr, "start key %llu\n", start_key);
    for (auto const &pair: sortedset){
      fprintf(stderr, "file %llu low %llu high %llu\n", pair.first, pair.second.first, pair.second.second);
    }*/

    auto it = sortedset.begin();
    int max_count = 0;
    for(; it != sortedset.end(); ++it){
      uint64_t min = (*it).second.first;
      uint64_t max = (*it).second.second;
      int count = btree_find_between_count(p_ctx->index, min, max, &pop_table_, popRank, (void*)(p_ctx->pop_cache_ptr), popThreshold);
      if (count > max_count) {
        max_count = count;
        overlap.first = min;
        overlap.second = max;
        ret = 0;
      }
     // fprintf(stderr, "%X\tSST overlap file %llu total_keys %llu overlap_keys %llu\n", std::this_thread::get_id(), (*it).first, p_ctx->file_entries[(*it).first]/1024, count);
    }
     //fprintf(stderr, "%X\tfindBestSSTOverlap returns %d unpopular keys, selects 1 file with min %llu max %llu\n", std::this_thread::get_id(), max_count, overlap.first, overlap.second);
    return ret;
  }

  int DBImpl::getOverlapSSTBoundsOpt(PartitionContext* p_ctx, std::vector<FileMetaData*>& overlapping_sst_files, uint64_t start_key, std::pair<uint64_t, uint64_t>& overlap, int num_files) {
  int ret = -1;
  // std::map<uint64, std::pair<uint64, uint64>> files[file_number] -> std::pair(smallest,largest)
  std::set<std::pair<uint64_t,std::pair<uint64_t,uint64_t>>, comp> sortedset(p_ctx->files.begin(), p_ctx->files.end());

  /*fprintf(stderr, "start key %llu\n", start_key);
  for (auto const &pair: sortedset){
    fprintf(stderr, "file %llu low %llu high %llu\n", pair.first, pair.second.first, pair.second.second);
  }*/

  auto it = sortedset.begin();
  int counter = 0;
  for(; it != sortedset.end(); ++it){
    counter++;
    uint64_t min = (*it).second.first;
    uint64_t max = (*it).second.second;
    if (start_key >= min && start_key <= max) {
      //overlapping_sst_files.push_back(entry.first);
      overlap.first = min;
      overlap.second = max;
      num_files--;
      ret = 0;
      //fprintf(stderr, "%X\tSST overlap file %llu min_bound %llu max_bound %llu\n", std::this_thread::get_id(), (*it).first, overlap.first, overlap.second);
      if (num_files==0){
        return ret;
      }
      break;
    }
  }

  //fprintf(stderr, "first counter: %d sortedset size %d\n", counter, sortedset.size());

  if(ret != -1){
    auto it_copy = it;
    while(++it != sortedset.end()){
      if (num_files == 0) {break;}
      uint64_t max = (*it).second.second;
      overlap.second = max;
      //fprintf(stderr, "second bound: %llu\n", overlap.second);
      num_files--;
    }

    while(it_copy-- != sortedset.begin()){
      if (num_files == 0) {break;}
      uint64_t min = (*it_copy).second.first;
      overlap.first = min;
      //fprintf(stderr, "first bound: %llu\n", overlap.first);
      num_files--;
    }
  }

  //fprintf(stderr, "SST overlap final bound: %llu %llu\n", overlap.first, overlap.second);

  /*for (auto entry : p_ctx->files){
    uint64_t min = entry.second.first;
    uint64_t max = entry.second.second;
    if (start_key >= min && start_key <= max) {
      //overlapping_sst_files.push_back(entry.first);
      overlap.first = min;
      overlap.second = max;
      ret = 0;
      break;
    }
  }*/

  return ret;
}



void DBImpl::MaybeScheduleCompaction(uint8_t pid, MigrationReason reason) {
  PartitionContext* p_ctx = &partitions[pid];
  //p_ctx->mutex.AssertHeld();
  if (p_ctx->background_compaction_scheduled) {
    // Already scheduled
    //fprintf(stderr, "PRISMDB: migration already scheduled\n");
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  // } else if (imm_ == nullptr && manual_compaction_ == nullptr &&
  //            !versions_->NeedsCompaction()) {
  //   // No work to be done
  } else {
    p_ctx->background_compaction_scheduled = true;
    //fprintf(stderr, "PRISMDB: scheduled a new migration\n");
    //env_->Schedule(&DBImpl::BGWork, this);
    if (options_.migration_logging) {
      fprintf(stderr, "%X\t migration scheduled  id#%d partition %d partition size %zu\n", std::this_thread::get_id(), p_ctx->migrationId, pid, partitions[pid].size_in_bytes);
    }
    p_ctx->mig_reason = reason;
    env_->SchedulePartition(&DBImpl::BGWork, this, pid);
  }
}

/*void DBImpl::MaybeScheduleCompaction(PartitionContext* p_ctx) {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) {
    // Already scheduled
    //fprintf(stderr, "PRISMDB: migration already scheduled\n");
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  // } else if (imm_ == nullptr && manual_compaction_ == nullptr &&
  //            !versions_->NeedsCompaction()) {
  //   // No work to be done
  } else {
    background_compaction_scheduled_ = true;
    //fprintf(stderr, "PRISMDB: scheduled a new migration\n");
    //env_->Schedule(&DBImpl::BGWork, this);
    env_->SchedulePartition(&DBImpl::BGWork, this, reinterpret_cast<void*>(p_ctx));
  }
}*/

void DBImpl::BGWork(void* db, uint8_t pid) {
  PartitionContext* p_ctx = &(reinterpret_cast<DBImpl*>(db)->partitions[pid]);
  //if (reinterpret_cast<DBImpl*>(db)->options_.migration_logging) {
  //  fprintf(stderr, "%X\t BackgroundCompaction partition=%d reason=%d\n", std::this_thread::get_id(), pid, p_ctx->mig_reason);
  //}
  reinterpret_cast<DBImpl*>(db)->BackgroundCall(p_ctx);
}

void DBImpl::BackgroundCall(PartitionContext* p_ctx) {
  using namespace std::chrono;
  //MutexLock l(&mutex_); //ASHL
  //mutex_.Unlock(); // unlock it right away, we will selectively lock it for lsm related code

  assert(p_ctx->background_compaction_scheduled); // TODO:
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    float migration_lower_bound = 0.95;
    do{
      uint64_t t1 = duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
      //fprintf(stderr, "%X\t microsecond %llu BackgroundCall_START %d Partition %d\n", std::this_thread::get_id(), t1, p_ctx->migrationId, p_ctx->pid);
      auto begin = high_resolution_clock::now();
      BackgroundCompaction(p_ctx); // main compaction function
      auto end = high_resolution_clock::now();
      uint64_t t2 = duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
      //fprintf(stderr, "%X\t microsecond %llu BackgroundCall_END %d Partition %d\n", std::this_thread::get_id(), t2, p_ctx->migrationId, p_ctx->pid);
      float total_compaction = duration_cast<nanoseconds>(end - begin).count();
      //fprintf(stderr, "%X\t total compaction takes %.f ms\n", std::this_thread::get_id(), (total_compaction/1000000));
      p_ctx->mig_backgroundcall = p_ctx->mig_backgroundcall + duration_cast<nanoseconds>(end - begin).count();
      // do not loop for upsert migrations
      if(p_ctx->mig_reason==MIG_REASON_UPSERT){
        break;
      }
    }
    while(p_ctx->size_in_bytes > (float)(maxDbSizeBytes*optaneThreshold*migration_lower_bound/(float)numPartitions));
  }

  p_ctx->background_compaction_scheduled = false;
  p_ctx->background_work_finished_signal.SignalAll();

}

/*void DBImpl::BackgroundCall(PartitionContext* p_ctx) {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_); // TODO:
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction(p_ctx);
  }

  background_compaction_scheduled_ = false;

  background_work_finished_signal_.SignalAll();
}*/

// PRISMDB
// Function selects the keys that need to be migrated from optane to flash and
// sorts them (low to high). Optionally, it also returns the overlapping sst files
std::mutex optane_mu;

// ASH: NOT USED, DELETE LATER
typedef struct KeyValue{
  uint64_t sn;
  ValueType vtype;
  std::string key;
  char value[101];
}KeyValue;
KeyValue kv[10];

uint64_t kv_idx = 0;

std::map<std::string, KeyValue*> optane_map;
uint64_t optane_size = 0;

void DBImpl::findSSTRanges(PartitionContext* p_ctx, std::vector<std::pair<uint64_t, uint64_t> >& bounds_lst, std::vector<uint64_t>& sst_fns, int migration_policy, int range_num) {

  if ((migration_policy == 1) || (p_ctx->mig_reason == MIG_REASON_UPSERT)) { // exhaustive search
    // select all ranges
    for (auto it = p_ctx->files.begin(); it != p_ctx->files.end(); ++it) {
      sst_fns.push_back(it->first);
      std::pair<uint64_t, uint64_t> file_bounds = it->second;
      bounds_lst.push_back(std::make_pair(file_bounds.first, file_bounds.second));
    }
  } else if (migration_policy == 2) { // random policies
    // create a vector copy for existing SST file numbers
    std::vector<uint64_t> file_numbers;
    for (auto it = p_ctx->files.begin(); it != p_ctx->files.end(); ++it) {
      file_numbers.push_back(it->first);
    }
    // iterate for range_num times
    for (int i = 0; i < range_num; i++) {
      // select a SST file from an random index
      uint64_t index = rand()%(file_numbers.size());
      uint64_t file_number = file_numbers.at(index);
      sst_fns.push_back(file_number);
      //fprintf(stderr, "%X\t select file number %llu\n", std::this_thread::get_id(), file_number);
      // get the file's bound and add it to bounds_lst
      std::pair<uint64_t, uint64_t> file_bounds = p_ctx->files.find(file_number)->second;
      bounds_lst.push_back(std::make_pair(file_bounds.first, file_bounds.second));
      // TODO: consider range_size
      // remove the selected file
      file_numbers.erase(file_numbers.begin() + index);
    }
  }
  return;
}

void DBImpl::findSSTRanges_Twitter(PartitionContext* p_ctx, std::vector<std::pair<uint64_t, uint64_t> >& bounds_lst, std::vector<uint64_t>& sst_fns, int migration_policy, int range_num) {
  int range_size = 2; // number of consecutive SST ranges to consider  HACK: set it to 0 to force single SST file only
  //int range_size = 1; // number of consecutive SST ranges to consider  HACK: set it to 0 to force single SST file only
  //fprintf(stderr, "DBG: before sortedset\n");
  // sort all current SST files by the value of their first element
  std::set<std::pair<uint64_t,std::pair<uint64_t,uint64_t>>, comp> sortedset(p_ctx->files.begin(), p_ctx->files.end());

  //if (sortedset.size() == 0) {
  //  return;
  //}

  //fprintf(stderr, "DBG: after sortedset\n");
  if (migration_policy == 1) { // exhaustive search
    // select all ranges
    for (auto it = sortedset.begin(); it != sortedset.end(); ++it) {
      auto it_copy = it;
      uint64_t start_fn = it->first;
      sst_fns.push_back(start_fn);

      std::pair<uint64_t, uint64_t> start_f_bound = it->second;
      uint64_t smallest = start_f_bound.first;
      uint64_t largest = start_f_bound.second;
      int count = 0;
      // select start_fn.first to start_fn_4.first as the range
      for (; it_copy != sortedset.end(); ++it_copy) {
        if (count > range_size) {
          break;
        }
        largest = (it_copy->second.first - 1);
        count++;
      }
      bounds_lst.push_back(std::make_pair(smallest, largest));
    }
  } else if (migration_policy == 2) { // random policies
    //if (sortedset.size() == 0) {
    //  return;
    //}
    // create a vector copy for existing SST file numbers
    std::vector<uint64_t> file_numbers;
    for (auto it = sortedset.begin(); it != sortedset.end(); ++it) {
      file_numbers.push_back(it->first);
    }
    //fprintf(stderr, "DBG: after file_numbers sortedset size %llu p_ctx_>files size %llu file numbers size %llu\n", sortedset.size(), p_ctx->files.size(), file_numbers.size());
    // iterate for range_num times
    //for (int i = 0; i < range_num; i++) {
    
    do {
      //if (file_numbers.size() == 0) {
      if (sst_fns.size() == sortedset.size()) {
        fprintf(stderr, "DBG: %X findSSTRanges_Twitter sst_fn size %d sortedset size %d\n", std::this_thread::get_id(), sst_fns.size(), sortedset.size());
        break;
      }

      // select a SST file from an random index
      uint64_t start_index = rand()%(file_numbers.size());
      //fprintf(stderr, "DBG: after start_index %llu\n", start_index);
      uint64_t start_fn = file_numbers[start_index];
      
      // if the sst file was previously selected, continue
      auto result = std::find(begin(sst_fns), end(sst_fns), start_fn);
      if (result != std::end(sst_fns)) {
        continue;
      }
      
      // TODO: if range_size is more then 1, then sst_fn should include all selected fns per selection
      sst_fns.push_back(start_fn);
      //fprintf(stderr, "DBG: after start_fn %llu\n", start_fn);

      uint64_t end_index = (start_index + range_size < file_numbers.size()) ? (start_index + range_size) : (file_numbers.size()- 1);
      //fprintf(stderr, "DBG: after end_index %llu\n", end_index);
      uint64_t end_fn = file_numbers[end_index];
      //fprintf(stderr, "DBG: after end_fn %llu\n", end_fn);

      std::pair<uint64_t, uint64_t> start_f_bound = p_ctx->files.find(start_fn)->second;
      std::pair<uint64_t, uint64_t> end_f_bound = p_ctx->files.find(end_fn)->second;

      uint64_t smallest = start_f_bound.first;
      uint64_t largest = (start_fn == end_fn) ? end_f_bound.second : (end_f_bound.first - 1);
      bounds_lst.push_back(std::make_pair(smallest, largest));
      //fprintf(stderr, "DBG: findSSTRanges start_fn %llu small %llu large %llu end_fn %llu small %llu large %llu smallest %llu largest %llu\n", start_fn, start_f_bound.first, start_f_bound.second, end_fn, end_f_bound.first, end_f_bound.second, smallest, largest);
      
      // remove the selected file(s)
      //file_numbers.erase(file_numbers.begin() + start_index, file_numbers.begin() + end_index);
      //fprintf(stderr, "DBG: erase from file_numbers\n");
    
    } while (sst_fns.size() < range_num);
  }

  fprintf(stderr, "DBG: %X findSSTRanges_Twitter selects %d random ranges\n", std::this_thread::get_id(), bounds_lst.size());
  // HACK
  // Consider 2 special ranges, the head and tail of btree index 
  uint64_t min_qlc_key = 0;
  uint64_t max_qlc_key = 0;
  if (sortedset.size() > 0) {
    min_qlc_key = sortedset.begin()->second.first;
    auto set_it = sortedset.end();
    set_it--;
    max_qlc_key = set_it->second.second; 
  } 
 
  uint64_t min_opt_key = btree_get_min(p_ctx->index);
  uint64_t max_opt_key = btree_get_max(p_ctx->index);

  fprintf(stderr, "DBG: %X findSSTRanges_Twitter selects, min qlc key %llu, max qlc key %llu, min optane key %llu, max optane key %llu \n", std::this_thread::get_id(), min_qlc_key, max_qlc_key, min_opt_key, max_opt_key);

  if (min_opt_key < min_qlc_key) {
    bounds_lst.push_back(std::make_pair(min_opt_key, min_qlc_key));
    fprintf(stderr, "DBG: %X findSSTRanges_Twitter min opt key < min qlc key\n", std::this_thread::get_id());
  }

  if (max_qlc_key < max_opt_key) {
    bounds_lst.push_back(std::make_pair(max_qlc_key, max_opt_key));
    fprintf(stderr, "DBG: %X findSSTRanges_Twitter max qlc key < max opt key\n", std::this_thread::get_id());
  }
   
  return;
}

float DBImpl::find_precise_M(PartitionContext* p_ctx, uint64_t min_key, uint64_t max_key, uint64_t sst_fn, uint64_t sst_size) {
  //fprintf(stderr, "DEBUG: find_precise_M is called; min key %llu, max key %llu, sst_size %llu\n", min_key, max_key, sst_size);
  std::tuple<uint64_t, uint64_t, uint64_t> val_tuple;

  mutex_.Lock();
  Version *current = versions_->current();
  current->Ref();
  mutex_.Unlock();
  btree_find_between_count_tuple(p_ctx->index, min_key, max_key, &pop_table_, popRank, (void*)(p_ctx->pop_cache_ptr), popThreshold, sst_fn, sst_size, current, &val_tuple);
  mutex_.Lock();
  current->Unref();
  mutex_.Unlock();

  uint64_t total_keys = std::get<0>(val_tuple);
  uint64_t pop_keys = std::get<1>(val_tuple);
  // overlap_keys is the number of unpopular keys that also exist in the SST file
  uint64_t overlap_keys = std::get<2>(val_tuple);

  //fprintf(stderr, "DEBUG: find_precise_M, total_keys is %llu, pop_keys is %llu, overlap_keys is %llu\n", total_keys, pop_keys, overlap_keys);
  float p = (float) pop_keys / total_keys;
  float N = (float) sst_size / (total_keys * maxKVSizeBytes);
  float s = (float) (overlap_keys * maxKVSizeBytes) / sst_size;
  float M = (1 - p) / (N * (1 - s));
  //fprintf(stderr, "%X\t precise_M (sst_size=%dMB) p %.3f N %.3f s %.3f M %.3f\n", std::this_thread::get_id(), sst_size/(2<<19), p, N, s, M);
  return M;
}

float DBImpl::find_precise_cost(PartitionContext* p_ctx, uint64_t min_key, uint64_t max_key, uint64_t sst_fn, uint64_t sst_size) {
  //fprintf(stderr, "DEBUG: find_precise_cost is called; min key %llu, max key %llu, sst_size %llu\n", min_key, max_key, sst_size);
  std::tuple<uint64_t, uint64_t, uint64_t> val_tuple;

  mutex_.Lock();
  Version *current = versions_->current();
  current->Ref();
  mutex_.Unlock();
  btree_find_between_count_tuple(p_ctx->index, min_key, max_key, &pop_table_, popRank, (void*)(p_ctx->pop_cache_ptr), popThreshold, sst_fn, sst_size, current, &val_tuple);
  mutex_.Lock();
  current->Unref();
  mutex_.Unlock();

  uint64_t total_keys = std::get<0>(val_tuple);
  uint64_t pop_keys = std::get<1>(val_tuple);
  // overlap_keys is the number of unpopular keys that also exist in the SST file
  uint64_t overlap_keys = std::get<2>(val_tuple);

  //fprintf(stderr, "DEBUG: find_precise_M, total_keys is %llu, pop_keys is %llu, overlap_keys is %llu\n", total_keys, pop_keys, overlap_keys);
  float p = (float) pop_keys / total_keys;
  float F = (float) sst_size / (total_keys * maxKVSizeBytes);
  float o = (float) (overlap_keys * maxKVSizeBytes) / sst_size;
  float cost = ((2-o)*F)/(1-p) + 1;
  //fprintf(stderr, "%X\t precise_M (sst_size=%dMB) p %.3f N %.3f s %.3f M %.3f\n", std::this_thread::get_id(), sst_size/(2<<19), p, N, s, M);
  return cost;
}

float DBImpl::find_precise_M_new(PartitionContext* p_ctx, uint64_t min_key, uint64_t max_key, uint64_t sst_fn, uint64_t sst_size) {
  //fprintf(stderr, "DEBUG: find_precise_M is called; min key %llu, max key %llu, sst_size %llu\n", min_key, max_key, sst_size);
  std::tuple<uint64_t, uint64_t, uint64_t> val_tuple;

  mutex_.Lock();
  Version *current = versions_->current();
  current->Ref();
  mutex_.Unlock();
  btree_find_between_count_tuple(p_ctx->index, min_key, max_key, &pop_table_, popRank, (void*)(p_ctx->pop_cache_ptr), popThreshold, sst_fn, sst_size, current, &val_tuple);
  mutex_.Lock();
  current->Unref();
  mutex_.Unlock();

  uint64_t total_keys = std::get<0>(val_tuple);
  uint64_t pop_keys = std::get<1>(val_tuple);
  // overlap_keys is the number of unpopular keys that also exist in the SST file
  uint64_t overlap_keys = std::get<2>(val_tuple);

  float non_overlap_keys = sst_size/maxKVSizeBytes - overlap_keys;
  float unpop_keys = total_keys - pop_keys;
  float M = (float) (unpop_keys * unpop_keys) / non_overlap_keys;
  //fprintf(stderr, "%X\t precise_M (sst_size=%dMB) unpop_keys %llu non_overlap_keys %.3f M %.3f\n", std::this_thread::get_id(), sst_size/(2<<19), unpop_keys, non_overlap_keys, M);
  return M;
}

//float DBImpl::find_approx_M(uint64_t min_key, uint64_t max_key, uint64_t sst_size) {
//  //fprintf(stderr, "DEBUG: find_approx_M is called; min key %llu, max key %llu, sst_size %llu\n", min_key, max_key, sst_size);
//  int start_bid = min_key / bucket_sz;
//  int end_bid = max_key / bucket_sz;
//  // calculate M = (1 - p) / N * (1 - s) where
//  // p: popularity ratio = # of popular keys / total # of keys in the key range
//  // N: fanout parameter  = the key range size / overlapped QLC file size
//  // s: overlap ratio = # of overlapped keys / total # of keys in the QLC file
//  float p = 0;
//  float N = 0;
//  float s = 0;
//  float M = 0;
//
//  uint64_t total_keys = 0;
//  uint64_t pop_keys = 0;
//  uint64_t range_sz = max_key - min_key;
//  //fprintf(stderr, "DEBUG: range size is %llu\n", range_sz);
//
//  // TODO: rename overlap_sz to be bucket_overlap_sz;
//  for (int i = start_bid; i <= end_bid; i++) {
//    uint64_t min_b = i * bucket_sz;
//    uint64_t max_b = (i + 1) * bucket_sz;
//    uint64_t overlap_sz;
//
//    if (min_b < min_key && max_b > max_key) {
//      overlap_sz = max_b - min_b;
//      float f = (float) range_sz / overlap_sz;
//      total_keys += BucketList[i].num_total_keys * f;
//      pop_keys += BucketList[i].num_pop_keys * f;
//      s += (float)BucketList[i].overlap_ratio;
//      //fprintf(stderr, "DEBUG: 1, bid %d bucket_total_keys %d bucket_pop_keys %d overlap size is %llu, f is %.3f, s add by %.3f\n", i, BucketList[i].num_total_keys, BucketList[i].num_pop_keys, overlap_sz, f, BucketList[i].overlap_ratio);
//
//    } else {
//      if (min_b < min_key && max_b < max_key) {
//        overlap_sz = max_b - min_key;
//        //fprintf(stderr, "DEBUG: 2, overlap size is %llu\n", overlap_sz);
//      } else if (min_b > min_key && max_b < max_key) {
//        overlap_sz = max_b - min_b;
//        //fprintf(stderr, "DEBUG: 3, overlap size is %llu\n", overlap_sz);
//      } else { // min_b > min_key && max_b > max_key
//        overlap_sz = max_key - min_b;
//        //fprintf(stderr, "DEBUG: 4, overlap size is %llu\n", overlap_sz);
//      }
//      float f1 = (float) overlap_sz / bucket_sz;
//      float f2 = (float) overlap_sz / range_sz;
//      total_keys += BucketList[i].num_total_keys * f1;
//      pop_keys += BucketList[i].num_pop_keys * f1;
//      s += (float) BucketList[i].overlap_ratio * f2;
//      //fprintf(stderr, "DEBUG: bucket %d has num_total_keys %llu, has num_pop_keys %llu, and overlap ratio %.5f\n", i, BucketList[i].num_total_keys, BucketList[i].num_pop_keys, BucketList[i].overlap_ratio);
//      //fprintf(stderr, "DEBUG: 2, overlap size is %llu, f1 is %.5f, f2 is %.5f\n", overlap_sz, f1, f2);
//    }
//    //fprintf(stderr, "DEBUG: bucket %d, total keys %llu, pop keys %llu, s is %.3f\n", i, total_keys, pop_keys, s);
//  }
//
//  if (total_keys == 0 || sst_size == 0) {
//    return 0;
//  }
//  p = (float) pop_keys / total_keys;
//  N = (float) sst_size / (total_keys * maxKVSizeBytes);
//  M = (1 - p)/(N * (1 - s));
//  //fprintf(stderr, "for this key range, estimated pop_keys is %llu, total_keys is %llu\n", pop_keys, total_keys);
//  //fprintf(stderr, "%X\t approx_M (sst_size=%dMB) p %.3f N %.3f s %.3f M %.3f\n", std::this_thread::get_id(), sst_size/(2<<19), p, N, s, M);
//  return M;
//}

float DBImpl::find_approx_M(uint64_t min_key, uint64_t max_key, uint64_t sst_size) {
  //fprintf(stderr, "DEBUG: find_approx_M is called; min key %llu, max key %llu, sst_size %llu\n", min_key, max_key, sst_size);
  int start_bid = min_key / bucket_sz;
  int end_bid = max_key / bucket_sz;
  // calculate M = (1 - p) / N * (1 - s) where
  // p: popularity ratio = # of popular keys / total # of keys in the key range
  // N: fanout parameter  = the key range size / overlapped QLC file size
  // s: overlap ratio = # of overlapped keys / total # of keys in the QLC file
  float p = 0;
  float F = 0;
  float o = 0;
  float M = 0;

  uint64_t total_keys = 0;
  uint64_t pop_keys = 0;
  uint64_t range_sz = max_key - min_key;
  //fprintf(stderr, "DEBUG: range size is %llu\n", range_sz);

  // TODO: rename overlap_sz to be bucket_overlap_sz;
  for (int i = start_bid; i <= end_bid; i++) {
    uint64_t min_b = i * bucket_sz;
    uint64_t max_b = (i + 1) * bucket_sz;
    uint64_t overlap_sz;

    if (min_b < min_key && max_b > max_key) {
      overlap_sz = max_b - min_b;
      float f = (float) range_sz / overlap_sz;
      total_keys += BucketList[i].num_total_keys * f;
      pop_keys += BucketList[i].num_pop_keys * f;
      o += (float)BucketList[i].overlap_ratio;
      //fprintf(stderr, "DEBUG: 1, bid %d bucket_total_keys %d bucket_pop_keys %d overlap size is %llu, f is %.3f, s add by %.3f\n", i, BucketList[i].num_total_keys, BucketList[i].num_pop_keys, overlap_sz, f, BucketList[i].overlap_ratio);

    } else {
      if (min_b < min_key && max_b < max_key) {
        overlap_sz = max_b - min_key;
        //fprintf(stderr, "DEBUG: 2, overlap size is %llu\n", overlap_sz);
      } else if (min_b > min_key && max_b < max_key) {
        overlap_sz = max_b - min_b;
        //fprintf(stderr, "DEBUG: 3, overlap size is %llu\n", overlap_sz);
      } else { // min_b > min_key && max_b > max_key
        overlap_sz = max_key - min_b;
        //fprintf(stderr, "DEBUG: 4, overlap size is %llu\n", overlap_sz);
      }
      float f1 = (float) overlap_sz / bucket_sz;
      float f2 = (float) overlap_sz / range_sz;
      total_keys += BucketList[i].num_total_keys * f1;
      pop_keys += BucketList[i].num_pop_keys * f1;
      o += (float) BucketList[i].overlap_ratio * f2;
      //fprintf(stderr, "DEBUG: bucket %d has num_total_keys %llu, has num_pop_keys %llu, and overlap ratio %.5f\n", i, BucketList[i].num_total_keys, BucketList[i].num_pop_keys, BucketList[i].overlap_ratio);
      //fprintf(stderr, "DEBUG: 2, overlap size is %llu, f1 is %.5f, f2 is %.5f\n", overlap_sz, f1, f2);
    }
    //fprintf(stderr, "DEBUG: bucket %d, total keys %llu, pop keys %llu, s is %.3f\n", i, total_keys, pop_keys, s);
  }

  if (total_keys == 0 || sst_size == 0) {
    return 0;
  }
  p = (float) pop_keys / total_keys;
  F = (float) sst_size / (total_keys * maxKVSizeBytes);
  float benefit = total_keys - pop_keys;
  //float benefit = pop_keys;
  //float benefit = (pop_keys > 0) ? (float) (1 / pop_keys) : 1;
  float cost = (F*(2-o))/(1-p) + 1;
  M = (cost != 0) ? (benefit / cost) : benefit;
  //fprintf(stderr, "for this key range, estimated pop_keys is %llu, total_keys is %llu\n", pop_keys, total_keys);
  //fprintf(stderr, "%X\t approx_M (sst_size=%dMB) p %.3f N %.3f s %.3f M %.3f\n", std::this_thread::get_id(), sst_size/(2<<19), p, N, s, M);
  return M;
}

float DBImpl::find_precise_cost_benefit(PartitionContext* p_ctx, uint64_t min_key, uint64_t max_key, uint64_t sst_fn, uint64_t sst_size) {
  //fprintf(stderr, "DEBUG: find_precise_M is called; min key %llu, max key %llu, sst_size %llu\n", min_key, max_key, sst_size);
  std::tuple<uint64_t, uint64_t, uint64_t> val_tuple;

  mutex_.Lock();
  Version *current = versions_->current();
  current->Ref();
  mutex_.Unlock();
  btree_find_between_count_tuple(p_ctx->index, min_key, max_key, &pop_table_, popRank, (void*)(p_ctx->pop_cache_ptr), popThreshold, sst_fn, sst_size, current, &val_tuple);
  mutex_.Lock();
  current->Unref();
  mutex_.Unlock();

  uint64_t total_keys = std::get<0>(val_tuple);
  uint64_t pop_keys = std::get<1>(val_tuple);
  // overlap_keys is the number of unpopular keys that also exist in the SST file
  uint64_t overlap_keys = std::get<2>(val_tuple);

  //fprintf(stderr, "DEBUG: find_precise_M, total_keys is %llu, pop_keys is %llu, overlap_keys is %llu\n", total_keys, pop_keys, overlap_keys);
  float p = (float) pop_keys / total_keys;
  float N = (float) sst_size / (total_keys * maxKVSizeBytes);
  float s = (float) (overlap_keys * maxKVSizeBytes) / sst_size;
  float io_cost = (N * (1 - s)) / (1 - p); // the cost: avg write IO on flash per migrated key
  //fprintf(stderr, "%X\t precise_M (sst_size=%dMB) p %.3f N %.3f s %.3f M %.3f\n", std::this_thread::get_id(), sst_size/(2<<19), p, N, s, M);
  
  int slabsizes[nb_slabs];
  for (int i = 0; i < nb_slabs; i++) {
    slabsizes[i] = p_ctx->slabContext->slabs[i]->item_size;
  }
  int num_pages = btree_find_between_count_uniq_pages(p_ctx->index, min_key, max_key, &pop_table_, popRank, (void*)(p_ctx->pop_cache_ptr), popThreshold, slabsizes);
  float free_density = (float) (total_keys - pop_keys) / num_pages; // the benefit: average free slots per page
  float benefit_cost = free_density / io_cost;
  fprintf(stderr, "%X\t precise_cost_benefit: M %.3f unpop_keys %llu num_pages %d free_density %.8f benefit_cost %.8f\n", std::this_thread::get_id(), io_cost, total_keys - pop_keys, num_pages, free_density, benefit_cost);
  return benefit_cost; 
}

float DBImpl::find_precise_free_density(PartitionContext* p_ctx, uint64_t min_key, uint64_t max_key, uint64_t sst_fn, uint64_t sst_size) {
  //fprintf(stderr, "DEBUG: find_precise_M is called; min key %llu, max key %llu, sst_size %llu\n", min_key, max_key, sst_size);
  int unpop_keys = btree_find_between_count(p_ctx->index, min_key, max_key, &pop_table_, popRank, (void*)(p_ctx->pop_cache_ptr), popThreshold);
  int slabsizes[nb_slabs];
  for (int i = 0; i < nb_slabs; i++) {
    slabsizes[i] = p_ctx->slabContext->slabs[i]->item_size;
  }
  int num_pages = btree_find_between_count_uniq_pages(p_ctx->index, min_key, max_key, &pop_table_, popRank, (void*)(p_ctx->pop_cache_ptr), popThreshold, slabsizes);
  float free_density = (float) unpop_keys / num_pages; // the benefit: average free slots per page
  fprintf(stderr, "%X\t precise_free_density: unpop_keys %llu num_pages %d free_density %.8f\n", std::this_thread::get_id(), unpop_keys, num_pages, free_density);
  return free_density; 
}
void DBImpl::selectBestRange(PartitionContext* p_ctx, std::vector<std::pair<uint64_t, uint64_t> >& bounds_lst, std::vector<uint64_t>& sst_fns, std::pair<uint64_t, uint64_t>& final_bound, int migration_metric) {
  if (p_ctx->mig_reason == MIG_REASON_UPSERT) { // pick the oldest current sst file, aka smallest file number
    float curr_val = 0;
    uint64_t chosen_fn = 0;
    for (auto it = sst_fns.begin(); it != sst_fns.end(); ++it) {
      uint64_t fn = (*it);
      float val = 1/(float)fn;
      if ((val >= curr_val) && (fn > p_ctx->last_upsert_fn)) {
        curr_val = val;
        chosen_fn = fn;
        final_bound.first = p_ctx->files[fn].first;
        final_bound.second = p_ctx->files[fn].second;
      }
    }
    if (chosen_fn == 0) {
      int index = rand()%(sst_fns.size());
      chosen_fn = sst_fns.at(index);
      final_bound.first = bounds_lst.at(index).first;
      final_bound.second = bounds_lst.at(index).second;
    }
    p_ctx->last_upsert_fn = chosen_fn;
    //fprintf(stderr, "MIG_UPSERT picks file number %llu smallest key %llu largest key %llu\n", chosen_fn, final_bound.first, final_bound.second);
    return;
  }

  if (migration_metric == 0) { // completely random
    // randomly select a key range
    int index = rand()%(bounds_lst.size());
    final_bound.first = bounds_lst.at(index).first;
    final_bound.second = bounds_lst.at(index).second;
    return;
  }
  // find out overlapped SST file sizes if using approximated or precise migration metric (2 or 3)
  std::vector<uint64_t> sst_sizes;
  if (migration_metric >= 2) {
    for (auto it = sst_fns.begin(); it != sst_fns.end(); ++it) {
      uint64_t fn = (*it);
      uint64_t fsz = p_ctx->file_entries[fn];
      //fprintf(stderr, "DEBUG: file number %llu, file size %llu\n", fn, fsz);
      sst_sizes.push_back(fsz);
    }
    if (sst_sizes.size() != bounds_lst.size()) {
      fprintf(stderr, "ERROR: mig metric 2, sst_size %llu, sst_fns %llu, bounds_lst %llu\n", sst_sizes.size(), sst_fns.size(), bounds_lst.size());
      abort();
    }
  }
  //fprintf(stderr, "DEBUG: mig metric 2, done with finding sst file sizes \n");
  float max_metric_val = 0;
  int idx = 0;
  // iterate over candidate key ranges and select the one with maximum metric value
  for (auto it = bounds_lst.begin(); it != bounds_lst.end(); ++it) {
    uint64_t min_key = it->first;
    uint64_t max_key = it->second;
    float curr_metric_val = 0;

    if (migration_metric == 1) { // find # of unpopular keys
      curr_metric_val = btree_find_between_count(p_ctx->index, min_key, max_key, &pop_table_, popRank, (void*)(p_ctx->pop_cache_ptr), popThreshold);
    } else if (migration_metric == 2) { // approximated metric M, cost-benefit
      curr_metric_val  = find_approx_M(min_key, max_key, sst_sizes.at(idx));
      idx += 1;
    } else if (migration_metric == 3) { // find precise cost (flash IO per migrated key)
      float cost = find_precise_cost(p_ctx, min_key, max_key, sst_fns.at(idx), sst_sizes.at(idx));
      curr_metric_val = (cost != 0) ? (1 / cost) : 0;
      idx += 1;
    } else if (migration_metric == 4) { // find precise benefit (sum of clock value of all popular keys)
      float benefit = btree_find_between_sum_inv_popvals(p_ctx->index, min_key, max_key, (void*)(p_ctx->pop_cache_ptr), popThreshold);
      //float sum_pop_vals = btree_find_between_sum_popvals(p_ctx->index, min_key, max_key, (void*)(p_ctx->pop_cache_ptr), popThreshold);
      curr_metric_val = benefit;
      //curr_metric_val = (sum_pop_vals != 0) ? (1 / sum_pop_vals) : 1;
    } else if (migration_metric == 5) { // find precise cost-benefit
      float benefit = btree_find_between_sum_inv_popvals(p_ctx->index, min_key, max_key, (void*)(p_ctx->pop_cache_ptr), popThreshold);
      //float sum_pop_vals = btree_find_between_sum_popvals(p_ctx->index, min_key, max_key, (void*)(p_ctx->pop_cache_ptr), popThreshold);
      //float benefit = (sum_pop_vals != 0) ? (1 / sum_pop_vals) : 1;
      float cost = find_precise_cost(p_ctx, min_key, max_key, sst_fns.at(idx), sst_sizes.at(idx));
      curr_metric_val = (cost != 0) ? (benefit / cost) : benefit;
      idx += 1;
    } else if (migration_metric == 6) { // find max number of key
      curr_metric_val = btree_find_between_total_count(p_ctx->index, min_key, max_key);
    } 

    //if (migration_metric == 1) { // find # of unpopular keys
    //  curr_metric_val = btree_find_between_count(p_ctx->index, min_key, max_key, &pop_table_, popRank, (void*)(p_ctx->pop_cache_ptr), popThreshold);
    //} else if (migration_metric == 2) { // approximated metric M = (1 - p) / N * ( 1- s)
    //  curr_metric_val  = find_approx_M(min_key, max_key, sst_sizes.at(idx));
    //  idx += 1;
    //} else if (migration_metric == 3) {
    //  curr_metric_val = find_precise_M(p_ctx, min_key, max_key, sst_fns.at(idx), sst_sizes.at(idx));
    //  idx += 1;
    //} else if (migration_metric == 4) {
    //  curr_metric_val = find_precise_M_new(p_ctx, min_key, max_key, sst_fns.at(idx), sst_sizes.at(idx));
    //  idx += 1;
    //} else if (migration_metric == 5) { // ratio of benefit and cost
    //  curr_metric_val = find_precise_cost_benefit(p_ctx, min_key, max_key, sst_fns.at(idx), sst_sizes.at(idx));
    //} else if (migration_metric == 6) { // free slot density
    //  curr_metric_val = find_precise_free_density(p_ctx, min_key, max_key, sst_fns.at(idx), sst_sizes.at(idx));
    //} 
    //fprintf(stderr, "DEBUG: curr_metric_val is %.3f\n", curr_metric_val);

    if (curr_metric_val >= max_metric_val) {
      max_metric_val = curr_metric_val;
      final_bound.first = min_key;
      final_bound.second = max_key;
    }
  }
  fprintf(stderr, "%X\t DEBUG: max migration metric value is %.3f\n", std::this_thread::get_id(), max_metric_val);
}

void DBImpl::findRandomSSTRanges(PartitionContext* p_ctx, std::vector<std::pair<uint64_t, uint64_t> >& bounds_lst, int range_num, int range_size) {
  // create a vector copy for existing SST file numbers
  std::vector<uint64_t> file_numbers;
  for (auto i = p_ctx->files.begin(); i != p_ctx->files.end(); ++i) {
    file_numbers.push_back(i->first);
  }

  // Iterate for range_num times
  for (int i = 0; i < range_num; i++) {
    // select a SST file from an random index
    uint64_t index = rand()%(file_numbers.size());
    uint64_t file_number = file_numbers.at(index);
    //fprintf(stderr, "%X\t select file number %llu\n", std::this_thread::get_id(), file_number);
    // get the file's bound and add it to bounds_lst
    std::pair<uint64_t, uint64_t> file_bounds = p_ctx->files.find(file_number)->second;
    bounds_lst.push_back(std::make_pair(file_bounds.first, file_bounds.second));
    // TODO: consider range_size

    // remove the selected file
    file_numbers.erase(file_numbers.begin() + index);
  }

  return;
}

void DBImpl::SelectMigrationKeys(PartitionContext* p_ctx, std::vector<index_entry>& migration_keys,
                  std::vector<uint64_t>& migration_keys_prefix, std::vector<FileMetaData*>& overlapping_sst_files){
  using namespace std::chrono;
  // Critical Section:
  auto begin_lock = high_resolution_clock::now();
  p_ctx->mtx.lock();
  auto end_lock = high_resolution_clock::now();
  index_scan keys;
  keys.nb_entries = 0;
  if (load_phase_ == true){ // Use round-robin for loading phase
    // multiplying maxfile size by 0.8 to leave some room for index and filter blocks
    keys = btree_find_n_bytes_rr(p_ctx->index, &p_ctx->prev_migration_key, (uint32_t)(0.8*(float)maxSstFileSizeBytes), &pop_table_, popRank, false, false, (void*)(p_ctx->pop_cache_ptr), popThreshold);
    //fprintf(stderr, "Load phase: prev_migration_key now is: %llu\n", p_ctx->prev_migration_key);
    if (keys.nb_entries == 0) {
      fprintf(stderr, "%X ERROR: load phase - btree_find_n_bytes_rr returns 0 entries\n", std::this_thread::get_id());
      delete [] keys.hashes;
      delete [] keys.entries;
      abort();
      // HACK: pick only 500 keys
      //keys = btree_find_n_bytes_rr(p_ctx->index, &p_ctx->prev_migration_key, (uint32_t)(500000), &pop_table_, popRank, false, (void*)(p_ctx->pop_cache_ptr), popThreshold);
      //if (keys.nb_entries == 0) {
      //  fprintf(stderr, "%X ERROR: load phase - btree_find_n_bytes_rr hack 500 returns 0 entries\n", std::this_thread::get_id());
      //}
    }
  }
  else{ // running phase
    std::vector<std::pair<uint64_t, uint64_t> > bounds_lst; // list of selected sst ranges of (smallest key, largest key)
    std::vector<uint64_t> sst_fns; // list of the corresponding sst file numbers
    //fprintf(stderr, "DBG: before findSSTRanges\n");

    if (p_ctx->num_warmup_migrations == 0){
      // Step 1: Pick # of candidate key ranges
      if (is_twitter_) {
        findSSTRanges_Twitter(p_ctx, bounds_lst, sst_fns, options_.migration_policy, options_.migration_rand_range_num);
      } else {
        findSSTRanges(p_ctx, bounds_lst, sst_fns, options_.migration_policy, options_.migration_rand_range_num);
        //findSSTRanges_Twitter(p_ctx, bounds_lst, sst_fns, options_.migration_policy, options_.migration_rand_range_num);
      }
      fprintf(stderr, "DBG: %X select %d candidate key ranges\n", std::this_thread::get_id(), bounds_lst.size());

      // options_.migration metric is an int of possible values:
      // 0: completely random
      // 1: maximize the # of unpopular keys in a range
      // 2: maximize the approximated metric, M = (1-p)/N*(1-s)
      // 3: maximize the precise metric, M = (1-p)/N*(1-s)

      if (bounds_lst.size() > 0) {
        std::pair<uint64_t, uint64_t> final_bound; // the final selected range - (smallest key, largest key)
        // Step 2: Pick the best key range based on the appropriate migration metric
        selectBestRange(p_ctx, bounds_lst, sst_fns, final_bound, options_.migration_metric);
        fprintf(stderr, "DBG: %X select best range, smallest key %llu, largest key %llu\n", std::this_thread::get_id(), final_bound.first, final_bound.second);

        // Step 3: Find unpopular keys from this key range for migration
        if (options_.migration_metric == 2) {
          std::map<uint64_t, std::pair<uint64_t, uint64_t>> updated_bucket_info;
          keys = btree_find_between_metric2(p_ctx->index, &p_ctx->prev_migration_key, final_bound.first, final_bound.second, &pop_table_, popRank, (void*)(p_ctx->pop_cache_ptr), popThreshold, (void*) &updated_bucket_info, bucket_sz);

          UpdateBucketNumKeys(updated_bucket_info);
        } else if (options_.migration_metric == 6) {
          keys = btree_find_between(p_ctx->index, &p_ctx->prev_migration_key, final_bound.first, final_bound.second, &pop_table_, popRank, nullptr, popThreshold);
        } else {
          keys = btree_find_between(p_ctx->index, &p_ctx->prev_migration_key, final_bound.first, final_bound.second, &pop_table_, popRank, (void*)(p_ctx->pop_cache_ptr), popThreshold);
        }
      }

      // if found no matching keys, fall back to round-robin
      // HACK
      if (keys.nb_entries == 0) {
        fprintf(stderr, "DBG: %X ERROR: btree_find_between returns 0 entries\n", std::this_thread::get_id());
        delete [] keys.hashes;
        delete [] keys.entries;
        // HACK: pick only 500 keys
        keys = btree_find_n_bytes_rr(p_ctx->index, &p_ctx->prev_migration_key, (uint32_t)(0.8*(float)maxSstFileSizeBytes), &pop_table_, popRank, false, true, (void*)(p_ctx->pop_cache_ptr), popThreshold);
      }
    } else {
      fprintf(stderr, "DBG: %X warmup migration round-robin\n", std::this_thread::get_id());
      keys = btree_find_n_bytes_rr(p_ctx->index, &p_ctx->prev_migration_key, (uint32_t)(0.8*(float)maxSstFileSizeBytes), &pop_table_, popRank, false, true, (void*)(p_ctx->pop_cache_ptr), popThreshold);
      // HACK - for twitter workload, use 1 warm up migration and select the entire partition key space for migration
      //std::pair<uint64_t, uint64_t> final_bound; // the final selected range - (smallest key, largest key)
      //final_bound.first = (p_ctx->pid*numKeys)/numPartitions;
      //final_bound.second = final_bound.first + numKeys/numPartitions - 1;
      //keys = btree_find_between(p_ctx->index, &p_ctx->prev_migration_key, final_bound.first, final_bound.second, &pop_table_, popRank, (void*)(p_ctx->pop_cache_ptr), popThreshold);
      p_ctx->num_warmup_migrations = (p_ctx->num_warmup_migrations > 0) ? (p_ctx->num_warmup_migrations - 1) : 0;
    }
  }

  if (options_.migration_logging) {
    fprintf(stderr, "DBG: %X\t Num selected keys for migration %d\n", std::this_thread::get_id(), keys.nb_entries);
  }
  auto end_select_btree = high_resolution_clock::now();

  ResetMigrationBitmap(p_ctx->pid);
  // Step 4: push the key to migration_keys
  for (size_t i = 0; i < keys.nb_entries; i++) {
    // set the selected key's under_mig to true/1 in the partition's bitmap
    SetMigrationBitmap(p_ctx->pid, keys.hashes[i], 1);

    migration_keys.push_back(keys.entries[i]);
    migration_keys_prefix.push_back(keys.hashes[i]);

    if (options_.migration_metric == 2) {
      // update the bucket's pop bitmap if the k is set
      uint64_t k = keys.hashes[i];
      int bucket_idx = k / bucket_sz;
      while (BucketList[bucket_idx].in_use.test_and_set()) {}
      int byte_idx = (k - bucket_idx * bucket_sz) / (sizeof(uint8_t)*8);
      int bit_idx = (k - bucket_idx * bucket_sz) % (sizeof(uint8_t)*8);
      uint8_t bit_mask = (1 << bit_idx);
      if ((BucketList[bucket_idx].pop_bitmap[byte_idx] & bit_mask) > 0 ) { // if bit is already set
        BucketList[bucket_idx].num_pop_keys--; // decrement it
        BucketList[bucket_idx].pop_bitmap[byte_idx] = (BucketList[bucket_idx].pop_bitmap[byte_idx] & ~bit_mask);
      }
      BucketList[bucket_idx].in_use.clear();
    }
  }

  auto end_copy_keys = high_resolution_clock::now();
  // free keys, which is malloc-ed in btree_find_between
  delete [] keys.hashes;
  delete [] keys.entries;
  //fprintf(stderr, "End of SelectMigrationKeys\n");

  p_ctx->mig_select_lock = p_ctx->mig_select_lock + duration_cast<nanoseconds>(end_lock - begin_lock).count();
  p_ctx->mig_select_btree = p_ctx->mig_select_btree + duration_cast<nanoseconds>(end_select_btree - end_lock).count();
  p_ctx->mig_select_copy = p_ctx->mig_select_copy + duration_cast<nanoseconds>(end_copy_keys - end_select_btree).count();

  return;
}

void DBImpl::BackgroundCompaction(PartitionContext* p_ctx) {

  // Precondition: Some worker trigger migration
  // 1. SelectMigrationKeys() picks a vector of selection migration keys (index entries)
  // 2. Use migration key index entries to read the key and value pair from optane
  // 3. PickMigration() - finds the overlapping SST files with migration keys, setups up compaction
  // NOTE: kvs array may cause this thread to run out of stack space
  // sorted list of keys that need to be migrated

  fprintf(stderr, "DBG: %X\t BackgroundCompaction partition=%d reason=%d\n", std::this_thread::get_id(), p_ctx->pid, p_ctx->mig_reason);

  //fprintf(stderr, "%X\tBackground compaction start partition %llu\n", std::this_thread::get_id(), p_ctx->pid);
  using namespace std::chrono;
  auto begin = high_resolution_clock::now();

  std::vector<index_entry> migration_keys;
  std::vector<uint64_t> migration_keys_prefix; // NOTE key prefixes are full keys right now
  std::vector<FileMetaData*> overlapping_sst_files;

  // record latest ts for the partition before calling SelectMigrationKeys
  //uint64_t cur_ts = p_ctx->sequenceNum;

  // Step 1: Select keys for migration
  if ((!load_phase_) && (popCacheSize > 0)){
    p_ctx->pop_cache_ptr->GenClockProbDist(popThreshold);
  }
  SelectMigrationKeys(p_ctx, migration_keys, migration_keys_prefix, overlapping_sst_files);
  auto end_selection = high_resolution_clock::now();
  assert(migration_keys.size() != 0);

  //mutex_.Lock(); //ASHL
  //mutex_.AssertHeld();

  // Step 2: Trigger background compaction to QLC
  int total_mig_keys = migration_keys.size();
  kv_pair smallest_kv, largest_kv;
  //smallest_kv.buf = new char[4096];
  //largest_kv.buf = new char[4096];
  char small_buf [4096] = " ";
  char large_buf [4096] = " ";
  smallest_kv.buf = small_buf;
  largest_kv.buf = large_buf;

  //fprintf(stderr, "before read item key val\n");
  read_item_key_val(p_ctx->slabContext, &migration_keys[0], &smallest_kv);
  //fprintf(stderr, "after smallest read item key val\n");
  read_item_key_val(p_ctx->slabContext, &migration_keys[total_mig_keys-1], &largest_kv);
  //fprintf(stderr, "after largest read item key val\n");

  // SM2
  p_ctx->mtx.unlock();

  //EncodeFixed64(smallest_kv.buf[smallest_kv.key_size], (0 << 8) | kTypeValue);
  //EncodeFixed64(smallest_kv.buf[smallest_kv.key_size], (0 << 8) | kTypeValue);

  InternalKey smallest = InternalKey(Slice((const char*)smallest_kv.buf, smallest_kv.key_size),0, kTypeValue);
  InternalKey largest = InternalKey(Slice((const char*)largest_kv.buf, largest_kv.key_size),0, kTypeValue);
  //fprintf(stderr, "SMALL %s\n", smallest.Encode().ToString(true).c_str());
  //fprintf(stderr, "LARGE %s\n", largest.Encode().ToString(true).c_str());
  //fprintf(stderr, "%X\t Migration start key %s end key %s\n", std::this_thread::get_id(), smallest.user_key().ToString(true).c_str(), largest.user_key().ToString(true).c_str());

  // create dummy sst metadata for migration keys
  FileMetaData dummy_mig_keys_file;
  dummy_mig_keys_file.refs=0;
  dummy_mig_keys_file.allowed_seeks=(1<<30);
  dummy_mig_keys_file.number=0;
  dummy_mig_keys_file.file_size=0;
  dummy_mig_keys_file.smallest=smallest;
  dummy_mig_keys_file.largest=largest;

  Compaction* c;
  auto begin_qlc_lock = high_resolution_clock::now();
  mutex_.Lock(); // ASHL
  auto end_qlc_lock = high_resolution_clock::now();

  c = versions_->PickMigration(&dummy_mig_keys_file, overlapping_sst_files);
  /*for(int i=0; i<c->num_input_files(1); i++){
    fprintf(stderr, "%X\tCOMPACTION FILES: %llu min %s max %s\n", std::this_thread::get_id(), c->input(1, i)->number, c->input(1, i)->smallest.user_key().ToString(true).c_str(), c->input(1, i)->largest.user_key().ToString(true).c_str());
  }*/

  //mutex_.Unlock(); //ASHL
  auto end_pick = high_resolution_clock::now();

  Status status;
  std::vector<std::pair<Slice, Slice>>upsert_keys; // vector stores keys that need to be upserted into optane
  if(p_ctx->mig_reason==MIG_REASON_UPSERT){
    migration_keys.clear();
  }

  if (c == nullptr) {
    // Nothing to do
  } else {
    CompactionState* compact = new CompactionState(c);
    if (options_.migration_metric == 2) {
       // TODO: note that migration_keys_prefix equal to key values
      int start_bid = migration_keys_prefix[0] / bucket_sz;
      int end_bid = migration_keys_prefix[migration_keys_prefix.size() - 1] / bucket_sz;
      status = DoCompactionWork(p_ctx, compact, migration_keys, migration_keys_prefix, start_bid, end_bid, upsert_keys);
    } else {
      status = DoCompactionWork(p_ctx, compact, migration_keys, migration_keys_prefix, 0, 0, upsert_keys);
    }
    // mutex_ is still in Lock state after DoCompactionWork
    auto end_compaction = high_resolution_clock::now();
    p_ctx->mig_compaction =  p_ctx->mig_compaction + duration_cast<nanoseconds>(end_compaction - end_pick).count();
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(p_ctx, compact);
    c->ReleaseInputs();
    if (p_ctx->mig_reason != MIG_REASON_UPSERT){
      RemoveObsoleteFiles(p_ctx);
    }
  }
  delete c;
  //delete[] smallest_kv.buf;
  //delete[] largest_kv.buf;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }
  mutex_.Unlock();

  //std::this_thread::sleep_for(seconds(1));

  // Step 3: Remove keys from Optane and btree index
  auto begin_remove = high_resolution_clock::now();
  // SM2
  p_ctx->mtx.lock();
  auto begin_remove_lock = high_resolution_clock::now();
  int remove_skipped = 0;
  //fprintf(stderr, "cur_ts = %llu\n", cur_ts);
  // remove entries stored on optane
  for (int i=0; i<migration_keys.size(); i++){
    // skip keys are updated during the migration
    //fprintf(stderr, "migration ts %llu key ts %llu\n", cur_ts, migration_keys[i].ts);

    // if this key's under_mig is false/0
    // meaning that there is an incoming update after migration starts
    // skip removing this key
    if (GetFromMigrationBitmap(p_ctx->pid, migration_keys_prefix[i]) == 0) {
      remove_skipped++;
      continue;
    }

    //fprintf(stderr, "reducing item size %d\n", migration_keys[i].slab->item_size);
    p_ctx->size_in_bytes -= p_ctx->slabContext->slabs[migration_keys[i].slab]->item_size;
    // remove from Optane's freelist
    int remove_success = update_freelist(p_ctx->slabContext, &migration_keys[i]);
    if (remove_success == -1) {
      fprintf(stderr, "DBG: failed removing migration key from optane i=%d\n", i);
      assert(false);
      p_ctx->mtx.unlock();
      return;
    }

    // remove from Optane
    /*int remove_success = remove_item_sync(&migration_keys[i], load_phase_);
    if (remove_success == -1) {
      fprintf(stderr, "failed removing migration key from optane i=%d\n", i);
      assert(false);
      p_ctx->mtx.unlock();
      return;
    }*/

    // remove from the btree
    btree_delete(p_ctx->index, (unsigned char*)&migration_keys_prefix[i], 8, true); // NOTE: hardcoded value 8
    if (!load_phase_){
      //fprintf(stderr, "Freeing index entry i=%d &e=%x e=%x *e=%x\n", i, &e, e, *e);
      //fprintf(stderr, "Migrate key %llu from slab %lu offset %lu to qlc\n", migration_keys_prefix[i], migration_keys[i].slab, migration_keys[i].slab_idx);
    }

    // remove key from pop cache
    // TODO: call this outside of the partition lock
    //if (popCacheSize > 0){
      //p_ctx->pop_cache_ptr->MarkOptaneBit(std::to_string(migration_keys_prefix[i]), false);
    //}
  }
  //FREELIST : sort the freelist of each slab
  //fprintf(stderr, "FREELIST: before sort_all_slab_freelist\n");
  sort_all_slab_freelist(p_ctx->slabContext);
  fprintf(stderr, "FREELIST: after sort_all_slab_freelist\n");

  // DEBUG ONLY: print slabs free list
  //if(p_ctx->migrationId % 100 == 0){
  //  print_freelist(p_ctx->slabContext);
  //}

  p_ctx->mtx.unlock();
  auto end_remove = high_resolution_clock::now();

  /*for (int i=0; i<upsert_keys.size(); i++){
    PutImpl(WriteOptions(), upsert_keys[i].first, upsert_keys[i].second, true);
    delete up
    // TODO: do i need to free any memory here?
  }
  auto end_put = high_resolution_clock::now();
  fprintf(stderr, "%X\tUPSERT: tid %d optane keys inserted %llu time %llu", std::this_thread::get_id(), p_ctx->pid, upsert_keys.size(), duration_cast<nanoseconds>(end_remove - end_put).count());
  */

  p_ctx->migrationId++;
  p_ctx->num_mig_keys += migration_keys.size();
  p_ctx->mig_select = p_ctx->mig_select + duration_cast<nanoseconds>(end_selection - begin).count();
  p_ctx->mig_pick = p_ctx->mig_pick + duration_cast<nanoseconds>(end_pick - end_selection).count();
  p_ctx->mig_pick_lock = p_ctx->mig_pick_lock + duration_cast<nanoseconds>(end_qlc_lock - begin_qlc_lock).count();
  p_ctx->mig_remove = p_ctx->mig_remove + duration_cast<nanoseconds>(end_remove - begin_remove).count();
  p_ctx->mig_remove_lock = p_ctx->mig_remove_lock + duration_cast<nanoseconds>(begin_remove_lock - begin_remove).count();

  if (options_.migration_logging) {
    fprintf(stderr, "DBG: %X\tBackground compaction end partition %llu, skipped %d keys during removal\n", std::this_thread::get_id(), p_ctx->pid, remove_skipped);
  }
}

void DBImpl::CleanupCompaction(PartitionContext* p_ctx, CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    p_ctx->pending_outputs.erase(out.number);
    uint64_t min = decode_size64((unsigned char*)out.smallest.user_key().data());
    uint64_t max = decode_size64((unsigned char*)out.largest.user_key().data());
    p_ctx->files[out.number] = std::make_pair(min, max);
    p_ctx->file_entries[out.number] = out.file_size;
    //fprintf(stderr, "%X\t erasing file from pending_outputs id %d\n", std::this_thread::get_id(), out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(PartitionContext* p_ctx, CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    using namespace std::chrono;
    auto start = high_resolution_clock::now();
    mutex_.Lock(); //ASHL
    auto end = high_resolution_clock::now();
    file_number = versions_->NewFileNumber();
    //pending_outputs_.insert(file_number);
    p_ctx->pending_outputs.insert(file_number);
    //p_ctx->files[file_number] = 1;
    //fprintf(stderr, "%X\t added new file to pending_outputs id %d\n", std::this_thread::get_id(), file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
    p_ctx->mig_compaction_lock += duration_cast<nanoseconds>(end-start).count();
  }

  //fprintf(stderr, "%X\t new compaction file id %d\n", std::this_thread::get_id(), file_number);

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  fprintf(stderr, "%X\t FinishCompactionOutputFile FILE %llu SMALLEST KEY %s encode %s\n", std::this_thread::get_id(), output_number, compact->current_output()->smallest.user_key().ToString(true).c_str(), compact->current_output()->smallest.Encode().ToString(true).c_str());
  fprintf(stderr, "%X\t FinishCompactionOutputFile FILE %llu LARGEST KEY %s encode %s\n", std::this_thread::get_id(), output_number, compact->current_output()->largest.user_key().ToString(true).c_str(), compact->current_output()->largest.Encode().ToString(true).c_str());

  // Check for iterator errors
  Status s = input->status();
  //fprintf(stderr, "%X\tFinishCompactionOutputFile 1 status %s\n", std::this_thread::get_id(), s.ToString().c_str());
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
    //fprintf(stderr, "%X\tFinishCompactionOutputFile 2 status %s\n", std::this_thread::get_id(), s.ToString().c_str());
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  //fprintf(stderr, "%X\tFinished compaction file %llu range (%s - %s) num_entries %d size_bytes %d\n", std::this_thread::get_id(), output_number, compact->current_output()->smallest.user_key().ToString(true).c_str(), compact->current_output()->largest.user_key().ToString(true).c_str(), current_entries, current_bytes);
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
    //fprintf(stderr, "%X\tFinishCompactionOutputFile 3 status %s\n", std::this_thread::get_id(), s.ToString().c_str());
  }
  if (s.ok()) {
    s = compact->outfile->Close();
    //fprintf(stderr, "%X\tFinishCompactionOutputFile 4 status %s\n", std::this_thread::get_id(), s.ToString().c_str());
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status();
    //fprintf(stderr, "%X\tFinishCompactionOutputFile 5 status %s\n", std::this_thread::get_id(), s.ToString().c_str());
    delete iter;
    //fprintf(stderr, "%X\tFinishCompactionOutputFile file %llu level %d num_keys %lld bytes %lld\n", (unsigned long long)output_number, compact->compaction->level(), (unsigned long long)current_entries, (unsigned long long)current_bytes);
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  //mutex_.Unlock(); //ASHL
  return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  //fprintf(stderr, "%X\tCompaction edit size %llu\n", std::this_thread::get_id(), compact->outputs.size());
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
    //fprintf(stderr, "%X\tCompaction edit file %llu\n", std::this_thread::get_id(), out.number);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

void DBImpl::SetCorrectBucketTotalKeys() {
  if (options_.migration_metric == 2) {
    fprintf(stderr, "DEBUG: setcorrectbucketotalkeys called\n");
    for (int pid = 0; pid < numPartitions; pid++) {
      size_t partition_size = numKeys / numPartitions;

      int start_bid = (pid * partition_size) / bucket_sz;
      int end_bid = ((pid + 1) * partition_size) / bucket_sz;
      for (int bid = start_bid; bid < end_bid; bid++) {
        uint64_t min_key = bid * bucket_sz;
        uint64_t max_key = (bid + 1) * bucket_sz;
        PartitionContext* p_ctx = &partitions[pid];
        uint64_t total_keys = btree_find_between_total_count(p_ctx->index, min_key, max_key);
        BucketList[bid].num_total_keys = total_keys;
        fprintf(stderr, "DEBUG: partition %d bucket %d has total keys %llu\n", pid, bid, BucketList[bid].num_total_keys);
      }
    }
  }
}


void DBImpl::UpdateBucketNumKeys(std::map<uint64_t, std::pair<uint64_t, uint64_t>>& updated_bucket_info) {
  for (auto it = updated_bucket_info.begin(); it != updated_bucket_info.end(); ++it) {
    int bid = it->first;
    // updated_keys = (# of keys to remove from num_total_keys, # of popular keys in the overlapped region)
    std::pair<uint64_t, uint64_t> updated_keys= it->second;
    if (BucketList[bid].num_total_keys < updated_keys.first) {
      fprintf(stderr, "ERROR: bucket %d has %llu total key, < num_mig_keys %llu\n", bid, BucketList[bid].num_total_keys, updated_keys.first);
      BucketList[bid].num_total_keys = 0;
      //abort();
    } else {
      BucketList[bid].num_total_keys -= updated_keys.first;
    }  
    //fprintf(stderr, "DEBUG: total_mig_key is %llu,  bucket %d now has %llu num_total_keys, %llu num_pop_keys, %.5f overlap ratio \n", updated_keys.first, bid, BucketList[bid].num_total_keys, BucketList[bid].num_pop_keys, BucketList[bid].overlap_ratio);
    // TODO: update pop keys
    //BucketList[bid].num_pop_keys = data.second;
  }
}
void DBImpl::UpdateBucketOverlapRatios(float new_s, int start_bid, int end_bid) {
  // update overlap ratio for buckets whose ids are from [start_bid, end_bid]
  for (int i = start_bid; i <= end_bid; i++) {
    float old_s = BucketList[i].overlap_ratio;
    float alpha = 0.5;
    BucketList[i].overlap_ratio = alpha * old_s + (1.0 - alpha) * new_s;
    //fprintf(stderr, "new_s is %.3f, s becomes %.3f\n", new_s, BucketList[i].overlap_ratio);
  }
}

Status DBImpl::DoCompactionWork(PartitionContext* p_ctx, CompactionState* compact,
                                std::vector<index_entry>& migration_keys, std::vector<uint64_t>& migration_keys_prefix, int start_bid, int end_bid, std::vector<std::pair<Slice, Slice>>& upsert_keys) {
  //fprintf(stderr, "DoCompactionWork\n");
  int num_overlapped_sst_files = compact->compaction->num_input_files(1);
  fprintf(stderr, "DBG: %X compaction has %d overlapped sst files\n", std::this_thread::get_id(), num_overlapped_sst_files);
  for (int i = 0; i < num_overlapped_sst_files; i++) {
    FileMetaData* meta = compact->compaction->input(1, i);
    fprintf(stderr, "DBG: %X compaction chooses sst file %llu, has %llu bytes\n", std::this_thread::get_id(), meta->number, meta->file_size);
  }
  using namespace std::chrono;
  uint32_t total_mig_keys_read=0;
  uint32_t total_sst_keys_read=0;
  uint32_t total_keys_written=0;
  uint32_t total_same_keys=0;
  uint32_t total_mig_keys_deleted = 0;
  uint64_t total_upserted_keys=0;
  const uint64_t start_micros = env_->NowMicros();

  //Log(options_.info_log, "Compacting %d@%d + %d@%d files",
  //    compact->compaction->num_input_files(0), compact->compaction->level(),
  //    compact->compaction->num_input_files(1),
  //    compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);

  // ASH: not sure what this does
  //auto first_lock = high_resolution_clock::now();
  //mutex_.Lock(); //ASHL
  //auto first_unlock = high_resolution_clock::now();

  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
    //fprintf(stderr, "%X\t compact snapshot-1 %d\n", std::this_thread::get_id(), compact->smallest_snapshot);
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
    //fprintf(stderr, "%X\t compact snapshot-2 %d\n", std::this_thread::get_id(), compact->smallest_snapshot);
  }
  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock(); //ASHL

  // HACK: figure out a good max sst file size
  uint64_t max_fsize = compact->compaction->MaxOutputFileSize();
  /*if ((is_twitter_ == true) && (p_ctx->num_warmup_migrations < 1)){
    if (compact->compaction->num_input_files(1) != 0){
      uint64_t input_fsize = p_ctx->file_entries[(compact->compaction->input(1, 0))->number];
      if (input_fsize > 0.75*compact->compaction->MaxOutputFileSize()){
        max_fsize = (compact->compaction->MaxOutputFileSize())/2;
      }
      fprintf(stderr, "TWITTER: DoCompactionWork input_fsize %llu, max_fsize %llu new_max_fsize %llu\n", input_fsize, compact->compaction->MaxOutputFileSize(), max_fsize);
    }
  }*/

  input->SeekToFirst();
  Status status;

  //ParsedInternalKey ikey;
  //std::string current_user_key;
  //bool has_current_user_key = false;
  //SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  // PRISMDB
  kv_pair kv;
  //kv.buf = new char[(2<<15)]; // NOTE: assumption is that KV pair size < 64K
  char kv_buf [(2<<15)] = " ";
  kv.buf = kv_buf;

  auto begin_while = high_resolution_clock::now();
  uint64_t write_qlc_time = 0;
  uint64_t finish_compaction_time = 0;
  // auto st = high_resolution_clock::now();
  uint32_t mig_key_idx=0;
  int prev_mig_key_idx = -1; // used to cache results of previous optane read

  while ((input->Valid() || mig_key_idx<migration_keys.size()) && !shutting_down_.load(std::memory_order_acquire)) {
    if (!load_phase_){
      //fprintf(stderr, "input valid %d mig_key_idx %d migration_keys_size %d\n", input->Valid(), mig_key_idx, migration_keys.size());
    }
    Slice key;
    InternalKey iter_key;
    Slice mig_key;
    ParsedInternalKey p_mkey, p_ikey;

    bool drop = false;
    bool is_mig_key=false;
    bool move_iter = false;

    // Debug variables
    high_resolution_clock::time_point begin_optane;
    high_resolution_clock::time_point end_optane;
    uint64_t qlc_iter_next_time = 0;
    //auto s0 = high_resolution_clock::now();

    if (input->Valid()){
      //auto s1 = high_resolution_clock::now();
      if (iter_key.DecodeFrom(input->key()) == false) {
        // reads the internal key slice string into internal key object
        fprintf(stderr, "ERROR: Unexpected-0\n");
      }
      //fprintf(stderr, "%X\ts1 %llu\n", std::this_thread::get_id(), duration_cast<nanoseconds>(high_resolution_clock::now() - s1).count());

      if (mig_key_idx < migration_keys.size()) {
        //mig_key = migration_keys[mig_key_idx].Encode();
	if (prev_mig_key_idx != mig_key_idx){
          //begin_optane = high_resolution_clock::now();
          read_item_key_val(p_ctx->slabContext, &migration_keys[mig_key_idx], &kv);
          // HACK: overwrite key with migration_keys_prefix[i]
        
          (void) encode_size64(kv.buf, migration_keys_prefix[mig_key_idx]); 
          //end_optane = high_resolution_clock::now();
          prev_mig_key_idx = mig_key_idx;
        }
	//fprintf(stderr, "optane_read_time-1 %llu\n", duration_cast<nanoseconds>(end_optane - begin_optane).count());
        //p_ctx->mig_compaction_read_optane += duration_cast<nanoseconds>(end_optane - begin_optane).count();
        auto mig_user_key = Slice((const char*)kv.buf, kv.key_size); //kv.buf

	//auto s2 = high_resolution_clock::now();
        int comp = user_comparator()->Compare(mig_user_key, iter_key.user_key());
	//fprintf(stderr, "%X\ts2 %llu\n", std::this_thread::get_id(), duration_cast<nanoseconds>(high_resolution_clock::now() - s2).count());

        //fprintf(stderr, "comp %d mig user key %s size is %llu , iter user key %s size is %llu\n", comp, mig_user_key.ToString(true).c_str(), mig_user_key.size(), iter_key.user_key().ToString(true).c_str(), iter_key.user_key().size());

        /*if (ParseInternalKey(mig_key, &p_mkey) == false){
          fprintf(stderr, "PRISMDB: Unexpected-1\n");
          assert(false);
        }
        if (ParseInternalKey(input->key(), &p_ikey) == false){
          fprintf(stderr, "PRISMDB: Unexpected-2\n");
	  fprintf(stderr, "input iter key %s\n", input->key().ToString(true).c_str());
          assert(false);
        }*/

        //fprintf(stderr, "comparison %d mig key %s sn %lu type %d iter key %s sn %lu type %d\n", comp, p_mkey.user_key.ToString(true).c_str(), p_mkey.sequence, p_mkey.type, p_ikey.user_key.ToString(true).c_str(), p_ikey.sequence, p_ikey.type);

        if (comp <= 0){
          // mig user key is smaller than iter key
          if (kv.key_size != -1){
            (void)EncodeFixed64(&kv.buf[kv.key_size], ((0<<8) | kTypeValue));
            key = Slice(kv.buf, kv.key_size+8);
            is_mig_key = true;

            // move the iterator forward if mig and iter user keys are same
            if (comp == 0){
              //auto s = high_resolution_clock::now();
              input->Next();
              total_sst_keys_read++;
              total_same_keys++;
              //qlc_iter_next_time = duration_cast<nanoseconds>(high_resolution_clock::now() - s).count();
              //p_ctx->mig_compaction_read_qlc = p_ctx->mig_compaction_read_qlc + qlc_iter_next_time;
              //fprintf(stderr, "picked mig key, iter and mig user keys same!\n");
            }
            //else{
              //fprintf(stderr, "picked mig key\n");
            //}
          } else {
            // migration key is of type kTypeDeletion
            mig_key_idx++;
            if (comp == 0) {
              // since iter key is same, delete that as well
              //auto s = high_resolution_clock::now();
              input->Next();
	      total_sst_keys_read++;
	      total_same_keys++;
              //qlc_iter_next_time = duration_cast<nanoseconds>(high_resolution_clock::now() - s).count();
              //p_ctx->mig_compaction_read_qlc = p_ctx->mig_compaction_read_qlc + qlc_iter_next_time;
            }
            total_mig_keys_deleted++;
            fprintf(stderr, "DBG: dropped mig key\n");
            continue;
          }
        } else { // qlc iterator key smaller than optane mig key
          if((p_ctx->mig_reason == MIG_REASON_UPSERT) && (p_ctx->pop_cache_ptr->IsClockPopular(decode_size64((unsigned char*)(input->key().data())), popThreshold))){
	  //if ((p_ctx->pop_cache_ptr->IsClockPopular(decode_size64((unsigned char*)(input->key().data())), popThreshold))){
            Status upsert_s = PutImpl(WriteOptions(), iter_key.user_key(), input->value(), true);

            //upsert_list.push_back(std::make_pair(Slice(input->key()), Slice(input->value())));
	    //fprintf(stderr, "%X\tUPSERT: partition %d upserted (A) key to optane keysize %d value size %d\n", std::this_thread::get_id(), p_ctx->pid, iter_key.user_key().size(), input->value().size());
            if (upsert_s.ok()) {
              total_upserted_keys++;
            }
	    // TODO: comment till continue to write upserted keys to qlc
            //auto s = high_resolution_clock::now();
            input->Next();
	    total_sst_keys_read++;
            //qlc_iter_next_time = duration_cast<nanoseconds>(high_resolution_clock::now() - s).count();
            //p_ctx->mig_compaction_read_qlc = p_ctx->mig_compaction_read_qlc + qlc_iter_next_time;
	    continue;
	    // TODO: comment next two lines to avoid writing upserted keys to qlc
	    //key = iter_key.Encode();
            //move_iter = true;
	  }
	  else {
            //auto s3 = high_resolution_clock::now();
            // iter user key is smaller than mig key
            key = iter_key.Encode();
            //fprintf(stderr, "%X\ts3 %llu\n", std::this_thread::get_id(), duration_cast<nanoseconds>(high_resolution_clock::now() - s3).count());
            move_iter = true;
            //fprintf(stderr, "picked iter key\n");
          }
        }
      } else { // no more optane mig keys, only qlc iterator keys left to migrate
        if((p_ctx->mig_reason == MIG_REASON_UPSERT) && (p_ctx->pop_cache_ptr->IsClockPopular(decode_size64((unsigned char*)(input->key().data())), popThreshold))){
	//if ((p_ctx->pop_cache_ptr->IsClockPopular(decode_size64((unsigned char*)(input->key().data())), popThreshold))){
            Status upsert_s = PutImpl(WriteOptions(), iter_key.user_key(), input->value(), true);
            //upsert_list.push_back(std::make_pair(Slice(input->key()), Slice(input->value())));
	    //fprintf(stderr, "%X\tUPSERT: partition %d upserted (B) key %llu to optane keysize %d value size %d\n", std::this_thread::get_id(), p_ctx->pid, decode_size64((unsigned char*)(input->key().data())), iter_key.user_key().size(), input->value().size());
            if (upsert_s.ok()) {
              total_upserted_keys++;
            }
	    // TODO: comment till continue to skip writing upserted keys to qlc
            //auto s = high_resolution_clock::now();
            input->Next();
	    total_sst_keys_read++;
            //qlc_iter_next_time = duration_cast<nanoseconds>(high_resolution_clock::now() - s).count();
            //p_ctx->mig_compaction_read_qlc = p_ctx->mig_compaction_read_qlc + qlc_iter_next_time;
            continue;
            // TODO: comment next two lines to avoid writing upserted keys to qlc
            //key = iter_key.Encode();
            //move_iter = true;
        }
	else {
          // for upserts, do not create any new SST file
          if(p_ctx->mig_reason == MIG_REASON_UPSERT){
            //auto s = high_resolution_clock::now();
            input->Next();
            total_sst_keys_read++;
            //qlc_iter_next_time = duration_cast<nanoseconds>(high_resolution_clock::now() - s).count();
            //p_ctx->mig_compaction_read_qlc = p_ctx->mig_compaction_read_qlc + qlc_iter_next_time;
            continue;
          }
          // only iter is valid
          key = iter_key.Encode();
          move_iter = true;
          //fprintf(stderr, "picked iter key (mig key not valid)\n");
	}
      }
    }
    else{ // iterator not valid, migrate optane mig key
      //begin_optane = high_resolution_clock::now();
      read_item_key_val(p_ctx->slabContext, &migration_keys[mig_key_idx], &kv);
      // HACK: overwrite key with migration_keys_prefix[i]
      (void) encode_size64(kv.buf, migration_keys_prefix[mig_key_idx]); 
      //end_optane = high_resolution_clock::now();
      //fprintf(stderr, "optane_read_time-2 %llu\n", duration_cast<nanoseconds>(end_optane - begin_optane).count());
      //p_ctx->mig_compaction_read_optane += duration_cast<nanoseconds>(end_optane - begin_optane).count();
      if (kv.key_size != -1){
        //auto s11 = high_resolution_clock::now();
        (void)EncodeFixed64(&kv.buf[kv.key_size], ((0<<8) | kTypeValue));
        key = Slice(kv.buf, kv.key_size+8);
        //fprintf(stderr, "KV BUF KEY %s\n", key.ToString(true).c_str());
        is_mig_key = true;
        //fprintf(stderr, "picked mig key (iter not valid) key slice is %s key int is %llu\n", key.ToString(true).c_str(),  decode_size64((unsigned char *)kv.buf));
        //fprintf(stderr, "s11 %llu\n", duration_cast<nanoseconds>(high_resolution_clock::now() - s11).count());
      }
      else {
        mig_key_idx++;
        total_mig_keys_deleted++;
        fprintf(stderr, "DBG: dropped mig key %llu (kv.key_size == -1)\n", decode_size64((unsigned char*)(key.data())));
        continue;
      }
    }
   
/*  
    if(p_ctx->migrationId > 15){ 
      if (is_mig_key){
        fprintf(stderr, "%X\tOPTANE single_loop_time %llu optane_read_time %llu qlc_iter_next_time %llu\n", std::this_thread::get_id(), duration_cast<nanoseconds>(high_resolution_clock::now() - s0).count(), duration_cast<nanoseconds>(end_optane - begin_optane).count(), qlc_iter_next_time);
      }
      else{
        fprintf(stderr, "%X\tQLC single_loop_time %llu qlc_iter_next_time %llu\n", std::this_thread::get_id(), duration_cast<nanoseconds>(high_resolution_clock::now() - s0).count(), qlc_iter_next_time);
      }
    }
*/

#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    // Open output file if necessary
    if (compact->builder == nullptr) {
      //auto st1 = high_resolution_clock::now();
      status = OpenCompactionOutputFile(p_ctx, compact);
      if (!status.ok()) {
        //fprintf(stderr, "%X\tCompaction edit 1 status %s\n", std::this_thread::get_id(), status.ToString().c_str());
        break;
      }
      //fprintf(stderr, "%X\tPROFILING: OpenCompactionOutputFile %llu\n", std::this_thread::get_id(), duration_cast<nanoseconds>(high_resolution_clock::now() - st1).count());
    }

    //fprintf(stderr, "%X\tBEFORE ADD key %s\n", std::this_thread::get_id(), key.ToString(true).c_str());
    if (compact->builder->NumEntries() == 0) {
      compact->current_output()->smallest.DecodeFrom(key);
      //fprintf(stderr, "%X\t SMALLEST KEY %s encode %s\n", std::this_thread::get_id(), compact->current_output()->smallest.user_key().ToString(true).c_str(), compact->current_output()->smallest.Encode().ToString(true).c_str());
    }
    compact->current_output()->largest.DecodeFrom(key);
    if (is_mig_key){
      //fprintf(stderr, "Before adding value size %d\n", kv.val_size);
      //auto s = high_resolution_clock::now();
      compact->builder->Add(key, Slice((const char*)&kv.buf[kv.key_size], kv.val_size));
      //fprintf(stderr, "adding to compactor optane key %llu with value size %lu\n", decode_size64((unsigned char *)kv.buf), kv.val_size);
      //write_qlc_time += duration_cast<nanoseconds>(high_resolution_clock::now() - s).count();
      //p_ctx->mig_compaction_write_qlc = p_ctx->mig_compaction_write_qlc + duration_cast<nanoseconds>(high_resolution_clock::now() - s).count();
      //fprintf(stderr, "add_optane_key %llu\n", duration_cast<nanoseconds>(high_resolution_clock::now() - s).count());
      mig_key_idx++;
      total_mig_keys_read++;
      total_keys_written++;
    } else {
      //fprintf(stderr, "adding to compactor qlc key %s with value size %lu\n", iter_key.user_key().ToString(true).c_str(), input->value().size());
      //auto s = high_resolution_clock::now();
      compact->builder->Add(key, input->value());
      //write_qlc_time += duration_cast<nanoseconds>(high_resolution_clock::now() - s).count();
      //p_ctx->mig_compaction_write_qlc = p_ctx->mig_compaction_write_qlc + duration_cast<nanoseconds>(high_resolution_clock::now() - s).count();
      //fprintf(stderr, "add_qlc_key %llu\n", duration_cast<nanoseconds>(high_resolution_clock::now() - s).count());
      //total_sst_keys_read++;
      total_keys_written++;
    }

    // Close output file if it is big enough
    //if (compact->builder->FileSize() >= compact->compaction->MaxOutputFileSize()) {
    //  fprintf(stderr, "DBG: %X DoCompactionWork builder size %llu maxfilesize %llu\n", std::this_thread::get_id(), compact->builder->FileSize(), compact->compaction->MaxOutputFileSize());
    if (compact->builder->FileSize() >= max_fsize) {
      fprintf(stderr, "DBG: %X DoCompactionWork builder size %llu maxfilesize %llu\n", std::this_thread::get_id(), compact->builder->FileSize(), max_fsize);
      auto s = high_resolution_clock::now();
      status = FinishCompactionOutputFile(compact, input);
      auto write_time = duration_cast<nanoseconds>(high_resolution_clock::now() - s).count();
      finish_compaction_time += write_time;
      //fprintf(stderr, "%X\twrite_qlc_file_time %llu\n", std::this_thread::get_id(), write_time);
      //write_qlc_time += duration_cast<nanoseconds>(high_resolution_clock::now() - s).count();
      p_ctx->mig_compaction_write_qlc += duration_cast<nanoseconds>(high_resolution_clock::now() - s).count();
      if (!status.ok()) {
        //fprintf(stderr, "%X\tCompaction edit 2 status %s\n", std::this_thread::get_id(), status.ToString().c_str());
        break;
      }
    }

    if (move_iter){
      //fprintf(stderr, "moving iter Next()\n");
      //auto s = high_resolution_clock::now();
      input->Next();
      total_sst_keys_read++;
      //auto qlc_iter_next_time = duration_cast<nanoseconds>(high_resolution_clock::now() - s).count();
      //p_ctx->mig_compaction_read_qlc = p_ctx->mig_compaction_read_qlc + qlc_iter_next_time;
      //fprintf(stderr, "%X\tAFTER qlc_iter_next_time %llu\n", std::this_thread::get_id(), qlc_iter_next_time);
    }
  }

  //delete[] kv.buf;
  auto end_while = high_resolution_clock::now();
  float while_duration = duration_cast<nanoseconds>(end_while - begin_while).count();
  if (options_.migration_logging) {
    fprintf(stderr, "DBG: %X\t while loop in compaction takes %.f ms mig_keys_read %llu sst_keys_read %llu deleted_mig_keys %llu same_keys %llu keys_written_to_qlc %llu upserted_keys %llu\n", std::this_thread::get_id(), (while_duration/1000000), total_mig_keys_read, total_sst_keys_read, total_mig_keys_deleted, total_same_keys, total_keys_written, total_upserted_keys);
    fprintf(stderr, "%X\tmigration partition %llu optane usage curr %llu max %llu\n", std::this_thread::get_id(), p_ctx->pid, p_ctx->size_in_bytes, p_ctx->max_optane_usage);
  }

  // Update overlap ratio for each bucket
  if (options_.migration_metric == 2) {
    if (total_sst_keys_read > 0 && (!load_phase_)) {
      float new_s = (float) total_same_keys / total_sst_keys_read;
      //fprintf(stderr, "DEBUG: new_s is %.3f\n", new_s);
      UpdateBucketOverlapRatios(new_s, start_bid, end_bid);
    }
  }


  //fprintf(stderr, "%X\tPROFILING: DoCompactionWork while loop %llu\n", std::this_thread::get_id(), duration_cast<nanoseconds>(high_resolution_clock::now() - st).count());

  //fprintf(stderr, "%X\tCompaction edit 3 status %s\n", std::this_thread::get_id(), status.ToString().c_str());
  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }
  //fprintf(stderr, "%X\tCompaction edit 4 status %s\n", std::this_thread::get_id(), status.ToString().c_str());
  //auto st2 = high_resolution_clock::now();
  if (status.ok() && compact->builder != nullptr) {
    auto s = high_resolution_clock::now();
    status = FinishCompactionOutputFile(compact, input);
    finish_compaction_time += duration_cast<nanoseconds>(high_resolution_clock::now() - s).count();
    //write_qlc_time += duration_cast<nanoseconds>(high_resolution_clock::now() - s).count();
    p_ctx->mig_compaction_write_qlc = p_ctx->mig_compaction_write_qlc + duration_cast<nanoseconds>(high_resolution_clock::now() - s).count();
  }
  //fprintf(stderr, "%X\twrite_qlc_time %llu\n", std::this_thread::get_id(), write_qlc_time);
  //fprintf(stderr, "%X\tfinish_comp_time %llu\n", std::this_thread::get_id(), finish_compaction_time);
  //fprintf(stderr, "%X\tPROFILING: FinishCompactionOutputFile %llu\n", std::this_thread::get_id(), duration_cast<nanoseconds>(high_resolution_clock::now() - st2).count());
  //fprintf(stderr, "%X\tCompaction edit 5 status %s\n", std::this_thread::get_id(), status.ToString().c_str());
  if (status.ok()) {
    status = input->status();
  }
  //fprintf(stderr, "%X\tCompaction edit 6 status %s\n", std::this_thread::get_id(), status.ToString().c_str());
  delete input;
  input = nullptr;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  //PRISMDB
  for (int which = 1; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  auto second_lock = high_resolution_clock::now();
  mutex_.Lock(); // ASHL
  auto second_unlock = high_resolution_clock::now();

  stats_[compact->compaction->level() + 1].Add(stats);

  //st = high_resolution_clock::now();
  if (p_ctx->mig_reason != MIG_REASON_UPSERT){
    if (status.ok()) {
      status = InstallCompactionResults(compact);
      //if (!load_phase_) {
      //  fprintf(stderr, "installcompactionresults done, current sequenceNum is %llu\n", versions_->LastSequence());
      //}
    }
  }
  //fprintf(stderr, "%X\tPROFILING: InstallCompactionResults %llu\n", std::this_thread::get_id(), duration_cast<nanoseconds>(high_resolution_clock::now() - st).count());
  //else {
  //  fprintf(stderr, "%X\tCompaction edit 7 non-ok status %s\n", std::this_thread::get_id(), status.ToString().c_str());
  //}
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  //fprintf(stderr, "%X\t migration done total_files_in_lsm %s\n", std::this_thread::get_id(), versions_->LevelSummary(&tmp));
  //PrintSstFiles(false);

  //float lock1 = duration_cast<nanoseconds>(first_unlock - first_lock).count();
  float lock2 = duration_cast<nanoseconds>(second_unlock - second_lock).count();
  p_ctx->mig_compaction_lock += lock2;
  //fprintf(stderr, "COMPACTION STATS opt %llu ns rqlc %llu ns wqlc %llu ns num_migs %llu\n", p_ctx->mig_compaction_read_optane, p_ctx->mig_compaction_read_qlc, p_ctx->mig_compaction_write_qlc, p_ctx->migrationId);

  return status;
}

// PRISMDB
// This function returns a vector of all SST file Metadata pointers
std::vector<FileMetaData*> DBImpl::GetSSTFileMetaData(){
  mutex_.AssertHeld();
  Version* current = versions_->current();
  // reference counting should not be needed
  //current->Ref();
  std::vector<FileMetaData*> files = current->GetLevelFiles(1);
  //fprintf(stderr, "GetSSTFileMetaData num_files_L0 %d num_files_L1 %d\n", current->GetLevelFiles(0).size(), files.size());
  //current->Unref();
  return files;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

// ASH: may need to implement the rocksdb optimization mentioned in this blog:
// https://rocksdb.org/blog/2014/06/27/avoid-expensive-locks-in-get.html
Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  using namespace std::chrono;
  auto begin = high_resolution_clock::now();

  // Step 1: Get partition for the key
  Status s;
  uint64_t k = decode_size64((unsigned char*)key.data());
  int p = DBImpl::getPartition(k);
  //fprintf(stderr, "key %d, partition %d\n", k, p);

  // Step 2: Check if key exists in Optane index
  auto begin_lock = high_resolution_clock::now();
  partitions[p].mtx.lock();
  auto end_lock = high_resolution_clock::now();
  index_entry e;
  int in_optane = btree_find(partitions[p].index, (unsigned char*) key.data(), key.size(), &e);
  auto end_find_index = high_resolution_clock::now();

  // Step 3.1: Read data from Optane
  if (in_optane) {
    //fprintf(stderr, "%X\tGET optane found key %s int_key %llu\n", std::this_thread::get_id(), key.ToString(true).c_str(), k);
    size_t item_size = e.item_size;

    //char val_char[item_size]; // val_char has a slightly larger capacity
    //bool key_was_valid = read_item_val_new(&e, val_char); // read value into val_char
    // dereference string pointer to access the string object
    //fprintf(stderr, "before optane get\n");
    char *val_ptr = &(*value)[0];
    bool key_was_valid = read_item_val(partitions[p].slabContext, &e, val_ptr); // read value into val_char

    // HACK for deleted keys, fix later
    if (!key_was_valid){
      partitions[p].mtx.unlock();
      fprintf(stderr, "Invalid optane key, key size = -1!\n");
      return s.OK();
    }
    //fprintf(stderr, "%X\tGET optane found key %s int_key %llu\n", std::this_thread::get_id(), key.ToString(true).c_str(), k);

    // Step 4.1: Update timing info and release the partition lock
    auto end_optane = high_resolution_clock::now();

    partitions[p].num_optane_gets ++;
    partitions[p].get_optane_time = partitions[p].get_optane_time + duration_cast<nanoseconds>(end_optane - begin).count();
    partitions[p].get_acquire_optane_lock = partitions[p].get_acquire_optane_lock + duration_cast<nanoseconds>(end_lock - begin_lock).count();
    partitions[p].get_find_optane_index = partitions[p].get_find_optane_index + duration_cast<nanoseconds>(end_find_index - end_lock).count();
    partitions[p].get_read_optane = partitions[p].get_read_optane + duration_cast<nanoseconds>(end_optane - end_find_index).count();

    if (options_.migration_metric == 2 && !(load_phase_)) {
      // Update bucket info for approximated migration metric
      int bucket_idx = k / bucket_sz;
      // COMMENT: no need to acquire bucket's atomic_flag because foreground Get() and background migration are under
      // the same partition lock
      if (BucketList[bucket_idx].num_pop_keys < bucket_sz) {
        //fprintf(stderr, "DEBUG: bucket %d, key %llu,  has %llu popular keys\n", bucket_idx, k, BucketList[bucket_idx].num_pop_keys);
        // TODO: filter using clock cache
        int byte_idx = (k - bucket_idx * bucket_sz) / (sizeof(uint8_t)*8); // 8bits in 1 byte
        int bit_idx = (k - bucket_idx * bucket_sz) % (sizeof(uint8_t)*8); // 8bits in 1 byte
        uint8_t bit_mask = (1 << bit_idx);
        //fprintf(stderr, "DEBUG: key %llu in bucket %d has byte index %d and bit index %d bit mask %d, flip bit mask %llu, pop_keys %llu\n", k, bucket_idx, byte_idx, bit_idx, bit_mask, ~bit_mask, BucketList[bucket_idx].num_pop_keys);
        if ((BucketList[bucket_idx].pop_bitmap[byte_idx] & bit_mask) == 0 ) { // if the bit is not set
          //fprintf(stderr, "DEBUG: pop_key increment\n");
          BucketList[bucket_idx].num_pop_keys++;
          BucketList[bucket_idx].pop_bitmap[byte_idx] = (BucketList[bucket_idx].pop_bitmap[byte_idx] | bit_mask);
        }
      }
    }

    if (options_.read_logging) {
      fprintf(stderr, "%X Optane Get %llu us\n", std::this_thread::get_id(), duration_cast<microseconds>(end_optane - end_find_index).count());
    }
    partitions[p].mtx.unlock();

    // for read popularity debugging
    partitions[p].optane_reads++;
    if (partitions[p].optane_reads + partitions[p].qlc_reads == partitions[p].read_ratio_tracking_freq){
      fprintf(stderr, "%X thread %d optane reads %d qlc reads %d\n", std::this_thread::get_id(), partitions[p].pid, (int)((partitions[p].optane_reads*100)/(partitions[p].optane_reads + partitions[p].qlc_reads)), (int)((partitions[p].qlc_reads*100)/(partitions[p].optane_reads + partitions[p].qlc_reads)));
      partitions[p].optane_reads=0;
      partitions[p].qlc_reads=0;
    }

    if (popCacheSize > 0){
      //std::string key_str = std::to_string(k);
      //partitions[p].pop_cache_ptr->Insert(key_str);
      partitions[p].pop_cache_ptr->Insert(k);
      //int pop_cache_val = partitions[p].pop_cache_ptr->Lookup(key_str);
      //fprintf(stderr, "CLOCK: lookup key=%s key=%c%c%c%c%c%c%c%c clock %d\n", key_str.c_str(), key_str[0],key_str[1],key_str[2],key_str[3],key_str[4],key_str[5],key_str[6],key_str[7], pop_cache_val);
    }

    // trigger migration when current size exceeds the pre-set upper bound
    //if (partitions[p].size_in_bytes > (float)(maxDbSizeBytes*optaneThreshold*partitions[p].migration_upper_bound/(float)numPartitions)) {
      //fprintf(stderr, "%X\tGet optane partition %llu optane size %f soft size %f \n", std::this_thread::get_id(), p, (float)(maxDbSizeBytes*optaneThreshold)/(float)numPartitions, (float)((maxDbSizeBytes*optaneThreshold*soft_limit)/(float)numPartitions));
      //MaybeScheduleCompaction(p);
    //}

    return s.OK();
  }
  partitions[p].mtx.unlock();
  //fprintf(stderr, "%X\tGET optane not found key %s int_key %llu\n", std::this_thread::get_id(), key.ToString(true).c_str(), k);

  // Step 3.2: Read data from the LSM on Flash
  auto begin_qlc_lock = high_resolution_clock::now();
  MutexLock l(&mutex_);
  auto end_qlc_lock = high_resolution_clock::now();
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  Version* current = versions_->current();
  current->Ref();
  //mutex_.Unlock();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  auto begin_qlc = high_resolution_clock::now();
  auto end_qlc = high_resolution_clock::now();
  {
    mutex_.Unlock();
    LookupKey lkey(key, snapshot);
    s = current->Get(options, lkey, value, &stats);
    end_qlc = high_resolution_clock::now();
    if (options_.read_logging) {
      fprintf(stderr, "%X QLC Get %llu us\n", std::this_thread::get_id(), duration_cast<microseconds>(end_qlc - begin_qlc).count());
    }
    mutex_.Lock();
  }
  auto begin_qlc_unref_lock = high_resolution_clock::now();
  //mutex_.Lock();
  //auto end_qlc = high_resolution_clock::now();
  current->Unref();
  //mutex_.Unlock();
  //auto end_qlc_unref_lock = high_resolution_clock::now();

  if (!s.ok()){
    //fprintf(stderr, "%X\tGET qlc NOT found key %s int_key %llu\n", std::this_thread::get_id(), key.ToString(true).c_str(), k);
    //std::abort();
    //int* aptr = nullptr;
    // manual crash
    //std::exit(1);
    //if (*aptr == 5){ *aptr=10;}
    return s.OK(); // maybe YCSB crashes on not ok status, so hack it to return OK
  }
  else {
   //fprintf(stderr, "%X\t GET qlc found key %s int_key %llu\n", std::this_thread::get_id(), key.ToString(true).c_str(), k);
  }

  if (popCacheSize > 0){
    //std::string key_str = std::to_string(k);
    //partitions[p].pop_cache_ptr->Insert(key_str);
    partitions[p].pop_cache_ptr->Insert(k);
    //int pop_cache_val = partitions[p].pop_cache_ptr->Lookup(key_str);
    //fprintf(stderr, "CLOCK: lookup key=%s key=%c%c%c%c%c%c%c%c clock %d\n", key_str.c_str(), key_str[0],key_str[1],key_str[2],key_str[3],key_str[4],key_str[5],key_str[6],key_str[7], pop_cache_val);
  }

  // Step 4.2: Update timing info
  auto end = high_resolution_clock::now();
  partitions[p].num_qlc_gets ++;
  partitions[p].get_qlc_time = partitions[p].get_qlc_time + duration_cast<nanoseconds>(end - begin).count();
  partitions[p].get_acquire_qlc_lock = partitions[p].get_acquire_qlc_lock + duration_cast<nanoseconds>(end_qlc_lock - begin_qlc_lock).count() + duration_cast<nanoseconds>(begin_qlc_unref_lock-end_qlc).count();
  partitions[p].get_read_qlc = partitions[p].get_read_qlc + duration_cast<nanoseconds>(end_qlc - begin_qlc).count();
  partitions[p].qlc_reads++;

  // trigger migration when current size exceeds the pre-set upper bound
  if (partitions[p].size_in_bytes > (float)(maxDbSizeBytes*optaneThreshold*partitions[p].migration_upper_bound/(float)numPartitions)) {
    MaybeScheduleCompaction(p);
  } else {
    CheckAndTriggerUpserts(&partitions[p]);
  }

  // for read popularity debugging
  if (partitions[p].optane_reads + partitions[p].qlc_reads == partitions[p].read_ratio_tracking_freq){
    fprintf(stderr, "%X thread %d optane reads %d qlc reads %d\n", std::this_thread::get_id(), partitions[p].pid, (int)((partitions[p].optane_reads*100)/(partitions[p].optane_reads + partitions[p].qlc_reads)), (int)((partitions[p].qlc_reads*100)/(partitions[p].optane_reads + partitions[p].qlc_reads)));
    partitions[p].optane_reads=0;
    partitions[p].qlc_reads=0;
  }

  return s;
}

void DBImpl::CheckAndTriggerUpserts(PartitionContext* p_ctx){

  if (p_ctx->num_optane_gets+p_ctx->num_qlc_gets > p_ctx->stop_upsert_trigger){
    return;
  }

  // wait for clock to warmup
  if (!(p_ctx->pop_cache_ptr->AreClockValuesNonZero())){
    return;
  }

  // warmup
  /*if (p_ctx->num_optane_gets+p_ctx->num_qlc_gets+(p_ctx->num_puts-p_ctx->num_upsert_puts) < p_ctx->clock_warmup_ops){
    return;
  }*/

  // check if we should trigger upserts
  if ((p_ctx->optane_reads + p_ctx->qlc_reads) < p_ctx->read_ratio_tracking_freq){
    return;
  }

  // check if workload is read dominated, trigger upserts if needed.
  float get_ratio = (float)(p_ctx->num_optane_gets+p_ctx->num_qlc_gets)/(p_ctx->num_optane_gets+p_ctx->num_qlc_gets+(p_ctx->num_puts-p_ctx->num_upsert_puts));
  if (get_ratio < p_ctx->read_dominated_threshold){
    return;
  }

  //fprintf(stderr, "%X\tUPSERT: workload is read dominated %f threshold %f\n", std::this_thread::get_id(), get_ratio, p_ctx->read_dominated_threshold);
  if (p_ctx->upsert_delay > 0){
    fprintf(stderr, "%X\tUPSERT: upsert delay (%llu) is > 0 \n", std::this_thread::get_id(), p_ctx->upsert_delay);
    p_ctx->upsert_delay -= p_ctx->read_ratio_tracking_freq;
    return;
  }

/*  float current_optane_read_percent = (float)(p_ctx->optane_reads*100)/(p_ctx->optane_reads + p_ctx->qlc_reads);
  if (p_ctx->prev_optane_read_percent != 0){
    float current_optane_read_percent = (float)(p_ctx->optane_reads*100)/(p_ctx->optane_reads + p_ctx->qlc_reads);
    float delta = (current_optane_read_percent - p_ctx->prev_optane_read_percent)/p_ctx->prev_optane_read_percent;
    if (delta < p_ctx->read_ratio_improv_threshold){
      fprintf(stderr, "%X\tUPSERT: optane read improvement prev %f curr %f delta %f not enough\n", std::this_thread::get_id(), p_ctx->prev_optane_read_percent, current_optane_read_percent, delta);
      p_ctx->upsert_delay = p_ctx->upsert_delay_threshold;
      p_ctx->prev_optane_read_percent = current_optane_read_percent;
      return;
    }
    p_ctx->prev_optane_read_percent = current_optane_read_percent;
  }
  else{
    p_ctx->prev_optane_read_percent = current_optane_read_percent;
  }
*/

  //fprintf(stderr, "%X\tUPSERT: Finally try to schedule a migration\n", std::this_thread::get_id());
  MaybeScheduleCompaction(p_ctx->pid, MIG_REASON_UPSERT);
}

// TODO: insert to clock cache
Status DBImpl::Scan(const ReadOptions& options, const Slice& start_key,
              const uint64_t scan_size,
              std::vector<std::pair<Slice, Slice>>* results) {
  Status s;
  Iterator* lsm_iter = DBImpl::NewIterator(options);
  lsm_iter->Seek(start_key);

  uint64_t k = decode_size64((unsigned char*)start_key.data());
  int curr_pid = DBImpl::getPartition(k);
  //fprintf(stderr, "DBImpl::Scan() called, start_key is %llu, scan_size is %llu, start_pid %d\n", k, scan_size, curr_pid);

   partitions[curr_pid].mtx.lock();
   btree::btree_map<uint64_t, struct index_entry>* index = static_cast< btree::btree_map<uint64_t, struct index_entry> * >(partitions[curr_pid].index);
   auto btree_iter = index->find_closest(k);
   //auto btree_iter = static_cast< btree::btree_map<uint64_t, struct index_entry> * >(index)->find_closest(k);
   //fprintf(stderr, "btree iterator first element is %llu\n", btree_iter->first);
   //fprintf(stderr, "lsm iterator first element is %llu\n", decode_size64((unsigned char*)lsm_iter->key().data()));

   uint64_t count = 0;
   while (count < scan_size) {
     // if btree iterator has reached the end of current partition's index
     if (btree_iter == index->end()) {
       if (curr_pid == (numPartitions-1)) { // check if it is the end of key space
         if (!lsm_iter->Valid()) { // and lsm iterator is no longer valid
          //fprintf(stderr, "both lsm and btree iter have reached the end\n");
          break;
         }
       } else {
          // release the current partition's lock
          // move to the beginning of next partition's index
          // acquire the next partition's lock
          partitions[curr_pid].mtx.unlock();
          //fprintf(stderr, "p%d btree iterator reaches the end\n", curr_pid);
          curr_pid++;
          index = static_cast< btree::btree_map<uint64_t, struct index_entry> * >(partitions[curr_pid].index);
          partitions[curr_pid].mtx.lock();
          btree_iter = index->begin();
          //fprintf(stderr, "now move p%d btree iterator, first element is %llu\n", curr_pid, btree_iter->first);
       }
     }
      if (lsm_iter->Valid() && btree_iter == index->end()) {
        // if lsm iter is valid and btree iter is invalid
        // add lsm key to results
        //fprintf(stderr, "invalid btree iter, lsm key %llu\n", decode_size64((unsigned char*)lsm_iter->key().data()));
        results->push_back(std::make_pair(lsm_iter->key(), lsm_iter->value()));
        //if (popCacheSize > 0){
        //  uint64_t k = decode_size64((unsigned char*)lsm_iter->key().data());
        //  std::string key_str = std::to_string(k);
        //  partitions[curr_pid].pop_cache_ptr->Insert(key_str);
        //  //int pop_cache_val = partitions[p].pop_cache_ptr->Lookup(key_str);
        //  //fprintf(stderr, "CLOCK: lookup key=%s key=%c%c%c%c%c%c%c%c clock %d\n", key_str.c_str(), key_str[0],key_str[1],key_str[2],key_str[3],key_str[4],key_str[5],key_str[6],key_str[7], pop_cache_val);
        //}

        lsm_iter->Next();
        count++;
      } else if (lsm_iter->Valid() && btree_iter != index->end() && decode_size64((unsigned char*)lsm_iter->key().data()) < btree_iter->first) {
        // if both lsm and btree iter are valid
        // and lsm key is smaller
        // add lsm key to results
        //fprintf(stderr, "lsm key %llu\n", decode_size64((unsigned char*)lsm_iter->key().data()));
        results->push_back(std::make_pair(lsm_iter->key(), lsm_iter->value()));
        //if (popCacheSize > 0){
        //  uint64_t k = decode_size64((unsigned char*)lsm_iter->key().data());
        //  std::string key_str = std::to_string(k);
        //  partitions[curr_pid].pop_cache_ptr->Insert(key_str);
        //  //int pop_cache_val = partitions[p].pop_cache_ptr->Lookup(key_str);
        //  //fprintf(stderr, "CLOCK: lookup key=%s key=%c%c%c%c%c%c%c%c clock %d\n", key_str.c_str(), key_str[0],key_str[1],key_str[2],key_str[3],key_str[4],key_str[5],key_str[6],key_str[7], pop_cache_val);
        //}
        lsm_iter->Next();
        count++;
      } else{
        // in all other conditions, that is
        // 1. invalid lsm iter, valid btree iter OR
        // 2. valid lsm and btree iter, and btree key is smaller or equal
        // add btree key
        char key_char[(2<<15)] = {};
        EncodeFixed64(key_char, btree_iter->first);

        // read value from Optane
        index_entry e = btree_iter->second;
        size_t item_size = e.item_size;
        char val_char[item_size];
        bool key_was_valid = read_item_val(partitions[curr_pid].slabContext, &e, val_char);

        if (key_was_valid) {
          // if the key is valid (non-deleted keys)
          // add the key value pair to results
          //fprintf(stderr, "btree key %llu\n", btree_iter->first);
          results->push_back(std::make_pair(Slice((const char*)key_char, sizeof(uint64_t)), Slice((const char*)val_char, item_size)));
          //if (popCacheSize > 0){
          //  std::string key_str = std::to_string(btree_iter->first);
          //  partitions[curr_pid].pop_cache_ptr->Insert(key_str);
          //  //int pop_cache_val = partitions[p].pop_cache_ptr->Lookup(key_str);
          //  //fprintf(stderr, "CLOCK: lookup key=%s key=%c%c%c%c%c%c%c%c clock %d\n", key_str.c_str(), key_str[0],key_str[1],key_str[2],key_str[3],key_str[4],key_str[5],key_str[6],key_str[7], pop_cache_val);
          //}
          count++;
        }
        btree_iter++;
      }
   }

  partitions[curr_pid].mtx.unlock();
  delete lsm_iter;
  //fprintf(stderr, "DBImpl::Scan() returned, start_key is %llu, scan_size is %llu, start_pid %d, return_size %llu\n", k, scan_size, curr_pid, count);
  return s.OK();
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number()
                            : latest_snapshot),
                       seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return PutImpl(o,key,val);
  //return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DeleteImpl(options, key);
  //return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {

  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(updates == nullptr);
  uint64_t last_sequence = versions_->LastSequence();
  fprintf(stderr, "SEQ_NUM %llu\n", last_sequence);
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_);
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

/*
Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(updates == nullptr);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_);
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}*/

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (allow_delay && versions_->NumLevelFiles(0) >=
                                  config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != nullptr) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_work_finished_signal_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = nullptr;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.store(true, std::memory_order_release);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;  // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               Compactions\n"
                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                      level, files, versions_->NumLevelBytes(level) / 1048576.0,
                      stats_[level].micros / 1e6,
                      stats_[level].bytes_read / 1048576.0,
                      stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  MutexLock l(&mutex_);
  Version* v = versions_->current();
  v->Ref();

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  v->Unref();
}

// JIANAN: helper function to determine which partition that a key should belong to
// Static Partition
int DBImpl::getPartition(uint64_t k) {
  size_t partition_size = numKeys / numPartitions;
  //fprintf(stderr, "getPartition returns \n");
  return k / partition_size;
  //return (k-1) / partition_size; // Adding -1 since keys start from 1 not 0
}

// used for throttling
uint32_t sleep_counter = 0;

// Below 3 functions are called after partition lock is acquired
void DBImpl::SetMigrationBitmap(int pid, uint64_t k, unsigned int under_mig) {
  size_t partition_size = numKeys / numPartitions;
  uint64_t start_k = pid * partition_size;
  int byte_idx = (k - start_k) / (sizeof(uint8_t)*8);
  int bit_idx = (k - start_k) % (sizeof(uint8_t)*8);
  uint8_t bit_mask = (1 << bit_idx);
  if (under_mig == 0) {
    partitions[pid].under_migration_bitmap[byte_idx] = (partitions[pid].under_migration_bitmap[byte_idx] & ~bit_mask);
  } else if (under_mig == 1) {
    partitions[pid].under_migration_bitmap[byte_idx] = (partitions[pid].under_migration_bitmap[byte_idx] | bit_mask);
  }
}

unsigned int DBImpl::GetFromMigrationBitmap(int pid, uint64_t k) {
  size_t partition_size = numKeys / numPartitions;
  uint64_t start_k = pid * partition_size;
  int byte_idx = (k - start_k) / (sizeof(uint8_t)*8);
  int bit_idx = (k - start_k) % (sizeof(uint8_t)*8);
  uint8_t bit_mask = (1 << bit_idx);
  if ((partitions[pid].under_migration_bitmap[byte_idx] & bit_mask) == 0) { // if under_mig bit is set to be 0
    return 0;
  } else {
    return 1;
  }
}

// Reset the partition's under_migration bitmap to be all zeros
void DBImpl::ResetMigrationBitmap(int pid) {
  memset(partitions[pid].under_migration_bitmap, (numKeys/numPartitions)/(sizeof(uint8_t)*8), 0);
}

Status DBImpl::PutImpl(const WriteOptions& opt, const Slice& key, const Slice& value, bool called_from_migration){
  using namespace std::chrono;
  auto begin = high_resolution_clock::now();
  Status s;

  // Step 1: Find the partition for key
  uint64_t k = decode_size64((unsigned char*)key.data());
  int p = DBImpl::getPartition(k);

  //fprintf(stderr, "PutImpl key str is %s and key int is %llu\n", key.ToString(true).c_str(), k);
  // check if optane is full, if not, busy waiting or sleep
  //fprintf(stderr, "partition_size: %lu\n", partitions[p].size_in_bytes);
  //fprintf(stderr, "%X\tPUT key %s, partition %d\n", std::this_thread::get_id(), key.ToString(true).c_str(), p);

  // Step 2: Convert key and value to the correct format on Optane
  auto begin_array = high_resolution_clock::now();
  size_t key_sz = key.size();
  size_t val_sz = value.size();
  size_t item_size = key_sz + val_sz + sizeof(struct item_metadata);
  //fprintf(stderr, "key size %zu, value size %zu, meta size %zu, item size %zu \n", key_sz, val_sz, sizeof(struct item_metadata), item_size);

  //char *item = new char[item_size];
  char item[item_size] = " ";
  //if (item == NULL) {
  //  fprintf(stderr, "create_item: malloc failed\n");
  //}

  item_metadata *meta = (item_metadata *)item;
  // meta->rdt = 0xC0FEC0FEC0FEC0FE;
  meta->key_size = key_sz;
  (void) EncodeFixed64(&item[sizeof(size_t)], key_sz);
  meta->value_size = val_sz;
  (void) EncodeFixed64(&item[2*sizeof(size_t)], val_sz);

  char *item_key = &item[sizeof(*meta)];
  char *item_value = &item[sizeof(*meta) + key_sz];
  memcpy(item_key, key.data(), key_sz);
  memcpy(item_value, value.data(), val_sz);

  // check if optane allocation has exceeded, then wait for migration to finish
  //while (partitions[p].size_in_bytes > (float)(maxDbSizeBytes*optaneThreshold/(float)numPartitions));
  //if (partitions[p].size_in_bytes > (float)(maxDbSizeBytes*optaneThreshold/(float)numPartitions)){
  //  fprintf(stderr, "ERROR: partition %d goes beyond max capacity!\n", p);
  //}
  //while (partitions[p].size_in_bytes > (float)(maxDbSizeBytes*optaneThreshold/(float)numPartitions)){
    //while (partitions[p].background_compaction_scheduled){
    //fprintf(stderr, "%X\tPUT_THROTTLE partition size %llu back_comp_sched %d\n", std::this_thread::get_id(), partitions[p].size_in_bytes, partitions[p].background_compaction_scheduled);
    //partitions[p].background_work_finished_signal.Wait();
  //}

  // Step 3: Acquire partition lock and check if key already exists in the index
  // Then insert/update Optane accordingly
  auto begin_lock = high_resolution_clock::now();
  partitions[p].mtx.lock();
  auto end_lock = high_resolution_clock::now();

  // set the timestamp of the item to be the latest sequence number of the partition
  meta->rdt = partitions[p].sequenceNum;
  // increment the partition's sequence number by one
  partitions[p].sequenceNum++;

  index_entry old_e;
  auto begin_find_index = high_resolution_clock::now();
  int in_optane = btree_find(partitions[p].index, (unsigned char*) key.data(), key.size(), &old_e);

  // if called from migration, check if key exists, if not only then insert it
  // TODO: not very efficient in terms of locking
  if(called_from_migration){
    if (in_optane){
      //delete item;
      //fprintf(stderr, "%X\tUPSERT: trying to insert migration key on optane, but a key already found\n", std::this_thread::get_id());
      partitions[p].mtx.unlock();
      return s.NotSupported("UPSERT SKIP", "Key Already in Optane");
    }
    //fprintf(stderr, "%X\tUPSERT: key not present on optane, inserting it\n", std::this_thread::get_id());
  }

  int old_item_size = 0;
  int new_item_size = 0;
  //op_result *res = new op_result;
  op_result result;
  op_result *res = &result;

  auto begin_optane = high_resolution_clock::now();
  if (in_optane) {
    old_item_size = partitions[p].slabContext->slabs[old_e.slab]->item_size;
    update_item_sync(&old_e, partitions[p].slabContext, item, item_size, res, load_phase_);
    //fprintf(stderr, "update key %llu done\n", k);

    // Update key popularity if it is re-accessed on NVM
    if (options_.migration_metric == 2 && !(load_phase_)) {
      // Update bucket info for approximated migration metric
      int bucket_idx = k / bucket_sz;
      // COMMENT: no need to acquire bucket's atomic_flag because foreground Get() and background migration are under
      // the same partition lock
      if (BucketList[bucket_idx].num_pop_keys < bucket_sz) {
        //fprintf(stderr, "DEBUG: bucket %d, key %llu,  has %llu popular keys\n", bucket_idx, k, BucketList[bucket_idx].num_pop_keys);
        // TODO: filter using clock cache
        int byte_idx = (k - bucket_idx * bucket_sz) / (sizeof(uint8_t)*8); // 8bits in 1 byte
        int bit_idx = (k - bucket_idx * bucket_sz) % (sizeof(uint8_t)*8); // 8bits in 1 byte
        uint8_t bit_mask = (1 << bit_idx);
        //fprintf(stderr, "DEBUG: key %llu in bucket %d has byte index %d and bit index %d bit mask %d, flip bit mask %llu, pop_keys %llu\n", k, bucket_idx, byte_idx, bit_idx, bit_mask, ~bit_mask, BucketList[bucket_idx].num_pop_keys);
        if ((BucketList[bucket_idx].pop_bitmap[byte_idx] & bit_mask) == 0 ) { // if the bit is not set
          //fprintf(stderr, "DEBUG: pop_key increment\n");
          BucketList[bucket_idx].num_pop_keys++;
          BucketList[bucket_idx].pop_bitmap[byte_idx] = (BucketList[bucket_idx].pop_bitmap[byte_idx] | bit_mask);
        }
      }
    }
  } else {
    add_item_sync(partitions[p].slabContext, item, item_size, res, load_phase_);
    //fprintf(stderr, "insert key %llu done\n", k);

    if (options_.migration_metric == 2 && !(load_phase_)) {
      // Update bucket info for approximated migration metric
      int bucket_idx = k / bucket_sz;
      assert(BucketList[bucket_idx].num_total_keys <= bucket_sz);
      BucketList[bucket_idx].num_total_keys += 1;
    }
  }
  auto end_optane = high_resolution_clock::now();

  if (res->success == -1) {
    fprintf(stderr, "%s\n", "put() failed");
    //delete res;
    //delete item;
    partitions[p].mtx.unlock();
    return s.IOError("PutImpl() failed"); // return a error status
  }

  // Step 4: Update the index with new information, that is (key, item_size || slab_id || slab_offset)
  int slab_id = res->slab_id;
  index_entry e = {
    (size_t)res->slab_idx, // object offset within this slab
    slab_id, // list index of the slab file
    item_size, // accurate size of the kv pair
  };

  auto begin_index = high_resolution_clock::now();
  if (in_optane) {
    // We need explictly remove key from the index first
    // This is because btree will not update the key with new value if key already exists. See indexes/btree.h line 1759
    btree_delete(partitions[p].index, (unsigned char*) key.data(), key.size(), false);
    auto end_delete_index = high_resolution_clock::now();
    partitions[p].put_delete_index_time = partitions[p].put_delete_index_time + duration_cast<nanoseconds>(end_delete_index - begin_index).count();
  }
  btree_insert(partitions[p].index, (unsigned char*) key.data(), key.size(), &e);
  auto end_insert_index = high_resolution_clock::now();

  // Update the partition's total size in bytes
  new_item_size = partitions[p].slabContext->slabs[e.slab]->item_size;
  //fprintf(stderr, "adding item size %d new %d old %d\n", (new_item_size - old_item_size), new_item_size, old_item_size);
  partitions[p].size_in_bytes += (new_item_size - old_item_size);

  SetMigrationBitmap(p, k, 0);

  //delete res;
  //delete item;

  // Track write popularity as well in the clock
  /*if (popCacheSize > 0){
    partitions[p].pop_cache_ptr->Insert(k);
  }*/

  // Step 5: Update timing info and release the partition lock
  auto end = high_resolution_clock::now();
  if(called_from_migration){
    partitions[p].num_upsert_puts++;
  }
  partitions[p].num_puts ++;
  partitions[p].put_time = partitions[p].put_time + duration_cast<nanoseconds>(end - begin).count();
  partitions[p].put_copy_array = partitions[p].put_copy_array + duration_cast<nanoseconds>(begin_lock - begin_array).count();
  partitions[p].put_acquire_lock = partitions[p].put_acquire_lock + duration_cast<nanoseconds>(end_lock - begin_lock).count();
  partitions[p].put_find_index_time = partitions[p].put_find_index_time + duration_cast<nanoseconds>(begin_optane - begin_find_index).count();
  partitions[p].put_insert_index_time = partitions[p].put_insert_index_time + duration_cast<nanoseconds>(end_insert_index - begin_index).count();
  if (in_optane) {
    partitions[p].num_updates ++;
    partitions[p].update_optane_time = partitions[p].update_optane_time + duration_cast<nanoseconds>(end_optane - begin_optane).count();
  } else {
    partitions[p].insert_optane_time = partitions[p].insert_optane_time + duration_cast<nanoseconds>(end_optane - begin_optane).count();
  }

  partitions[p].mtx.unlock();

  // Step 6: Trigger migration if partition size in bytes exceeds the threshold
  if (partitions[p].size_in_bytes > partitions[p].max_optane_usage){
    if (!load_phase_) {
      partitions[p].max_optane_usage = partitions[p].size_in_bytes;
    }
    //fprintf(stderr, "%X\tpartition %llu max optane usage %llu\n", std::this_thread::get_id(), p, partitions[p].max_optane_usage);
  }
  if (partitions[p].num_put_reqs++ % 1000000 == 0){
    fprintf(stderr, "%X\tpartition %llu optane usage curr %llu max %llu\n", std::this_thread::get_id(), p, partitions[p].size_in_bytes, partitions[p].max_optane_usage);
  }
  //fprintf(stderr, "psize %llu maxdb %llu optthresh %f soft_limit %f num_p %llu\n", partitions[p].size_in_bytes, maxDbSizeBytes, optaneThreshold, soft_limit, numPartitions);

  // trigger migration when current size exceeds the pre-set upper bound
  if (partitions[p].size_in_bytes > (float)(maxDbSizeBytes*optaneThreshold*partitions[p].migration_upper_bound/(float)numPartitions)) {
    //fprintf(stderr, "%X\tpartition %llu optane size %f soft size %f \n", std::this_thread::get_id(), p, (float)(maxDbSizeBytes*optaneThreshold)/(float)numPartitions, (float)((maxDbSizeBytes*optaneThreshold*soft_limit)/(float)numPartitions));
    MaybeScheduleCompaction(p); // TODO: add partition_ctx
  }

  // trigger the rate limiter when current size exceeds the pre-set rate-limit threshold
  if (partitions[p].size_in_bytes > (float)(maxDbSizeBytes*optaneThreshold*partitions[p].ratelimit_threshold/(float)numPartitions)) {
    if (++sleep_counter%5 == 0){
      env_->SleepForMicroseconds(20); // was 20 for YCSB, setting 100 for twitter
      //fprintf(stderr, "%X\tpartition %llu rate limit", std::this_thread::get_id(), p);
    }
  }

  return s.OK();
}

//PRISMDB
// JIANAN: changes PutImpl for Optane-only.
/*Status DBImpl::PutImpl(const WriteOptions& opt, const Slice& key, const Slice& value){
  Status s;
  using namespace std::chrono;
  auto begin = high_resolution_clock::now();
  // Step 1: find the partition for key
  //uint64_t k = std::stoull(key.ToString(true), nullptr, 16); // TODO: assumes 8byte key
  //fprintf(stderr, "PutImpl key %ull\n", k);
  uint64_t k = decode_size64((unsigned char*)key.data());
  //uint64_t hash = *(uint64_t*)((unsigned char*)(key.data()));
  //fprintf(stderr, "PutImpl key %llu\n", k);
  int p = DBImpl::getPartition(k);
  //fprintf(stderr, "PutImpl key %llu, partition %d\n", k, p);

  // Step 2: convert key and value to a record format on Optane:
  // metadata = timestamp || key size || value size
  // key
  // value
  auto begin_array = high_resolution_clock::now();
  size_t key_sz = key.size();
  size_t val_sz = value.size();
  size_t item_size = key_sz + val_sz + sizeof(struct item_metadata);
  //fprintf(stderr, "key size %zu, value size %zu, meta size %zu, item size %zu \n", key_sz, val_sz, sizeof(struct item_metadata), item_size);

  char *item = new char[item_size];
  if (item == NULL) {
    fprintf(stderr, "create_item: malloc failed\n");
  }

  item_metadata *meta = (item_metadata *)item;
  meta->rdt = 0xC0FEC0FEC0FEC0FE;
  meta->key_size = key_sz;
  (void) EncodeVarint64(&item[sizeof(size_t)], key_sz);
  meta->value_size = val_sz;
  (void) EncodeVarint64(&item[2*sizeof(size_t)], val_sz);

  char *item_key = &item[sizeof(*meta)];
  char *item_value = &item[sizeof(*meta) + key_sz];
  memcpy(item_key, key.data(), key_sz);
  memcpy(item_value, value.data(), val_sz);
  auto end_array = high_resolution_clock::now();
  put_copy_array = put_copy_array + duration_cast<nanoseconds>(end_array - begin_array).count();
  //for (int i=0; i<key_sz; i++){
    //item_key[i] = key.data()[i];
  //  fprintf(stderr, "CHAR %c\n", item_key[i]);
  //}
  //fprintf(stderr, "KEY COMP item_key %s key %s\n", Slice(item_key, 8).ToString(true).c_str(), key.ToString(true).c_str());
  // test
  auto end_before_btree_find = high_resolution_clock::now();
  put_before_btree_find = put_before_btree_find + duration_cast<nanoseconds>(end_before_btree_find - begin).count();
  auto begin_lock = high_resolution_clock::now();

  // Critical Section
  partitions[p].mtx.lock();

  auto end_lock = high_resolution_clock::now();
  put_acquire_lock = put_acquire_lock + duration_cast<nanoseconds>(end_lock - begin_lock).count();

  op_result *res = new op_result;

  // Step 3: check if key already exists
  index_entry old_e;
  auto begin_index_1 = high_resolution_clock::now();
  int in_optane = btree_find(partitions[p].index, (unsigned char*) key.data(), key.size(), &old_e);
  auto end_index_1 = high_resolution_clock::now();
  put_index_time = put_index_time + duration_cast<nanoseconds>(end_index_1 - begin_index_1).count();

  // test
  auto end_before_optane = high_resolution_clock::now();
  put_before_optane = put_before_optane + duration_cast<nanoseconds>(end_before_optane - end_before_btree_find).count();

  if (in_optane) {
    // We need explictly remove key from the index first
    // This is because btree will not update the key with new value if key already exists
    // see indexes/btree.h line 1759
    auto begin_index = high_resolution_clock::now();
    btree_delete(partitions[p].index, (unsigned char*) key.data(), key.size(), false);
    auto end_index = high_resolution_clock::now();
    update_item_sync(&old_e, partitions[p].slabContext, item, item_size, res, load_phase_);
    auto end_optane = high_resolution_clock::now();
    put_index_time = put_index_time + duration_cast<nanoseconds>(end_index - begin_index).count();
    put_optane_time = put_optane_time + duration_cast<nanoseconds>(end_optane - end_index).count();
  } else {
    auto begin_optane = high_resolution_clock::now();
    add_item_sync(partitions[p].slabContext, item, item_size, res, load_phase_);
    auto end_optane = high_resolution_clock::now();
    put_optane_time = put_optane_time + duration_cast<nanoseconds>(end_optane - begin_optane).count();
    int slab_id = get_slab_id_new(partitions[p].slabContext, item_size);
    partitions[p].size_in_bytes += partitions[p].slabContext->slabs[slab_id]->item_size;
    // TODO: handle the update case when item size changes
  }

  if (res->success == -1) {
    fprintf(stderr, "%s\n", "put() failed");
    delete res;
    delete item;
    partitions[p].mtx.unlock();
    return s.IOError("PutImpl() failed");
  }

  // Step 4: insert (key, item_size || slab_id || slab_offset)
  int slab_id = res->slab_id;
  index_entry e = {
    partitions[p].slabContext->slabs[slab_id],// pointer to the slab
    (size_t)res->slab_idx, // object offset within this slab
    item_size, // JIANAN: TODO, should this be the item size on slab, say 256?
    0, // under_migration is default to false/0
  };


  // JIANAN: the second parameter should be the key prefix, and the third parameter being the size of the prefix
  // Here we just use the entire key char array and its length
  //btree_insert(partitions[p].index, (unsigned char*) key_char, strlen(key_char), &e);
  btree_insert(partitions[p].index, (unsigned char*)key.data(), key.size(), &e);

  delete res;
  delete item;

  partitions[p].mtx.unlock();
  if (partitions[p].size_in_bytes > (float)(maxDbSizeBytes*optaneThreshold/(float)numPartitions)) {
    fprintf(stderr, "Schedule compaction trigger partition %d, partition size %lu numPartitions %lu maxDbSize %lu optaneThreshold %f\n", p, partitions[p].size_in_bytes, numPartitions, maxDbSizeBytes, optaneThreshold);
    MaybeScheduleCompaction(&partitions[p]); // TODO: add partition_ctx
  }
  return s.OK();
}*/

//PRISMDB
/*Status DBImpl::PutImpl(const WriteOptions& opt, const Slice& key, const Slice& value){
  //fprintf(stderr, "Put called\n");
  Status s;
  uint64_t sequence_num = versions_->LastSequence()+1;
  InternalKey ikey = InternalKey(key, sequence_num, kTypeValue);
  optane_mu.lock();
  kv[kv_idx].sn = sequence_num;
  kv[kv_idx].vtype = kTypeValue;
  //sprintf(kv[kv_idx].key, "%.8s", key.ToString().c_str());
  kv[kv_idx].key = key.ToString();
  sprintf(kv[kv_idx].value, "%.100s", value.ToString().c_str());
  //fprintf(stderr, "Put key %s value (sn %lu type %d)\n", key.ToString(true).c_str(), kv[kv_idx].sn, kv[kv_idx].vtype);
  optane_map[key.ToString(true)] = &kv[kv_idx];
  //fprintf(stderr, "VALUE %s\n", optane_map[key.ToString(true)].ToString(true).c_str());
  optane_size += key.size() + value.size();
  kv_idx = (kv_idx+1)%10000000;

  fprintf(stderr, "Put called key %s seq %lu type %d optane_num_keys %d\n", key.ToString(true).c_str(), sequence_num, kTypeValue, optane_map.size());
  versions_->SetLastSequence(sequence_num);
  if (optane_map.size() > 200000) {
    MaybeScheduleCompaction(nullptr); // TODO: add partition_ctx
  }
  optane_mu.unlock();
  return s.OK();
}*/

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  //return PutImpl(opt, key, value);
  //WriteBatch batch;
  //batch.Put(key, value);
  //return Write(opt, &batch);
}

//PRISMDB
Status DBImpl::DeleteImpl(const WriteOptions& opt, const Slice& key){
  Status s;
  //fprintf(stderr, "%s\n", "Delete");

  // Step 1: find the partition for key
  uint64_t k = decode_size64((unsigned char*)key.data());
  int p = DBImpl::getPartition(k);
  //fprintf(stderr, "key %d, partition %d\n", k, p);

  // Critical Section:
  partitions[p].mtx.lock();

  // Step 2: find index entry for the key
  index_entry e;
  int in_optane = btree_find(partitions[p].index, (unsigned char*) key.data(), key.size(), &e);

  if (in_optane) {
    // Step 3.1: delete the key on optane
    // Remove the item from optane and add its location to a per-slab freelist
    //fprintf(stderr, "before calling remove_item_sync \n");
    int success = remove_item_sync(partitions[p].slabContext, &e, load_phase_);
    // Remove the item from optane index
    btree_delete(partitions[p].index, (unsigned char*) key.data(), key.size(), false);
    // Decrement the partition's total size in bytes
    struct slab_new ** p_slabs = partitions[p].slabContext->slabs;
    partitions[p].size_in_bytes -= p_slabs[e.slab]->item_size;
  } else {
    // Step 3.2: delete a key on QLC (or might on QLC)
    // Create a tombstone message whose metadata has key size of -1 and value size of 0
    size_t key_sz = -1;
    size_t val_sz = 0;
    size_t item_size = sizeof(struct item_metadata);
    char *item = new char[item_size];
    if (item == NULL) {
      fprintf(stderr, "create_item: malloc failed\n");
    }
    item_metadata *meta = (item_metadata *)item;
    //meta->rdt = 0xC0FEC0FEC0FEC0FE;
    meta->rdt = partitions[p].sequenceNum;
    partitions[p].sequenceNum++;
    meta->key_size = key_sz;
    (void) EncodeFixed64(&item[sizeof(size_t)], key_sz);
    meta->value_size = val_sz;
    (void) EncodeFixed64(&item[2*sizeof(size_t)], val_sz);

    // Add the tombstone message for this item on optane
    op_result *res = new op_result;
    add_item_sync(partitions[p].slabContext, item, item_size, res, load_phase_);
    if (res->success == -1) {
      fprintf(stderr, "%s\n", "delete() on qlc failed");
      delete res;
      delete item;
      partitions[p].mtx.unlock();
      return s.IOError("DeleteImpl() failed for QLC"); // return a error status
    }

    // Insert the item to optane index as if it is a valid item
    int slab_id = res->slab_id;
    index_entry new_e = {
      (size_t)res->slab_idx, // object offset within this slab
      slab_id,
      item_size,
    };
    btree_insert(partitions[p].index, (unsigned char*) key.data(), key.size(), &new_e);

    // Increment the partition's total size in bytes
    partitions[p].size_in_bytes += partitions[p].slabContext->slabs[new_e.slab]->item_size;

    delete res;
    delete item;
  }

  partitions[p].mtx.unlock();
  return s.OK();

  /*//fprintf(stderr, "Delete called\n");
  Status s;
  uint64_t sequence_num = versions_->LastSequence()+1;
  InternalKey ikey = InternalKey(key, sequence_num, kTypeDeletion);
  optane_mu.lock();
  optane_map.erase(ikey.Encode().ToString());
  // TODO: need to update the optane size correctly
  //optane_size += key.size() + value.size();
  optane_mu.unlock();
  fprintf(stderr, "Delete called key %s seq %lu type %d optane_num_keys %d \n", key.ToString().c_str(), sequence_num, kTypeDeletion, optane_map.size());
  versions_->SetLastSequence(sequence_num);
  return s.OK();*/
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  //return DeleteImpl(opt, key);
  // ASH:
  //fprintf(stderr, "Delete called\n");
  optane_map.erase(key.ToString());
  Status s;
  return s.OK();
  //WriteBatch batch;
  //batch.Delete(key);
  //return Write(opt, &batch);
}

DB::~DB() = default;

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }

  // PRISM: check if this if statement needs to be enabled
  //if (s.ok()) {
    //impl->RemoveObsoleteFiles();
    //impl->MaybeScheduleCompaction(); // ASH: disable for now
  //}
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->RemoveFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->RemoveFile(lockname);
    env->RemoveDir(dbname);  // Ignore error in case dir contains other files
  }
  // PRISMDB - temp
  optane_map.clear();
  optane_size = 0;
  return result;
}

ClockCache::ClockCache(size_t capacity, Bucket* bl, int migration_metric) : capacity_(0), migration_metric_(0), usage_(0) {

  table_clock_iter_ = table_clock_.begin();
  bucket_list_ = bl;
  migration_metric_ = migration_metric;

  for (uint8_t i=0; i<=CLOCK_BITS_MAX_VALUE; i++){
    clock_cache_value_hist_[i] = 0;
  }
  SetCapacity(capacity);
}

ClockCache::~ClockCache() {
  // empty
}

// return -1 if key not found, else returns clock value
//int8_t ClockCache::Lookup(const std::string& key) {
int8_t ClockCache::Lookup(const uint64_t key) {

  int32_t value = -1;
  HashTableClock::const_accessor ca;

  if (table_clock_.find(ca, key)) {
    uint8_t on_optane = (ca->second & 0x01);
    uint8_t clock_value = ((ca->second & 0xFE)>>1);
    if (on_optane) {
      value = (int8_t)(clock_value);
    }
  }
  ca.release();
  return value;
}

void ClockCache::EvictIfCacheFull(){

  if (usage_ < capacity_){
    return;
  }

  while (true) {

    bool ret = false;
    //fprintf(stderr, "CLOCK: Starting iterator table_size=%lu\n", table_clock_.size());

    uint32_t loopcount = 0;
    for(; table_clock_iter_ != table_clock_.end();) {
      HashTableClock::value_type snapshot = *table_clock_iter_++;
      uint8_t on_optane = (snapshot.second & 0x01);
      uint8_t clock_value = ((snapshot.second & 0xFE)>>1);

      if ((clock_value == 0) || (on_optane == 0)){
        //fprintf(stderr, "CLOCK: Candidate eviction key=%s clock %d on_optane %d usage %d\n", snapshot.first.c_str(), clock_value, on_optane, usage_);

        HashTableClock::accessor accessor;
        if (table_clock_.find(accessor, snapshot.first)) {
          assert(!accessor.empty());
          table_clock_.erase(accessor);
          accessor.release();
          // subtract key size and 1 byte for clock
          usage_ -= (sizeof(snapshot.first)+1);
          clock_cache_value_hist_[clock_value]--;
          //fprintf(stderr, "CLOCK: Evicted key=%llu keysize %d usage %d\n", snapshot.first, sizeof(snapshot.first), usage_);
          ret = true;

          // remove the key from bucekt pop_bitmap if exists
          //std::string key_str = snapshot.first;
          //uint64_t k = std::stoull(key_str);
          //fprintf(stderr, "DEBUG: clock cache evict key %llu\n", k);
          //EvictBucketPopKeys(key);
	  EvictBucketPopKeys(snapshot.first);

          break;
        }
      }
      else{

        //fprintf(stderr, "CLOCK: Decrementing key=%s value=%lu, it=%x\n", snapshot.first.c_str(), snapshot.second, *table_clock_iter_);
        //fprintf(stderr, "CLOCK: Decrement candidate key=%s value=%lu usage=%d\n", snapshot.first.c_str(), snapshot.second, usage_);

        HashTableClock::accessor accessor;
        if (table_clock_.find(accessor, snapshot.first)){
          assert(!accessor.empty());
          uint8_t clock_value = (accessor->second & 0xFE)>>1;
          clock_cache_value_hist_[clock_value]--;
          clock_value--;
          accessor->second = (clock_value<<1) | (accessor->second & 0x01);
          clock_cache_value_hist_[clock_value]++;
          snapshot.second = accessor->second; // only needed for printing
          accessor.release();
          //uint8_t value = snapshot.second;
          //fprintf(stderr, "CLOCK: Decremented key=%s clock=%d value=%u usage=%d\n", snapshot.first.c_str(), clock_value, snapshot.second, usage_);
        }
        else{
          fprintf(stderr, "CLOCK: ERROR - Decr key not found key=%llu \n", snapshot.first);
        }
      }

      /*if(loopcount%500000 == 0){
        PrintClockCacheValueHist("After 500000 loops");
      }*/
      loopcount++;
    }
    if (ret){
      break;
    }
    table_clock_iter_ = table_clock_.begin();
    PrintClockCacheValueHist("After full iteration");
  }
}

//void ClockCache::Insert(const std::string& key) {
void ClockCache::Insert(const uint64_t key) {

  uint8_t clock_init_value = 0;

  // check if key is already present
  HashTableClock::accessor accessor;
  if (table_clock_.find(accessor, key)) {
    uint8_t clock_value = (accessor->second & 0xFE)>>1;
    uint8_t on_optane_prev = accessor->second & 0x01;
    uint8_t on_optane = 1;
    clock_cache_value_hist_[clock_value]--;

    accessor->second = (CLOCK_BITS_MAX_VALUE << 1)  | (on_optane);
    clock_cache_value_hist_[CLOCK_BITS_MAX_VALUE]++;

    //fprintf(stderr, "CLOCK: Updated key %llu keysize %d clock %d on_optane %d on_optane_prev %d value %d usage %d\n", key, sizeof(key), clock_value, on_optane, on_optane_prev, accessor->second, usage_);
    accessor.release();
  }
  else{
    // check if cache is full, if so do eviction
    EvictIfCacheFull();

    uint8_t on_optane = 1;
    uint8_t cache_entry = ( clock_init_value << 1 ) | ( on_optane );
    table_clock_.insert(HashTableClock::value_type(key, cache_entry));
    // charge is key size plus 1 byte for clock
    usage_ += sizeof(key)+1;
    clock_cache_value_hist_[clock_init_value]++;
    //fprintf(stderr, "CLOCK: Inserted key %llu keysize %d clock %d on_optane %d value %d usage %d\n", key, sizeof(key), clock_init_value, on_optane, cache_entry, usage_);
    //PrintClockCacheValueHist("CLOCK-DBG");
  }
}

bool ClockCache::AreClockValuesNonZero(){

  //if (clock_cache_value_hist_[0]==0 || clock_cache_value_hist_[1]==0 || clock_cache_value_hist_[2]==0 || clock_cache_value_hist_[3]==0){
  if (clock_cache_value_hist_[3]==0){
    return false;
  }
  return true;
}

void ClockCache::PrintClockCacheValueHist(const std::string& msg){

  /*fprintf(stderr, "CLOCK: %s used=%lu, cap=%lu, value histogram [0]=%lu, [1]=%lu, [2]=%lu, [3]=%lu, [4]=%lu, [5]=%lu, [6]=%lu, [7]=%lu\n",
  msg.c_str(), usage_.load(std::memory_order_relaxed), capacity_.load(std::memory_order_relaxed),
  clock_cache_value_hist_[0].load(std::memory_order_relaxed),
  clock_cache_value_hist_[1].load(std::memory_order_relaxed),
  clock_cache_value_hist_[2].load(std::memory_order_relaxed),
  clock_cache_value_hist_[3].load(std::memory_order_relaxed),
  clock_cache_value_hist_[4].load(std::memory_order_relaxed),
  clock_cache_value_hist_[5].load(std::memory_order_relaxed),
  clock_cache_value_hist_[6].load(std::memory_order_relaxed),
  clock_cache_value_hist_[7].load(std::memory_order_relaxed));*/

  fprintf(stderr, "CLOCK: %s used %lu cap %lu tbb_size %lu value histogram [0] %lu [1] %lu [2] %lu [3] %lu\n",
  msg.c_str(), usage_, capacity_, table_clock_.size(),
  clock_cache_value_hist_[0],
  clock_cache_value_hist_[1],
  clock_cache_value_hist_[2],
  clock_cache_value_hist_[3]);

}

//void ClockCache::MarkOptaneBit(const std::string& key, bool set_optane_bit) {
void ClockCache::MarkOptaneBit(const uint64_t key, bool set_optane_bit) {

  HashTableClock::accessor accessor;

  if (table_clock_.find(accessor, key)) {
    uint8_t on_optane = (accessor->second & 0x01);
    uint8_t clock_value = ((accessor->second & 0xFE)>>1);
    if (set_optane_bit == true) {
      if (on_optane == 1){
        fprintf(stderr, "CLOCK: WARNING! Optane bit already 1\n");
      }
      else {
        on_optane = 1;
        accessor->second = (clock_value << 1)  | (on_optane);
        //fprintf(stderr, "CLOCK: marking optane bit key=%s clock %d on_optane %d value %d\n", key.c_str(), clock_value, on_optane, accessor->second);
      }
    }
    else{
      if (on_optane == 0){
        fprintf(stderr, "CLOCK: WARNING! Optane bit already 0\n");
      }
      else{
        on_optane = 0;
        accessor->second = (clock_value << 1)  | (on_optane);
	//fprintf(stderr, "CLOCK: marking optane bit key=%s clock %d on_optane %d value %d\n", key.c_str(), clock_value, on_optane, accessor->second);
      }
    }
  }
  accessor.release();
}

bool ClockCache::IsClockPopular(uint64_t key, float pop_threshold) {

  //std::string key_str = std::to_string(key);
  int clock_value = Lookup(key);
  if (clock_value != -1) {
    //uint8_t clock_value = (cache_val & 0xFE) >> 1;
    //fprintf(stderr, "MIGRATION: key=%llu found in clock cache\n", key);

    if (clk_prob_dist[clock_value] != -1 && (static_cast<float>(rand())/(static_cast<float>(RAND_MAX))) <= clk_prob_dist[clock_value]){
      return true;
    }
    else {
      return false;
    }
  }
  else {
    //fprintf(stderr, "MIGRATION: key=%llu missing in clock cache\n", key);
    return false;
  }
}

// Return a pair of information: whether the key is popular, it clock value
std::pair<bool, int> ClockCache::GetKeyInfo(uint64_t key, float pop_threshold) {
  bool is_popular = false;  
  int clock_value = Lookup(key);
  
  if (clock_value != -1) {
    if (clk_prob_dist[clock_value] != -1 && (static_cast<float>(rand())/(static_cast<float>(RAND_MAX))) <= clk_prob_dist[clock_value]){
      is_popular = true;
    }
  }
  return std::make_pair(is_popular, clock_value);
}

// THIS FUNCTION CONSIDERS CLOCK VALUE 0
/*void ClockCache::GenClockProbDist(float pop_threshold){

  // initialize random seed:
  std::srand (std::time(NULL));
  //int threshold_percent = pop_threshold*100;
  float threshold_percent = pop_threshold*100;
  uint32_t num_clk0 = clock_cache_value_hist_[0];
  uint32_t num_clk1 = clock_cache_value_hist_[1];
  uint32_t num_clk2 = clock_cache_value_hist_[2];
  uint32_t num_clk3 = clock_cache_value_hist_[3];
  fprintf(stderr, "CLOCK: clock hist values %lu %lu %lu %lu\n", num_clk0, num_clk1, num_clk2, num_clk3);
  uint32_t total = num_clk0 + num_clk1 + num_clk2 + num_clk3;
  for (int i=0; i<=CLOCK_BITS_MAX_VALUE; i++){
    clk_prob_dist[i] = -1;
  }
  if (total != 0 && threshold_percent != 0){
    //uint32_t clk0_percent = (num_clk0*100)/total;
    //uint32_t clk1_percent = (num_clk1*100)/total;
    //uint32_t clk2_percent = (num_clk2*100)/total;
    //uint32_t clk3_percent = (num_clk3*100)/total;
    float clk0_percent = (float)(num_clk0*100)/total;
    float clk1_percent = (float)(num_clk1*100)/total;
    float clk2_percent = (float)(num_clk2*100)/total;
    float clk3_percent = (float)(num_clk3*100)/total;
    //if (clk3_percent!=0 && clk2_percent!=0 && clk1_percent!=0 && clk0_percent!=0){
    //if (clk3_percent>=0 && clk2_percent>=0 && clk1_percent>=0 && clk0_percent>=0){
      //fprintf(stderr, "CLOCK: clock percents %lu %lu %lu %lu\n", clk0_percent, clk1_percent, clk2_percent, clk3_percent);
      fprintf(stderr, "CLOCK: clock percents %.2f %.2f %.2f %.2f\n", clk0_percent, clk1_percent, clk2_percent, clk3_percent);
      if (clk3_percent >= threshold_percent){
        clk_prob_dist[3] = (float_t)threshold_percent/(float_t)clk3_percent;
      }
      else{
        clk_prob_dist[3] = 1.0;
        threshold_percent -= clk3_percent;
        if (clk2_percent >= threshold_percent){
          clk_prob_dist[2] = (float_t)threshold_percent/(float_t)clk2_percent;
        }
        else{
          clk_prob_dist[2] = 1.0;
      threshold_percent -= clk2_percent;
          if (clk1_percent >= threshold_percent){
            clk_prob_dist[1] = (float_t)threshold_percent/(float_t)clk1_percent;
          }
          else{
            clk_prob_dist[1] = 1.0;
            threshold_percent -= clk1_percent;
            if (clk0_percent >= threshold_percent){
              clk_prob_dist[0] = (float_t)threshold_percent/(float_t)clk0_percent;
            }
            else{
              clk_prob_dist[0] = 1.0;
              threshold_percent -= clk0_percent;
            }
          }
        }
      }
    }
  fprintf(stderr, "CLOCK: clock prob dist %f %f %f %f\n", clk_prob_dist[0], clk_prob_dist[1], clk_prob_dist[2], clk_prob_dist[3]);
}*/

// THIS FUNCTION IGNORES CLK VAL 0
void ClockCache::GenClockProbDist(float pop_threshold){

  // initialize random seed:
  std::srand (std::time(NULL));
  //int threshold_percent = pop_threshold*100;
  float threshold_percent = pop_threshold*100;
  uint32_t num_clk0 = clock_cache_value_hist_[0];
  uint32_t num_clk1 = clock_cache_value_hist_[1];
  uint32_t num_clk2 = clock_cache_value_hist_[2];
  uint32_t num_clk3 = clock_cache_value_hist_[3];
  fprintf(stderr, "CLOCK: clock hist values %lu %lu %lu %lu\n", num_clk0, num_clk1, num_clk2, num_clk3);
  uint32_t total = num_clk0 + num_clk1 + num_clk2 + num_clk3;
  for (int i=0; i<=CLOCK_BITS_MAX_VALUE; i++){
    clk_prob_dist[i] = -1;
  }
  if (total != 0 && threshold_percent != 0){
    //uint32_t clk0_percent = (num_clk0*100)/total;
    //uint32_t clk1_percent = (num_clk1*100)/total;
    //uint32_t clk2_percent = (num_clk2*100)/total;
    //uint32_t clk3_percent = (num_clk3*100)/total;
    float clk0_percent = (float)(num_clk0*100)/total;
    float clk1_percent = (float)(num_clk1*100)/total;
    float clk2_percent = (float)(num_clk2*100)/total;
    float clk3_percent = (float)(num_clk3*100)/total;
    //if (clk3_percent!=0 && clk2_percent!=0 && clk1_percent!=0 && clk0_percent!=0){
    //if (clk3_percent>=0 && clk2_percent>=0 && clk1_percent>=0 && clk0_percent>=0){
      //fprintf(stderr, "CLOCK: clock percents %lu %lu %lu %lu\n", clk0_percent, clk1_percent, clk2_percent, clk3_percent);
      fprintf(stderr, "CLOCK: clock percents %.2f %.2f %.2f %.2f\n", clk0_percent, clk1_percent, clk2_percent, clk3_percent);
      if (clk3_percent >= threshold_percent){
        clk_prob_dist[3] = (float_t)threshold_percent/(float_t)clk3_percent;
      }
      else if (clk3_percent > 0) {
        clk_prob_dist[3] = 1.0;
        threshold_percent -= clk3_percent;
        if (clk2_percent >= threshold_percent){
          clk_prob_dist[2] = (float_t)threshold_percent/(float_t)clk2_percent;
        }
        else if (clk2_percent > 0){
          clk_prob_dist[2] = 1.0;
            threshold_percent -= clk2_percent;
            if (clk1_percent >= threshold_percent){
            clk_prob_dist[1] = (float_t)threshold_percent/(float_t)clk1_percent;
          }
          else if (clk1_percent > 0){
            clk_prob_dist[1] = 1.0;
            //threshold_percent -= clk1_percent;
            //if (clk0_percent >= threshold_percent){
            //  clk_prob_dist[0] = (float_t)threshold_percent/(float_t)clk0_percent;
            //}
            //else{
            //  clk_prob_dist[0] = 1.0;
            //  threshold_percent -= clk0_percent;
            //}
          }
        }
      }
      //if (clk3_percent >= threshold_percent){
      //  clk_prob_dist[3] = (float_t)threshold_percent/(float_t)clk3_percent;
      //}
      //else{
      //  clk_prob_dist[3] = 1.0;
      //  threshold_percent -= clk3_percent;
      //  if (clk2_percent >= threshold_percent){
      //    clk_prob_dist[2] = (float_t)threshold_percent/(float_t)clk2_percent;
      //  }
      //  else{
      //    clk_prob_dist[2] = 1.0;
      //threshold_percent -= clk2_percent;
      //    if (clk1_percent >= threshold_percent){
      //      clk_prob_dist[1] = (float_t)threshold_percent/(float_t)clk1_percent;
      //    }
      //    else{
      //      clk_prob_dist[1] = 1.0;
      //      threshold_percent -= clk1_percent;
      //      if (clk0_percent >= threshold_percent){
      //        clk_prob_dist[0] = (float_t)threshold_percent/(float_t)clk0_percent;
      //      }
      //      else{
      //        clk_prob_dist[0] = 1.0;
      //        threshold_percent -= clk0_percent;
      //      }
      //    }
      //  }
      //}
    }
  fprintf(stderr, "CLOCK: clock prob dist %f %f %f %f\n", clk_prob_dist[0], clk_prob_dist[1], clk_prob_dist[2], clk_prob_dist[3]);
}

void ClockCache::EvictBucketPopKeys(uint64_t k) {
  if (migration_metric_ == 2) { // approximated metric
    // remove the key from bucekt pop_bitmap if exists
    uint64_t bucket_sz = BUCKET_SZ;
    int bucket_idx = k / bucket_sz;
    // make sure migration is not modifying pop bitmap at the same time
    while (bucket_list_[bucket_idx].in_use.test_and_set()) {}
    if (bucket_list_[bucket_idx].num_pop_keys > 0) {
      int byte_idx = (k - bucket_idx * bucket_sz) / (sizeof(uint8_t)*8);
      int bit_idx = (k - bucket_idx * bucket_sz) % (sizeof(uint8_t)*8);
      uint8_t bit_mask = (1 << bit_idx);
      //fprintf(stderr, "DEBUG: Evict key %llu in bucket %d has byte index %d and bit index %d bit mask %d, flip bit mask %llu\n", k, bucket_idx, byte_idx, bit_idx, bit_mask, ~bit_mask);
      if ((bucket_list_[bucket_idx].pop_bitmap[byte_idx] & bit_mask) > 0 ) { // if bit is already set
        bucket_list_[bucket_idx].num_pop_keys--; // decrement it
        bucket_list_[bucket_idx].pop_bitmap[byte_idx] = (bucket_list_[bucket_idx].pop_bitmap[byte_idx] & ~bit_mask);
      }
    }
    bucket_list_[bucket_idx].in_use.clear();
  }
}

}  // namespace leveldb
