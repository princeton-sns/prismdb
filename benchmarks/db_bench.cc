// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <sys/types.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <inttypes.h>
#include <unordered_map>
#include <thread>
#include <chrono>
#include <ctime>
#include <vector>
#include <algorithm>
#include <random>
#include <iostream>
#include <fstream>

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/write_batch.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"
#include "util/zipf.h"
#include "util/latest_generator.h"
#include "ThreadPool.h"
#include <iostream>

// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//      fillseq       -- write N values in sequential key order in async mode
//      fillrandom    -- write N values in random key order in async mode
//      overwrite     -- overwrite N values in random key order in async mode
//      fillsync      -- write N/100 values in random key order in sync mode
//      fill100K      -- write N/1000 100K values in random order in async mode
//      deleteseq     -- delete N keys in sequential order
//      deleterandom  -- delete N keys in random order
//      readseq       -- read N times sequentially
//      readreverse   -- read N times in reverse order
//      readrandom    -- read N times in random order
//      readmissing   -- read N missing keys in random order
//      readhot       -- read N times in random order from 1% section of DB
//      seekrandom    -- N random seeks
//      seekordered   -- N ordered seeks
//      open          -- cost of opening a DB
//      crc32c        -- repeated crc32c of 4K of data
//   Meta operations:
//      compact     -- Compact the entire DB
//      stats       -- Print DB stats
//      sstables    -- Print sstable info
//      heapprofile -- Dump a heap profile (if supported by this port)
static const char* FLAGS_benchmarks =
    "fillseq,"
    "fillsync,"
    "fillrandom,"
    "overwrite,"
    "readrandom,"
    "readrandom,"  // Extra run to allow previous compactions to quiesce
    "readseq,"
    "readreverse,"
    "compact,"
    "readrandom,"
    "readseq,"
    "readreverse,"
    "fill100K,"
    "crc32c,"
    "snappycomp,"
    "snappyuncomp,"
    "ycsbfilldb,"
    "ycsbwarmup,"
    "ycsbwklda,"
    "ycsbwkldb,"
    "ycsbwkldc,"
    "ycsbwkldd,"
    "ycsbwklde,"
    "ycsbwkldf,"
    "ycsbfilldb_slabs,"
    "ycsb_slabs,"
    "twitter,"
    "recover"
    ;

// Set the warmup ratio
static double FLAGS_warmup_ratio = 0.2;

// Closed Loop or Open Loop
static int FLAGS_open_loop = false;

// Number of key/values to place in database
static int FLAGS_num = 8000000;

// Number of read operations to do.  If negative, do FLAGS_num reads.
static int FLAGS_reads = 10000000;

// Popularity file
static const char* FLAGS_pop_file = nullptr;

// Number of concurrent threads to run.
static int FLAGS_threads = 4;

// Number of concurrent threads to load.
static int FLAGS_load_threads = 8;

// Number of threads in thread pool
static int FLAGS_threadpool_num = 8;

// Size of each value
static int FLAGS_value_size = 980; // for 1024 object size

// Arrange to generate values that shrink to this fraction of
// their original size after compression
static double FLAGS_compression_ratio = 0.5;

// Print histogram of operation timings
static bool FLAGS_histogram = true;

// Count the number of string comparisons performed
static bool FLAGS_comparisons = false;

// Number of bytes to buffer in memtable before compacting
// (initialized to default value by "main")
static int FLAGS_write_buffer_size = 0;

// Number of bytes written to each file.
// (initialized to default value by "main")
static int FLAGS_max_file_size = 0;

// Approximate size of user data packed per block (before compression.
// (initialized to default value by "main")
static int FLAGS_block_size = 0;

// Number of bytes to use as a cache of uncompressed data.
// Negative means use default settings.
static int FLAGS_cache_size = -1;

// Maximum number of files to keep open at the same time (use default if == 0)
static int FLAGS_open_files = 0;

// Bloom filter bits per key.
// Negative means use default settings.
static int FLAGS_bloom_bits = -1;

// Common key prefix length.
static int FLAGS_key_prefix = 0;


// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
static bool FLAGS_use_existing_db = false;

// If true, reuse existing log/MANIFEST files when re-opening a database.
static bool FLAGS_reuse_logs = false;

// Use the db with the following name.
static const char* FLAGS_db = nullptr;

// Size of each key
static int FLAGS_key_size = 8;

// flag for ycsb uniform distribution
static bool FLAGS_YCSB_uniform_distribution = false;

// flag for zipfian distribution alpha param
static double FLAGS_YCSB_zipfian_alpha = 0.99;

// Write keys can come from a different distribution
// flag for ycsb uniform distribution
static bool FLAGS_YCSB_separate_write = false;
static bool FLAGS_YCSB_uniform_distribution_write = false;
static double FLAGS_YCSB_zipfian_alpha_write = 0.5;
static double FLAGS_read_ratio = 0.8;

// Set the number of warmup ops 
static uint64_t FLAGS_ycsb_warmup_ops = 20000000;

// Time in seconds for the random-ops tests to run. When 0 then num & reads determine the test duration
static int FLAGS_duration = 0;

// Check duration limit every x ops
// The default reduces the overhead of reading time with flash. With HDD, which
// offers much less throughput, however, this number better to be set to 1.
static int FLAGS_ops_between_duration_checks = 1000;

// Stats are reported every N operations when 
// this is greater than zero. When 0 the interval grows over time.
static int64_t FLAGS_stats_interval = 1000;

//Report stats every N seconds. This overrides stats_interval when both are > 0
static int64_t FLAGS_stats_interval_seconds = 0;

// Reports additional stats per interval when this is greater than 0
static int FLAGS_stats_per_interval = 0;

// Takes and report a snapshot of the current status of each thread
// when this is greater than 0.
static int FLAGS_thread_status_per_interval = 0;

// JIANAN
// configurable logging
static bool FLAGS_read_logging = false;

static bool FLAGS_migration_logging = false;

static int FLAGS_migration_policy = 1;
static int FLAGS_migration_rand_range_num = 1;
static int FLAGS_migration_rand_range_size = 1;
static int FLAGS_migration_metric = 1;
// TODO: use enum

//static ThreadPool pool(FLAGS_threadpool_num);
static ThreadPool pool(0); // HACK to disable threadpool

//static ThreadPool* pool1[4];

enum Operation
{
  PUT,
  GET
};

namespace leveldb {

namespace {
leveldb::Env* g_env = nullptr;

class CountComparator : public Comparator {
 public:
  CountComparator(const Comparator* wrapped) : wrapped_(wrapped) {}
  ~CountComparator() override {}
  int Compare(const Slice& a, const Slice& b) const override {
    count_.fetch_add(1, std::memory_order_relaxed);
    return wrapped_->Compare(a, b);
  }
  const char* Name() const override { return wrapped_->Name(); }
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    wrapped_->FindShortestSeparator(start, limit);
  }

  void FindShortSuccessor(std::string* key) const override {
    return wrapped_->FindShortSuccessor(key);
  }

  size_t comparisons() const { return count_.load(std::memory_order_relaxed); }

  void reset() { count_.store(0, std::memory_order_relaxed); }

 private:
  mutable std::atomic<size_t> count_{0};
  const Comparator* const wrapped_;
};

// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  Slice Generate(size_t len) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};

// TODO: why is this 8 vs 16?
class KeyBuffer {
 public:
  KeyBuffer() {
    assert(FLAGS_key_prefix < sizeof(buffer_));
    memset(buffer_, 'a', FLAGS_key_prefix);
  }
  KeyBuffer& operator=(KeyBuffer& other) = delete;
  KeyBuffer(KeyBuffer& other) = delete;

  void Set(int k) {
    std::snprintf(buffer_ + FLAGS_key_prefix,
                  sizeof(buffer_) - FLAGS_key_prefix, "%08d", k);
  }

  Slice slice() const { return Slice(buffer_, FLAGS_key_prefix + 8); }

 private:
  char buffer_[1024];
};

#if defined(__linux)
static Slice TrimSpace(Slice s) {
  size_t start = 0;
  while (start < s.size() && isspace(s[start])) {
    start++;
  }
  size_t limit = s.size();
  while (limit > start && isspace(s[limit - 1])) {
    limit--;
  }
  return Slice(s.data() + start, limit - start);
}
#endif

static void AppendWithSpace(std::string* str, Slice msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

// a class that reports stats to CSV file
/*class ReporterAgent {
 public:
  ReporterAgent(uint64_t report_interval_secs)
      : total_ops_done_(0),
        last_report_(0),
        report_interval_secs_(report_interval_secs),
        stop_(false) {
    reporting_thread_ = std::Thread([&]() { SleepAndReport(); });
  }
  ~ReporterAgent() {
    {
      std::unique_lock<std::mutex> lk(mutex_);
      stop_ = true;
      stop_cv_.notify_all();
    }
    reporting_thread_.join();
  }
  // thread safe
  void ReportFinishedOps(int64_t num_ops) {
    total_ops_done_.fetch_add(num_ops);
  }
 private:
  std::string Header() const { return "secs_elapsed,interval_qps"; }
  void SleepAndReport() {
    uint64_t kMicrosInSecond = 1000 * 1000;
    auto time_started = env_->NowMicros();
    while (true) {
      {
        std::unique_lock<std::mutex> lk(mutex_);
        if (stop_ ||
            stop_cv_.wait_for(lk, std::chrono::seconds(report_interval_secs_),
                              [&]() { return stop_; })) {
          // stopping
          break;
        }
        // else -> timeout, which means time for a report!
      }
      auto total_ops_done_snapshot = total_ops_done_.load();
      // round the seconds elapsed
      auto secs_elapsed =
          (g_env->NowMicros() - time_started + kMicrosInSecond / 2) /
          kMicrosInSecond;
      last_report_ = total_ops_done_snapshot;
    }
  }
  std::atomic<int64_t> total_ops_done_;
  int64_t last_report_;
  const uint64_t report_interval_secs_;
  std::Thread reporting_thread_;
  std::mutex mutex_;
  // will notify on stop
  std::condition_variable stop_cv_;
  bool stop_;
};
*/

enum OperationType : unsigned char {
  kRead = 0,
  kReadDisk,
  kWrite,
  kRmw,
  kWriteLarge,
  kDelete,
  kSeek,
  kMerge,
  kUpdate,
  kCompress,
  kUncompress,
  kCrc,
  kHash,
  kOthers
};

static std::unordered_map<OperationType, std::string, std::hash<unsigned char>>
                          OperationTypeString = {
  {kRead, "read"},
  {kReadDisk, "readDisk"},
  {kWrite, "write"},
  {kRmw, "rmw"},
  {kWriteLarge, "writeLarge"},
  {kDelete, "delete"},
  {kSeek, "seek"},
  {kMerge, "merge"},
  {kUpdate, "update"},
  {kCompress, "compress"},
  {kCompress, "uncompress"},
  {kCrc, "crc"},
  {kHash, "hash"},
  {kOthers, "op"}
};

class CombinedStats;
class StatsYCSB {
 private:
  int id_;
  uint64_t start_;
  uint64_t finish_;
  double seconds_;
  uint64_t done_;
  uint64_t last_report_done_;
  uint64_t next_report_;
  uint64_t bytes_;
  uint64_t last_op_finish_;
  uint64_t last_report_finish_;
  uint64_t warmup_finish_; 
  uint64_t num_warmup_keys_;
  std::unordered_map<OperationType, std::shared_ptr<Histogram>,
                     std::hash<unsigned char>> hist_;
  std::string message_;
  bool exclude_from_merge_;
  //ReporterAgent* reporter_agent_;  // does not own
  friend class CombinedStats;

 public:
  StatsYCSB() { Start(-1); }

  //void SetReporterAgent(ReporterAgent* reporter_agent) {
  //  reporter_agent_ = reporter_agent;
  //}

  void Start(int id) {
    id_ = id;
    next_report_ = FLAGS_stats_interval ? FLAGS_stats_interval : 100;
    last_op_finish_ = start_;
    hist_.clear();
    done_ = 0;
    last_report_done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = g_env->NowMicros();
    finish_ = start_;
    last_report_finish_ = start_;
    warmup_finish_ = start_;
    num_warmup_keys_ = 0;
    message_.clear();
    // When set, stats from this thread won't be merged with others.
    exclude_from_merge_ = false;
  }

  void Merge(const StatsYCSB& other) {
    if (other.exclude_from_merge_)
      return;

    for (auto it = other.hist_.begin(); it != other.hist_.end(); ++it) {
      auto this_it = hist_.find(it->first);
      if (this_it != hist_.end()) {
        this_it->second->Merge(*(other.hist_.at(it->first)));
      } else {
        hist_.insert({ it->first, it->second });
      }
    }

    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = g_env->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) {
    AppendWithSpace(&message_, msg);
  }

  void SetId(int id) { id_ = id; }
  void SetExcludeFromMerge() { exclude_from_merge_ = true; }

  /*void PrintThreadStatus() {
    std::vector<ThreadStatus> thread_list;
    g_env->GetThreadList(&thread_list);
    fprintf(stderr, "\n%18s %10s %12s %20s %13s %45s %12s %s\n",
        "ThreadID", "ThreadType", "cfName", "Operation",
        "ElapsedTime", "Stage", "State", "OperationProperties");
    int64_t current_time = 0;
    Env::Default()->GetCurrentTime(&current_time);
    for (auto ts : thread_list) {
      fprintf(stderr, "%18" PRIu64 " %10s %12s %20s %13s %45s %12s",
          ts.thread_id,
          ThreadStatus::GetThreadTypeName(ts.thread_type).c_str(),
          ts.cf_name.c_str(),
          ThreadStatus::GetOperationName(ts.operation_type).c_str(),
          ThreadStatus::MicrosToString(ts.op_elapsed_micros).c_str(),
          ThreadStatus::GetOperationStageName(ts.operation_stage).c_str(),
          ThreadStatus::GetStateName(ts.state_type).c_str());
      auto op_properties = ThreadStatus::InterpretOperationProperties(
          ts.operation_type, ts.op_properties);
      for (const auto& op_prop : op_properties) {
        fprintf(stderr, " %s %" PRIu64" |",
            op_prop.first.c_str(), op_prop.second);
      }
      fprintf(stderr, "\n");
    }
  }*/

  void ResetLastOpTime() {
    // Set to now to avoid latency from calls to SleepForMicroseconds
    last_op_finish_ = g_env->NowMicros();
  }

  void FinishedOps_OL(int64_t num_ops, uint64_t op_lat, int tid=-1, enum OperationType op_type = kOthers) {
    if (FLAGS_histogram) {
      uint64_t now = g_env->NowMicros();
      // uint64_t micros = now - last_op_finish_;
      uint64_t micros = op_lat;
      //fprintf(stderr, "micros %llu\n", micros);

      if (hist_.find(op_type) == hist_.end())
      {
        auto hist_temp = std::make_shared<Histogram>();
	      hist_temp->Clear();
        hist_.insert({op_type, std::move(hist_temp)});
      }
      hist_[op_type]->Add(micros);

      if (micros > 20000 && !FLAGS_stats_interval) {
        fprintf(stderr, "long op: %" PRIu64 " micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
      //fprintf(stderr, "1 Adding to hist tid %d op_type %d time %d\n", tid, op_type, micros);
    }

    done_ += num_ops;
    if (done_ >= next_report_) {
      if (!FLAGS_stats_interval) {
        if      (next_report_ < 1000)   next_report_ += 100;
        else if (next_report_ < 5000)   next_report_ += 500;
        else if (next_report_ < 10000)  next_report_ += 1000;
        else if (next_report_ < 50000)  next_report_ += 5000;
        else if (next_report_ < 100000) next_report_ += 10000;
        else if (next_report_ < 500000) next_report_ += 50000;
        else                            next_report_ += 100000;
        fprintf(stderr, "... finished %" PRIu64 " ops%30s\r", done_, "");
      } else {
        uint64_t now = g_env->NowMicros();
        int64_t usecs_since_last = now - last_report_finish_;

        // Determine whether to print status where interval is either
        // each N operations or each N seconds.

        if (FLAGS_stats_interval_seconds &&
            usecs_since_last < (FLAGS_stats_interval_seconds * 1000000)) {
          // Don't check again for this many operations
          next_report_ += FLAGS_stats_interval;

        } else {
          using namespace std::chrono;
          uint64_t microseconds_since_epoch = duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
          fprintf(stderr,
                  "microsecond %llu ... thread %d: (%" PRIu64 ",%" PRIu64 ") ops and "
                  "(%.1f,%.1f) ops/second in (%.6f,%.6f) seconds\n",
                  microseconds_since_epoch,
                  tid,
                  done_ - last_report_done_, done_,
                  (done_ - last_report_done_) /
                  (usecs_since_last / 1000000.0),
                  done_ / ((now - start_) / 1000000.0),
                  (now - last_report_finish_) / 1000000.0,
                  (now - start_) / 1000000.0);

          for (auto it = hist_.begin(); it != hist_.end(); ++it) {
                fprintf(stderr, "Thread %d Microseconds per %s %d :\n%.200s\n", tid,
                OperationTypeString[it->first].c_str(), id_,
                it->second->ToString().c_str());
          }
          next_report_ += FLAGS_stats_interval;
          last_report_finish_ = now;
          last_report_done_ = done_;
        }
      }

      fflush(stderr);
    }
  }

  void FinishedOps_OL_Warmup(int64_t num_ops, uint64_t op_lat, int tid=-1, enum OperationType op_type = kOthers) {
    if (FLAGS_histogram) {
      uint64_t now = g_env->NowMicros();
      uint64_t micros = op_lat;
      //fprintf(stderr, "micros %llu\n", micros);

      ////////////
      // JIANAN: only add latency stats to the histogram after the warmup
      if (done_ >= num_warmup_keys_) {
        if (hist_.find(op_type) == hist_.end())
        {
          auto hist_temp = std::make_shared<Histogram>();
          hist_temp->Clear();
          hist_.insert({op_type, std::move(hist_temp)});
        }
        hist_[op_type]->Add(micros);
	//fprintf(stderr, "2 Adding to hist tid %d op_type %d time %d\n", tid, op_type, micros);
      }
      ////////////

      if (micros > 20000 && !FLAGS_stats_interval) {
        fprintf(stderr, "long op: %" PRIu64 " micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_ += num_ops;
    if (done_ >= next_report_) {
      if (!FLAGS_stats_interval) {
        if      (next_report_ < 1000)   next_report_ += 100;
        else if (next_report_ < 5000)   next_report_ += 500;
        else if (next_report_ < 10000)  next_report_ += 1000;
        else if (next_report_ < 50000)  next_report_ += 5000;
        else if (next_report_ < 100000) next_report_ += 10000;
        else if (next_report_ < 500000) next_report_ += 50000;
        else                            next_report_ += 100000;
        fprintf(stderr, "... finished %" PRIu64 " ops%30s\r", done_, "");
      } else {
        uint64_t now = g_env->NowMicros();
        int64_t usecs_since_last = now - last_report_finish_;

        // Determine whether to print status where interval is either
        // each N operations or each N seconds.
        if (FLAGS_stats_interval_seconds &&
            usecs_since_last < (FLAGS_stats_interval_seconds * 1000000)) {
          // Don't check again for this many operations
          next_report_ += FLAGS_stats_interval;

        } else {
          using namespace std::chrono;
          uint64_t microseconds_since_epoch = duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
          ////////////
          // JIANAN: skip average throughput report during warmup, and update warmup_finish_
          if (done_ <= num_warmup_keys_) {
            fprintf(stderr,
                  "microsecond %llu ... thread %d: (%" PRIu64 ",%" PRIu64 ") ops and "
                  "(%.1f,%s) ops/second in (%.6f,%.6f) seconds\n",
                  microseconds_since_epoch,
                  tid,
                  done_ - last_report_done_, done_,
                  (done_ - last_report_done_) /
                  (usecs_since_last / 1000000.0),
                  "-nan",
                  (now - last_report_finish_) / 1000000.0,
                  (now - start_) / 1000000.0);
            warmup_finish_ = now;
          } 
          else {
            fprintf(stderr,
                  "microsecond %llu ... thread %d: (%" PRIu64 ",%" PRIu64 ") ops and "
                  "(%.1f,%.1f) ops/second in (%.6f,%.6f) seconds\n",
                  microseconds_since_epoch,
                  tid,
                  done_ - last_report_done_, done_,
                  (done_ - last_report_done_) /
                  (usecs_since_last / 1000000.0),
                  (done_ - num_warmup_keys_) / ((now - warmup_finish_) / 1000000.0), // TODO: change this
                  (now - last_report_finish_) / 1000000.0,
                  (now - start_) / 1000000.0);

            for (auto it = hist_.begin(); it != hist_.end(); ++it) {
                  fprintf(stderr, "Microseconds per %s %d :\n%.200s\n",
                  OperationTypeString[it->first].c_str(), id_,
                  it->second->ToString().c_str());
            }
          }
          next_report_ += FLAGS_stats_interval;
          last_report_finish_ = now;
          last_report_done_ = done_;
        }
      }
      fflush(stderr);
    }
  }

  void SetNumWarmupKeys(uint64_t num_warmup) {
		// floor num_warmup_keys to the closest multiple of FLAGS_stats_interval
    num_warmup_keys_ = (num_warmup/FLAGS_stats_interval) * FLAGS_stats_interval;
  }

  void FinishedOps_Warmup(DB* db, int64_t num_ops, int tid=-1,
                   enum OperationType op_type = kOthers) {

    if (FLAGS_histogram) {
      uint64_t now = g_env->NowMicros();
      uint64_t micros = now - last_op_finish_;

      if (micros > 20000 && !FLAGS_stats_interval) {
        fprintf(stderr, "long op: %" PRIu64 " micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;

      ////////////
      // JIANAN: only add latency stats to the histogram after the warmup
      if (done_ >= num_warmup_keys_) {
        if (hist_.find(op_type) == hist_.end())
        {
          auto hist_temp = std::make_shared<Histogram>();
          hist_temp->Clear();
          hist_.insert({op_type, std::move(hist_temp)});
        }
        hist_[op_type]->Add(micros);
	//fprintf(stderr, "3 Adding to hist tid %d op_type %d time %d\n", tid, op_type, micros);
      }
      //fprintf(stderr, "3a tid %d op_type %d time %d done %d num_warmup %d\n", tid, op_type, micros, done_, num_warmup_keys_);
      ////////////
    }
    //fprintf(stderr, "3b tid %d op_type %d done %d num_warmup %d\n", tid, op_type, done_, num_warmup_keys_);

    done_ += num_ops;
    if (done_ >= next_report_) {
      if (!FLAGS_stats_interval) {
        if      (next_report_ < 1000)   next_report_ += 100;
        else if (next_report_ < 5000)   next_report_ += 500;
        else if (next_report_ < 10000)  next_report_ += 1000;
        else if (next_report_ < 50000)  next_report_ += 5000;
        else if (next_report_ < 100000) next_report_ += 10000;
        else if (next_report_ < 500000) next_report_ += 50000;
        else                            next_report_ += 100000;
        fprintf(stderr, "... finished %" PRIu64 " ops%30s\r", done_, "");
      } else {
        uint64_t now = g_env->NowMicros();
        int64_t usecs_since_last = now - last_report_finish_;

        // Determine whether to print status where interval is either
        // each N operations or each N seconds.
        if (FLAGS_stats_interval_seconds &&
            usecs_since_last < (FLAGS_stats_interval_seconds * 1000000)) {
          // Don't check again for this many operations
          next_report_ += FLAGS_stats_interval;

        } else {
          using namespace std::chrono;
          uint64_t microseconds_since_epoch = duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
          //fprintf(stderr, "warmup phase keys %llu, done: %llu\n", num_warmup_keys_, done_);

          ////////////
          // JIANAN: skip average throughput report during warmup, and update warmup_finish_
          if (done_ <= num_warmup_keys_) {
						//fprintf(stderr, "thread %d, last_report_finish %llu, warmup_finish: %llu\n", tid, last_report_finish_, warmup_finish_);
            fprintf(stderr,
                  "microsecond %llu ... thread %d: (%" PRIu64 ",%" PRIu64 ") ops and "
                  "(%.1f,%s) ops/second in (%.6f,%.6f) seconds\n",
                  microseconds_since_epoch,
                  tid,
                  done_ - last_report_done_, done_,
                  (done_ - last_report_done_) /
                  (usecs_since_last / 1000000.0),
                  "-nan",
                  (now - last_report_finish_) / 1000000.0,
                  (now - start_) / 1000000.0);
            warmup_finish_ = now;
          } 
          else {
						//fprintf(stderr, "thread %d, last_report_finish %llu, warmup_finish: %llu\n", tid, last_report_finish_, warmup_finish_);
						//fprintf(stderr, "thread %d, last_report_done %llu, warmup_keys_done: %llu\n", tid, last_report_done_, num_warmup_keys_);
            fprintf(stderr,
                  "microsecond %llu ... thread %d: (%" PRIu64 ",%" PRIu64 ") ops and "
                  "(%.1f,%.1f) ops/second in (%.6f,%.6f) seconds\n",
                  microseconds_since_epoch,
                  tid,
                  done_ - last_report_done_, done_,
                  (done_ - last_report_done_) /
                  (usecs_since_last / 1000000.0),
                  (done_ - num_warmup_keys_) / ((now - warmup_finish_) / 1000000.0), // change this
                  (now - last_report_finish_) / 1000000.0,
                  (now - start_) / 1000000.0);
						// JIANAN: TODO: uncomment this
            //for (auto it = hist_.begin(); it != hist_.end(); ++it) {
            //      fprintf(stderr, "Microseconds per %s %d :\n%.200s\n",
            //      OperationTypeString[it->first].c_str(), id_,
            //      it->second->ToString().c_str());
            //}
          }
          next_report_ += FLAGS_stats_interval;
          last_report_finish_ = now;
          last_report_done_ = done_;
        }
      }
      fflush(stderr);
    }
  }

  void FinishedOps(DB* db, int64_t num_ops, int tid=-1,
                   enum OperationType op_type = kOthers) {
    //if (reporter_agent_) {
    //  reporter_agent_->ReportFinishedOps(num_ops);
    //}
    if (FLAGS_histogram) {
      uint64_t now = g_env->NowMicros();
      uint64_t micros = now - last_op_finish_;

      if (hist_.find(op_type) == hist_.end())
      {
        auto hist_temp = std::make_shared<Histogram>();
	      hist_temp->Clear();
        hist_.insert({op_type, std::move(hist_temp)});
      }
      hist_[op_type]->Add(micros);

      if (micros > 20000 && !FLAGS_stats_interval) {
        fprintf(stderr, "long op: %" PRIu64 " micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
      //fprintf(stderr, "4 Adding to hist tid %d op_type %d time %d\n", tid, op_type, micros);
    }

    done_ += num_ops;
    if (done_ >= next_report_) {
      if (!FLAGS_stats_interval) {
        if      (next_report_ < 1000)   next_report_ += 100;
        else if (next_report_ < 5000)   next_report_ += 500;
        else if (next_report_ < 10000)  next_report_ += 1000;
        else if (next_report_ < 50000)  next_report_ += 5000;
        else if (next_report_ < 100000) next_report_ += 10000;
        else if (next_report_ < 500000) next_report_ += 50000;
        else                            next_report_ += 100000;
        fprintf(stderr, "... finished %" PRIu64 " ops%30s\r", done_, "");
      } else {
        uint64_t now = g_env->NowMicros();
        int64_t usecs_since_last = now - last_report_finish_;

        // Determine whether to print status where interval is either
        // each N operations or each N seconds.

        if (FLAGS_stats_interval_seconds &&
            usecs_since_last < (FLAGS_stats_interval_seconds * 1000000)) {
          // Don't check again for this many operations
          next_report_ += FLAGS_stats_interval;

        } else {
          using namespace std::chrono;
          uint64_t microseconds_since_epoch = duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
          fprintf(stderr,
                  "microsecond %llu ... thread %d: (%" PRIu64 ",%" PRIu64 ") ops and "
                  "(%.1f,%.1f) ops/second in (%.6f,%.6f) seconds\n",
                  microseconds_since_epoch,
                  tid,
                  done_ - last_report_done_, done_,
                  (done_ - last_report_done_) /
                  (usecs_since_last / 1000000.0),
                  done_ / ((now - start_) / 1000000.0),
                  (now - last_report_finish_) / 1000000.0,
                  (now - start_) / 1000000.0);

          for (auto it = hist_.begin(); it != hist_.end(); ++it) {
                fprintf(stderr, "Microseconds per %s %d :\n%.200s\n",
                OperationTypeString[it->first].c_str(), id_,
                it->second->ToString().c_str());
          }

          //TODO: maybe reset the histogram?

          /*if (id_ == 1 && FLAGS_stats_per_interval) {
            std::string stats;
            if (db_with_cfh && db_with_cfh->num_created.load()) {
              for (size_t i = 0; i < db_with_cfh->num_created.load(); ++i) {
                if (db->GetProperty(db_with_cfh->cfh[i], "rocksdb.cfstats",
                                    &stats))
                  fprintf(stderr, "%s\n", stats.c_str());
                if (FLAGS_show_table_properties) {
                  for (int level = 0; level < FLAGS_num_levels; ++level) {
                    if (db->GetProperty(
                            db_with_cfh->cfh[i],
                            "rocksdb.aggregated-table-properties-at-level" +
                                ToString(level),
                            &stats)) {
                      if (stats.find("# entries=0") == std::string::npos) {
                        fprintf(stderr, "Level[%d]: %s\n", level,
                                stats.c_str());
                      }
                    }
                  }
                }
              }
            } else if (db) {
              if (db->GetProperty("rocksdb.stats", &stats)) {
                fprintf(stderr, "%s\n", stats.c_str());
              }
              if (FLAGS_show_table_properties) {
                for (int level = 0; level < FLAGS_num_levels; ++level) {
                  if (db->GetProperty(
                          "rocksdb.aggregated-table-properties-at-level" +
                              ToString(level),
                          &stats)) {
                    if (stats.find("# entries=0") == std::string::npos) {
                      fprintf(stderr, "Level[%d]: %s\n", level, stats.c_str());
                    }
                  }
                }
              }
            }
          }*/

          next_report_ += FLAGS_stats_interval;
          last_report_finish_ = now;
          last_report_done_ = done_;
        }
      }
      /*if (id_ ==1 && FLAGS_thread_status_per_interval) {
        PrintThreadStatus();
      }*/
      fflush(stderr);
    }
  }

  void AddBytes(int64_t n) {
    bytes_ += n;
  }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedOps().
    if (done_ < 1) done_ = 1;

    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      double elapsed = (finish_ - start_) * 1e-6;
      char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s",
               (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);
    double elapsed = (finish_ - start_) * 1e-6;
    double throughput = (double)done_/elapsed;

    fprintf(stdout, "%-12s : %11.3f micros/op %ld ops/sec;%s%s\n",
            name.ToString().c_str(),
            elapsed * 1e6 / done_,
            (long)throughput,
            (extra.empty() ? "" : " "),
            extra.c_str());
    if (FLAGS_histogram) {
      // jianan
      fprintf(stderr, "LATENCY HISTOGRAM\n");
      for (auto it = hist_.begin(); it != hist_.end(); ++it) {
        fprintf(stdout, "Microseconds per %s:\n%s\n",
                OperationTypeString[it->first].c_str(),
                it->second->ToString().c_str());
      }
    }
    /*if (FLAGS_report_file_operations) {
      ReportFileOpEnv* env = static_cast<ReportFileOpEnv*>(FLAGS_env);
      ReportFileOpCounters* counters = env->counters();
      fprintf(stdout, "Num files opened: %d\n",
              counters->open_counter_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num Read(): %d\n",
              counters->read_counter_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num Append(): %d\n",
              counters->append_counter_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num bytes read: %" PRIu64 "\n",
              counters->bytes_read_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num bytes written: %" PRIu64 "\n",
              counters->bytes_written_.load(std::memory_order_relaxed));
      env->reset();
    }*/
    fflush(stdout);
  }
};

class CombinedStats {
 public:
  void AddStats(const StatsYCSB& stat) {
    uint64_t total_ops = stat.done_;
    uint64_t total_bytes_ = stat.bytes_;
    double elapsed;

    if (total_ops < 1) {
      total_ops = 1;
    }

    elapsed = (stat.finish_ - stat.start_) * 1e-6;
    throughput_ops_.emplace_back(total_ops / elapsed);

    if (total_bytes_ > 0) {
      double mbs = (total_bytes_ / 1048576.0);
      throughput_mbs_.emplace_back(mbs / elapsed);
    }
  }

  void Report(const std::string& bench_name) {
    const char* name = bench_name.c_str();
    int num_runs = static_cast<int>(throughput_ops_.size());

    if (throughput_mbs_.size() == throughput_ops_.size()) {
      fprintf(stdout,
              "%s [AVG    %d runs] : %d ops/sec; %6.1f MB/sec\n"
              "%s [MEDIAN %d runs] : %d ops/sec; %6.1f MB/sec\n",
              name, num_runs, static_cast<int>(CalcAvg(throughput_ops_)),
              CalcAvg(throughput_mbs_), name, num_runs,
              static_cast<int>(CalcMedian(throughput_ops_)),
              CalcMedian(throughput_mbs_));
    } else {
      fprintf(stdout,
              "%s [AVG    %d runs] : %d ops/sec\n"
              "%s [MEDIAN %d runs] : %d ops/sec\n",
              name, num_runs, static_cast<int>(CalcAvg(throughput_ops_)), name,
              num_runs, static_cast<int>(CalcMedian(throughput_ops_)));
    }
  }

 private:
  double CalcAvg(std::vector<double> data) {
    double avg = 0;
    for (double x : data) {
      avg += x;
    }
    avg = avg / data.size();
    return avg;
  }

  double CalcMedian(std::vector<double> data) {
    assert(data.size() > 0);
    std::sort(data.begin(), data.end());

    size_t mid = data.size() / 2;
    if (data.size() % 2 == 1) {
      // Odd number of entries
      return data[mid];
    } else {
      // Even number of entries
      return (data[mid] + data[mid - 1]) / 2;
    }
  }

  std::vector<double> throughput_ops_;
  std::vector<double> throughput_mbs_;
};

class Stats {
 private:
  double start_;
  double finish_;
  double seconds_;
  int done_;
  int next_report_;
  int64_t bytes_;
  double last_op_finish_;
  Histogram hist_;
  std::string message_;

 public:
  Stats() { Start(); }

  void Start() {
    next_report_ = 100;
    hist_.Clear();
    done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    message_.clear();
    start_ = finish_ = last_op_finish_ = g_env->NowMicros();
  }

  void Merge(const Stats& other) {
    hist_.Merge(other.hist_);
    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = g_env->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) { AppendWithSpace(&message_, msg); }

  void FinishedSingleOp() {
    if (FLAGS_histogram) {
      double now = g_env->NowMicros();
      double micros = now - last_op_finish_;
      hist_.Add(micros);
      if (micros > 20000) {
        std::fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
        std::fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_++;
    if (done_ >= next_report_) {
      if (next_report_ < 1000)
        next_report_ += 100;
      else if (next_report_ < 5000)
        next_report_ += 500;
      else if (next_report_ < 10000)
        next_report_ += 1000;
      else if (next_report_ < 50000)
        next_report_ += 5000;
      else if (next_report_ < 100000)
        next_report_ += 10000;
      else if (next_report_ < 500000)
        next_report_ += 50000;
      else
        next_report_ += 100000;
      std::fprintf(stderr, "... finished %d ops%30s\f", done_, "\n");
      std::fflush(stderr);
    }
  }

  void AddBytes(int64_t n) { bytes_ += n; }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;

    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      double elapsed = (finish_ - start_) * 1e-6;
      char rate[100];
      std::snprintf(rate, sizeof(rate), "%6.1f MB/s",
                    (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);

    std::fprintf(stdout, "%-12s : %11.3f micros/op;%s%s\n",
                 name.ToString().c_str(), seconds_ * 1e6 / done_,
                 (extra.empty() ? "" : " "), extra.c_str());
    if (FLAGS_histogram) {
      std::fprintf(stdout, "Microseconds per op:\n%s\n",
                   hist_.ToString().c_str());
    }
    std::fflush(stdout);
  }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  port::Mutex mu;
  port::CondVar cv GUARDED_BY(mu);
  int total GUARDED_BY(mu);

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  int num_initialized GUARDED_BY(mu);
  int num_done GUARDED_BY(mu);
  bool start GUARDED_BY(mu);

  SharedState(int total)
      : cv(&mu), total(total), num_initialized(0), num_done(0), start(false) {}
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;      // 0..n-1 when running in n threads
  Random rand;  // Has different seeds for different threads
  Stats stats;
  StatsYCSB stats_ycsb;
  SharedState* shared;

  std::mutex mtx;
  std::condition_variable cv;
  uint64_t reads_done;
  uint64_t writes_done;
  uint64_t total_ops;
  
  ThreadState(int index, int seed) : tid(index), rand(seed), shared(nullptr) {}
};

}  // namespace

class Duration {
 public:
  Duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0) {
    max_seconds_ = max_seconds;
    max_ops_= max_ops;
    ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
    ops_ = 0;
    start_at_ = g_env->NowMicros();
  }

  int64_t GetStage() { return std::min(ops_, max_ops_ - 1) / ops_per_stage_; }

  bool Done(int64_t increment) {
    if (increment <= 0) increment = 1;    // avoid Done(0) and infinite loops
    ops_ += increment;

    if (max_seconds_) {
      // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
      auto granularity = FLAGS_ops_between_duration_checks;
      if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
        uint64_t now = g_env->NowMicros();
        return ((now - start_at_) / 1000000) >= max_seconds_;
      } else {
        return false;
      }
    } else {
      return ops_ > max_ops_;
    }
  }

 private:
  uint64_t max_seconds_;
  int64_t max_ops_;
  int64_t ops_per_stage_;
  int64_t ops_;
  uint64_t start_at_;
};

class Benchmark {
 private:
  Cache* cache_;
  const FilterPolicy* filter_policy_;
  DB* db_;
  int num_;
  int value_size_;
  int key_size_;
  int entries_per_batch_;
  WriteOptions write_options_;
  int reads_;
  int heap_counter_;
  CountComparator count_comparator_;
  int total_thread_count_;

  int64_t deletes_;
  int64_t writes_;
  int64_t readwrites_;
  std::vector<uint64_t> **threadKeys_;
  std::vector<uint64_t> **threadWriteKeys_;
  std::vector<uint8_t> **threadOps_; // thread operations 0=put, 1=get
  std::vector<uint16_t> **threadValSizes_; // put operation value size

  // used/updated by ThreadPool
  RandomGenerator gen_;
  std::atomic<uint64_t> reads_done_;
  std::atomic<uint64_t> writes_done_;

  void PrintHeader() {
    const int kKeySize = 16 + FLAGS_key_prefix;
    PrintEnvironment();
    std::fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
    std::fprintf(
        stdout, "Values:     %d bytes each (%d bytes after compression)\n",
        FLAGS_value_size,
        static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
    std::fprintf(stdout, "Entries:    %d\n", num_);
    std::fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
                 ((static_cast<int64_t>(kKeySize + FLAGS_value_size) * num_) /
                  1048576.0));
    std::fprintf(
        stdout, "FileSize:   %.1f MB (estimated)\n",
        (((kKeySize + FLAGS_value_size * FLAGS_compression_ratio) * num_) /
         1048576.0));
    PrintWarnings();
    std::fprintf(stdout, "------------------------------------------------\n");
  }

  void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    std::fprintf(
        stdout,
        "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
    std::fprintf(
        stdout,
        "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif

    // See if snappy is working by attempting to compress a compressible string
    const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
    std::string compressed;
    if (!port::Snappy_Compress(text, sizeof(text), &compressed)) {
      std::fprintf(stdout, "WARNING: Snappy compression is not enabled\n");
    } else if (compressed.size() >= sizeof(text)) {
      std::fprintf(stdout, "WARNING: Snappy compression is not effective\n");
    }
  }

  void PrintEnvironment() {
    std::fprintf(stderr, "LevelDB:    version %d.%d\n", kMajorVersion,
                 kMinorVersion);

#if defined(__linux)
    time_t now = time(nullptr);
    std::fprintf(stderr, "Date:       %s",
                 ctime(&now));  // ctime() adds newline

    FILE* cpuinfo = std::fopen("/proc/cpuinfo", "r");
    if (cpuinfo != nullptr) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
        const char* sep = strchr(line, ':');
        if (sep == nullptr) {
          continue;
        }
        Slice key = TrimSpace(Slice(line, sep - 1 - line));
        Slice val = TrimSpace(Slice(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.ToString();
        } else if (key == "cache size") {
          cache_size = val.ToString();
        }
      }
      std::fclose(cpuinfo);
      std::fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      std::fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#endif
  }

 public:
  Benchmark()
      : cache_(FLAGS_cache_size > 0 ? NewLRUCache(FLAGS_cache_size) : nullptr),
        filter_policy_(FLAGS_bloom_bits >= 0
                           ? NewBloomFilterPolicy(FLAGS_bloom_bits)
                           : nullptr),
        db_(nullptr),
        num_(FLAGS_num),
        value_size_(FLAGS_value_size),
        key_size_(FLAGS_key_size),
        entries_per_batch_(1),
        reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
        heap_counter_(0),
        count_comparator_(BytewiseComparator()),
        total_thread_count_(0), 
        threadKeys_(nullptr), 
        threadWriteKeys_(nullptr), 
        threadOps_(nullptr),
        threadValSizes_(nullptr) {
    std::vector<std::string> files;
    g_env->GetChildren(FLAGS_db, &files);
    for (size_t i = 0; i < files.size(); i++) {
      if (Slice(files[i]).starts_with("heap-")) {
        g_env->RemoveFile(std::string(FLAGS_db) + "/" + files[i]);
      }
    }
    if (!FLAGS_use_existing_db) {
      DestroyDB(FLAGS_db, Options());
    }
  }

  ~Benchmark() {
    delete db_;
    delete cache_;
    delete filter_policy_;
  }

  Slice AllocateKey(std::unique_ptr<const char[]>* key_guard) {
    char* data = new char[key_size_];
    const char* const_data = data;
    key_guard->reset(const_data);
    return Slice(key_guard->get(), key_size_);
  }

  // Generate key according to the given specification and random number.
  // The resulting key will have the following format (if keys_per_prefix_
  // is positive), extra trailing bytes are either cut off or padded with '0'.
  // The prefix value is derived from key value.
  //   ----------------------------
  //   | prefix 00000 | key 00000 |
  //   ----------------------------
  // If keys_per_prefix_ is 0, the key is simply a binary representation of
  // random number followed by trailing '0's
  //   ----------------------------
  //   |        key 00000         |
  //   ----------------------------
  void GenerateKeyFromInt(uint64_t v, int64_t num_keys, Slice* key) {
    char* start = const_cast<char*>(key->data());
    char* pos = start;
    // ASH: hack
    int64_t keys_per_prefix = 0;
    int32_t prefix_size = 0;
    if (keys_per_prefix > 0) {
      int64_t num_prefix = num_keys / keys_per_prefix;
      int64_t prefix = v % num_prefix;
      int bytes_to_fill = std::min(prefix_size, 8);
      if (port::kLittleEndian) {
        for (int i = 0; i < bytes_to_fill; ++i) {
          pos[i] = (prefix >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
        }
      } else {
        memcpy(pos, static_cast<void*>(&prefix), bytes_to_fill);
      }
      if (prefix_size > 8) {
        // fill the rest with 0s
        memset(pos + 8, '0', prefix_size - 8);
      }
      pos += prefix_size;
    }

    int bytes_to_fill = std::min(key_size_ - static_cast<int>(pos - start), 8);
    if (port::kLittleEndian) {
      for (int i = 0; i < bytes_to_fill; ++i) {
        pos[i] = (v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
      }
    } else {
      memcpy(pos, static_cast<void*>(&v), bytes_to_fill);
    }
    pos += bytes_to_fill;
    if (key_size_ > pos - start) {
      memset(pos, '0', key_size_ - (pos - start));
    }
    //fprintf(stderr, "SSS v %lu num_keys %d key_size %d\n", v, num_keys, key_size_);
    //fprintf(stderr, "PPP key %s size %d\n", key->ToString(true).c_str(), key->size());
  }

  void Run() {
    PrintHeader();
    Open();

    const char* benchmarks = FLAGS_benchmarks;
    while (benchmarks != nullptr) {
      const char* sep = strchr(benchmarks, ',');
      Slice name;
      if (sep == nullptr) {
        name = benchmarks;
        benchmarks = nullptr;
      } else {
        name = Slice(benchmarks, sep - benchmarks);
        benchmarks = sep + 1;
      }

      // Reset parameters that may be overridden below
      num_ = FLAGS_num;
      reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
      value_size_ = FLAGS_value_size;
      key_size_ = FLAGS_key_size;
      entries_per_batch_ = 1;
      write_options_ = WriteOptions();

      void (Benchmark::*method)(ThreadState*) = nullptr;
      bool fresh_db = false;
      int num_threads = FLAGS_threads;

      if (name == Slice("open")) {
        method = &Benchmark::OpenBench;
        num_ /= 10000;
        if (num_ < 1) num_ = 1;
      } else if (name == Slice("fillseq")) {
        fresh_db = true;
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillbatch")) {
        fresh_db = true;
        entries_per_batch_ = 1000;
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillrandom")) {
        fresh_db = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("overwrite")) {
        fresh_db = false;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fillsync")) {
        fresh_db = true;
        num_ /= 1000;
        write_options_.sync = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fill100K")) {
        fresh_db = true;
        num_ /= 1000;
        value_size_ = 100 * 1000;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("readseq")) {
        method = &Benchmark::ReadSequential;
      } else if (name == Slice("readreverse")) {
        method = &Benchmark::ReadReverse;
      } else if (name == Slice("readrandom")) {
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("readmissing")) {
        method = &Benchmark::ReadMissing;
      } else if (name == Slice("seekrandom")) {
        method = &Benchmark::SeekRandom;
      } else if (name == Slice("seekordered")) {
        method = &Benchmark::SeekOrdered;
      } else if (name == Slice("readhot")) {
        method = &Benchmark::ReadHot;
      } else if (name == Slice("readrandomsmall")) {
        reads_ /= 1000;
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("deleteseq")) {
        method = &Benchmark::DeleteSeq;
      } else if (name == Slice("deleterandom")) {
        method = &Benchmark::DeleteRandom;
      } else if (name == Slice("readwhilewriting")) {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::ReadWhileWriting;
      } else if (name == Slice("compact")) {
        method = &Benchmark::Compact;
      } else if (name == Slice("crc32c")) {
        method = &Benchmark::Crc32c;
      } else if (name == Slice("snappycomp")) {
        method = &Benchmark::SnappyCompress;
      } else if (name == Slice("snappyuncomp")) {
        method = &Benchmark::SnappyUncompress;
      } else if (name == Slice("heapprofile")) {
        HeapProfile();
      } else if (name == Slice("stats")) {
        PrintStats("leveldb.stats");
      } else if (name == Slice("sstables")) {
        PrintStats("leveldb.sstables");
      } else if (name == "recover") {
        fresh_db = false;
        FLAGS_use_existing_db = true;
        method = &Benchmark::RECOVER;
      } else if (name == "ycsbfilldb") {
        fresh_db = true;
        // HACK for loading
				num_threads = FLAGS_load_threads;
        //num_threads = 4;
        //FLAGS_threads = 4;
        //fprintf(stderr, "YCSB FillDB close loop\n");
        method = &Benchmark::YCSBFillDB_CL;
        //if (FLAGS_open_loop == false){
        //  fprintf(stderr, "YCSB FillDB close loop\n");
        //  method = &Benchmark::YCSBFillDB_CL;
	      //} else {
        //  fprintf(stderr, "YCSB FillDB open loop\n");
        //  method = &Benchmark::YCSBFillDB_OL;
        //}
      } else if (name == "ycsbwarmup") {
        // use YCSB-A as a warmup workload for read-dominated workloads
        FLAGS_use_existing_db = true;
        db_->SetDbMode(false);
        if (FLAGS_migration_metric == 2) {
          fprintf(stderr, "DEBUG: db_bench set correct bucket keys\n");
          db_->SetCorrectBucketTotalKeys();
        }
        method = &Benchmark::YCSBWorkloadA_CL_Warmup;
      } else if ((name == "ycsbwklda") || (name == "ycsbwkldb") || (name == "ycsbwkldc") || (name == "ycsbwkldd"))  {
        FLAGS_use_existing_db = true;
        if (name == "ycsbwkldd") {
          FLAGS_YCSB_separate_write = true;
        }
        db_->SetDbMode(false); // load flag is false
        if (FLAGS_migration_metric == 2) {
          fprintf(stderr, "DEBUG: db_bench set correct bucket keys\n");
          db_->SetCorrectBucketTotalKeys();
        }
        if (FLAGS_open_loop == false){
          fprintf(stderr, "YCSB Workload A close loop\n");
          method = &Benchmark::YCSBWorkloadA_CL;
        } 
        else {
          fprintf(stderr, "YCSB Workload A open loop\n");
          method = &Benchmark::YCSBWorkloadA_OL;
					//method = &Benchmark::YCSBWorkloadA_OL_Batch;
					//method = &Benchmark::YCSBWorkloadA_OL_Batch_Preprocess;
        }
      /*}else if (name == "ycsbwkldb") {
        FLAGS_use_existing_db = true;
        //FLAGS_duration = 10;
        db_->SetDbMode(false); // load flag is false
        if (FLAGS_open_loop == false){
          method = &Benchmark::YCSBWorkloadB_CL;
        }
      }else if (name == "ycsbwkldc") {
        FLAGS_use_existing_db = true;
        //FLAGS_duration = 10;
        db_->SetDbMode(false); // load flag is false
        if (FLAGS_open_loop == false){
          method = &Benchmark::YCSBWorkloadC_CL;
        } 
        //else {
        //  method = &Benchmark::YCSBWorkloadC_OL;
        //}
      }else if (name == "ycsbwkldd") {
        method = &Benchmark::YCSBWorkloadA_CL;*/
      }else if (name == "ycsbwklde") {
        FLAGS_use_existing_db = true;
        db_->SetDbMode(false); // load flag is false
        if (FLAGS_migration_metric == 2) {
          fprintf(stderr, "DEBUG: db_bench set correct bucket keys\n");
          db_->SetCorrectBucketTotalKeys();
        }
        FLAGS_read_ratio = 0.95;
        method = &Benchmark::YCSBWorkloadE_CL;
      }else if (name == "ycsbwkldf") {
        FLAGS_use_existing_db = true;
        db_->SetDbMode(false); // load flag is false
        FLAGS_read_ratio = 0.5;
        if (FLAGS_migration_metric == 2) {
          fprintf(stderr, "DEBUG: db_bench set correct bucket keys\n");
          db_->SetCorrectBucketTotalKeys();
        }
        method = &Benchmark::YCSBWorkloadF_CL;
      } else if (name == "twitter") {
        FLAGS_use_existing_db = false;
        fresh_db = true;
        db_->SetDbMode(false); // load flag is false
        if (FLAGS_migration_metric == 2) {
          fprintf(stderr, "DEBUG: db_bench set correct bucket keys\n");
          db_->SetCorrectBucketTotalKeys();
        }
        method = &Benchmark::TwitterWorkload;
        fprintf(stderr, "Twitter Workload close loop\n");
      } else if (name == "ycsbfilldb_slabs") {
        fresh_db = true;
        num_threads = FLAGS_load_threads;
        method = &Benchmark::YCSBFillDB_CL_VariableKVSize;
      }else if (name == "ycsb_slabs") { // test case for different slab sizes
        FLAGS_use_existing_db = true;
        db_->SetDbMode(false); // load flag is false
        method = &Benchmark::YCSBSlabs;
      } else {
        if (!name.empty()) {  // No error message for empty name
          std::fprintf(stderr, "unknown benchmark '%s'\n",
                       name.ToString().c_str());
        }
      }

      if (fresh_db) {
        if (FLAGS_use_existing_db) {
          std::fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n",
                       name.ToString().c_str());
          method = nullptr;
        } else {
          delete db_;
          db_ = nullptr;
          DestroyDB(FLAGS_db, Options());
          Open();
          // P2 HACK: Set DB mode again for twitter workload, since the previous db was destroyed
          if (name == "twitter"){
            db_->SetDbMode(false); // load flag is false
            if (FLAGS_migration_metric == 2) {
              fprintf(stderr, "DEBUG: db_bench set correct bucket keys\n");
              db_->SetCorrectBucketTotalKeys();
            }
          }
        }
      }

      if (method != nullptr) {
        using namespace std::chrono;
        uint64_t t1 = duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
        fprintf(stderr, "microsecond %llu DBBENCH_START %s fresh_db %d use_existing_db %d\n", t1, name.ToString().c_str(), fresh_db, FLAGS_use_existing_db);
        RunBenchmark(num_threads, name, method);
        uint64_t t2 = duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
        fprintf(stderr, "microsecond %llu DBBENCH_END %s\n", t2, name.ToString().c_str());
        // Sleep for 10 seconds waiting for all compactions of the current experiment to finish
        // before moving on to the next experiment
	
        // flush os page cache before running the next experiment 
        system("sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches");
        std::this_thread::sleep_for(std::chrono::seconds(150)); // original 150
        db_->ReportMigrationStats();
        db_->ResetMigrationStats();

      	/*fprintf(stderr, "DBBENCH START %s fresh_db %d use_existing_db %d\n", name.ToString().c_str(), fresh_db, FLAGS_use_existing_db);
        RunBenchmark(num_threads, name, method);
	fprintf(stderr, "DBBENCH END %s\n", name.ToString().c_str());
        db_->ReportMigrationStats();
        db_->ResetMigrationStats();*/
      }
    }
  }

 private:
  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    void (Benchmark::*method)(ThreadState*);
  };

  static void ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    {
      MutexLock l(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.SignalAll();
      }
      while (!shared->start) {
        shared->cv.Wait();
      }
    }

    thread->stats.Start();
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.SignalAll();
      }
    }
  }

  // Each ycsb client calls this function to read the trace shard
  void ReadTwitterTraceShard(int tid /* thread id */){

   //std::string trace_file = "/home/ashwini/hetsys/tlc_ssd/twitter_traces/cluster19/cluster19_ycsb_"+std::to_string(tid);
   //std::string trace_file = "/home/ashwini/hetsys/tlc_ssd/twitter_traces/cluster39/cluster39_ycsb_"+std::to_string(tid);
   std::string trace_file = "/home/ashwini/hetsys/tlc_ssd/twitter_traces/cluster51/cluster51_ycsb_"+std::to_string(tid);

    threadKeys_[tid] = new std::vector<uint64_t>;
    threadOps_[tid] = new std::vector<uint8_t>;
    threadValSizes_[tid] = new std::vector<uint16_t>;

    // read the trace file
    fprintf(stderr, "TWITTER: reading cache trace file %s\n", trace_file.c_str());
    std::fstream newfile;
    newfile.open(trace_file,std::ios::in);
    uint32_t num_ops = 0;
    if (newfile.is_open()){
      std::string line;
      while(std::getline(newfile, line)) {
        // Vector of string to save tokens
        std::vector <std::string> tokens;
        // stringstream class check1
        std::stringstream check1(line);
        std::string intermediate;
        // Tokenizing w.r.t. space ' '
        while(std::getline(check1, intermediate, ' ')){
          tokens.push_back(intermediate);
        }
        uint8_t operation = 0;
        uint64_t key = 0;
        uint16_t valsize = 0;
        if (tokens[0] == "set"){
          operation=0;
        } else if (tokens[0] == "get"){
          operation=1;
        } else {
          fprintf(stderr, "ERROR: reading twitter cache trace, unknown operation %d\n", tokens[0]);
        }
        key = uint64_t(std::stoull(tokens[1]));
        if (operation == 0){
          valsize = uint16_t(std::stoul(tokens[2]));
        }
        //uint32_t key_shard = key/(num_keys_in_trace/n);
        //fprintf(stderr, "TWITTER: op %d key %llu key_shard %llu valsize %d\n", operation, key, key_shard, valsize);
        threadKeys_[tid]->push_back(key);
        threadOps_[tid]->push_back(operation);
        threadValSizes_[tid]->push_back(valsize);
        num_ops++;
      }
      newfile.close(); //close the file object.
    }

    fprintf(stderr, "TWITTER: trace_file %s shard_id %llu threadKeys_ len %llu threadOps_ %llu threadValSizes_ %llu\n", trace_file.c_str(), tid, threadKeys_[tid]->size(), threadOps_[tid]->size(), threadValSizes_[tid]->size());
  }

  void ProcessTwitterTraceParallel(int n /* num threads*/){

    threadKeys_ = new std::vector<uint64_t>*[n];
    threadOps_ = new std::vector<uint8_t>*[n];
    threadValSizes_ = new std::vector<uint16_t>*[n];

    std::vector<std::thread> threads;
    //ReadTwitterTraceShard(thread->tid);
    for (int i=0; i<n; ++i){
      threads.emplace_back(std::thread(&leveldb::Benchmark::ReadTwitterTraceShard, this, i));
    }

    //wait for them to complete
    for (auto& th : threads) {
      th.join();
    }
  }

  void ProcessTwitterTrace(int n /*num threads*/){

    // TODO: setup these two fields manually
    std::string home = getenv("HOME");
    //std::string trace_file = "/home/ashwini/hetsys/tlc_ssd/twitter_traces/cluster19/cluster19_ycsb";
    //uint64_t num_keys_in_trace = 328491889; //100000000; //328491889;

    //std::string trace_file = "/home/ashwini/hetsys/tlc_ssd/twitter_traces/cluster39/cluster39_ycsb";
    //uint64_t num_keys_in_trace = 1247278616;
    
    std::string trace_file = "/home/ashwini/hetsys/tlc_ssd/twitter_traces/cluster51/cluster51_ycsb";
    uint64_t num_keys_in_trace = 2980912; //1674704;

    // align num_keys_in_trace to n
    uint32_t align = num_keys_in_trace%n;
    if (align != 0){
      num_keys_in_trace = num_keys_in_trace + (n-align);
    }
    num_keys_in_trace = num_keys_in_trace + (n - num_keys_in_trace%n);
    uint64_t keys_per_shard = num_keys_in_trace/n;

    threadKeys_ = new std::vector<uint64_t>*[n];
    threadOps_ = new std::vector<uint8_t>*[n];
    threadValSizes_ = new std::vector<uint16_t>*[n];

    for (int i = 0; i < n; i++) {
      threadKeys_[i] = new std::vector<uint64_t>;
      threadOps_[i] = new std::vector<uint8_t>;
      threadValSizes_[i] = new std::vector<uint16_t>;
    }

    // read the trace file
    fprintf(stderr, "TWITTER: reading cache trace file %s\n", trace_file.c_str());
    std::fstream newfile;
    newfile.open(trace_file,std::ios::in);
    uint32_t num_ops = 0;
    if (newfile.is_open()){
      std::string line;
      // DELETE LATER
      //uint64_t seq_key[8] = {0, (1247278616/8), 2*(1247278616/8), 3*(1247278616/8), 4*(1247278616/8), 5*(1247278616/8), 6*(1247278616/8), 7*(1247278616/8)};
      //uint64_t modulus[8] = {(1247278616/8), 2*(1247278616/8), 3*(1247278616/8), 4*(1247278616/8), 5*(1247278616/8), 6*(1247278616/8), 7*(1247278616/8), 8*(1247278616/8)};
      while(std::getline(newfile, line)) {
        // Vector of string to save tokens
        std::vector <std::string> tokens;
        // stringstream class check1
        std::stringstream check1(line);
        std::string intermediate;
        // Tokenizing w.r.t. space ' '
        while(std::getline(check1, intermediate, ' ')){
          tokens.push_back(intermediate);
        }
        uint8_t operation = 0;
        uint64_t key = 0;
        uint16_t valsize = 0;
        if (tokens[0] == "set"){
          operation=0;
        } else if (tokens[0] == "get"){
          operation=1;
        } else {
          fprintf(stderr, "ERROR: reading twitter cache trace, unknown operation %d\n", tokens[0]);
        }
        key = uint64_t(std::stoull(tokens[1]));
	
	// HACK: 1kb object size
	//if (key >= 100000000){
        //  continue;
	//}

        if (operation == 0){
          valsize = uint16_t(std::stoul(tokens[2]));
	  //valsize = 980; // HACK: 1k object size
        }
        uint32_t key_shard = key/(num_keys_in_trace/n);
	//key = seq_key[key_shard];
	//seq_key[key_shard] = (seq_key[key_shard] + 1)%modulus[key_shard];
        //fprintf(stderr, "TWITTER: op %d key %llu key_shard %llu valsize %d\n", operation, key, key_shard, valsize);
        threadKeys_[key_shard]->push_back(key);
        threadOps_[key_shard]->push_back(operation);
        threadValSizes_[key_shard]->push_back(valsize);
        num_ops++;
        //if (num_ops > 512000000){
        //  break;
        //}
      }
      newfile.close(); //close the file object.
    }
    //assert(num_keys_in_trace == num_ops);
    for (int i = 0; i < n; i++) {
      fprintf(stderr, "TWITTER: trace_file %s shard_id %llu threadKeys_ len %llu threadOps_ %llu threadValSizes_ %llu\n", trace_file.c_str(), i, threadKeys_[i]->size(), threadOps_[i]->size(), threadValSizes_[i]->size());
    }
  }

  void RunBenchmark(int n, Slice name,
                    void (Benchmark::*method)(ThreadState*)) {
    SharedState shared(n);

    const auto name_str = std::string(name.data());
    // preprocess twitter trace
    if (name == Slice("twitter")) {
      ProcessTwitterTrace(n);
      //ProcessTwitterTraceParallel(n);
    }
    // pre-process for ycsb tests:
    // populate each thread with an array of keys belonging to the corresponding partition
    else if (((name != Slice("ycsbfilldb")) && (name != Slice("ycsbfilldb_slabs"))) && (name_str.find("ycsb") != std::string::npos)) { // YCSB workloads
			if (FLAGS_open_loop == false) {
				// pre-process keys if close loop  
				// only used in close loop
				threadKeys_ = new std::vector<uint64_t>*[n];
				threadWriteKeys_ = new std::vector<uint64_t>*[n];

				for (int i = 0; i < n; i++) {
					threadKeys_[i] = new std::vector<uint64_t>;
					threadWriteKeys_[i] = new std::vector<uint64_t>;
				}

				fprintf(stderr, "workload %s: prepare keys now!\n", name);

				Random rand = Random(1000);  // supply a seed 
				RandomGenerator gen;
				//ReadOptions options;
				
				// Prepare read keys
				init_latestgen(FLAGS_num);
				init_zipf_generator(0, FLAGS_num, FLAGS_YCSB_zipfian_alpha);
        //init_zipf_generator(0, FLAGS_num);
				long shard_size = FLAGS_num/FLAGS_threads;
				
        if (name == Slice("ycsbwarmup")) {
          Duration duration(0, FLAGS_ycsb_warmup_ops);
          while (!duration.Done(1)) {
            uint64_t k;
            if (FLAGS_YCSB_uniform_distribution){ //Generate number from uniform distribution
              k = rand.Next() % FLAGS_num;
            } else { //Default: Generate number from zipf distribution
              uint64_t temp = nextValue() % FLAGS_num;
              std::hash<std::string> hash_str;
              k = hash_str(std::to_string(temp))%FLAGS_num;
            }
            int p = k / shard_size;
            threadKeys_[p]->push_back(k);
          }
        } else {
          if (!FLAGS_YCSB_separate_write) {
            Duration duration(0, FLAGS_reads);
            while (!duration.Done(1)) {
              uint64_t k;
              if (FLAGS_YCSB_uniform_distribution){ //Generate number from uniform distribution
                //fprintf(stderr, "random distribution\n");
                k = rand.Next() % FLAGS_num;
              } else { //Default: Generate number from zipf distribution
                uint64_t temp = nextValue() % FLAGS_num;
                std::hash<std::string> hash_str;
                k = hash_str(std::to_string(temp))%FLAGS_num;
              }
              //fprintf(stderr, "Generate %llu\n", k);

              int p = k / shard_size;
              threadKeys_[p]->push_back(k);
            }

          } else {
            fprintf(stderr, "read and write are from different dist\n");
            if (name == Slice("ycsbwkldd")){
              fprintf(stderr, "Running YCSB-D, reads generated from latest distribution\n");
            }
            float epsilon = 0.2;
            fprintf(stderr, "uniform is %d\n", FLAGS_YCSB_uniform_distribution);
            Duration read_duration(0, FLAGS_reads * (FLAGS_read_ratio + epsilon));
            //Duration read_duration(0, FLAGS_reads);
            while (!read_duration.Done(1)) {
              uint64_t k;
              if (FLAGS_YCSB_uniform_distribution){ //Generate number from uniform distribution
                //fprintf(stderr, "random distribution\n");
                k = rand.Next() % FLAGS_num;
              } else { //Default: Generate number from zipf distribution for YCSB-A, latest for YCSB-D
                uint64_t temp;
                if (name == Slice("ycsbwkldd")){
                  temp = next_value_latestgen() % FLAGS_num;
                }
                else{
                  temp = nextValue() % FLAGS_num;
                }
                std::hash<std::string> hash_str;
                k = hash_str(std::to_string(temp))%FLAGS_num;
              }
              //fprintf(stderr, "Generate %llu\n", k);

              int p = k / shard_size;
              threadKeys_[p]->push_back(k);
            }
            fprintf(stderr, "workload %s: read keys prepare done\n", name);

            // Prepare write keys
            init_latestgen(FLAGS_num);
            init_zipf_generator(0, FLAGS_num, FLAGS_YCSB_zipfian_alpha_write);
            fprintf(stderr, "uniform is %d\n", FLAGS_YCSB_uniform_distribution_write);
            Duration write_duration(0, FLAGS_reads * (1 - FLAGS_read_ratio + epsilon));
            //Duration write_duration(0, FLAGS_reads);
            while (!write_duration.Done(1)) {
              uint64_t k;
              if (FLAGS_YCSB_uniform_distribution_write){ //Generate number from uniform distribution
                //fprintf(stderr, "random distribution\n");
                k = rand.Next() % FLAGS_num;
              } else { //Default: Generate number from zipf distribution
                uint64_t temp = nextValue() % FLAGS_num;
                std::hash<std::string> hash_str;
                k = hash_str(std::to_string(temp))%FLAGS_num;
              }
              //fprintf(stderr, "Generate %llu\n", k);

              int p = k / shard_size;
              threadWriteKeys_[p]->push_back(k);
            }
            fprintf(stderr, "workload %s: write keys prepare done\n", name);
          }
        }
				fprintf(stderr, "workload %s: prepare done\n", name);
				for (int i = 0; i < n; i++) {
					fprintf(stderr, "thread %d has %zu read keys\n", i, threadKeys_[i]->size());
					fprintf(stderr, "thread %d has %zu write keys\n", i, threadWriteKeys_[i]->size());
				}
			}
    }
    ThreadArg* arg = new ThreadArg[n];
    for (int i = 0; i < n; i++) {
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      ++total_thread_count_;
      // Seed the thread's random state deterministically based upon thread
      // creation across all benchmarks. This ensures that the seeds are unique
      // but reproducible when rerunning the same set of benchmarks.
      arg[i].thread = new ThreadState(i, /*seed=*/1000 + total_thread_count_);
      arg[i].thread->shared = &shared;
      g_env->StartThread(ThreadBody, &arg[i]);
    }

    shared.mu.Lock();
    while (shared.num_initialized < n) {
      shared.cv.Wait();
    }

    shared.start = true;
    shared.cv.SignalAll();

    while (shared.num_done < n) {
      shared.cv.Wait();
    }
    
    shared.mu.Unlock();

    for (int i = 1; i < n; i++) {
      arg[0].thread->stats.Merge(arg[i].thread->stats);
      arg[0].thread->stats_ycsb.Merge(arg[i].thread->stats_ycsb); // for merging histogram stats
    }
    //arg[0].thread->stats.Report(name);
    arg[0].thread->stats_ycsb.Report(name);
    if (FLAGS_comparisons) {
      fprintf(stdout, "Comparisons: %ld\n", count_comparator_.comparisons());
      count_comparator_.reset();
      fflush(stdout);
    }

    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;

    // JIANAN: free threadKeys_
    if (name == Slice("twitter")) {
        for (int i = 0; i < n; i++) {
            threadKeys_[i]->clear();
            threadKeys_[i]->shrink_to_fit();
            delete threadKeys_[i];

            threadOps_[i]->clear();
            threadOps_[i]->shrink_to_fit();
            delete threadOps_[i];

            threadValSizes_[i]->clear();
            threadValSizes_[i]->shrink_to_fit();
            delete threadValSizes_[i];
        }
        delete[] threadKeys_;
        delete[] threadOps_;
        delete[] threadValSizes_;
    }
    else if (((name != Slice("ycsbfilldb")) && (name != Slice("ycsbfilldb_slabs"))) && (name_str.find("ycsb") != std::string::npos)) { // YCSB workloads
			if (FLAGS_open_loop == false) {
				for (int i = 0; i < n; i++) {
						threadKeys_[i]->clear();
						threadKeys_[i]->shrink_to_fit();
						delete threadKeys_[i];

						threadWriteKeys_[i]->clear();
						threadWriteKeys_[i]->shrink_to_fit();
						delete threadWriteKeys_[i];
				}
				delete[] threadKeys_;
				delete[] threadWriteKeys_;
			}
    }
  }

  void Crc32c(ThreadState* thread) {
    // Checksum about 500MB of data total
    const int size = 4096;
    const char* label = "(4K per op)";
    std::string data(size, 'x');
    int64_t bytes = 0;
    uint32_t crc = 0;
    while (bytes < 500 * 1048576) {
      crc = crc32c::Value(data.data(), size);
      thread->stats.FinishedSingleOp();
      bytes += size;
    }
    // Print so result is not dead
    std::fprintf(stderr, "... crc=0x%x\r", static_cast<unsigned int>(crc));

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(label);
  }

  void SnappyCompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    int64_t bytes = 0;
    int64_t produced = 0;
    bool ok = true;
    std::string compressed;
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
      produced += compressed.size();
      bytes += input.size();
      thread->stats.FinishedSingleOp();
    }

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "(output: %.1f%%)",
                    (produced * 100.0) / bytes);
      thread->stats.AddMessage(buf);
      thread->stats.AddBytes(bytes);
    }
  }

  void SnappyUncompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    std::string compressed;
    bool ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
    int64_t bytes = 0;
    char* uncompressed = new char[input.size()];
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok = port::Snappy_Uncompress(compressed.data(), compressed.size(),
                                   uncompressed);
      bytes += input.size();
      thread->stats.FinishedSingleOp();
    }
    delete[] uncompressed;

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      thread->stats.AddBytes(bytes);
    }
  }
  
  /*DB* SelectDB(ThreadState* thread) {
    return SelectDBWithCfh(thread)->db;
  }
  DBWithColumnFamilies* SelectDBWithCfh(ThreadState* thread) {
    return SelectDBWithCfh(thread->rand.Next());
  }
  DBWithColumnFamilies* SelectDBWithCfh(uint64_t rand_int) {
    if (db_.db != nullptr) {
      return &db_;
    } else  {
      return &multi_dbs_[rand_int % multi_dbs_.size()];
    }
  }*/

  void MYTEST(ThreadState* thread) {
    //ReadOptions options(FLAGS_verify_checksum, true);
    ReadOptions options;
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;

    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, 0);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    // first put 1000 keys where some will be migrated to qlc
    for (int i = 1; i <= 1000; i++){
      long k = i;
      GenerateKeyFromInt(k, FLAGS_num, &key);
      db_->Put(write_options_, key, gen.Generate(value_size_));
      //printf("K= %d\n", k);
    }
    fprintf(stderr, "puts done\n");

    // get from optane
    long k = 8;
    GenerateKeyFromInt(k, FLAGS_num, &key);
    Status s = db_->Get(options, key, &value);

    // get from qlc
    k = 9;
    GenerateKeyFromInt(k, FLAGS_num, &key);
    s = db_->Get(options, key, &value);

  }

  void RECOVER(ThreadState* thread) {
    fprintf(stderr, "recover completes\n");
  }

  // ------- YCSB CLOSED LOOP IMPLEMENTATIONS----------//
  void YCSBFillDB_CL_VariableKVSize(ThreadState* thread) {
    //ReadOptions options(FLAGS_verify_checksum, true);
    ReadOptions options;
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;

    //write in order
    long shard_size = FLAGS_num/FLAGS_load_threads;
    long low=thread->tid*shard_size;

		std::vector<long> keys;
    for (long k = low; k < (low+shard_size); k++){
			keys.push_back(k);
	  }
		// JIANAN: If sequential loading, comment the line below	
		unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
		shuffle(keys.begin(), keys.end(), std::default_random_engine(seed));
		//random_shuffle(keys.begin(), keys.end());

    thread->stats_ycsb.SetNumWarmupKeys(static_cast<uint64_t>(shard_size * FLAGS_warmup_ratio));
    // JIANAN: update last_op_finish_ of StatsYCSB, otherwise the very first op will have a crazy latency
    thread->stats_ycsb.ResetLastOpTime();
		for (int i = 0; i < shard_size; i++) {
      std::unique_ptr<const char[]> key_guard;
      Slice key = AllocateKey(&key_guard);
			long k = keys.at(i);
      GenerateKeyFromInt(k, FLAGS_num, &key);

      // generate a value size from [450, 1450]
      int curr_valsize = rand() % 1000 + 450;
      fprintf(stderr, "Workload A, Thread %d: Put %llu valsize %d\n", thread->tid, k, curr_valsize);
      //write
      Status s = db_->Put(write_options_, key, gen.Generate(curr_valsize));
      //fprintf(stderr, "Done db_bench %lu\n", k);
      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        exit(1);
      }
      writes_done++;
      //thread->stats_ycsb.FinishedOps(db_, 1, thread->tid, kWrite);
      thread->stats_ycsb.FinishedOps_Warmup(db_, 1, thread->tid, kWrite);
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats_ycsb.AddMessage(msg);
  }
  void YCSBSlabs(ThreadState* thread) { // test for different slab sizes
    fprintf(stderr, "workload YCSB_Slabs close-loop called \n");
    ReadOptions options;
    RandomGenerator gen; // used to generate values, not keys

    std::vector<uint64_t> *keys = threadKeys_[thread->tid];
    std::vector<uint64_t> *write_keys = threadWriteKeys_[thread->tid];

    auto it_read = keys->begin();
    auto it_write = write_keys->begin();

    std::string value (4096, '0');
    int64_t found = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
   
    int total = 0; 
    if (!FLAGS_YCSB_separate_write) {
      total = keys->size();
    } else {
      total = FLAGS_reads/FLAGS_threads;
    }
    thread->stats_ycsb.SetNumWarmupKeys(static_cast<uint64_t>( total * FLAGS_warmup_ratio));
    thread->stats_ycsb.ResetLastOpTime();
    
    //fprintf(stderr, "Before loop %llu\n", thread->tid);

    for (int i = 0; i < total; i++) {
      uint64_t k;
      int next_op = thread->rand.Next() % 100;
      if (next_op < (100 * FLAGS_read_ratio)) { // Read
        if (!FLAGS_YCSB_separate_write) { // if using the same dist
          //fprintf(stderr, "Before k %llu\n", thread->tid);
          k = keys->at(i);
	  //fprintf(stderr, "After k %llu\n", thread->tid);
        } else { // if reads and writes have diff dist
          if (it_read != keys->end()) { // access read key from preprocessed read key array
            k = *it_read;
            ++it_read;
          } else { // need to generate more read keys, increase epsilon in RunBenchmark()
            fprintf(stderr, "ERROR: thread %d runs out of read keys!\n", thread->tid);
            abort();
          }
        }
        GenerateKeyFromInt(k, FLAGS_num, &key); 
        fprintf(stderr, "Workload A, Thread %d: Get %llu\n", thread->tid, k);
        Status s = db_->Get(options, key, &value);
        if (!s.ok() && !s.IsNotFound()) {
          //fprintf(stderr, "k=%d; get error: %s\n", k, s.ToString().c_str());
          //exit(1);
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          //fprintf(stderr, "Workload A, Thread %d: Get %llu return %s\n", thread->tid, k, Slice(value).ToString(true).c_str());
          found++;
          thread->stats_ycsb.FinishedOps_Warmup(db_, 1, thread->tid, kRead);
        }
        reads_done++;

      } else{ // Write
        if (!FLAGS_YCSB_separate_write) { // if using the same dist
          //fprintf(stderr, "Before put k %llu\n", thread->tid);
          k = keys->at(i);
	  //fprintf(stderr, "After put k %llu\n", thread->tid);
        } else { // if reads and writes have diff dist
          if (it_write != write_keys->end()) { // access write key from preprocessed write key array
            k = *it_write;
            ++it_write;
          } else { // need to generate more write keys, increase epsilon in RunBenchmark()
            fprintf(stderr, "ERROR: thread %d runs out of write keys!\n", thread->tid);
            abort();
          }
        }
        GenerateKeyFromInt(k, FLAGS_num, &key);
        //fprintf(stderr, "Workload A, Thread %d: Put %llu\n", thread->tid, k);

        // HACK 
        //char val_char[980] = {0};
        //std::sprintf(val_char, "%d", k);
        //Slice val = Slice(val_char, 980);
        //Status s = db_->Put(write_options_, key, val);
        //fprintf(stderr, "Workload A, Thread %d: Put %llu val %s\n", thread->tid, k, val.ToString(true).c_str());
        // generate a value size from [450, 1450]
        int curr_valsize = rand() % 1000 + 450;
        fprintf(stderr, "Workload A, Thread %d: Put %llu valsize %d\n", thread->tid, k, curr_valsize);
        Status s = db_->Put(write_options_, key, gen.Generate(curr_valsize));
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          //exit(1);
        } else{
          writes_done++;
          thread->stats_ycsb.FinishedOps_Warmup(db_, 1, thread->tid, kWrite);
        }
      }
    }
    //delete [] val_char;
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats_ycsb.AddMessage(msg);
  }


  void YCSBFillDB_CL(ThreadState* thread) {
    //ReadOptions options(FLAGS_verify_checksum, true);
    ReadOptions options;
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;

    //write in order
    long shard_size = FLAGS_num/FLAGS_load_threads;
    long low=thread->tid*shard_size;

		std::vector<long> keys;
    for (long k = low; k < (low+shard_size); k++){
			keys.push_back(k);
	  }
		// JIANAN: If sequential loading, comment the line below	
		unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
		shuffle(keys.begin(), keys.end(), std::default_random_engine(seed));
		//random_shuffle(keys.begin(), keys.end());

    thread->stats_ycsb.SetNumWarmupKeys(static_cast<uint64_t>(shard_size * FLAGS_warmup_ratio));
    // JIANAN: update last_op_finish_ of StatsYCSB, otherwise the very first op will have a crazy latency
    thread->stats_ycsb.ResetLastOpTime();
		for (int i = 0; i < shard_size; i++) {
      std::unique_ptr<const char[]> key_guard;
      Slice key = AllocateKey(&key_guard);
			long k = keys.at(i);
      GenerateKeyFromInt(k, FLAGS_num, &key);

      //write
      Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
      //fprintf(stderr, "Done db_bench %lu\n", k);
      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        exit(1);
      }
      writes_done++;
      //thread->stats_ycsb.FinishedOps(db_, 1, thread->tid, kWrite);
      thread->stats_ycsb.FinishedOps_Warmup(db_, 1, thread->tid, kWrite);
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats_ycsb.AddMessage(msg);
  }

  void YCSBWorkloadA_CL_Warmup(ThreadState* thread) {
    fprintf(stderr, "ycsbwarmup close-loop called\n");
    ReadOptions options;
    RandomGenerator gen; // used to generate values, not keys
    std::vector<uint64_t> *keys = threadKeys_[thread->tid];
    std::string value (4096, '0');
    int64_t found = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    
    int total = keys->size();
    thread->stats_ycsb.SetNumWarmupKeys(static_cast<uint64_t>( total * FLAGS_warmup_ratio));
    thread->stats_ycsb.ResetLastOpTime();

    for (int i = 0; i < total; i++) {
      uint64_t k;
      int next_op = thread->rand.Next() % 100;
      k = keys->at(i);
      GenerateKeyFromInt(k, FLAGS_num, &key);

      if (next_op < 50) {
        // Read
        Status s = db_->Get(options, key, &value);
        if (!s.ok() && !s.IsNotFound()) {
        } else if (!s.IsNotFound()) {
          found++;
          thread->stats_ycsb.FinishedOps_Warmup(db_, 1, thread->tid, kRead);
        }
        reads_done++;
      } else{
        // Write
        Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
        } else{
          writes_done++;
          thread->stats_ycsb.FinishedOps_Warmup(db_, 1, thread->tid, kWrite);
        }
      }
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats_ycsb.AddMessage(msg);
  }

  // Workload A: Update heavy workload
  // This workload has a mix of 50/50 reads and writes.
  // An application example is a session store recording recent actions.
  // Read/update ratio: 50/50
  // Default data size: 1 KB records
  // Request distribution: zipfian
  void YCSBWorkloadA_CL(ThreadState* thread) {
    fprintf(stderr, "workload A close-loop called \n");
    ReadOptions options;
    RandomGenerator gen; // used to generate values, not keys

    std::vector<uint64_t> *keys = threadKeys_[thread->tid];
    std::vector<uint64_t> *write_keys = threadWriteKeys_[thread->tid];

    auto it_read = keys->begin();
    auto it_write = write_keys->begin();

    std::string value (4096, '0');
    int64_t found = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
   
    int total = 0; 
    if (!FLAGS_YCSB_separate_write) {
      total = keys->size();
    } else {
      total = FLAGS_reads/FLAGS_threads;
    }
    thread->stats_ycsb.SetNumWarmupKeys(static_cast<uint64_t>( total * FLAGS_warmup_ratio));
    thread->stats_ycsb.ResetLastOpTime();
    
    //fprintf(stderr, "Before loop %llu\n", thread->tid);

    for (int i = 0; i < total; i++) {
      uint64_t k;
      int next_op = thread->rand.Next() % 100;
      if (next_op < (100 * FLAGS_read_ratio)) { // Read
        if (!FLAGS_YCSB_separate_write) { // if using the same dist
          //fprintf(stderr, "Before k %llu\n", thread->tid);
          k = keys->at(i);
	  //fprintf(stderr, "After k %llu\n", thread->tid);
        } else { // if reads and writes have diff dist
          if (it_read != keys->end()) { // access read key from preprocessed read key array
            k = *it_read;
            ++it_read;
          } else { // need to generate more read keys, increase epsilon in RunBenchmark()
            fprintf(stderr, "ERROR: thread %d runs out of read keys!\n", thread->tid);
            abort();
          }
        }
        GenerateKeyFromInt(k, FLAGS_num, &key); 
        //fprintf(stderr, "Workload A, Thread %d: Get %llu\n", thread->tid, k);
        Status s = db_->Get(options, key, &value);
        if (!s.ok() && !s.IsNotFound()) {
          //fprintf(stderr, "k=%d; get error: %s\n", k, s.ToString().c_str());
          //exit(1);
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          //fprintf(stderr, "Workload A, Thread %d: Get %llu return %s\n", thread->tid, k, Slice(value).ToString(true).c_str());
          found++;
          thread->stats_ycsb.FinishedOps_Warmup(db_, 1, thread->tid, kRead);
        }
        reads_done++;

      } else{ // Write
        if (!FLAGS_YCSB_separate_write) { // if using the same dist
          //fprintf(stderr, "Before put k %llu\n", thread->tid);
          k = keys->at(i);
	  //fprintf(stderr, "After put k %llu\n", thread->tid);
        } else { // if reads and writes have diff dist
          if (it_write != write_keys->end()) { // access write key from preprocessed write key array
            k = *it_write;
            ++it_write;
          } else { // need to generate more write keys, increase epsilon in RunBenchmark()
            fprintf(stderr, "ERROR: thread %d runs out of write keys!\n", thread->tid);
            abort();
          }
        }
        GenerateKeyFromInt(k, FLAGS_num, &key);
        //fprintf(stderr, "Workload A, Thread %d: Put %llu\n", thread->tid, k);

        // HACK 
        //char val_char[980] = {0};
        //std::sprintf(val_char, "%d", k);
        //Slice val = Slice(val_char, 980);
        //Status s = db_->Put(write_options_, key, val);
        //fprintf(stderr, "Workload A, Thread %d: Put %llu val %s\n", thread->tid, k, val.ToString(true).c_str());
        Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          //exit(1);
        } else{
          writes_done++;
          thread->stats_ycsb.FinishedOps_Warmup(db_, 1, thread->tid, kWrite);
        }
      }
    }
    //delete [] val_char;
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats_ycsb.AddMessage(msg);
  }


  void YCSBWorkloadE_CL(ThreadState* thread) {
    fprintf(stderr, "workload E close-loop called \n");
    ReadOptions options;
    RandomGenerator gen; // used to generate values, not keys

    std::vector<uint64_t> *keys = threadKeys_[thread->tid];
    std::vector<uint64_t> *write_keys = threadWriteKeys_[thread->tid];

    auto it_read = keys->begin();
    auto it_write = write_keys->begin();

    //std::string value;
    std::vector<std::pair<Slice, std::string>> values;
    int64_t found = 0;
    int64_t scans_done = 0;
    int64_t writes_done = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
   
    int total = 0; 
    if (!FLAGS_YCSB_separate_write) {
      total = keys->size();
    } else {
      total = FLAGS_reads/FLAGS_threads;
    }
    thread->stats_ycsb.SetNumWarmupKeys(static_cast<uint64_t>( total * FLAGS_warmup_ratio));
    thread->stats_ycsb.ResetLastOpTime();
    
    for (int i = 0; i < total; i++) {
      uint64_t k;
      int next_op = thread->rand.Next() % 100;
      if (next_op < 95){ // Scan
        if (!FLAGS_YCSB_separate_write) {
          k = keys->at(i);
        } else {
          if (it_read != keys->end()) {
            k = *it_read;
            ++it_read;
          } else {
            fprintf(stderr, "ERROR: thread %d runs out of scan keys!\n", thread->tid);
            abort();
          }
        }

        std::vector<std::pair<Slice, Slice>> results;

        // generate the scan size from a uniform distribution between [1, 100]
        uint64_t scan_size = (thread->rand.Next() % 100) + 1;
	
        GenerateKeyFromInt(k, FLAGS_num, &key); 
        //fprintf(stderr, "Workload E, Thread %d: Scan start_key %llu scan_size %llu\n", thread->tid, k, scan_size);
        Status s = db_->Scan(options, key, scan_size, &results);
        if (!s.ok() && !s.IsNotFound()) {
          fprintf(stderr, "start_key=%d; scan error: %s\n", k, s.ToString().c_str());
          //exit(1);
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          found++;
          thread->stats_ycsb.FinishedOps_Warmup(db_, 1, thread->tid, kRead);
        }
        scans_done++;
      } else{ // Write
        if (!FLAGS_YCSB_separate_write) {
          k = keys->at(i);
        } else {
          if (it_write != write_keys->end()) {
            k = *it_write;
            ++it_write;
          } else {
            fprintf(stderr, "ERROR: thread %d runs out of write keys!\n", thread->tid);
            abort();
          }
        }

        GenerateKeyFromInt(k, FLAGS_num, &key);
        //fprintf(stderr, "Workload E, Thread %d: Put key %llu\n", thread->tid, k);
        Status s = db_->Put(write_options_, key, gen.Generate(value_size_));

        if (!s.ok()) {
          fprintf(stderr, "key=%d; put error: %s\n", k, s.ToString().c_str());
          //exit(1);
        } else{
          writes_done++;
          thread->stats_ycsb.FinishedOps_Warmup(db_, 1, thread->tid, kWrite);
        }
      }
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( scans:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             scans_done, writes_done, readwrites_, found);
    thread->stats_ycsb.AddMessage(msg);
  }
  
  // Workload F: 50% reads, 50% rmw
  // This workload has a mix of 50/50 reads and rmw
  // An application example is a session store recording recent actions.
  // Read/update ratio: 50/50
  // Default data size: 1 KB records
  // Request distribution: zipfian
  void YCSBWorkloadF_CL(ThreadState* thread) {
    fprintf(stderr, "workload F close-loop called \n");
    ReadOptions options;
    RandomGenerator gen; // used to generate values, not keys

    std::vector<uint64_t> *keys = threadKeys_[thread->tid];
    std::vector<uint64_t> *write_keys = threadWriteKeys_[thread->tid];

    auto it_read = keys->begin();
    auto it_write = write_keys->begin();

    std::string value (4096, '0');
    int64_t found = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
   
    int total = 0; 
    if (!FLAGS_YCSB_separate_write) {
      total = keys->size();
    } else {
      total = FLAGS_reads/FLAGS_threads;
    }
    thread->stats_ycsb.SetNumWarmupKeys(static_cast<uint64_t>( total * FLAGS_warmup_ratio));
    thread->stats_ycsb.ResetLastOpTime();
    
    for (int i = 0; i < total; i++) {
      uint64_t k;
      int next_op = thread->rand.Next() % 100;
      FLAGS_read_ratio = 0.50;
      if (next_op < (100 * FLAGS_read_ratio)) { // Read
        if (!FLAGS_YCSB_separate_write) { // if using the same dist
          k = keys->at(i);
        } else { // if reads and writes have diff dist
          if (it_read != keys->end()) { // access read key from preprocessed read key array
            k = *it_read;
            ++it_read;
          } else { // need to generate more read keys, increase epsilon in RunBenchmark()
            fprintf(stderr, "ERROR: thread %d runs out of read keys!\n", thread->tid);
            abort();
          }
        }
        GenerateKeyFromInt(k, FLAGS_num, &key); 
        //fprintf(stderr, "Workload A, Thread %d: Get %llu\n", thread->tid, k);
        Status s = db_->Get(options, key, &value);
        if (!s.ok() && !s.IsNotFound()) {
          //fprintf(stderr, "k=%d; get error: %s\n", k, s.ToString().c_str());
          //exit(1);
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          found++;
          thread->stats_ycsb.FinishedOps_Warmup(db_, 1, thread->tid, kRead);
        }
        reads_done++;

      } else{ // Write
        if (!FLAGS_YCSB_separate_write) { // if using the same dist
          k = keys->at(i);
        } else { // if reads and writes have diff dist
          if (it_write != write_keys->end()) { // access write key from preprocessed write key array
            k = *it_write;
            ++it_write;
          } else { // need to generate more write keys, increase epsilon in RunBenchmark()
            fprintf(stderr, "ERROR: thread %d runs out of write keys!\n", thread->tid);
            abort();
          }
        }
        GenerateKeyFromInt(k, FLAGS_num, &key);
        //fprintf(stderr, "Workload A, Thread %d: Put %llu\n", thread->tid, k);
	Status s1 = db_->Get(options, key, &value);
        if (!s1.ok() && !s1.IsNotFound()) {
          //fprintf(stderr, "k=%d; get error: %s\n", k, s.ToString().c_str());
          //exit(1);
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s1.IsNotFound()) {
          found++;
          //reads_done++;
          Status s2 = db_->Put(write_options_, key, gen.Generate(value_size_));
          if (!s2.ok()) {
            //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
            //exit(1);
          } else{
            writes_done++;
            thread->stats_ycsb.FinishedOps_Warmup(db_, 1, thread->tid, kRmw);
          }
	}
      }
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats_ycsb.AddMessage(msg);
  }

  // Twitter Workload:
  void TwitterWorkload(ThreadState* thread) {
    fprintf(stderr, "Twitter workload close-loop called \n");
    ReadOptions options;
    RandomGenerator gen;

    std::vector<uint64_t> *keys = threadKeys_[thread->tid];
    std::vector<uint8_t> *ops = threadOps_[thread->tid];
    std::vector<uint16_t> *valsizes = threadValSizes_[thread->tid];

    //auto it_keys = keys->begin();
    //auto it_ops = ops->begin();
    //auto it_valsizes = valsizes->begin();

    std::string value (4096, '0');
    int64_t found = 0;
    int64_t reads_done = 0;
    uint64_t reads_not_found = 0;
    int64_t writes_done = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    int total = keys->size();
    thread->stats_ycsb.SetNumWarmupKeys(static_cast<uint64_t>( total * FLAGS_warmup_ratio));
    thread->stats_ycsb.ResetLastOpTime();

    for (uint32_t i = 0; i < total; i++) {
      uint64_t k = keys->at(i);
      // HACK to use 100M only keys from twitter trace
      //uint64_t start_range = thread->tid*100000000/8;
      //if ((k < start_range) || (k >= (start_range + 100000000/8))){
      //  continue;
      //}
      uint8_t operation = ops->at(i);
      uint16_t valsize = valsizes->at(i);
      if (operation == 1){
        // Read operation
        GenerateKeyFromInt(k, FLAGS_num, &key);
        //fprintf(stderr, "Twitter Workload, Thread %d: Get %llu\n", thread->tid, k);
        //fprintf(stderr, "GET operation tid %d key %llu\n", thread->tid, k);
        // TODO: value needs to be 4KB after moving to sns
        Status s = db_->Get(options, key, &value);
        if (!s.ok() && !s.IsNotFound()) {
        } else if (!s.IsNotFound()) {
          found++;
          thread->stats_ycsb.FinishedOps_Warmup(db_, 1, thread->tid, kRead);
          //fprintf(stderr, "read found total done  %llu missing %llu\n", reads_done, reads_not_found);
        }
        else if(s.IsNotFound()){
          reads_not_found++;
          thread->stats_ycsb.FinishedOps_Warmup(db_, 1, thread->tid, kRead);
          //fprintf(stderr, "read not found total done %llu missing %llu\n", reads_done, reads_not_found);
        }
        reads_done++;
      } else if (operation == 0){
        // Write operation
        //fprintf(stderr, "tid %d WRITE KEY %llu\n", thread->tid, k);
        GenerateKeyFromInt(k, FLAGS_num, &key);
        //fprintf(stderr, "Twitter Workload, Thread %d: Put %llu\n", thread->tid, k);
        Status s;
        //fprintf(stderr, "PUT operation tid %d key %llu valsize %llu\n", thread->tid, k, valsize);
        s = db_->Put(write_options_, key, gen.Generate(valsize));
        if (!s.ok()) {
          //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          //exit(1);
        } else{
          writes_done++;
          thread->stats_ycsb.FinishedOps_Warmup(db_, 1, thread->tid, kWrite);
        }
      }
      if (i%1000000 == 0){
        fprintf(stderr, "Thread-%d total_reads %llu reads_not_found %llu\n", thread->tid, reads_done, reads_not_found);
      }
    }

    fprintf(stderr, "Thread-%d final total_reads %llu reads_not_found %llu\n", thread->tid, reads_done, reads_not_found);

    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats_ycsb.AddMessage(msg);
  }

  /*
  // Workload B: Read mostly workload
  // This workload has a 95/5 reads/write mix.
  // Application example: photo tagging; add a tag is an update,
  // but most operations are to read tags.

  // Read/update ratio: 95/5
  // Default data size: 1 KB records
  // Request distribution: zipfian
  void YCSBWorkloadB_CL(ThreadState* thread) {
    //ReadOptions options(FLAGS_verify_checksum, true);
    ReadOptions options;
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num, FLAGS_YCSB_zipfian_alpha);


    std::string value;
    int64_t found = 0;

    int64_t reads_done = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
   
    int total = 0; 
    if (!FLAGS_YCSB_separate_write) {
      total = keys->size();
    } else {
      total = FLAGS_reads/FLAGS_threads;
    }
    thread->stats_ycsb.SetNumWarmupKeys(static_cast<uint64_t>( total * FLAGS_warmup_ratio));
    thread->stats_ycsb.ResetLastOpTime();
    
    for (int i = 0; i < total; i++) {
      uint64_t k;
      int next_op = thread->rand.Next() % 100;
      if (next_op < (100 * FLAGS_read_ratio)) { // Read
        if (!FLAGS_YCSB_separate_write) { // if using the same dist
          k = keys->at(i);
        } else { // if reads and writes have diff dist
          if (it_read != keys->end()) { // access read key from preprocessed read key array
            k = *it_read;
            ++it_read;
          } else { // need to generate more read keys, increase epsilon in RunBenchmark()
            fprintf(stderr, "ERROR: thread %d runs out of read keys!\n", thread->tid);
            abort();
          }
        }
        GenerateKeyFromInt(k, FLAGS_num, &key); 
        //fprintf(stderr, "Workload A, Thread %d: Get %llu\n", thread->tid, k);
        Status s = db_->Get(options, key, &value);
        if (!s.ok() && !s.IsNotFound()) {
          //fprintf(stderr, "k=%d; get error: %s\n", k, s.ToString().c_str());
          //exit(1);
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          found++;
          thread->stats_ycsb.FinishedOps_Warmup(db_, 1, thread->tid, kRead);
        }
        reads_done++;

      } else{ // Write
        if (!FLAGS_YCSB_separate_write) { // if using the same dist
          k = keys->at(i);
        } else { // if reads and writes have diff dist
          if (it_write != write_keys->end()) { // access write key from preprocessed write key array
            k = *it_write;
            ++it_write;
          } else { // need to generate more write keys, increase epsilon in RunBenchmark()
            fprintf(stderr, "ERROR: thread %d runs out of write keys!\n", thread->tid);
            abort();
          }
        }
        GenerateKeyFromInt(k, FLAGS_num, &key);
        //fprintf(stderr, "Workload A, Thread %d: Put %llu\n", thread->tid, k);
        Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          //exit(1);
        } else{
          writes_done++;
          thread->stats_ycsb.FinishedOps_Warmup(db_, 1, thread->tid, kWrite);
        }
      }
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats_ycsb.AddMessage(msg);
  }

  // Workload B: Read mostly workload
  // This workload has a 95/5 reads/write mix.
  // Application example: photo tagging; add a tag is an update,
  // but most operations are to read tags.

  // Read/update ratio: 95/5
  // Default data size: 1 KB records
  // Request distribution: zipfian
  void YCSBWorkloadB_CL(ThreadState* thread) {
    //ReadOptions options(FLAGS_verify_checksum, true);
    ReadOptions options;
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num, FLAGS_YCSB_zipfian_alpha);


    std::string value;
    int64_t found = 0;

    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(0, FLAGS_reads/FLAGS_threads);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    //if (FLAGS_benchmark_write_rate_limit > 0) {
    //   printf(">>>> FLAGS_benchmark_write_rate_limit YCSBA \n");
    //  thread->shared->write_rate_limiter.reset(
    //      NewGenericRateLimiter(FLAGS_benchmark_write_rate_limit));
    //}

    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      //DB* db = SelectDB(thread);

          uint64_t k;
          if (FLAGS_YCSB_uniform_distribution){
            //Generate number from uniform distribution
            k = thread->rand.Next() % FLAGS_num;
          } else { //default
            //Generate number from zipf distribution
            uint64_t temp = nextValue() % FLAGS_num;
            //std::hash<std::string> hash_str;
            //k = hash_str(std::to_string(temp))%FLAGS_num;
            k = temp;
            //fprintf(stderr, "KEY temp %llu hash %llu\n", temp, k);
          }
          GenerateKeyFromInt(k, FLAGS_num, &key);

          int next_op = thread->rand.Next() % 100;
          if (next_op < 95){
            //read
            Status s = db_->Get(options, key, &value);
            if (!s.ok() && !s.IsNotFound()) {
              //fprintf(stderr, "k=%d; get error: %s\n", k, s.ToString().c_str());
              //exit(1);
              // we continue after error rather than exiting so that we can
              // find more errors if any
            } else if (!s.IsNotFound()) {
              found++;
              thread->stats_ycsb.FinishedOps(db_, 1, thread->tid, kRead);
            }
            reads_done++;
          } else{
            //write
            //if (FLAGS_benchmark_write_rate_limit > 0) {

            //    thread->shared->write_rate_limiter->Request(
            //        value_size_ + key_size_, Env::IO_HIGH,
            //        nullptr, RateLimiter::OpType::kWrite);
            //    thread->stats_ycsb.ResetLastOpTime();
            //}
            Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
            if (!s.ok()) {
              //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
              //exit(1);
            } else{
             writes_done++;
             thread->stats_ycsb.FinishedOps(db_, 1, thread->tid, kWrite);
            }
        }
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats_ycsb.AddMessage(msg);
  }

  // Workload C: Read only
  // This workload is 100% read. Application example: user profile cache,
  // where profiles are constructed elsewhere (e.g., Hadoop).
  // Read/update ratio: 100/0
  // Default data size: 1 KB records
  // Request distribution: zipfian
  void YCSBWorkloadC_CL(ThreadState* thread) {
    //ReadOptions options(FLAGS_verify_checksum, true);
    ReadOptions options;
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num, FLAGS_YCSB_zipfian_alpha);


    std::string value;
    int64_t found = 0;

    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(0, FLAGS_reads/FLAGS_threads);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    //if (FLAGS_benchmark_write_rate_limit > 0) {
    //   printf(">>>> FLAGS_benchmark_write_rate_limit YCSBA \n");
    //  thread->shared->write_rate_limiter.reset(
    //      NewGenericRateLimiter(FLAGS_benchmark_write_rate_limit));
    //}

    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      //DB* db = SelectDB(thread);

          uint64_t k;
          if (FLAGS_YCSB_uniform_distribution){
            //Generate number from uniform distribution
            k = thread->rand.Next() % FLAGS_num;
          } else { //default
            //Generate number from zipf distribution
            uint64_t temp = nextValue() % FLAGS_num;
            //std::hash<std::string> hash_str;
            //k = hash_str(std::to_string(temp))%FLAGS_num;
                  k = temp;
            //fprintf(stderr, "KEY temp %llu hash %llu\n", temp, k);
          }
          GenerateKeyFromInt(k, FLAGS_num, &key);
          //read
          Status s = db_->Get(options, key, &value);
          if (!s.ok() && !s.IsNotFound()) {
            //fprintf(stderr, "k=%d; get error: %s\n", k, s.ToString().c_str());
            //exit(1);
            // we continue after error rather than exiting so that we can
            // find more errors if any
          } else if (!s.IsNotFound()) {
            found++;
            thread->stats_ycsb.FinishedOps(db_, 1, thread->tid, kRead);
          }
          reads_done++;
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats_ycsb.AddMessage(msg);
  }
*/

/*  
  // Workload D: Read latest workload
  // In this workload, new records are inserted, and the most recently
  // inserted records are the most popular. Application example:
  // user status updates; people want to read the latest.

  // Read/update/insert ratio: 95/0/5
  // Default data size: 1 KB records
  // Request distribution: latest

  // The insert order for this is hashed, not ordered. The "latest" items may be
  // scattered around the keyspace if they are keyed by userid.timestamp. A workload
  // which orders items purely by time, and demands the latest, is very different than
  // workload here (which we believe is more typical of how people build systems.)
  void YCSBWorkloadD_CL(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num, FLAGS_YCSB_zipfian_alpha);
    std::string value;
    int64_t found = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, 0);
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);


      long k;
      if (FLAGS_YCSB_uniform_distribution){
        //Generate number from uniform distribution
        k = thread->rand.Next() % FLAGS_num;
      } else { //default
        //Generate number from latest distribution
        k = next_value_latestgen() % FLAGS_num;
      }
      GenerateKeyFromInt(k, FLAGS_num, &key);
      int next_op = thread->rand.Next() % 100;
      if (next_op < 95){
        //read
        Status s = db->Get(options, key, &value);
        if (!s.ok() && !s.IsNotFound()) {
          //fprintf(stderr, "get error: %s\n", s.ToString().c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          found++;
          thread->stats.FinishedOps(nullptr, db, 1, kRead);
        }
        reads_done++;

      } else{
        //write
        Status s = db->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          //exit(1);
        } else{
            writes_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kWrite);
        }
      }
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);

  }

  // Workload E: Short ranges.
  // In this workload, short ranges of records are queried,
  // instead of individual records. Application example:
  // threaded conversations, where each scan is for the posts
  // in a given thread (assumed to be clustered by thread id).

  // Scan/insert ratio: 95/5
  // Default data size: 1 KB records
  // Request distribution: latest
  // Scan Length Distribution=uniform
  // Max scan length = 100

  // The insert order is hashed, not ordered. Although the scans are ordered, it does not necessarily
  // follow that the data is inserted in order. For example, posts for thread
  // 342 may not be inserted contiguously, but
  // instead interspersed with posts from lots of other threads. The way the YCSB
  // client works is that it will pick a start
  // key, and then request a number of records; this works fine even for hashed insertion.
  void YCSBWorkloadE_CL(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num, FLAGS_YCSB_zipfian_alpha);
    std::string value;
    int64_t found = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, 0);
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      long k;
      if (FLAGS_YCSB_uniform_distribution){
        //Generate number from uniform distribution
        k = thread->rand.Next() % FLAGS_num;
      } else { //default
        //Generate number from zipf distribution
        k = nextValue() % FLAGS_num;
      }
      GenerateKeyFromInt(k, FLAGS_num, &key);

      int next_op = thread->rand.Next() % 100;
      if (next_op < 95){
        //scan

        //TODO need to draw a random number for the scan length
        //for now, scan lenght constant
        int scan_length = thread->rand.Next() % 100;

        Iterator* iter = db->NewIterator(options);
        int64_t i = 0;
        int64_t bytes = 0;
        for (iter->Seek(key); i < 100 && iter->Valid(); iter->Next()) {
          bytes += iter->key().size() + iter->value().size();
          //thread->stats.FinishedOps(nullptr, db, 1, kRead);
          ++i;

        }

        delete iter;

        reads_done++;
        thread->stats.FinishedOps(nullptr, db, 1, kRead);
      } else{
        //write
        Status s = db->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          //exit(1);
        } else{
            writes_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kWrite);
        }
      }

    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);


  }
  
  // Workload F: Read-modify-write workload
  // In this workload, the client will read a record,
  // modify it, and write back the changes. Application
  // example: user database, where user records are read
  // and modified by the user or to record user activity.

  // Read/read-modify-write ratio: 50/50
  // Default data size: 1 KB records
  // Request distribution: zipfian

  void YCSBWorkloadF_CL(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num, FLAGS_YCSB_zipfian_alpha);

    std::string value;
    int64_t found = 0;

    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, 0);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);


    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);

      long k;
      if (FLAGS_YCSB_uniform_distribution){
        //Generate number from uniform distribution
        k = thread->rand.Next() % FLAGS_num;
      } else { //default
        //Generate number from zipf distribution
        k = nextValue() % FLAGS_num;
      }
      GenerateKeyFromInt(k, FLAGS_num, &key);

      int next_op = thread->rand.Next() % 100;
      if (next_op < 50){
        //read
        Status s = db->Get(options, key, &value);
        if (!s.ok() && !s.IsNotFound()) {
          //fprintf(stderr, "get error: %s\n", s.ToString().c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          found++;
          thread->stats.FinishedOps(nullptr, db, 1, kRead);
        }
        reads_done++;

      } else{
        //read-modify-write.
        Status s = db->Get(options, key, &value);
        s = db->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          //exit(1);
        } else{
            writes_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kWrite);
        }
      }

    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
  }

*/

  // ------- YCSB OPEN LOOP IMPLEMENTATIONS----------//  

  // static void ycsb_callback(Benchmark* bptr, ThreadState* thread, int tid, Status s, Operation op, uint64_t op_lat_ns, uint64_t start_time) {
  static void ycsb_callback(Benchmark* bptr, ThreadState* thread, int tid, Status s, Operation op, uint64_t op_lat, bool toLock) {
	if (toLock) { thread->mtx.lock();}

    if (!s.ok()) {
      fprintf(stderr, "OPERATION FAILED\n");
      //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
      //exit(1);
    }
    else {
      if (op == Operation::PUT){
        thread->writes_done++;
        // thread->stats_ycsb.FinishedOps_OL(1, op_lat, tid, kWrite);
        thread->stats_ycsb.FinishedOps_OL_Warmup(1, op_lat, tid, kWrite);

        //if (thread->writes_done%1000 == 0) {
        //  fprintf(stderr, "CALLBACK PUT tid %d writes_done %lu latency %llu\n", tid, thread->writes_done, op_lat);
        //}
      }
      else if (op == Operation::GET) {
        thread->reads_done++;
        // thread->stats_ycsb.FinishedOps_OL(1, op_lat, tid, kRead);
        thread->stats_ycsb.FinishedOps_OL_Warmup(1, op_lat, tid, kRead);
        
        //if (thread->writes_done%1000 == 0) {
        //  fprintf(stderr, "CALLBACK GET tid %d reads_done %lu latency %llu\n", tid, thread->reads_done, op_lat);
        //}
      }
      else { fprintf(stderr, "UNKNOWN Operation\n"); }
    }
    if (toLock) { thread->mtx.unlock();}

    //fprintf(stderr, "thread %d, callback done = %llu\n", tid, (thread->reads_done + thread->writes_done));
    if ((thread->reads_done + thread->writes_done) == thread->total_ops) {
      fprintf(stderr, "thread %d, total ops: %llu\n", tid, thread->total_ops);
      std::unique_lock<std::mutex> lck(thread->mtx);
      thread->cv.notify_all();
    }
  }

  void YCSBFillDB_OL(ThreadState* thread) {
		using namespace std::chrono;
    std::string value;

    //write in order
    // thread->tid is same as partition id (pid)
    long shard_size = FLAGS_num/FLAGS_threads;
    long low = thread->tid*shard_size;
		std::vector<long> keys;
    for (long k = low; k < (low+shard_size); k++){
			keys.push_back(k);
	  }
		// JIANAN: If sequential loading, comment the line below	
		unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
		shuffle(keys.begin(), keys.end(), std::default_random_engine(seed));

    thread->reads_done = 0;
    thread->writes_done = 0;
    thread->total_ops = shard_size;
    thread->stats_ycsb.SetNumWarmupKeys(static_cast<uint64_t>(shard_size * FLAGS_warmup_ratio));

		void (*y_cb)(Benchmark*, ThreadState*, int, Status, Operation, uint64_t,bool);
		y_cb = &ycsb_callback;
	
		// set open-loop batch size to be 50 kops per sec 
		// batch_size needs to be a full multiple of shard size
		// current best throughput is around 40 kops, current batch_size already saturate
    int batch_size = 100000;
		int done = 0;
		while (done < shard_size) {
			int cur_batch_size = std::min(batch_size, (static_cast<int>(shard_size) - done));
			for (int i = 0; i < cur_batch_size; i++) {
				long k = keys.at(done);
				//fprintf(stderr, "thread %d: Put key %lu\n", thread->tid, k);
				auto req_time = std::chrono::high_resolution_clock::now();
				// thread pool enqueue
				// first arg specifies the partition thread id
				pool.enqueueOp(thread->tid, [this](ThreadState* thread, int tid, long k, time_point<high_resolution_clock> req_time, void (*cb)(Benchmark*,ThreadState*, int,Status,Operation,uint64_t,bool))
					{
						auto st = high_resolution_clock::now();
						std::unique_ptr<const char[]> key_guard;
						Slice key = AllocateKey(&key_guard);
						GenerateKeyFromInt(k, FLAGS_num, &key);
						//fprintf(stderr, "Initialization %llu\n", std::chrono::duration_cast<nano>(std::chrono::high_resolution_clock::now() - st).count());
						auto begin = high_resolution_clock::now();
						//fprintf(stderr, "Time to start %llu\n", std::chrono::duration_cast<nano>(std::chrono::high_resolution_clock::now() - req_time).count());
						Status s = db_->Put(write_options_, key, gen_.Generate(value_size_));
						//fprintf(stderr, "thread %d, key %llu\n", tid, k);
						//fprintf(stderr, "Latency of Put (us) %llu\n", std::chrono::duration_cast<nano>(std::chrono::high_resolution_clock::now() - begin).count()/1000);
						uint64_t op_lat_us = duration_cast<microseconds>(high_resolution_clock::now() - req_time).count();
						//fprintf(stderr, "Latency of Put %llu\n", op_lat_ns);
						cb(this, thread, tid, s, Operation::PUT, op_lat_us, false);
					}, thread, thread->tid, k, req_time, y_cb);
					done++;
				}
      std::this_thread::sleep_for(seconds(1));
    }   
      
    std::unique_lock<std::mutex> lck(thread->mtx);
    while ((thread->reads_done + thread->writes_done) != thread->total_ops) {
      thread->cv.wait(lck);
    }

		//using namespace std::chrono;
    //std::string value;
    //int64_t found = 0;

    //int64_t reads_done = 0;
    //int64_t writes_done = 0;

    ////write in order
    //// thread->tid is same as partition id (pid)
    //long shard_size = FLAGS_num/FLAGS_threads;
    //long low = thread->tid*shard_size;

    //thread->reads_done = 0;
    //thread->writes_done = 0;
    //thread->total_ops = shard_size;
    //thread->stats_ycsb.SetNumWarmupKeys(static_cast<uint64_t>(shard_size * FLAGS_warmup_ratio));

		//void (*y_cb)(Benchmark*, ThreadState*, int, Status, Operation, uint64_t,bool);
		//y_cb = &ycsb_callback;
	
		//// set open-loop batch size to be 50 kops per sec 
		//// batch_size needs to be a full multiple of shard size
		//// current best throughput is around 40 kops, current batch_size already saturate
    //int batch_size = 100000;
		//long k = low;
		//while (k < (low+shard_size)) {
    ////for (long k = low; k < (low+shard_size); k++){
		//	for (int i = 0; i < batch_size; i++) {
		//		//fprintf(stderr, "thread %d: Put key %lu\n", thread->tid, k);
		//		auto req_time = std::chrono::high_resolution_clock::now();
		//		// thread pool enqueue
		//		// first arg specifies the partition thread id
		//		pool.enqueueOp(thread->tid, [this](ThreadState* thread, int tid, long k, time_point<high_resolution_clock> req_time, void (*cb)(Benchmark*,ThreadState*, int,Status,Operation,uint64_t,bool))
		//			{
		//				auto st = high_resolution_clock::now();
		//				std::unique_ptr<const char[]> key_guard;
		//				Slice key = AllocateKey(&key_guard);
		//				GenerateKeyFromInt(k, FLAGS_num, &key);
		//				//fprintf(stderr, "Initialization %llu\n", std::chrono::duration_cast<nano>(std::chrono::high_resolution_clock::now() - st).count());
		//				auto begin = high_resolution_clock::now();
		//				//fprintf(stderr, "Time to start %llu\n", std::chrono::duration_cast<nano>(std::chrono::high_resolution_clock::now() - req_time).count());
		//				Status s = db_->Put(write_options_, key, gen_.Generate(value_size_));
		//				//fprintf(stderr, "thread %d, key %llu\n", tid, k);
		//				//fprintf(stderr, "Latency of Put (us) %llu\n", std::chrono::duration_cast<nano>(std::chrono::high_resolution_clock::now() - begin).count()/1000);
		//				uint64_t op_lat_us = duration_cast<microseconds>(high_resolution_clock::now() - req_time).count();
		//				//fprintf(stderr, "Latency of Put %llu\n", op_lat_ns);
		//				cb(this, thread, tid, s, Operation::PUT, op_lat_us, false);
		//			}, thread, thread->tid, k, req_time, y_cb);
		//			k++;
		//		}
    //  std::this_thread::sleep_for(seconds(1));
    //}   
    //  
    //std::unique_lock<std::mutex> lck(thread->mtx);
    //while ((thread->reads_done + thread->writes_done) != thread->total_ops) {
    //  thread->cv.wait(lck);
    //}
    //fprintf(stderr, "woke up\n");
		// JIANAN: not sure if we need the following reports anymore
    //char msg[100];
    //snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
    //         " total:%" PRIu64 " found:%" PRIu64 ")",
    //         reads_done, writes_done, readwrites_, found);
    //thread->stats_ycsb.AddMessage(msg);
  }



  void YCSBWorkloadA_OL(ThreadState* thread) {
    using namespace std::chrono;
    //ReadOptions options(FLAGS_verify_checksum, true);
    ReadOptions options;
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num, FLAGS_YCSB_zipfian_alpha);

    std::string value;
    long shard_size = FLAGS_num/FLAGS_threadpool_num;
    thread->reads_done = 0;
    thread->writes_done = 0;
		// floor total_ops to be a multiple of batch_size (easier to report performance)
    thread->total_ops = static_cast<uint64_t>(FLAGS_reads/FLAGS_threads);
    thread->stats_ycsb.SetNumWarmupKeys(static_cast<uint64_t>(thread->total_ops * FLAGS_warmup_ratio));

    void (*y_cb)(Benchmark*, ThreadState*, int, Status, Operation, uint64_t, bool);
    y_cb = &ycsb_callback;

    Duration duration(0, thread->total_ops);
    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
			uint64_t k;
			if (FLAGS_YCSB_uniform_distribution){
				//Generate number from uniform distribution            
				k = thread->rand.Next() % FLAGS_num;
			} else { //default
				//Generate number from zipf distribution
				uint64_t temp = nextValue() % FLAGS_num;
				std::hash<std::string> hash_str;    
				k = hash_str(std::to_string(temp))%FLAGS_num;
			}

			k = static_cast<long>(k);
			int p = k / shard_size;
			int next_op = thread->rand.Next() % 100;
			auto req_time = std::chrono::high_resolution_clock::now();
			if (next_op < 0){
			//if (next_op < 50){
				// thread pool enqueue
				pool.enqueueOp(p, [this](ThreadState* thread, int tid, long k, std::string *val_ptr, ReadOptions &options, time_point<high_resolution_clock> req_time, void (*cb)(Benchmark*,ThreadState*, int,Status,Operation,uint64_t,bool))
				{
					std::unique_ptr<const char[]> key_guard;
					Slice key = AllocateKey(&key_guard);
					GenerateKeyFromInt(k, FLAGS_num, &key);

					auto begin = high_resolution_clock::now();
					// JIANAN: value field is not thread-safe, TODO later 
					Status s = db_->Get(options, key, val_ptr);
					uint64_t op_lat_us = duration_cast<microseconds>(high_resolution_clock::now() - begin).count();
					//fprintf(stderr, "thread %d: Latency of Get %llu us\n", tid, op_lat_us);
					cb(this, thread, tid, s, Operation::GET, op_lat_us, true);
				}, thread, thread->tid, k, &value, options, req_time, y_cb);
			} else{
				// thread pool enqueue        
				pool.enqueueOp(p, [this](ThreadState* thread, int tid, long k, RandomGenerator *gen, time_point<high_resolution_clock> req_time, void (*cb)(Benchmark*,ThreadState*, int,Status,Operation,uint64_t,bool))
				{
					std::unique_ptr<const char[]> key_guard;
					Slice key = AllocateKey(&key_guard);
					GenerateKeyFromInt(k, FLAGS_num, &key);

					auto begin = high_resolution_clock::now();
					Status s = db_->Put(write_options_, key, gen->Generate(value_size_));
					uint64_t op_lat_us = duration_cast<microseconds>(high_resolution_clock::now() - begin).count();
					//fprintf(stderr, "thread %d: Latency of Put %llu us\n", tid, op_lat_us);
					cb(this, thread, tid, s, Operation::PUT, op_lat_us,true);
				}, thread, thread->tid, k, &gen, req_time, y_cb);             
			} 
    }
    std::unique_lock<std::mutex> lck(thread->mtx);
    while ((thread->reads_done + thread->writes_done) != thread->total_ops) {
      thread->cv.wait(lck);
    }
  }
	
  void YCSBWorkloadA_OL_Batch(ThreadState* thread) {
		// JIANAN: without pre-processing
    using namespace std::chrono;
    //ReadOptions options(FLAGS_verify_checksum, true);
    ReadOptions options;
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num, FLAGS_YCSB_zipfian_alpha);

		// set open-loop batch size to be 50 kops per sec 
		// batch_size needs to be a full multiple of shard size
    int batch_size = 150000;

    std::string value;
    long shard_size = FLAGS_num/FLAGS_threads;
    thread->reads_done = 0;
    thread->writes_done = 0;
		// floor total_ops to be a multiple of batch_size (easier to report performance)
    thread->total_ops = static_cast<uint64_t>(((FLAGS_reads/FLAGS_threads)/batch_size)*batch_size);
    thread->stats_ycsb.SetNumWarmupKeys(static_cast<uint64_t>(thread->total_ops * FLAGS_warmup_ratio));

    void (*y_cb)(Benchmark*, ThreadState*, int, Status, Operation, uint64_t, bool);
    y_cb = &ycsb_callback;

    //Duration duration(0, FLAGS_reads/FLAGS_threads);
    Duration duration(0, thread->total_ops);
    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(batch_size)) {

      auto begin_for = high_resolution_clock::now();
      // batch 50k ops, then sleep for 1s
      for (int i = 0; i < batch_size; i++) {        

        uint64_t k;
        if (FLAGS_YCSB_uniform_distribution){
          //Generate number from uniform distribution            
          k = thread->rand.Next() % FLAGS_num;
        } else { //default
          //Generate number from zipf distribution
          uint64_t temp = nextValue() % FLAGS_num;
          std::hash<std::string> hash_str;    
          k = hash_str(std::to_string(temp))%FLAGS_num;
        }

        k = static_cast<long>(k);
        int p = k / shard_size;
        int next_op = thread->rand.Next() % 100;
        auto req_time = std::chrono::high_resolution_clock::now();
        if (next_op < 0){
        //if (next_op < 50){
          // thread pool enqueue
          pool.enqueueOp(p, [this](ThreadState* thread, int tid, long k, std::string *val_ptr, ReadOptions &options, time_point<high_resolution_clock> req_time, void (*cb)(Benchmark*,ThreadState*, int,Status,Operation,uint64_t,bool))
          {
            std::unique_ptr<const char[]> key_guard;
            Slice key = AllocateKey(&key_guard);
            GenerateKeyFromInt(k, FLAGS_num, &key);

            auto begin = high_resolution_clock::now();
            // JIANAN: value field is not thread-safe, TODO later 
            Status s = db_->Get(options, key, val_ptr);
            uint64_t op_lat_us = duration_cast<microseconds>(high_resolution_clock::now() - begin).count();
            //fprintf(stderr, "thread %d: Latency of Get %llu us\n", tid, op_lat_us);
            cb(this, thread, tid, s, Operation::GET, op_lat_us, true);
          }, thread, thread->tid, k, &value, options, req_time, y_cb);
        } else{
          // thread pool enqueue        
          pool.enqueueOp(p, [this](ThreadState* thread, int tid, long k, RandomGenerator *gen, time_point<high_resolution_clock> req_time, void (*cb)(Benchmark*,ThreadState*, int,Status,Operation,uint64_t,bool))
          {
            std::unique_ptr<const char[]> key_guard;
            Slice key = AllocateKey(&key_guard);
            GenerateKeyFromInt(k, FLAGS_num, &key);

            auto begin = high_resolution_clock::now();
            Status s = db_->Put(write_options_, key, gen->Generate(value_size_));
            uint64_t op_lat_us = duration_cast<microseconds>(high_resolution_clock::now() - begin).count();
            //fprintf(stderr, "thread %d: Latency of Put %llu us\n", tid, op_lat_us);
            cb(this, thread, tid, s, Operation::PUT, op_lat_us,true);
          }, thread, thread->tid, k, &gen, req_time, y_cb);             
        } 
        // fprintf(stderr, "thread %d: item %llu\n", thread->tid, count);
      }
      auto end_for = high_resolution_clock::now();
      //fprintf(stderr, "thread %d: for loop takes %llu ms\n", thread->tid, duration_cast<milliseconds>(end_for - begin_for).count());
      std::this_thread::sleep_for(seconds(1));
    }
    std::unique_lock<std::mutex> lck(thread->mtx);
    while ((thread->reads_done + thread->writes_done) != thread->total_ops) {
      thread->cv.wait(lck);
    }
  }

  void YCSBWorkloadA_OL_Batch_Preprocess(ThreadState* thread) {
		// JIANAN: with pre-processing
    using namespace std::chrono;
    fprintf(stderr, "workload A open-loop called \n");
    ReadOptions options;
    RandomGenerator gen; // used to generate values, not keys

    std::vector<uint64_t> *keys = threadKeys_[thread->tid];
		// set open-loop batch size to be 50 kops per sec 
		// batch_size needs to be a full multiple of shard size
    int batch_size = 150000;

    std::string value;
    long shard_size = FLAGS_num/FLAGS_threads;
    thread->reads_done = 0;
    thread->writes_done = 0;
		// ceil total ops to be a multiple of batch_size
    thread->total_ops = static_cast<uint64_t>((keys->size()/batch_size) * batch_size);
    thread->stats_ycsb.SetNumWarmupKeys(static_cast<uint64_t>(thread->total_ops * FLAGS_warmup_ratio));

    void (*y_cb)(Benchmark*, ThreadState*, int, Status, Operation, uint64_t, bool);
    y_cb = &ycsb_callback;

    Duration duration(0, thread->total_ops);
    //fprintf(stderr, "ycsb thread will issue %d requests\n", (FLAGS_reads/FLAGS_threads));

		int done = 0; 
		int total = thread->total_ops;
    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(batch_size)) {
			//cur_batch_size = std::min((total-done), batch_size);
      auto begin_for = high_resolution_clock::now();
      // batch 50k ops, then sleep for 1s
      for (int i = 0; i < batch_size; i++) {        

        uint64_t k = static_cast<long>(keys->at(done));
        //int p = k / shard_size;
        int next_op = thread->rand.Next() % 100;
        auto req_time = std::chrono::high_resolution_clock::now();
        if (next_op < 0){
        //if (next_op < 50){
          // thread pool enqueue
          pool.enqueueOp(thread->tid, [this](ThreadState* thread, int tid, long k, std::string *val_ptr, ReadOptions &options, time_point<high_resolution_clock> req_time, void (*cb)(Benchmark*,ThreadState*, int,Status,Operation,uint64_t,bool))
          {
            std::unique_ptr<const char[]> key_guard;
            Slice key = AllocateKey(&key_guard);
            GenerateKeyFromInt(k, FLAGS_num, &key);

            auto begin = high_resolution_clock::now();
            // JIANAN: value field is not thread-safe, TODO later 
            Status s = db_->Get(options, key, val_ptr);
            uint64_t op_lat_us = duration_cast<microseconds>(high_resolution_clock::now() - begin).count();
            //fprintf(stderr, "thread %d: Latency of Get %llu us\n", tid, op_lat_us);
            cb(this, thread, tid, s, Operation::GET, op_lat_us, false);
          }, thread, thread->tid, k, &value, options, req_time, y_cb);
        } else{
          // thread pool enqueue        
          pool.enqueueOp(thread->tid, [this](ThreadState* thread, int tid, long k, RandomGenerator *gen, time_point<high_resolution_clock> req_time, void (*cb)(Benchmark*,ThreadState*, int,Status,Operation,uint64_t,bool))
          {
            std::unique_ptr<const char[]> key_guard;
            Slice key = AllocateKey(&key_guard);
            GenerateKeyFromInt(k, FLAGS_num, &key);

            auto begin = high_resolution_clock::now();
            Status s = db_->Put(write_options_, key, gen->Generate(value_size_));
            uint64_t op_lat_us = duration_cast<microseconds>(high_resolution_clock::now() - begin).count();
            //fprintf(stderr, "Workload A, Thread %d: Key %lu, Latency of Put %llu us\n", tid, k, op_lat_us);
            cb(this, thread, tid, s, Operation::PUT, op_lat_us, false);
          }, thread, thread->tid, k, &gen, req_time, y_cb);             
        } 
				done++;
        // fprintf(stderr, "thread %d: item %llu\n", thread->tid, count);
      }
      auto end_for = high_resolution_clock::now();
      //fprintf(stderr, "thread %d: for loop takes %llu ms\n", thread->tid, duration_cast<milliseconds>(end_for - begin_for).count());
      std::this_thread::sleep_for(seconds(1));
    }
		fprintf(stderr, "thread %d: item done %d\n", thread->tid, done);
    std::unique_lock<std::mutex> lck(thread->mtx);
    while ((thread->reads_done + thread->writes_done) != thread->total_ops) {
      thread->cv.wait(lck);
    }
	}

  /*
  // Workload A: Update heavy workload
  // This workload has a mix of 50/50 reads and writes. 
  // An application example is a session store recording recent actions.
  // Read/update ratio: 50/50
  // Default data size: 1 KB records 
  // Request distribution: zipfian
  void YCSBWorkloadA_OL(ThreadState* thread) {
    //ReadOptions options(FLAGS_verify_checksum, true);
    ReadOptions options;
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num, FLAGS_YCSB_zipfian_alpha);

    
    std::string value;
    int64_t found = 0;

    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(0, FLAGS_reads/FLAGS_threads);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    //if (FLAGS_benchmark_write_rate_limit > 0) {
    //   printf(">>>> FLAGS_benchmark_write_rate_limit YCSBA \n");
    //  thread->shared->write_rate_limiter.reset(
    //      NewGenericRateLimiter(FLAGS_benchmark_write_rate_limit));
    //}

    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      //DB* db = SelectDB(thread);
       
          uint64_t k;
          if (FLAGS_YCSB_uniform_distribution){
            //Generate number from uniform distribution            
            k = thread->rand.Next() % FLAGS_num;
          } else { //default
            //Generate number from zipf distribution
            uint64_t temp = nextValue() % FLAGS_num;
            std::hash<std::string> hash_str;    
            k = hash_str(std::to_string(temp))%FLAGS_num;
	    //k = temp;
	    //fprintf(stderr, "KEY temp %llu hash %llu\n", temp, k);
          }
          GenerateKeyFromInt(k, FLAGS_num, &key);

          int next_op = thread->rand.Next() % 100;
          if (next_op < 50){
            //read
            Status s = db_->Get(options, key, &value);
            if (!s.ok() && !s.IsNotFound()) {
              //fprintf(stderr, "k=%d; get error: %s\n", k, s.ToString().c_str());
              //exit(1);
              // we continue after error rather than exiting so that we can
              // find more errors if any
            } else if (!s.IsNotFound()) {
              found++;
	      thread->stats_ycsb.FinishedOps(db_, 1, thread->tid, kRead);
            }
            reads_done++;
            
          } else{
            //write
            //if (FLAGS_benchmark_write_rate_limit > 0) {
                
            //    thread->shared->write_rate_limiter->Request(
            //        value_size_ + key_size_, Env::IO_HIGH,
            //        nullptr, RateLimiter::OpType::kWrite);
            //    thread->stats_ycsb.ResetLastOpTime();
            //}
            Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
            if (!s.ok()) {
              //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
              //exit(1);
            } else{
             writes_done++;
	     thread->stats_ycsb.FinishedOps(db_, 1, thread->tid, kWrite);
            }                
        }
    } 
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats_ycsb.AddMessage(msg);
  }

  // Workload B: Read mostly workload
  // This workload has a 95/5 reads/write mix. 
  // Application example: photo tagging; add a tag is an update, 
  // but most operations are to read tags.

  // Read/update ratio: 95/5
  // Default data size: 1 KB records 
  // Request distribution: zipfian
  void YCSBWorkloadB_OL(ThreadState* thread) {
    //ReadOptions options(FLAGS_verify_checksum, true);
    ReadOptions options;
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num, FLAGS_YCSB_zipfian_alpha);

    
    std::string value;
    int64_t found = 0;

    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(0, FLAGS_reads/FLAGS_threads);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    //if (FLAGS_benchmark_write_rate_limit > 0) {
    //   printf(">>>> FLAGS_benchmark_write_rate_limit YCSBA \n");
    //  thread->shared->write_rate_limiter.reset(
    //      NewGenericRateLimiter(FLAGS_benchmark_write_rate_limit));
    }

    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      //DB* db = SelectDB(thread);
       
          uint64_t k;
          if (FLAGS_YCSB_uniform_distribution){
            //Generate number from uniform distribution            
            k = thread->rand.Next() % FLAGS_num;
          } else { //default
            //Generate number from zipf distribution
            uint64_t temp = nextValue() % FLAGS_num;
            //std::hash<std::string> hash_str;    
            //k = hash_str(std::to_string(temp))%FLAGS_num;
	    k = temp;
	    //fprintf(stderr, "KEY temp %llu hash %llu\n", temp, k);
          }
          GenerateKeyFromInt(k, FLAGS_num, &key);

          int next_op = thread->rand.Next() % 100;
          if (next_op < 95){
            //read
            Status s = db_->Get(options, key, &value);
            if (!s.ok() && !s.IsNotFound()) {
              //fprintf(stderr, "k=%d; get error: %s\n", k, s.ToString().c_str());
              //exit(1);
              // we continue after error rather than exiting so that we can
              // find more errors if any
            } else if (!s.IsNotFound()) {
              found++;
	      thread->stats_ycsb.FinishedOps(db_, 1, thread->tid, kRead);
            }
            reads_done++;
            
          } else{
            //write
            //if (FLAGS_benchmark_write_rate_limit > 0) {
                
            //    thread->shared->write_rate_limiter->Request(
            //        value_size_ + key_size_, Env::IO_HIGH,
            //        nullptr, RateLimiter::OpType::kWrite);
            //    thread->stats_ycsb.ResetLastOpTime();
            }
            Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
            if (!s.ok()) {
              //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
              //exit(1);
            } else{
             writes_done++;
	     thread->stats_ycsb.FinishedOps(db_, 1, thread->tid, kWrite);
            }                
        }
    } 
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats_ycsb.AddMessage(msg);
  }

  // Workload C: Read only
  // This workload is 100% read. Application example: user profile cache, 
  // where profiles are constructed elsewhere (e.g., Hadoop).
  // Read/update ratio: 100/0
  // Default data size: 1 KB records 
  // Request distribution: zipfian
  void YCSBWorkloadC_OL(ThreadState* thread) {
    //ReadOptions options(FLAGS_verify_checksum, true);
    ReadOptions options;
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num, FLAGS_YCSB_zipfian_alpha);

    
    std::string value;
    int64_t found = 0;

    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(0, FLAGS_reads/FLAGS_threads);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    //if (FLAGS_benchmark_write_rate_limit > 0) {
    //   printf(">>>> FLAGS_benchmark_write_rate_limit YCSBA \n");
    //  thread->shared->write_rate_limiter.reset(
    //      NewGenericRateLimiter(FLAGS_benchmark_write_rate_limit));
    }

    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      //DB* db = SelectDB(thread);
       
          uint64_t k;
          if (FLAGS_YCSB_uniform_distribution){
            //Generate number from uniform distribution            
            k = thread->rand.Next() % FLAGS_num;
          } else { //default
            //Generate number from zipf distribution
            uint64_t temp = nextValue() % FLAGS_num;
            //std::hash<std::string> hash_str;    
            //k = hash_str(std::to_string(temp))%FLAGS_num;
	          k = temp;
	    //fprintf(stderr, "KEY temp %llu hash %llu\n", temp, k);
          }
          GenerateKeyFromInt(k, FLAGS_num, &key);
          //read
          Status s = db_->Get(options, key, &value);
          if (!s.ok() && !s.IsNotFound()) {
            //fprintf(stderr, "k=%d; get error: %s\n", k, s.ToString().c_str());
            //exit(1);
            // we continue after error rather than exiting so that we can
            // find more errors if any
          } else if (!s.IsNotFound()) {
            found++;
            thread->stats_ycsb.FinishedOps(db_, 1, thread->tid, kRead);
          }
          reads_done++;
    } 
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats_ycsb.AddMessage(msg);
  }


  // Workload D: Read latest workload
  // In this workload, new records are inserted, and the most recently 
  // inserted records are the most popular. Application example: 
  // user status updates; people want to read the latest.
  
  // Read/update/insert ratio: 95/0/5
  // Default data size: 1 KB records 
  // Request distribution: latest
  // The insert order for this is hashed, not ordered. The "latest" items may be 
  // scattered around the keyspace if they are keyed by userid.timestamp. A workload
  // which orders items purely by time, and demands the latest, is very different than 
  // workload here (which we believe is more typical of how people build systems.)
  void YCSBWorkloadD(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num, FLAGS_YCSB_zipfian_alpha);
    std::string value;
    int64_t found = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, 0);
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      long k;
      if (FLAGS_YCSB_uniform_distribution){
        //Generate number from uniform distribution            
        k = thread->rand.Next() % FLAGS_num;
      } else { //default
        //Generate number from latest distribution
        k = next_value_latestgen() % FLAGS_num;           
      }
      GenerateKeyFromInt(k, FLAGS_num, &key);
      int next_op = thread->rand.Next() % 100;
      if (next_op < 95){
        //read
        Status s = db->Get(options, key, &value);
        if (!s.ok() && !s.IsNotFound()) {
          //fprintf(stderr, "get error: %s\n", s.ToString().c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          found++;
          thread->stats.FinishedOps(nullptr, db, 1, kRead);
        }
        reads_done++;
        
      } else{
        //write
        Status s = db->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          //exit(1);
        } else{
            writes_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kWrite);
        }
      }
    } 
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
    
  }
  // Workload E: Short ranges. 
  // In this workload, short ranges of records are queried,
  // instead of individual records. Application example: 
  // threaded conversations, where each scan is for the posts 
  // in a given thread (assumed to be clustered by thread id).
  
  // Scan/insert ratio: 95/5
  // Default data size: 1 KB records 
  // Request distribution: latest
  // Scan Length Distribution=uniform
  // Max scan length = 100
  // The insert order is hashed, not ordered. Although the scans are ordered, it does not necessarily
  // follow that the data is inserted in order. For example, posts for thread 
  // 342 may not be inserted contiguously, but
  // instead interspersed with posts from lots of other threads. The way the YCSB 
  // client works is that it will pick a start
  // key, and then request a number of records; this works fine even for hashed insertion.
  void YCSBWorkloadE(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num, FLAGS_YCSB_zipfian_alpha);
    
    std::string value;
    int64_t found = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, 0);
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      long k;
      if (FLAGS_YCSB_uniform_distribution){
        //Generate number from uniform distribution            
        k = thread->rand.Next() % FLAGS_num;
      } else { //default
        //Generate number from zipf distribution
        k = nextValue() % FLAGS_num;            
      }
      GenerateKeyFromInt(k, FLAGS_num, &key);
      int next_op = thread->rand.Next() % 100;
      if (next_op < 95){
        //scan
        
        //TODO need to draw a random number for the scan length
        //for now, scan lenght constant
        int scan_length = thread->rand.Next() % 100;
        Iterator* iter = db->NewIterator(options);
        int64_t i = 0;
        int64_t bytes = 0;
        for (iter->Seek(key); i < 100 && iter->Valid(); iter->Next()) {
          bytes += iter->key().size() + iter->value().size();
          //thread->stats.FinishedOps(nullptr, db, 1, kRead);
          ++i;
        }
        delete iter;
        reads_done++;
        thread->stats.FinishedOps(nullptr, db, 1, kRead);
      } else{
        //write
        Status s = db->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          //exit(1);
        } else{
            writes_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kWrite);
        }
      }
    } 
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
    
  }
  
  // Workload F: Read-modify-write workload
  // In this workload, the client will read a record, 
  // modify it, and write back the changes. Application 
  // example: user database, where user records are read 
  // and modified by the user or to record user activity.
  // Read/read-modify-write ratio: 50/50
  // Default data size: 1 KB records 
  // Request distribution: zipfian
  void YCSBWorkloadF(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    init_latestgen(FLAGS_num);
    init_zipf_generator(0, FLAGS_num, FLAGS_YCSB_zipfian_alpha);
    std::string value;
    int64_t found = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, 0);
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      long k;
      if (FLAGS_YCSB_uniform_distribution){
        //Generate number from uniform distribution            
        k = thread->rand.Next() % FLAGS_num;
      } else { //default
        //Generate number from zipf distribution
        k = nextValue() % FLAGS_num;            
      }
      GenerateKeyFromInt(k, FLAGS_num, &key);
      int next_op = thread->rand.Next() % 100;
      if (next_op < 50){
        //read
        Status s = db->Get(options, key, &value);
        if (!s.ok() && !s.IsNotFound()) {
          //fprintf(stderr, "get error: %s\n", s.ToString().c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          found++;
          thread->stats.FinishedOps(nullptr, db, 1, kRead);
        }
        reads_done++;
        
      } else{
        //read-modify-write.
        Status s = db->Get(options, key, &value);
        s = db->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          //fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          //exit(1);
        } else{
            writes_done++;
            thread->stats.FinishedOps(nullptr, db, 1, kWrite);
        }
      }
    } 
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
  }
*/  

  void Open() {
    assert(db_ == nullptr);
    Options options;
    options.env = g_env;
    options.create_if_missing = !FLAGS_use_existing_db;
    options.block_cache = cache_;
    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_file_size = FLAGS_max_file_size;
    options.block_size = FLAGS_block_size;
    if (FLAGS_comparisons) {
      options.comparator = &count_comparator_;
    }
    options.max_open_files = FLAGS_open_files;
    options.filter_policy = filter_policy_;
    options.reuse_logs = FLAGS_reuse_logs;
    options.migration_logging = FLAGS_migration_logging;
    options.read_logging = FLAGS_read_logging;
    options.migration_policy = FLAGS_migration_policy;
    options.migration_metric = FLAGS_migration_metric;
    options.migration_rand_range_num = FLAGS_migration_rand_range_num;
    options.migration_rand_range_size = FLAGS_migration_rand_range_size;
    Status s = DB::Open(options, FLAGS_db, &db_);
    if (!s.ok()) {
      std::fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      std::exit(1);
    }

    // Jianan
    // load the popularity file 
    // only for testing the oracle scheme
    db_->SetPopFile(FLAGS_pop_file);
  }

  void OpenBench(ThreadState* thread) {
    for (int i = 0; i < num_; i++) {
      delete db_;
      Open();
      thread->stats.FinishedSingleOp();
    }
  }

  void WriteSeq(ThreadState* thread) { DoWrite(thread, true); }

  void WriteRandom(ThreadState* thread) { DoWrite(thread, false); }

  void DoWrite(ThreadState* thread, bool seq) {
    if (num_ != FLAGS_num) {
      char msg[100];
      std::snprintf(msg, sizeof(msg), "(%d ops)", num_);
      thread->stats.AddMessage(msg);
    }

    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    int64_t bytes = 0;
    KeyBuffer key;
    for (int i = 0; i < num_; i += entries_per_batch_) {
      batch.Clear();
      for (int j = 0; j < entries_per_batch_; j++) {
        const int k = seq ? i + j : thread->rand.Uniform(FLAGS_num);
        key.Set(k);
	//ASH:
        //batch.Put(key.slice(), gen.Generate(value_size_));
        s=db_->Put(write_options_, key.slice(), gen.Generate(value_size_));
        bytes += value_size_ + key.slice().size();
        thread->stats.FinishedSingleOp();
      }
      //ASH:
      //s = db_->Write(write_options_, &batch);
      if (!s.ok()) {
        std::fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        std::exit(1);
      }
    }
    thread->stats.AddBytes(bytes);
  }

  void ReadSequential(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    int i = 0;
    int64_t bytes = 0;
    for (iter->SeekToFirst(); i < reads_ && iter->Valid(); iter->Next()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedSingleOp();
      ++i;
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }

  void ReadReverse(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    int i = 0;
    int64_t bytes = 0;
    for (iter->SeekToLast(); i < reads_ && iter->Valid(); iter->Prev()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedSingleOp();
      ++i;
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }

  void ReadRandom(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    int found = 0;
    KeyBuffer key;
    for (int i = 0; i < reads_; i++) {
      const int k = thread->rand.Uniform(FLAGS_num);
      key.Set(k);
      if (db_->Get(options, key.slice(), &value).ok()) {
        found++;
      }
      thread->stats.FinishedSingleOp();
    }
    char msg[100];
    std::snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }

  void ReadMissing(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    KeyBuffer key;
    for (int i = 0; i < reads_; i++) {
      const int k = thread->rand.Uniform(FLAGS_num);
      key.Set(k);
      Slice s = Slice(key.slice().data(), key.slice().size() - 1);
      db_->Get(options, s, &value);
      thread->stats.FinishedSingleOp();
    }
  }

  void ReadHot(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    const int range = (FLAGS_num + 99) / 100;
    KeyBuffer key;
    for (int i = 0; i < reads_; i++) {
      const int k = thread->rand.Uniform(range);
      key.Set(k);
      db_->Get(options, key.slice(), &value);
      thread->stats.FinishedSingleOp();
    }
  }

  void SeekRandom(ThreadState* thread) {
    ReadOptions options;
    int found = 0;
    KeyBuffer key;
    for (int i = 0; i < reads_; i++) {
      Iterator* iter = db_->NewIterator(options);
      const int k = thread->rand.Uniform(FLAGS_num);
      key.Set(k);
      iter->Seek(key.slice());
      if (iter->Valid() && iter->key() == key.slice()) found++;
      delete iter;
      thread->stats.FinishedSingleOp();
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }

  void SeekOrdered(ThreadState* thread) {
    ReadOptions options;
    Iterator* iter = db_->NewIterator(options);
    int found = 0;
    int k = 0;
    KeyBuffer key;
    for (int i = 0; i < reads_; i++) {
      k = (k + (thread->rand.Uniform(100))) % FLAGS_num;
      key.Set(k);
      iter->Seek(key.slice());
      if (iter->Valid() && iter->key() == key.slice()) found++;
      thread->stats.FinishedSingleOp();
    }
    delete iter;
    char msg[100];
    std::snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }

  void DoDelete(ThreadState* thread, bool seq) {
    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    KeyBuffer key;
    for (int i = 0; i < num_; i += entries_per_batch_) {
      batch.Clear();
      for (int j = 0; j < entries_per_batch_; j++) {
        const int k = seq ? i + j : (thread->rand.Uniform(FLAGS_num));
        key.Set(k);
        //batch.Delete(key.slice());
	s=db_->Delete(write_options_, key.slice());
        thread->stats.FinishedSingleOp();
      }
      //s = db_->Write(write_options_, &batch);
      if (!s.ok()) {
        std::fprintf(stderr, "del error: %s\n", s.ToString().c_str());
        std::exit(1);
      }
    }
  }

  void DeleteSeq(ThreadState* thread) { DoDelete(thread, true); }

  void DeleteRandom(ThreadState* thread) { DoDelete(thread, false); }

  void ReadWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
      ReadRandom(thread);
    } else {
      // Special thread that keeps writing until other threads are done.
      RandomGenerator gen;
      KeyBuffer key;
      while (true) {
        {
          MutexLock l(&thread->shared->mu);
          if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
            // Other threads have finished
            break;
          }
        }

        const int k = thread->rand.Uniform(FLAGS_num);
        key.Set(k);
        Status s =
            db_->Put(write_options_, key.slice(), gen.Generate(value_size_));
        if (!s.ok()) {
          std::fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          std::exit(1);
        }
      }

      // Do not count any of the preceding work/delay in stats.
      thread->stats.Start();
    }
  }

  void Compact(ThreadState* thread) { db_->CompactRange(nullptr, nullptr); }

  void PrintStats(const char* key) {
    std::string stats;
    if (!db_->GetProperty(key, &stats)) {
      stats = "(failed)";
    }
    std::fprintf(stdout, "\n%s\n", stats.c_str());
  }

  static void WriteToFile(void* arg, const char* buf, int n) {
    reinterpret_cast<WritableFile*>(arg)->Append(Slice(buf, n));
  }

  void HeapProfile() {
    char fname[100];
    std::snprintf(fname, sizeof(fname), "%s/heap-%04d", FLAGS_db,
                  ++heap_counter_);
    WritableFile* file;
    Status s = g_env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      std::fprintf(stderr, "%s\n", s.ToString().c_str());
      return;
    }
    bool ok = port::GetHeapProfile(WriteToFile, file);
    delete file;
    if (!ok) {
      std::fprintf(stderr, "heap profiling not supported\n");
      g_env->RemoveFile(fname);
    }
  }
};

}  // namespace leveldb

int main(int argc, char** argv) {
  FLAGS_write_buffer_size = leveldb::Options().write_buffer_size;
  FLAGS_max_file_size = leveldb::Options().max_file_size;
  FLAGS_block_size = leveldb::Options().block_size;
  FLAGS_open_files = leveldb::Options().max_open_files;
  std::string default_db_path;

  for (int i = 1; i < argc; i++) {
    double d;
    int n;
    char junk;
    if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
      FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
    } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
      FLAGS_compression_ratio = d;
    } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_histogram = n;
    } else if (sscanf(argv[i], "--comparisons=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_comparisons = n;
    } else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_use_existing_db = n;
    } else if (sscanf(argv[i], "--reuse_logs=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_reuse_logs = n;
    } else if (sscanf(argv[i], "--open_loop=%d%c", &n, &junk) == 1 && 
               (n == 0 || n == 1)) {
      FLAGS_open_loop = n;
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
      FLAGS_reads = n;
    } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
      FLAGS_threads = n;
    } else if (sscanf(argv[i], "--load_threads=%d%c", &n, &junk) == 1) {
      FLAGS_load_threads = n;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
      FLAGS_write_buffer_size = n;
    } else if (sscanf(argv[i], "--max_file_size=%d%c", &n, &junk) == 1) {
      FLAGS_max_file_size = n;
    } else if (sscanf(argv[i], "--block_size=%d%c", &n, &junk) == 1) {
      FLAGS_block_size = n;
    } else if (sscanf(argv[i], "--key_prefix=%d%c", &n, &junk) == 1) {
      FLAGS_key_prefix = n;
    } else if (sscanf(argv[i], "--cache_size=%d%c", &n, &junk) == 1) {
      FLAGS_cache_size = n;
    } else if (sscanf(argv[i], "--bloom_bits=%d%c", &n, &junk) == 1) {
      FLAGS_bloom_bits = n;
    } else if (sscanf(argv[i], "--open_files=%d%c", &n, &junk) == 1) {
      FLAGS_open_files = n;
    } else if (strncmp(argv[i], "--db=", 5) == 0) {
      FLAGS_db = argv[i] + 5;
    } else if (strncmp(argv[i], "--pop_file=", 11) == 0) {
      FLAGS_pop_file = argv[i] + 11;
      fprintf(stderr, "pop file: %s\n", FLAGS_pop_file);
    } else if (sscanf(argv[i], "--warmup_ratio=%lf%c", &d, &junk) == 1) {
      FLAGS_warmup_ratio = d;
      fprintf(stderr, "warmup ratio: %.2f\n", FLAGS_warmup_ratio);
    } else if (sscanf(argv[i], "--key_size=%d%c", &n, &junk) == 1) {
      FLAGS_key_size = n;
    } else if (sscanf(argv[i], "--YCSB_uniform_distribution=%d%c", &n, &junk) == 1 && 
                       (n == 0 || n == 1)){
      FLAGS_YCSB_uniform_distribution = n;
    } else if (sscanf(argv[i], "--YCSB_zipfian_alpha=%lf%c", &d, &junk) == 1) {
      FLAGS_YCSB_zipfian_alpha = d;
    } else if (sscanf(argv[i], "--duration=%d%c", &n, &junk) == 1) {
      FLAGS_duration = n;
    } else if (sscanf(argv[i], "--ops_between_duration_checks=%d%c", &n, &junk) == 1) {
      FLAGS_ops_between_duration_checks = n;

    } else if (sscanf(argv[i], "--read_logging=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_read_logging = n;
    } else if (sscanf(argv[i], "--migration_logging=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_migration_logging = n;
    } else if (sscanf(argv[i], "--migration_policy=%d%c", &n, &junk) == 1) {
      FLAGS_migration_policy = n;
    } else if (sscanf(argv[i], "--migration_rand_range_num=%d%c", &n, &junk) == 1) {
      FLAGS_migration_rand_range_num = n;
    } else if (sscanf(argv[i], "--migration_rand_range_size=%d%c", &n, &junk) == 1) {
      FLAGS_migration_rand_range_size = n;
    } else if (sscanf(argv[i], "--migration_metric=%d%c", &n, &junk) == 1) {
      FLAGS_migration_metric = n;
    } else if (sscanf(argv[i], "--YCSB_separate_write=%d%c", &n, &junk) == 1 &&
                       (n == 0 || n == 1)){
      FLAGS_YCSB_separate_write = n;
    } else if (sscanf(argv[i], "--YCSB_uniform_distribution_write=%d%c", &n, &junk) == 1 &&
                       (n == 0 || n == 1)){
      FLAGS_YCSB_uniform_distribution_write = n;
    } else if (sscanf(argv[i], "--YCSB_zipfian_alpha_write=%lf%c", &d, &junk) == 1) {
      FLAGS_YCSB_zipfian_alpha_write = d;
    } else if (sscanf(argv[i], "--read_ratio=%lf%c", &d, &junk) == 1) {
      FLAGS_read_ratio = d;
    } else {
      std::fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      std::exit(1);
    }
  }

  leveldb::g_env = leveldb::Env::Default();

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db == nullptr) {
    leveldb::g_env->GetTestDirectory(&default_db_path);
    default_db_path += "/dbbench";
    FLAGS_db = default_db_path.c_str();
  }

  leveldb::Benchmark benchmark;

  // TODO: not sure if this is needed, in RunBenchmark, init zipfian generator anyways
  // Below might be redundant
  // Initialize the zipf distribution for YCSB
  //init_zipf_generator(0, FLAGS_num, FLAGS_YCSB_zipfian_alpha);
  //init_latestgen(FLAGS_nua);

  benchmark.Run();
  return 0;
}
