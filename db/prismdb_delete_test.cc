// Copyright (c) 2026 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "db/db_impl.h"
#include "db/filename.h"
#include "db/nvm/indexes/btree.h"
#include "db/nvm/options.h"
#include "gtest/gtest.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "util/testutil.h"

namespace leveldb {

namespace {

class TableSyncEnv : public EnvWrapper {
 public:
  TableSyncEnv() : EnvWrapper(Env::Default()) {}

  Status NewWritableFile(const std::string& filename,
                         WritableFile** result) override {
    Status status = target()->NewWritableFile(filename, result);
    uint64_t number;
    FileType type;
    const size_t slash = filename.find_last_of('/');
    const std::string basename = filename.substr(slash + 1);
    if (status.ok() && ParseFileName(basename, &number, &type) &&
        type == kTableFile) {
      *result = new TableFile(*result, this);
    }
    return status;
  }

  void Arm() {
    std::lock_guard<std::mutex> lock(mutex_);
    armed_ = true;
    reached_ = false;
    released_ = false;
  }

  bool WaitForTableSync() {
    std::unique_lock<std::mutex> lock(mutex_);
    return changed_.wait_for(lock, std::chrono::seconds(30),
                             [this] { return reached_; });
  }

  void Release() {
    std::lock_guard<std::mutex> lock(mutex_);
    armed_ = false;
    released_ = true;
    changed_.notify_all();
  }

 private:
  class TableFile : public WritableFile {
   public:
    TableFile(WritableFile* target, TableSyncEnv* env)
        : target_(target), env_(env) {}
    ~TableFile() override { delete target_; }

    Status Append(const Slice& data) override { return target_->Append(data); }
    Status Close() override { return target_->Close(); }
    Status Flush() override { return target_->Flush(); }
    Status Sync() override {
      Status status = target_->Sync();
      if (status.ok()) env_->AfterTableSync();
      return status;
    }

   private:
    WritableFile* target_;
    TableSyncEnv* env_;
  };

  void AfterTableSync() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!armed_) return;
    armed_ = false;
    reached_ = true;
    changed_.notify_all();
    changed_.wait(lock, [this] { return released_; });
  }

  std::mutex mutex_;
  std::condition_variable changed_;
  bool armed_ = false;
  bool reached_ = false;
  bool released_ = false;
};

}  // namespace

// PrismDB currently configures slab paths and partition counts at build time.
// Run this fixture in an isolated environment with an empty slab directory.
// It refuses to overwrite existing slab files and removes only its own files.
class PrismDBDeleteTest : public testing::Test {
 protected:
  void SetUp() override {
    const int kConfiguredPartitions = 8;
    std::vector<std::string> slab_paths;
    for (int partition = 0; partition < kConfiguredPartitions; ++partition) {
      char path[512];
      const char* path_format = PATH;
      const int length = std::snprintf(
          path, sizeof(path), path_format, 0,
          static_cast<unsigned long>(partition), 0UL, 1024UL);
      ASSERT_GT(length, 0);
      ASSERT_LT(static_cast<size_t>(length), sizeof(path));
      const std::string slab_path(path);
      const size_t slash = slab_path.find_last_of('/');
      ASSERT_NE(std::string::npos, slash);
      ASSERT_TRUE(env_->FileExists(slab_path.substr(0, slash)))
          << "Create the configured slab directory in an isolated test "
             "environment before running this fixture. "
          << slab_path.substr(0, slash);
      ASSERT_FALSE(env_->FileExists(slab_path))
          << "Refusing to overwrite an existing slab file. " << slab_path;
      slab_paths.push_back(slab_path);
    }

    static unsigned int instance = 0;
    dbname_ = testing::TempDir() + "prismdb_delete_test_" +
              std::to_string(env_->NowMicros()) + "_" +
              std::to_string(++instance);
    Options options;
    options.env = env_;
    options.create_if_missing = true;
    options.error_if_exists = true;
    options.max_file_size = 1 << 20;
    options.max_open_files = 74;
    ConfigureOptions(&options);
    owned_slab_paths_.swap(slab_paths);
    ASSERT_LEVELDB_OK(DB::Open(options, dbname_, &db_));
    impl_ = static_cast<DBImpl*>(db_);
    ASSERT_EQ(kConfiguredPartitions, impl_->numPartitions);
    ASSERT_TRUE(impl_->load_phase_);
  }

  void TearDown() override {
    delete db_;
    db_ = nullptr;
    impl_ = nullptr;
    if (!dbname_.empty()) {
      EXPECT_LEVELDB_OK(DestroyDB(dbname_, Options()));
    }
    for (const std::string& path : owned_slab_paths_) {
      if (env_->FileExists(path)) {
        EXPECT_LEVELDB_OK(env_->RemoveFile(path));
      }
    }
  }

  static std::string Key(uint64_t number) {
    std::string result(sizeof(number), '\0');
    encode_size64(&result[0], number);
    return result;
  }

  virtual void ConfigureOptions(Options* options) {}

  void StartRunningPhase() {
    db_->SetCorrectBucketTotalKeys();
    db_->SetDbMode(false);
  }

  void ExpectBucketTotal(uint64_t number, uint64_t expected) {
    ASSERT_EQ(expected,
              impl_->BucketList[number / impl_->bucket_sz].num_total_keys);
  }

  void Put(uint64_t number, const std::string& value) {
    ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), Key(number), value));
  }

  void Delete(uint64_t number) {
    ASSERT_LEVELDB_OK(db_->Delete(WriteOptions(), Key(number)));
  }

  void ExpectValue(uint64_t number, const std::string& expected) {
    // The existing fast-tier API requires caller-provided output storage.
    std::string actual(expected.size(), '?');
    const Status status = db_->Get(ReadOptions(), Key(number), &actual);
    ASSERT_LEVELDB_OK(status);
    ASSERT_EQ(expected, actual);
  }

  void ExpectMissing(uint64_t number) {
    const std::string sentinel = "unchanged output";
    std::string actual = sentinel;
    const Status status = db_->Get(ReadOptions(), Key(number), &actual);
    ASSERT_TRUE(status.IsNotFound())
        << status.ToString() << " for key " << number << " with value " << actual;
    ASSERT_EQ(sentinel, actual);
  }

  void Migrate() {
    // Every test key belongs to partition zero. Calling the production
    // migration synchronously avoids timing-dependent capacity thresholds.
    DBImpl::PartitionContext* partition = &impl_->partitions[0];
    ASSERT_FALSE(partition->background_compaction_scheduled);
    ASSERT_GT(btree_get_size(partition->index), 0U);
    impl_->BackgroundCompaction(partition);
    ASSERT_LEVELDB_OK(impl_->bg_error_);
    ASSERT_EQ(0U, btree_get_size(partition->index));
    ASSERT_EQ(0U, partition->size_in_bytes);
  }

  void CheckTombstonePosition(uint64_t deleted_key) {
    const uint64_t keys[] = {10, 20, 30};
    for (uint64_t key : keys) {
      ASSERT_NO_FATAL_FAILURE(Put(key, "old"));
    }
    ASSERT_NO_FATAL_FAILURE(Migrate());
    for (uint64_t key : keys) {
      ASSERT_NO_FATAL_FAILURE(ExpectValue(key, "old"));
      if (key == deleted_key) {
        ASSERT_NO_FATAL_FAILURE(Delete(key));
        ASSERT_NO_FATAL_FAILURE(ExpectMissing(key));
      } else {
        ASSERT_NO_FATAL_FAILURE(Put(key, "new"));
      }
    }
    ASSERT_NO_FATAL_FAILURE(Migrate());
    for (uint64_t key : keys) {
      if (key == deleted_key) {
        ASSERT_NO_FATAL_FAILURE(ExpectMissing(key));
      } else {
        ASSERT_NO_FATAL_FAILURE(ExpectValue(key, "new"));
      }
    }
  }

  void DeleteDuringMigration(uint64_t number, unsigned int repetitions = 1,
                             bool expect_selected = true) {
    DBImpl::PartitionContext* partition = &impl_->partitions[0];
    ASSERT_FALSE(partition->background_compaction_scheduled);
    ASSERT_GT(btree_get_size(partition->index), 0U);
    sync_env_.Arm();
    partition->background_compaction_scheduled = true;
    std::thread migration([this, partition] {
      impl_->BackgroundCompaction(partition);
    });
    const bool reached_sync = sync_env_.WaitForTableSync();
    Status delete_status;
    bool selected_before_delete = false;
    if (reached_sync) {
      // The selected values have reached the new SST, but migration has
      // not removed their fast-tier index entries yet.
      {
        std::lock_guard<std::mutex> lock(partition->mtx);
        selected_before_delete = impl_->GetFromMigrationBitmap(0, number) != 0;
      }
      for (unsigned int i = 0; i < repetitions; ++i) {
        delete_status = db_->Delete(WriteOptions(), Key(number));
        if (!delete_status.ok()) break;
      }
    }
    sync_env_.Release();
    migration.join();
    partition->background_compaction_scheduled = false;
    ASSERT_TRUE(reached_sync) << "Migration did not reach the table sync hook";
    ASSERT_EQ(expect_selected, selected_before_delete);
    ASSERT_LEVELDB_OK(delete_status);
    ASSERT_LEVELDB_OK(impl_->bg_error_);
  }

  TableSyncEnv sync_env_;
  Env* env_ = &sync_env_;
  DB* db_ = nullptr;
  DBImpl* impl_ = nullptr;
  std::string dbname_;
  std::vector<std::string> owned_slab_paths_;
};

TEST_F(PrismDBDeleteTest, MigrationPreservesValue) {
  const std::string expected = "01234567abcdefghABCDEFGH";
  ASSERT_NO_FATAL_FAILURE(Put(1, expected));
  ASSERT_NO_FATAL_FAILURE(ExpectValue(1, expected));
  ASSERT_NO_FATAL_FAILURE(Migrate());
  ASSERT_NO_FATAL_FAILURE(ExpectValue(1, expected));
}

TEST_F(PrismDBDeleteTest, DeleteFastTierValue) {
  ASSERT_NO_FATAL_FAILURE(Put(1, "old"));
  ASSERT_NO_FATAL_FAILURE(ExpectValue(1, "old"));
  ASSERT_NO_FATAL_FAILURE(Delete(1));
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(1));
}

TEST_F(PrismDBDeleteTest, DeleteFastTierUpdateDoesNotRevealOldLowerTierValue) {
  ASSERT_NO_FATAL_FAILURE(Put(1, "old"));
  ASSERT_NO_FATAL_FAILURE(Migrate());
  ASSERT_NO_FATAL_FAILURE(ExpectValue(1, "old"));
  ASSERT_NO_FATAL_FAILURE(Put(1, "new"));
  ASSERT_NO_FATAL_FAILURE(ExpectValue(1, "new"));
  ASSERT_NO_FATAL_FAILURE(Delete(1));
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(1));
}

TEST_F(PrismDBDeleteTest, RepeatedDeleteDoesNotRevealLowerTierValue) {
  ASSERT_NO_FATAL_FAILURE(Put(1, "old"));
  ASSERT_NO_FATAL_FAILURE(Migrate());
  ASSERT_NO_FATAL_FAILURE(ExpectValue(1, "old"));
  ASSERT_NO_FATAL_FAILURE(Delete(1));
  ASSERT_NO_FATAL_FAILURE(Delete(1));
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(1));
}

TEST_F(PrismDBDeleteTest, RepeatedDeleteOfMissingKey) {
  ASSERT_NO_FATAL_FAILURE(Delete(1));
  ASSERT_NO_FATAL_FAILURE(Delete(1));
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(1));
}

TEST_F(PrismDBDeleteTest, DeletedKeyStaysMissingWhenLowerTierIsInitiallyEmpty) {
  ASSERT_NO_FATAL_FAILURE(Delete(1));
  ASSERT_NO_FATAL_FAILURE(Put(2, "live"));
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(1));
  ASSERT_NO_FATAL_FAILURE(Migrate());
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(1));
  ASSERT_NO_FATAL_FAILURE(ExpectValue(2, "live"));
}

TEST_F(PrismDBDeleteTest, MigrateFirstKeyTombstone) {
  ASSERT_NO_FATAL_FAILURE(CheckTombstonePosition(10));
}

TEST_F(PrismDBDeleteTest, MigrateInteriorKeyTombstone) {
  ASSERT_NO_FATAL_FAILURE(CheckTombstonePosition(20));
}

TEST_F(PrismDBDeleteTest, MigrateLastKeyTombstone) {
  ASSERT_NO_FATAL_FAILURE(CheckTombstonePosition(30));
}

TEST_F(PrismDBDeleteTest, MigrateOnlyTombstone) {
  ASSERT_NO_FATAL_FAILURE(Put(1, "old"));
  ASSERT_NO_FATAL_FAILURE(Migrate());
  ASSERT_NO_FATAL_FAILURE(Delete(1));
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(1));
  ASSERT_NO_FATAL_FAILURE(Migrate());
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(1));
}

TEST_F(PrismDBDeleteTest, PutAfterDeleteSurvivesMigration) {
  ASSERT_NO_FATAL_FAILURE(Put(1, "old"));
  ASSERT_NO_FATAL_FAILURE(Migrate());
  ASSERT_NO_FATAL_FAILURE(Delete(1));
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(1));
  ASSERT_NO_FATAL_FAILURE(Put(1, "new"));
  ASSERT_NO_FATAL_FAILURE(ExpectValue(1, "new"));
  ASSERT_NO_FATAL_FAILURE(Migrate());
  ASSERT_NO_FATAL_FAILURE(ExpectValue(1, "new"));
}

TEST_F(PrismDBDeleteTest, DeleteDuringMigrationDoesNotResurrectValue) {
  ASSERT_NO_FATAL_FAILURE(Put(1, "old"));
  ASSERT_NO_FATAL_FAILURE(Put(2, "unrelated value survives"));
  ASSERT_NO_FATAL_FAILURE(DeleteDuringMigration(1));
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(1));
  ASSERT_NO_FATAL_FAILURE(ExpectValue(2, "unrelated value survives"));
  ASSERT_NO_FATAL_FAILURE(Migrate());
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(1));
  ASSERT_NO_FATAL_FAILURE(ExpectValue(2, "unrelated value survives"));
}

class PrismDBDeleteMetric2Test : public PrismDBDeleteTest {
 protected:
  void ConfigureOptions(Options* options) override {
    options->migration_metric = 2;
  }
};

TEST_F(PrismDBDeleteMetric2Test, RunningPhaseDeletionCountsTombstoneOnce) {
  ASSERT_NO_FATAL_FAILURE(Put(1, "old"));
  ASSERT_NO_FATAL_FAILURE(Put(2, "unrelated value survives"));
  ASSERT_NO_FATAL_FAILURE(Migrate());
  StartRunningPhase();
  ASSERT_NO_FATAL_FAILURE(ExpectBucketTotal(1, 0));

  ASSERT_NO_FATAL_FAILURE(Delete(1));
  ASSERT_NO_FATAL_FAILURE(ExpectBucketTotal(1, 1));
  ASSERT_NO_FATAL_FAILURE(Delete(1));
  ASSERT_NO_FATAL_FAILURE(ExpectBucketTotal(1, 1));
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(1));
  ASSERT_NO_FATAL_FAILURE(Migrate());
  ASSERT_NO_FATAL_FAILURE(ExpectBucketTotal(1, 0));
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(1));
  ASSERT_NO_FATAL_FAILURE(ExpectValue(2, "unrelated value survives"));
}

TEST_F(PrismDBDeleteMetric2Test, RunningPhaseConcurrentDeleteRetainsBucketCount) {
  ASSERT_NO_FATAL_FAILURE(Put(1, "old"));
  ASSERT_NO_FATAL_FAILURE(Put(2, "old unrelated value"));
  ASSERT_NO_FATAL_FAILURE(Migrate());
  StartRunningPhase();
  ASSERT_NO_FATAL_FAILURE(Put(1, "new"));
  ASSERT_NO_FATAL_FAILURE(Put(2, "new unrelated value"));
  ASSERT_NO_FATAL_FAILURE(ExpectBucketTotal(1, 2));

  ASSERT_NO_FATAL_FAILURE(DeleteDuringMigration(1));
  ASSERT_NO_FATAL_FAILURE(ExpectBucketTotal(1, 1));
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(1));
  ASSERT_NO_FATAL_FAILURE(ExpectValue(2, "new unrelated value"));
  ASSERT_NO_FATAL_FAILURE(Migrate());
  ASSERT_NO_FATAL_FAILURE(ExpectBucketTotal(1, 0));
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(1));
}

TEST_F(PrismDBDeleteMetric2Test,
       RunningPhaseRepeatedConcurrentDeleteRestoresCountOnce) {
  ASSERT_NO_FATAL_FAILURE(Put(1, "old"));
  ASSERT_NO_FATAL_FAILURE(Put(2, "unrelated value survives"));
  ASSERT_NO_FATAL_FAILURE(Migrate());
  StartRunningPhase();
  ASSERT_NO_FATAL_FAILURE(Put(1, "new"));
  ASSERT_NO_FATAL_FAILURE(ExpectBucketTotal(1, 1));

  ASSERT_NO_FATAL_FAILURE(DeleteDuringMigration(1, 2));
  ASSERT_NO_FATAL_FAILURE(ExpectBucketTotal(1, 1));
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(1));
  ASSERT_NO_FATAL_FAILURE(ExpectValue(2, "unrelated value survives"));
  ASSERT_NO_FATAL_FAILURE(Migrate());
  ASSERT_NO_FATAL_FAILURE(ExpectBucketTotal(1, 0));
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(1));
}

TEST_F(PrismDBDeleteMetric2Test,
       RunningPhaseNonselectedDeleteClearsPreviousMembership) {
  ASSERT_NO_FATAL_FAILURE(Put(1, "old"));
  ASSERT_NO_FATAL_FAILURE(Put(2, "old unrelated value"));
  ASSERT_NO_FATAL_FAILURE(Migrate());
  StartRunningPhase();
  ASSERT_NO_FATAL_FAILURE(Put(1, "new"));
  ASSERT_NO_FATAL_FAILURE(ExpectBucketTotal(1, 1));

  // Key two belonged to the previous migration. It remains only on flash
  // and must not be marked selected by this migration of key one.
  ASSERT_NO_FATAL_FAILURE(
      DeleteDuringMigration(2, 1, /*expect_selected=*/false));
  ASSERT_NO_FATAL_FAILURE(ExpectBucketTotal(2, 1));
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(2));
  ASSERT_NO_FATAL_FAILURE(ExpectValue(1, "new"));
  ASSERT_NO_FATAL_FAILURE(Migrate());
  ASSERT_NO_FATAL_FAILURE(ExpectBucketTotal(2, 0));
  ASSERT_NO_FATAL_FAILURE(ExpectMissing(2));
  ASSERT_NO_FATAL_FAILURE(ExpectValue(1, "new"));
}

}  // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
