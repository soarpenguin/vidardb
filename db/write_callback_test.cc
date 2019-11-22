//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#include <string>
#include <utility>
#include <vector>

#include "db/db_impl.h"
#include "db/write_callback.h"
#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"
#include "util/logging.h"
#include "util/sync_point.h"
#include "util/testharness.h"

using std::string;

namespace rocksdb {

class WriteCallbackTest : public testing::Test {
 public:
  string dbname;

  WriteCallbackTest() {
    dbname = test::TmpDir() + "/write_callback_testdb";
  }
};

class WriteCallbackTestWriteCallback1 : public WriteCallback {
 public:
  bool was_called = false;

  Status Callback(DB *db) override {
    was_called = true;

    // Make sure db is a DBImpl
    DBImpl* db_impl = dynamic_cast<DBImpl*> (db);
    if (db_impl == nullptr) {
      return Status::InvalidArgument("");
    }

    return Status::OK();
  }

  bool AllowWriteBatching() override { return true; }
};

class WriteCallbackTestWriteCallback2 : public WriteCallback {
 public:
  Status Callback(DB *db) override {
    return Status::Busy();
  }
  bool AllowWriteBatching() override { return true; }
};

class MockWriteCallback : public WriteCallback {
 public:
  bool should_fail_ = false;
  bool was_called_ = false;
  bool allow_batching_ = false;

  Status Callback(DB* db) override {
    was_called_ = true;
    if (should_fail_) {
      return Status::Busy();
    } else {
      return Status::OK();
    }
  }

  bool AllowWriteBatching() override { return allow_batching_; }
};

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr,
          "SKIPPED as WriteWithCallback is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
