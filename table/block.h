//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stddef.h>
#include <stdint.h>
#ifdef VIDARDB_MALLOC_USABLE_SIZE
#include <malloc.h>
#endif

#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "vidardb/iterator.h"
#include "vidardb/options.h"
#include "table/internal_iterator.h"

#include "format.h"

namespace vidardb {

struct BlockContents;
class Comparator;
class BlockIter;

class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(BlockContents&& contents);

  ~Block() = default;

  size_t size() const { return size_; }

  const char* data() const { return data_; }

  bool cachable() const { return contents_.cachable; }

  size_t usable_size() const {
#ifdef VIDARDB_MALLOC_USABLE_SIZE
    if (contents_.allocation.get() != nullptr) {
      return malloc_usable_size(contents_.allocation.get());
    }
#endif  // VIDARDB_MALLOC_USABLE_SIZE
    return size_;
  }

  uint32_t NumRestarts() const;

  CompressionType compression_type() const {
    return contents_.compression_type;
  }

  // If iter is null, return new Iterator
  // If iter is not null, update this one and return it as Iterator*
  InternalIterator* NewIterator(const Comparator* comparator,
                                BlockIter* iter = nullptr, bool column = false);

  // Report an approximation of how much memory has been used.
  size_t ApproximateMemoryUsage() const;

 protected:
  BlockContents contents_;
  const char* data_;            // contents_.data.data()
  size_t size_;                 // contents_.data.size()
  uint32_t restart_offset_;     // Offset in data_ of restart array

  // No copying allowed
  Block(const Block&);
  void operator=(const Block&);
};

class BlockIter : public InternalIterator {
 public:
  BlockIter()
      : comparator_(nullptr),
        data_(nullptr),
        restarts_(0),
        num_restarts_(0),
        current_(0),
        restart_index_(0),
        status_(Status::OK()) {}

  BlockIter(const Comparator* comparator, const char* data, uint32_t restarts,
            uint32_t num_restarts)
      : BlockIter() {
    Initialize(comparator, data, restarts, num_restarts);
  }

  virtual void Initialize(const Comparator* comparator, const char* data,
                          uint32_t restarts, uint32_t num_restarts) {
    assert(data_ == nullptr);           // Ensure it is called only once
    assert(num_restarts > 0);           // Ensure the param is valid

    comparator_ = comparator;
    data_ = data;
    restarts_ = restarts;
    num_restarts_ = num_restarts;
    current_ = restarts_;
    restart_index_ = num_restarts_;
  }

  virtual void SetStatus(Status s) {
    status_ = s;
  }

  virtual bool Valid() const override { return current_ < restarts_; }

  virtual Status status() const override { return status_; }

  virtual Slice key() const override {
    assert(Valid());
    return key_.GetKey();
  }

  virtual Slice value() const override {
    assert(Valid());
    return value_;
  }

  virtual void Next() override;

  virtual void Prev() override;

  virtual void Seek(const Slice& target) override;

  virtual void SeekToFirst() override;

  virtual void SeekToLast() override;

#ifndef NDEBUG
  virtual ~BlockIter() {
    // Assert that the BlockIter is never deleted while Pinning is Enabled.
    assert(!pinned_iters_mgr_ ||
           (pinned_iters_mgr_ && !pinned_iters_mgr_->PinningEnabled()));
  }

  virtual void SetPinnedItersMgr(
      PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }

  PinnedIteratorsManager* pinned_iters_mgr_ = nullptr;
#endif

  virtual bool IsKeyPinned() const override { return key_.IsKeyPinned(); }

 protected:
  const Comparator* comparator_;
  const char* data_;       // underlying block contents
  uint32_t restarts_;      // Offset of restart array (list of fixed32)
  uint32_t num_restarts_;  // Number of uint32_t entries in restart array

  // current_ is offset in data_ of current entry.  >= restarts_ if !Valid
  uint32_t current_;
  uint32_t restart_index_;  // Index of restart block in which current_ falls
  IterKey key_;
  Slice value_;
  Status status_;

  virtual inline int Compare(const Slice& a, const Slice& b) const {
    return comparator_->Compare(a, b);
  }

  // Return the offset in data_ just past the end of the current entry.
  virtual inline uint32_t NextEntryOffset() const {
    // NOTE: We don't support files bigger than 2GB
    return static_cast<uint32_t>((value_.data() + value_.size()) - data_);
  }

  virtual uint32_t GetRestartPoint(uint32_t index) {
    assert(index < num_restarts_);
    return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
  }

  virtual void SeekToRestartPoint(uint32_t index) {
    key_.Clear();
    restart_index_ = index;
    // current_ will be fixed by ParseNextKey();

    // ParseNextKey() starts at the end of value_, so set value_ accordingly
    uint32_t offset = GetRestartPoint(index);
    value_ = Slice(data_ + offset, 0);
  }

  virtual void CorruptionError();

  virtual bool ParseNextKey();

  virtual bool BinarySeek(const Slice& target, uint32_t left, uint32_t right,
                          uint32_t* index);
};

class ColumnBlockIter : public BlockIter {
 public:
  ColumnBlockIter() : BlockIter() {}
  ColumnBlockIter(const Comparator* comparator, const char* data,
                  uint32_t restarts, uint32_t num_restarts)
      : ColumnBlockIter() {
    Initialize(comparator, data, restarts, num_restarts);
  }

  virtual void Seek(const Slice& target) override;

 private:

  virtual bool ParseNextKey();

  virtual bool BinarySeek(const Slice& target, uint32_t left, uint32_t right,
                          uint32_t* index);
};

}  // namespace vidardb
