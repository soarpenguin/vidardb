
#pragma once
#include <vector>

#include <stdint.h>
#include "vidardb/slice.h"
#include "table/block_builder.h"

namespace vidardb {

class ColumnBlockBuilder : public BlockBuilder {
 public:
  ColumnBlockBuilder(const ColumnBlockBuilder&) = delete;
  void operator=(const ColumnBlockBuilder&) = delete;

  explicit ColumnBlockBuilder(int block_restart_interval)
    : BlockBuilder(block_restart_interval) {}

  // REQUIRES: Finish() has not been callled since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  virtual void Add(const Slice& key, const Slice& value) override;

  // Returns an estimated block size after appending key and value.
  virtual size_t EstimateSizeAfterKV(const Slice& key,
                                     const Slice& value) const override;

  // Called after Add
  virtual bool IsKeyStored() const override {
    return counter_ == 1;
  }
};

}  // namespace vidardb
