#include <iostream>
#include <string>
using namespace std;

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "../table/adaptive_table_factory.h"
using namespace rocksdb;


string kDBPath = "/home/jsc/Desktop/VidarDB";

int main() {
  DB* db;
  Options options;
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

//  shared_ptr<TableFactory> block_based_factory(NewBlockBasedTableFactory());
//  shared_ptr<TableFactory> column_table_factory(NewColumnTableFactory());
//  static_cast<ColumnTableOptions*>(column_table_factory->GetOptions())->column_num = 2;
//
//  options.table_factory.reset(NewAdaptiveTableFactory(
//        column_table_factory, block_based_factory, nullptr, nullptr, column_table_factory));

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

//  // Put key-value
//  s = db->Put(WriteOptions(), "key1", "value");
//  assert(s.ok());
//  std::string value;
//  // get value
//  s = db->Get(ReadOptions(), "key1", &value);
//  assert(s.ok());
//  assert(value == "value");
//
//
//  s = db->Get(ReadOptions(), "key1", &value);
//  assert(s.IsNotFound());
//
//  db->Get(ReadOptions(), "key2", &value);
//  assert(value == "value");

  delete db;

  return 0;
}
