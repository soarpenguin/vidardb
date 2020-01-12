
#include <iostream>
using namespace std;

#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/options.h"
using namespace rocksdb;

string kDBPath = "/tmp/range_query_example";

int main(int argc, char* argv[]) {
  // remove existed db path
  system("rm -rf /tmp/range_query_example");

  // open database
  DB* db; // db ref
  Options options;
  options.create_if_missing = true;
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // insert data
  WriteOptions write_options;
  // write_options.sync = true;
  s = db->Put(write_options, "1", "data1");
  assert(s.ok());
  s = db->Put(write_options, "2", "data2");
  assert(s.ok());
  s = db->Put(write_options, "3", "data3");
  assert(s.ok());
  s = db->Put(write_options, "4", "data4");
  assert(s.ok());
  s = db->Put(write_options, "5", "data5");
  assert(s.ok());
  s = db->Put(write_options, "6", "data6");
  assert(s.ok());
  s = db->Delete(write_options, "1");
  assert(s.ok());
  s = db->Put(write_options, "3", "data333");
  assert(s.ok());
  s = db->Put(write_options, "6", "data666");
  assert(s.ok());
  s = db->Put(write_options, "1", "data1111");
  assert(s.ok());
  s = db->Delete(write_options, "3");
  assert(s.ok());

  // test blocked sstable or memtable
  s = db->Flush(FlushOptions());
  assert(s.ok());

  ReadOptions read_options;
  // read_options.batch_capacity = 0; // full search // ok
  read_options.batch_capacity = 2; // in batch // ok

//  Range range; // full search // ok
  Range range("2", "4"); // [2, 4] // ok
//  Range range("1", "6"); // [1, 6] // ok
//  Range range("1", kRangeQueryMax); // [1, max] // ok

  vector<string> res;
  bool next = true;
  while (next) { // range query loop
    next = db->RangeQuery(read_options, range, res, &s);
    assert(s.ok());
    for (auto it : res) {
      cout << it << " ";
    }
    cout << endl;
  }

  delete db;
  return 0;
}
