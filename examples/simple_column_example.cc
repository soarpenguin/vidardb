
#include <iostream>
using namespace std;

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "../table/adaptive_table_factory.h"
using namespace rocksdb;

unsigned int M = 2;
string kDBPath = "/home/jsc/Desktop/VidarDB" + to_string(M);

int main() {
  system(string("rm -rf " + kDBPath).c_str());

  DB* db;
  Options options;
  options.create_if_missing = true;

  shared_ptr<TableFactory> column_table_factory(NewColumnTableFactory());
  static_cast<ColumnTableOptions*>(column_table_factory->GetOptions())->column_num = M;
  options.table_factory.reset(column_table_factory.get());

//  options.table_factory.reset(NewBlockBasedTableFactory());

  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  s = db->Put(WriteOptions(), "column", "val1|val2");
  db->Flush(FlushOptions());
  if (!s.ok()) cout << "Put not ok!" << endl;

  ReadOptions ro;
  ro.columns = {1};

  vector<string> resRQ;
  s = db->RangeQuery(ro, Range(), resRQ, nullptr);
  if (!s.ok()) cout << "RangQuery not ok!" << endl;
  cout << "RangeQuery count: " << resRQ.size() << endl;

  string val;
  s = db->Get(ro, "column", &val);
  if (!s.ok()) cout << "Get not ok!" << endl;
  cout<<val<<endl;

  Iterator *it = db->NewIterator(ro);
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    cout<<it->key().ToString()<<endl;
    s = db->Delete(WriteOptions(), it->key());
    if (!s.ok()) cout << "Delete not ok!" << endl;
  }
  delete it;

  delete db;

  cout << "finished!" << endl;
  return 0;
}
