
#include <iostream>
using namespace std;

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "../table/adaptive_table_factory.h"
using namespace rocksdb;

unsigned int M = 2;
string kDBPath = "/tmp/rocksdb_simple_example" + to_string(M);

int main() {
  system(string("rm -rf " + kDBPath).c_str());

  DB* db;
  Options options;
  options.create_if_missing = true;

  TableFactory* column_table_factory = NewColumnTableFactory();
  static_cast<ColumnTableOptions*>(column_table_factory->GetOptions())->column_num = M;
  options.table_factory.reset(column_table_factory);

  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  s = db->Put(WriteOptions(), "column1", "val1|val2");
  s = db->Put(WriteOptions(), "column2", "val3|val4");
  db->Flush(FlushOptions());
  if (!s.ok()) cout << "Put not ok!" << endl;

  ReadOptions ro;
//  ro.columns = {1};

//  vector<string> resRQ;
//  s = db->RangeQuery(ro, Range(), resRQ, nullptr);
//  if (!s.ok()) cout << "RangQuery not ok!" << endl;
//  cout << "RangeQuery count: " << resRQ.size() << endl;

  string val;
  s = db->Get(ro, "column2", &val);
  if (!s.ok()) cout << "Get not ok!" << endl;
  cout<<val<<endl;

  Iterator *it = db->NewIterator(ro);
  it->Seek("column1");
  if (it->Valid()) {
    cout<<"value: "<<it->value().ToString()<<endl;
  }
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    cout<<"key: "<<it->key().ToString()
        <<" value: "<<it->value().ToString()<<endl;
    s = db->Delete(WriteOptions(), it->key());
    if (!s.ok()) cout << "Delete not ok!" << endl;
  }
  delete it;

  delete db;

  cout << "finished!" << endl;
  return 0;
}
