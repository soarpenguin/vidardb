#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/options.h"
#include "../table/adaptive_table_factory.h"

using namespace std;
using namespace rocksdb;

unsigned int M = 3;
std::string kDBPath = "/tmp/range_query_column_example";

void PrintResult(std::vector<std::string>& res) {
    std::cout << "*****" << std::endl;
    for (auto it = res.begin(); it != res.end(); it++) {
        std::cout << *it << std::endl;
    }
    std::cout << "*****" << std::endl;
}

int main(int argc, char* argv[]) {
    // remove existed db path
    system("rm -rf /tmp/range_query_column_example");

    // open database
    DB* db; // db ref
    Options options;
    options.create_if_missing = true;

    // column table
    TableFactory* table_factory = NewColumnTableFactory();
    static_cast<ColumnTableOptions*>(table_factory->GetOptions())
        ->column_num = M;
    options.table_factory.reset(table_factory);

    Status s = DB::Open(options, kDBPath, &db);
    assert(s.ok());

    // insert data
    WriteOptions write_options;
    // write_options.sync = true;
    s = db->Put(write_options, "1", "chen|33|hangzhou");
    assert(s.ok());
    s = db->Put(write_options, "2", "wang|32|wuhan");
    assert(s.ok());
    s = db->Put(write_options, "3", "zhao|35|nanjing");
    assert(s.ok());
    s = db->Put(write_options, "4", "liao|28|beijing");
    assert(s.ok());
    s = db->Put(write_options, "5", "jiang|30|shanghai");
    assert(s.ok());
    s = db->Put(write_options, "6", "lian|30|changsha");
    assert(s.ok());
    s = db->Delete(write_options, "1");
    assert(s.ok());

    // force flush to disk
    s = db->Flush(FlushOptions());
    assert(s.ok());

    ReadOptions read_options;
    // read_options.max_result_num = 0; // full search // ok
    read_options.max_result_num = 2; // in batch // ok

    // Range range; // full search // ok
    // Range range("2", "5"); // [2, 5] // ok
    Range range("1", kMax); // [1, max] // ok

    std::vector<std::string> res;
    bool next = db->RangeQuery(read_options, range, res, &s);
    assert(s.ok());
    PrintResult(res);

    for (;next;) { // range query loop
        next = db->RangeQuery(read_options, range, res, &s);
        assert(s.ok());
        PrintResult(res);
    }

    delete db;
    return 0;
}