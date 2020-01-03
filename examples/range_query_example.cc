#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/options.h"

using namespace std;
using namespace rocksdb;

std::string kDBPath = "/tmp/range_query_example";

void PrintResult(std::vector<std::string>& res) {
    std::cout << "*****" << std::endl;
    for (auto it = res.begin(); it != res.end(); it++) {
        std::cout << *it << std::endl;
    }
    std::cout << "*****" << std::endl;
}

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

    // test sstable or memtable
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