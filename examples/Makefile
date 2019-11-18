include ../make_config.mk

.PHONY: clean librocksdb

all: simple_example column_families_example compact_files_example c_simple_example optimistic_transaction_example transaction_example

simple_example: librocksdb simple_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

column_families_example: librocksdb column_families_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

compaction_filter_example: librocksdb compaction_filter_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

compact_files_example: librocksdb compact_files_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

transaction_example: librocksdb transaction_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

simple_column_example: librocksdb simple_column_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf ./simple_example ./column_families_example ./compact_files_example ./compaction_filter_example ./transaction_example ./simple_column_example

librocksdb:
	cd .. && $(MAKE) static_lib
