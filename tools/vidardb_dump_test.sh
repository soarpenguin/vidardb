TESTDIR=`mktemp -d /tmp/vidardb-dump-test.XXXXX`
DUMPFILE="tools/sample-dump.dmp"

# Verify that the sample dump file is undumpable and then redumpable.
./vidardb_undump --dump_location=$DUMPFILE --db_path=$TESTDIR/db
./vidardb_dump --anonymous --db_path=$TESTDIR/db --dump_location=$TESTDIR/dump
cmp $DUMPFILE $TESTDIR/dump
