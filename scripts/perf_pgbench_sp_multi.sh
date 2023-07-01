#initialize database
export PGPORT=54321
export PGDATA=pgdata
SF=300
TIME=300
OFFSET=512
MEMBER=4096

if [ "$1" = "help" ]; then
echo "perf_pgbench_sp_multi.sh 0/1 simple/sp/multi install_folder_name long"
echo "first param : help or 0 (do not initialize database) or 1 (initialize database and load data)"
echo "second param : simple/sp/multi to run just pgbench or pgbench with suboverflow or pgbench with multixact script"
echo "forth param: long - if you want to run concurrent long running transaction"
exit
fi

echo "run test in directory '$3'"
cd "$3"/bin

# create multixact script
cat > multixact.sql <<EOF
\set aid random(1, 100000 * :scale)
\set bid random(1, 1 * :scale)
\set tid random(1, 10 * :scale)
\set delta random(-5000, 5000)
BEGIN;
SELECT FROM pgbench_accounts WHERE aid = :aid FOR UPDATE;
SAVEPOINT S1;
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
END;
EOF

#create savepoint script
cat > savepoint.sql << EOF
BEGIN;
EOF

#Iterate the loop until a less than 10
a=0
while [ $a -lt 350 ]
do
    # Print the values
    cat >> savepoint.sql << EOF
    SAVEPOINT S1;
    INSERT INTO pgbench_test VALUES(1);
EOF
    # increment the value
    a=`expr $a + 1`    
done

cat >> savepoint.sql << EOF
select pg_sleep(1);
COMMIT;
EOF

#create longrunning script
cat > longrunning.sql << EOF
BEGIN;
INSERT INTO pgbench_test VALUES(1);
select pg_sleep(3000);
COMMIT;
EOF

if [ "$1" = "0" ]; then
        if [ "$3" = "patch" ]; then
		./postgres -c multixact_offsets_buffers=$OFFSET -c multixact_members_buffers=$MEMBER -c shared_buffers=20GB -c checkpoint_timeout=40min -c max_wal_size=20GB -c max_connections=200 -c maintenance_work_mem=1GB&
	else
		./postgres  -c shared_buffers=20GB -c checkpoint_timeout=40min -c max_wal_size=20GB -c max_connections=200 -c maintenance_work_mem=1GB&
	fi
fi

if [ "$1" = "1" ]; then
	echo "initialize database"
 	rm -rf $PGDATA
	./initdb -D $PGDATA
	if [ "$3" = "patch" ]; then
                ./postgres -c multixact_offsets_buffers=$OFFSET -c multixact_members_buffers=$MEMBER -c shared_buffers=20GB -c checkpoint_timeout=40min -c max_wal_size=20GB -c max_connections=200 -c maintenance_work_mem=1GB&
        else
                ./postgres  -c shared_buffers=20GB -c checkpoint_timeout=40min -c max_wal_size=20GB -c max_connections=200 -c maintenance_work_mem=1GB&
        fi

	sleep 5
	./pgbench -i -s $SF -p $PGPORT postgres
fi


sleep 5

./psql -d postgres -c "CREATE TABLE pgbench_test(a int)"

if [ "$2" = "sp" ]; then
	./pgbench -c 128 -j 128 -T $TIME -P5  -M prepared -p $PGPORT postgres &
	sleep 60
	echo ".. Creating suboverflow..."
	./pgbench -c 1 -j 1 -T $TIME  -M prepared -f savepoint.sql -p $PGPORT postgres &
fi

if [ "$2" = "multi" ]; then
	echo ".. starting pgbench test with multixact script..."
	./pgbench -c 128 -j 128 -T $TIME  -P5 -M prepared -f multixact.sql -p $PGPORT postgres &
fi

if [ "$2" = "simple" ]; then
	echo ".. starting pgbench test..."
	./pgbench -c 128 -j 128 -T $TIME -P5 -M prepared -p $PGPORT postgres &
fi

if [ "$4" = "long" ]; then
	sleep 60
	echo ".. Starting long runnning transaction..."
        ./pgbench -c 1 -j 1 -T $TIME  -M prepared -f longrunning.sql -p $PGPORT postgres &
fi

sleep $TIME

./pg_ctl -D $PGDATA stop
