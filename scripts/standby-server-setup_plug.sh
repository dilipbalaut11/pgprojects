export PGPORT=5432                      #MASTER PORT
PGSQL_DIR=$(pwd)
PGSQL_BIN=$PGSQL_DIR/bin
PGSQL_MASTER=$PGSQL_DIR/bin/master              #DATA FOLDER FOR PRIMARY/MASTER SERVER
PGSQL_STANDBY=$PGSQL_DIR/bin/standby    #DATA FOLDER FOR BACKUP/STANDBY SERVER
/Users/dilipkumar/Documents/Virtual Machines.localized/CentOS 64-bit_1.vmwarevm
#Cleanup the master and slave data directory and create a new one.
rm -rf $PGSQL_MASTER $PGSQL_STANDBY
mkdir $PGSQL_MASTER $PGSQL_STANDBY
chmod 700 $PGSQL_MASTER
chmod 700 $PGSQL_STANDBY

#Initialize MASTER
$PGSQL_BIN/initdb -D $PGSQL_MASTER
#echo "wal_level = hot_standby" >> $PGSQL_MASTER/postgresql.conf
#echo "max_wal_senders = 3" >> $PGSQL_MASTER/postgresql.conf
#echo "wal_keep_segments = 10" >> $PGSQL_MASTER/postgresql.conf
#echo "hot_standby = on" >> $PGSQL_MASTER/postgresql.conf
#echo "max_standby_streaming_delay= -1" >> $PGSQL_MASTER/postgresql.conf
#echo "wal_consistency_checking='all'" >> $PGSQL_MASTER/postgresql.conf
echo "autovacuum=off" >> $PGSQL_MASTER/postgresql.conf
echo "max_wal_size=5GB" >> $PGSQL_MASTER/postgresql.conf
#echo "shared_preload_libraries = '\$libdir/pg_failover_slots'" >> $PGSQL_MASTER/postgresql.conf
#echo "pg_failover_slots.synchronize_slot_names=name:my_slot" >> $PGSQL_MASTER/postgresql.conf
echo "wal_level=logical" >> $PGSQL_MASTER/postgresql.conf
#Setup replication settings

#Start Master
export PGPORT=5432
echo "Starting Master.."
$PGSQL_BIN/pg_ctl -D $PGSQL_MASTER -c -w -l master_logfile start
$PGSQL_BIN/psql -d postgres -c "SELECT pg_create_physical_replication_slot('standby_1')"

#Perform Backup in the Standy Server
$PGSQL_BIN/pg_basebackup --pgdata=$PGSQL_STANDBY -P
echo "primary_conninfo= 'port=5432'" >> $PGSQL_STANDBY/postgresql.conf
#echo "shared_preload_libraries = '\$libdir/pg_failover_slots'" >> $PGSQL_STANDBY/postgresql.conf
#echo "pg_failover_slots.synchronize_slot_names=name:my_slot" >> $PGSQL_STANDBY/postgresql.conf
echo "hot_standby_feedback=on" >> $PGSQL_STANDBY/postgresql.conf
echo "primary_slot_name=standby_1" >> $PGSQL_STANDBY/postgresql.conf
echo "log_min_messages = debug1" >> $PGSQL_STANDBY/postgresql.conf
touch $PGSQL_STANDBY/standby.signal
#echo "standby_mode = on" >> $PGSQL_STANDBY/postgresql.conf
#Start STANDBY
export PGPORT=5433
echo "Starting Slave.."
$PGSQL_BIN/pg_ctl -D $PGSQL_STANDBY -c -w -l slave_logfile start

#--for failover slot do this
#--create my_slot on primary
#SELECT pg_create_logical_replication_slot('my_slot', 'test_decoding');
#create table test(a int);
#insert into test values(1);
#SELECT data FROM pg_logical_slot_get_changes('my_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');
#SELECT data FROM pg_logical_slot_get_changes('my_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');
#insert into test select i from generate_series(1,100000) AS i;
# --move catalog xmin
#SELECT data FROM pg_logical_slot_get_changes('my_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');
#SELECT data FROM pg_logical_slot_get_changes('my_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');
