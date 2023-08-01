echo "----------EDB AWR REPORT START--------" > $1
echo "--------------------------------------" >> $1
echo " " >> $1
echo " " >> $1
echo "--------System Information--------" >> $1
echo " " >> $1
./psql -d postgres -c  "select * from edb_wait_states_header();" >> $1

echo "--------Other information--------" >> $1
echo " " >> $1
./psql -d postgres -c "select sum(dbtime) AS total_dbtime from edb_wait_states_dbtime($2, $3);" >> $1
./psql -d postgres -c "SELECT session_user, current_database();" >> $1
./psql -d postgres -c "SELECT date_trunc('second', current_timestamp - pg_postmaster_start_time()) as uptime;" >> $1

echo " " >> $1
echo " " >> $1
echo "--------Top sql statements --------" >> $1
echo " " >> $1
./psql -d postgres -c  "select * from edb_wait_states_sql_statements($2, $3);" >> $1
echo " " >> $1
echo " " >> $1
echo "--------Top wait events --------" >> $1
echo " " >> $1
./psql -d postgres -c "select * from edb_wait_states_top_wait_events($2, $3);" >> $1
echo " " >> $1
echo " " >> $1
echo "----------EDB AWR REPORT END--------" >> $1
