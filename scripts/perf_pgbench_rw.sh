#Pre condition:
#Create and initialize the database and set the export PGDATA 
export PGDATA=/mnt/data-ssd/dilip.kumar/pgdata
export PGPORT=54321
export LD_LIBRARY_PATH=/mnt/data-ssd/dilip.kumar/install/lib:$LD_LIBRARY_PATH

#for i in "scale_factor shared_buffers time_for_readings no_of_readings orig_or_patch"
#for i in "3 8GB 30 3 1" "3 8GB 30 3 0"
for i in "300 8GB 1800 3 0"
do
scale_factor=`echo $i | cut -d" " -f1`
shared_bufs=`echo $i | cut -d" " -f2`
time_for_reading=`echo $i | cut -d" " -f3`
no_of_readings=`echo $i | cut -d" " -f4`
orig_or_patch=`echo $i | cut -d" " -f5`

if [ $orig_or_patch = "0" ]
then
	run_bin="install"
elif [ $orig_or_patch = "1" ]
then
	run_bin="patch"    
else 
	run_bin="sbe_slock_diffcl_1_v5_3"    
fi

# -----------------------------------------------

echo "Start of script for $scale_factor $shared_bufs " >> test_results.txt


#echo "============== $run_bin =============" >> test_results.txt
#cp postgres_${run_bin} postgres

#Start the server
#./postgres -c shared_buffers=$shared_bufs &
#./pg_ctl start -l postgres_bgw.log

#sleep 5 
cd ${run_bin}/bin
rm -rf $PGDATA
./initdb -D $PGDATA
for threads in 64
do       
	#./pgbench -c $threads -j $threads -T $time_for_reading -S  postgres  >> test_results.txt
        #Start taking reading
	for ((readcnt = 1 ; readcnt <= $no_of_readings ; readcnt++))
	do
		echo "================================================"  >> test_results.txt
		echo $scale_factor, $shared_bufs, $threads, $threads, $time_for_reading Reading - ${readcnt}  >> test_results.txt
		#start server

	#	if [ $orig_or_patch = "0" ]
        #        then
        #                cd install/bin
        #        else
	#		cd 96/bin
         #       fi

	

#		if [ $orig_or_patch = "0" ]
#		then
			./postgres -c shared_buffers=$shared_bufs -c checkpoint_timeout=40min -c max_wal_size=20GB -c max_connections=200 -c maintenance_work_mem=1GB&	
#		else
#			./postgres -c shared_buffers=$shared_bufs -c max_wal_size=20GB -c wal_write_chunk_size=4096 -c checkpoint_timeout=40min -c max_connections=150 -c wal_sync_method=open_datasync -c full_page_writes=off -c maintenance_work_mem=1GB& 		
#		fi

 		sleep 5
		#drop and recreate database
		./dropdb postgres
		./createdb postgres
		#initialize database
		./pgbench -i -s $scale_factor postgres
		sleep 5
		# Run pgbench	
		#./pgbench -c $threads -j $threads -T $time_for_reading -S -M prepared  postgres  >> test_results.txt
		#./pgbench -c $threads -j $threads -T $time_for_reading -S  postgres  >> test_results.txt
		./pgbench -c $threads -j $threads -T $time_for_reading -P5  -M prepared postgres >> test_results.txt
		sleep 10
		./psql -d postgres -c "checkpoint" >> test_results.txt
		./pg_ctl stop
		
		cp test_results.txt /mnt/data-ssd/dilip.kumar/test_results_${scale_factor}_${orig_or_patch}.txt
	#	cd ../..
	done;
done;

#./psql -d postgres -c "checkpoint" >> test_results.txt
#./pg_ctl stop
sleep 1


#mv test_results.txt test_results_list_${scale_factor}_orig_or_patchd_bufs}_${run_bin}.txt
done;
