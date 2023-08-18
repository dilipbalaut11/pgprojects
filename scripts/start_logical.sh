ulimit -c unlimited
./start.sh
./psql -p 5432 -d postgres -c "create table t(a int PRIMARY KEY ,b text);"
./psql -p 5432 -d postgres -c "create table t1(a int PRIMARY KEY ,b text);"
./psql -p 5433 -d postgres -c "create table t(a int PRIMARY KEY ,b text, c int);"
./psql -p 5433 -d postgres -c "create table t1(a int PRIMARY KEY ,b text);"
./psql -p 5432 -d postgres -c "create publication test_pub for all tables with(PUBLISH='insert,delete,update,truncate');"
./psql -p 5432 -d postgres -c "alter table t replica identity FULL ;"
./psql -p 5432 -d postgres -c "CREATE OR REPLACE FUNCTION large_val() RETURNS TEXT LANGUAGE SQL AS 'select array_agg(md5(g::text))::text from generate_series(1, 2000) g';"
./psql -d postgres -p 5433 -c "create subscription test_sub CONNECTION 'host=127.0.0.1 port=5432 dbname=postgres' PUBLICATION test_pub WITH ( streaming=on);"
