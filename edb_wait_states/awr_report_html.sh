#!/bin/bash
#
# pgsql_report.sh
# example run: sh awr_report_html.sh "'30-APR-23 17:20:30.45226'" "'30-JUL-24 17:20:20.45226'"
start_time="start=$1"
end_time="end=$2"
echo $start_time
echo $end_time
echo ${SQL} | ./psql -p 5444 -d postgres -v "$start_time" -v "$end_time" -f awr_report.sql

