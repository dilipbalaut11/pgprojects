#!/bin/bash
#
# pgsql_report.sh
#
echo ${SQL} | ./psql -p 5444 -d postgres -f awr_report.sql

