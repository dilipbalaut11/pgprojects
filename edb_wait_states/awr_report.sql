\o edb_awr_report.html

\T 'cellspacing=0 cellpadding=0'
\qecho '<html><head><style>H2{background:#e6e6e6}</style>'
\qecho '<title>PostgreSQL Report</title></head><body>'
\qecho '<table><tr valign=''top''><td>'

\qecho '<h2>Report Date</h2>'
\pset format html
SELECT current_timestamp AS report_timestamp;

\qecho '<h2>PostgreSQL Basic Information</h2>'
\pset format html
SELECT version() AS PostgreSQL_Version;

\x off
\t off

\H
\qecho '<h2>System Information</h2>'
\pset format html
select * from edb_wait_states_header();
\H
\qecho '<h2>DBTime</h2>'
\pset format html
select sum(dbtime) AS total_dbtime from edb_wait_states_dbtime(:start, :end);
\H
\qecho '<h2>Session Informations</h2>'
\pset format html
SELECT session_user, current_database();
\H
\qecho '<h2>Server Uptime</h2>'
\pset format html
SELECT date_trunc('second', current_timestamp - pg_postmaster_start_time()) as uptime;
\H
\qecho '<h2>Top wait events</h2>'
\pset format html
select * from edb_wait_states_top_wait_events(:start, :end);
\H
\qecho '<h2>Top SQL statements</h2>'
\pset format html
select * from edb_wait_states_sql_statements(:start, :end);

\qecho '</td></tr></table>'
\qecho '</body></html>'

\o

