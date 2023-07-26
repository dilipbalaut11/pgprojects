/* contrib/edb_wait_states/edb_wait_states--1.0.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "CREATE EXTENSION edb_wait_states" to load this file. \quit

-- The reader funtions expose queries, utility statements which might contain
-- sensitive information. To avoid exposing this information, we don't want any
-- of the functions defined in this extension to be available to a
-- non-superuser. Monitoring tools such as PEM may want these functions to be
-- accessible by a user who is part monitoring roles and thus would change the
-- permissions accordingly.
CREATE FUNCTION edb_wait_states_samples(
	IN start_ts timestamptz default '-infinity'::timestamptz,
	IN end_ts timestamptz default 'infinity'::timestamptz,
    OUT query_id int8,
    OUT session_id int4,
    OUT query_start_time timestamptz,
    OUT sample_time timestamptz,
	OUT wait_event_type text,
	OUT wait_event text,
	OUT sampling_interval int4
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;
REVOKE ALL ON FUNCTION edb_wait_states_samples(timestamptz, timestamptz) FROM PUBLIC;

CREATE FUNCTION edb_wait_states_header(
	OUT host_name text,
	OUT db_uptime text,
	OUT cpu_info text,
	OUT mem_info text,
	OUT db_info text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;
REVOKE ALL ON FUNCTION edb_wait_states_header() FROM PUBLIC;

CREATE FUNCTION edb_wait_states_queries(
	IN start_ts timestamptz default '-infinity'::timestamptz,
	IN end_ts timestamptz default 'infinity'::timestamptz,
    OUT query_id int8,
    OUT query text,
	OUT	ref_start_ts timestamptz,
	OUT ref_end_ts timestamptz
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;
REVOKE ALL ON FUNCTION edb_wait_states_queries(timestamptz, timestamptz) FROM PUBLIC;

CREATE FUNCTION edb_wait_states_sessions(
	IN start_ts timestamptz default '-infinity'::timestamptz,
	IN end_ts timestamptz default 'infinity'::timestamptz,
    OUT session_id int4,
    OUT dbname text,
	OUT	username text,
	OUT	ref_start_ts timestamptz,
	OUT ref_end_ts timestamptz
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;
REVOKE ALL ON FUNCTION edb_wait_states_sessions(timestamptz, timestamptz) FROM PUBLIC;

CREATE FUNCTION edb_wait_states_data(
	IN start_ts timestamptz default '-infinity'::timestamptz,
	IN end_ts timestamptz default 'infinity'::timestamptz,
    OUT session_id int4,
    OUT dbname text,
	OUT	username text,
    OUT query text,
	OUT query_start_time timestamptz,
    OUT sample_time timestamptz,
	OUT wait_event_type text,
	OUT wait_event text
) RETURNS SETOF record
AS $$
SELECT ss.session_id, ss.dbname, ss.username, q.query, s.query_start_time, s.sample_time, s.wait_event_type, s.wait_event
	FROM edb_wait_states_samples(start_ts, end_ts) s,
		 edb_wait_states_sessions(start_ts, end_ts) ss,
		 edb_wait_states_queries(start_ts, end_ts) q
	WHERE s.session_id = ss.session_id AND s.sample_time >= ss.ref_start_ts AND s.sample_time < ss.ref_end_ts AND
		  s.query_id = q.query_id AND s.sample_time >= q.ref_start_ts AND s.sample_time < q.ref_end_ts; $$
LANGUAGE SQL;
REVOKE ALL ON FUNCTION edb_wait_states_data(timestamptz, timestamptz) FROM PUBLIC;

CREATE FUNCTION edb_wait_states_purge(
	IN start_ts timestamptz default '-infinity'::timestamptz,
	IN end_ts timestamptz default 'infinity'::timestamptz
)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C;
REVOKE ALL ON FUNCTION edb_wait_states_purge(timestamptz, timestamptz) FROM PUBLIC;

CREATE FUNCTION edb_wait_states_dbtime(
	IN start_ts timestamptz default '-infinity'::timestamptz,
	IN end_ts timestamptz default 'infinity'::timestamptz,
	OUT query_id int8,
    OUT dbtime int4
) RETURNS SETOF record
AS $$
SELECT query_id, SUM(sampling_interval) AS dbtime FROM edb_wait_states_samples() GROUP BY query_id;
$$
LANGUAGE SQL;
REVOKE ALL ON FUNCTION edb_wait_states_dbtime(timestamptz, timestamptz) FROM PUBLIC;

CREATE FUNCTION edb_wait_states_cputime(
	IN start_ts timestamptz default '-infinity'::timestamptz,
	IN end_ts timestamptz default 'infinity'::timestamptz,
	OUT query_id int8,
    OUT cputime int4
) RETURNS SETOF record
AS $$
SELECT query_id, SUM(sampling_interval) AS cputime FROM edb_wait_states_samples() WHERE wait_event IS NULL GROUP BY query_id;
$$
LANGUAGE SQL;
REVOKE ALL ON FUNCTION edb_wait_states_cputime(timestamptz, timestamptz) FROM PUBLIC;

CREATE FUNCTION edb_wait_states_waittime(
	IN start_ts timestamptz default '-infinity'::timestamptz,
	IN end_ts timestamptz default 'infinity'::timestamptz,
	OUT query_id int8,
    OUT waittime int4
) RETURNS SETOF record
AS $$
SELECT query_id, SUM(sampling_interval) AS waittime FROM edb_wait_states_samples() WHERE wait_event IS NOT NULL GROUP BY query_id;
$$
LANGUAGE SQL;
REVOKE ALL ON FUNCTION edb_wait_states_waittime(timestamptz, timestamptz) FROM PUBLIC;

CREATE FUNCTION edb_wait_states_waitevent(
	IN start_ts timestamptz default '-infinity'::timestamptz,
	IN end_ts timestamptz default 'infinity'::timestamptz,
	OUT query_id int8,
    OUT wait_event text,
	OUT wait_time int4
) RETURNS SETOF record
AS $$
SELECT query_id, wait_event, SUM(sampling_interval) AS waittime FROM edb_wait_states_samples() WHERE wait_event IS NOT NULL GROUP BY query_id, wait_event;
$$
LANGUAGE SQL;
REVOKE ALL ON FUNCTION edb_wait_states_waittime(timestamptz, timestamptz) FROM PUBLIC;

CREATE FUNCTION edb_wait_states_waitevents(
	IN start_ts timestamptz default '-infinity'::timestamptz,
	IN end_ts timestamptz default 'infinity'::timestamptz,
	OUT wait_event text,
    OUT waittime int4
) RETURNS SETOF record
AS $$
SELECT wait_event, SUM(sampling_interval) AS waittime FROM edb_wait_states_samples() WHERE wait_event IS NOT NULL GROUP BY wait_event;
$$
LANGUAGE SQL;
REVOKE ALL ON FUNCTION edb_wait_states_waittime(timestamptz, timestamptz) FROM PUBLIC;

CREATE TYPE waitevents AS (wait_event text, waittime int, pct_dbtime int);
CREATE TYPE statements_event AS (queryid int8, dbtime int, waittime int, cputime int, top_waitevent text);

CREATE FUNCTION edb_wait_states_top_wait_events_json(
	IN start_ts timestamptz default '-infinity'::timestamptz,
	IN end_ts timestamptz default 'infinity'::timestamptz
) RETURNS SETOF json
AS $$
 SELECT row_to_json(row(wait_event, waittime, (waittime*100/d.dbtime))::waitevents)
 FROM edb_wait_states_waitevents(),
 (select SUM(dbtime) AS dbtime FROM edb_wait_states_dbtime) AS d;
$$
LANGUAGE SQL;

REVOKE ALL ON FUNCTION edb_wait_states_top_wait_events_json(timestamptz, timestamptz) FROM PUBLIC;

CREATE FUNCTION edb_wait_states_sql_statements_json(
	IN start_ts timestamptz default '-infinity'::timestamptz,
	IN end_ts timestamptz default 'infinity'::timestamptz
) RETURNS SETOF json
AS $$
SELECT row_to_json(row(we.query_id, dt.dbtime, wt.waittime, ct.cputime, we.wait_event)::statements_event)
FROM edb_wait_states_dbtime(start_ts, end_ts) dt, edb_wait_states_waittime(start_ts, end_ts) wt, edb_wait_states_cputime(start_ts, end_ts) ct, edb_wait_states_waitevent(start_ts, end_ts) we,
(SELECT MAX(wait_time) top_wait_time
 FROM edb_wait_states_waitevent()
 GROUP BY query_id) AS maxwt
 WHERE dt.query_id=wt.query_id AND wt.query_id=ct.query_id AND ct.query_id=we.query_id AND we.wait_time=maxwt.top_wait_time;
$$
LANGUAGE SQL;
REVOKE ALL ON FUNCTION edb_wait_states_sql_statements_json(timestamptz, timestamptz) FROM PUBLIC;

CREATE FUNCTION edb_wait_states_top_wait_events(
        IN start_ts timestamptz default '-infinity'::timestamptz,
        IN end_ts timestamptz default 'infinity'::timestamptz,
		OUT waitevent TEXT,
		OUT waittime int,
		OUT pct_dbtime int
) RETURNS SETOF RECORD
AS $$
 SELECT wait_event, waittime, (waittime*100/d.dbtime) AS pct_dbtime
 FROM edb_wait_states_waitevents(),
 (select SUM(dbtime) AS dbtime FROM edb_wait_states_dbtime) AS d;
$$
LANGUAGE SQL;

REVOKE ALL ON FUNCTION edb_wait_states_top_wait_events(timestamptz, timestamptz) FROM PUBLIC;

CREATE FUNCTION edb_wait_states_sql_statements(
        IN start_ts timestamptz default '-infinity'::timestamptz,
        IN end_ts timestamptz default 'infinity'::timestamptz,
		OUT queryid int8,
		OUT dbtime int,
		OUT waittime int,
		OUT cputime int,
		OUT top_waitevent text
) RETURNS SETOF RECORD
AS $$
SELECT we.query_id queryid, dt.dbtime dbtime, wt.waittime waittime , ct.cputime cputime, we.wait_event top_waitevent
FROM edb_wait_states_dbtime(start_ts, end_ts) dt, edb_wait_states_waittime(start_ts, end_ts) wt, edb_wait_states_cputime(start_ts, end_ts) ct, edb_wait_states_waitevent(start_ts, end_ts) we,
(SELECT MAX(wait_time) top_wait_time
 FROM edb_wait_states_waitevent()
 GROUP BY query_id) AS maxwt
 WHERE dt.query_id=wt.query_id AND wt.query_id=ct.query_id AND ct.query_id=we.query_id AND we.wait_time=maxwt.top_wait_time;
$$
LANGUAGE SQL;
REVOKE ALL ON FUNCTION edb_wait_states_sql_statements(timestamptz, timestamptz) FROM PUBLIC;

/*
TO BE REMOVED - queries for generating html output
--Q1
SELECT wait_event, waittime, (waittime*100/d.dbtime) AS pct_dbtime
FROM edb_wait_states_waitevents(),
(select SUM(dbtime) AS dbtime FROM edb_wait_states_dbtime) AS d;
  wait_event   | waittime | pct_dbtime 
---------------+----------+------------
 tuple         |       10 |         14
 transactionid |       40 |         57

--Q2
SELECT we.query_id, dt.dbtime dbtime, wt.waittime waittime, ct.cputime cputime, we.wait_event top_wait_event
FROM edb_wait_states_dbtime() dt, edb_wait_states_waittime() wt, edb_wait_states_cputime() ct, edb_wait_states_waitevent() we,
(SELECT MAX(wait_time) top_wait_time
 FROM edb_wait_states_waitevent()
 GROUP BY query_id) AS maxwt
 WHERE dt.query_id=wt.query_id AND wt.query_id=ct.query_id AND ct.query_id=we.query_id AND we.wait_time=maxwt.top_wait_time;

       query_id       | dbtime | waittime | cputime | top_wait_event 
----------------------+--------+----------+---------+----------------
 -2331406789928976424 |     12 |        5 |       7 | transactionid
  1341496867771568417 |     50 |       45 |       5 | transactionid

*/
