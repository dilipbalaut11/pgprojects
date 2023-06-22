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
	OUT wait_event text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;
REVOKE ALL ON FUNCTION edb_wait_states_samples(timestamptz, timestamptz) FROM PUBLIC;

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
