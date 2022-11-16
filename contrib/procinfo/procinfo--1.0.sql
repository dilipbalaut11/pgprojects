-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION procinfo" to load this file. \quit


CREATE FUNCTION pg_get_all_procinfo()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'pg_get_all_procinfo'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION pg_get_next_multixact()
RETURNS xid
AS 'MODULE_PATHNAME', 'pg_get_next_multixact'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW pg_procinfo AS
	SELECT P.* FROM pg_get_all_procinfo() AS P
	(pid integer, xid xid, subxact_count int, isoverflow bool);
