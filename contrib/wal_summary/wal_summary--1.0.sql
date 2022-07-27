/* contrib/wal_summary/wal_summary--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION wal_summary" to load this file. \quit

--
-- generate_wal_summary_file()
--
CREATE FUNCTION generate_wal_summary_file(pg_lsn, pg_lsn)
RETURNS void
AS 'MODULE_PATHNAME', 'generate_wal_summary_file'
LANGUAGE C STRICT PARALLEL RESTRICTED;

CREATE FUNCTION print_wal_summary_file(pg_lsn, pg_lsn)
RETURNS void
AS 'MODULE_PATHNAME', 'print_wal_summary_file'
LANGUAGE C STRICT PARALLEL RESTRICTED;

CREATE FUNCTION read_wal_summary_relinfo(pg_lsn, pg_lsn, oid, oid, oid, integer)
RETURNS void
AS 'MODULE_PATHNAME', 'read_wal_summary_relinfo'
LANGUAGE C STRICT PARALLEL RESTRICTED;
