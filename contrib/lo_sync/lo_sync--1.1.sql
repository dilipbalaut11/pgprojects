/* contrib/losync/lo_sync--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION lo_sync" to load this file. \quit

--
-- Sync large objects
--
CREATE FUNCTION lo_get_info(loid Oid, pageno integer, lsn pg_lsn)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'verify_heapam'
LANGUAGE C;

CREATE TABLE lo_sync_status (
    loid Oid,
    pageno integer,
    lsn pg_lsn
);
--REVOKE ALL ON TABLE lo_sync_status FROM PUBLIC;
