/* contrib/check_stale_read/check_stale_read--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION check_stale_read" to load this file. \quit

CREATE FUNCTION @extschema@.check_stale_read_stats(
  OUT block_registered bigint,
  OUT block_validated bigint,
  OUT block_stale bigint,
  OUT live_entries bigint,
  OUT insert_skipped bigint,
  OUT last_flush_lsn pg_lsn)
RETURNS record
AS 'MODULE_PATHNAME'
LANGUAGE C;