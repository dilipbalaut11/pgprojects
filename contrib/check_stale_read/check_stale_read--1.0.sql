/* contrib/check_stale_read/check_stale_read--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION check_stale_read" to load this file. \quit

CREATE SCHEMA csr;
GRANT USAGE ON SCHEMA csr TO public;

-- Everything should assume the 'pgactive' prefix
SET LOCAL search_path = csr;

CREATE TABLE check_stale_read_table
{
  seq       bigserial,
  spcOid    OID,
  dbOid     OID,
  relnumber bigint,
  foknumber int,
  blknumber int,
  lsn       pg_lsn
  PRIMARY KEY(seq)
};

CREATE INDEX check_stale_read_table_key_index
    ON check_stale_read_table(spcOid, dbOid, relnumber, foknumber, blknumber);

CREATE FUNCTION check_stale_read_stats(
  OUT block_registered bigint,
  OUT block_validated bigint,
  OUT block_stale bigint,
  OUT live_entries bigint,
  OUT insert_skipped bigint,
  OUT last_flush_lsn pg_lsn)
RETURNS record
AS 'MODULE_PATHNAME'
LANGUAGE C;