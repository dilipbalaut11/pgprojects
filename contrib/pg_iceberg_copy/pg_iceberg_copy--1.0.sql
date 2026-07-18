/* contrib/pg_iceberg_copy/pg_iceberg_copy--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_iceberg_copy" to load this file. \quit

CREATE FUNCTION pg_iceberg_copy_run(export_dir text DEFAULT NULL)
RETURNS text
AS 'MODULE_PATHNAME', 'pg_iceberg_copy_run'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION pg_iceberg_copy_status(
    OUT worker_pid int4,
    OUT enabled bool,
    OUT export_directory text,
    OUT last_export_time timestamptz,
    OUT files_exported int8,
    OUT bytes_written int8
)
AS 'MODULE_PATHNAME', 'pg_iceberg_copy_status'
LANGUAGE C STRICT PARALLEL RESTRICTED;
