/* contrib/cm_lz4/cm_lz4--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION cm_lz4" to load this file. \quit

CREATE FUNCTION lz4handler(internal)
RETURNS compression_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Compression method
CREATE COMPRESSION METHOD lz4 HANDLER lz4handler;
COMMENT ON COMPRESSION METHOD lz4 IS 'lz4 compression method';
