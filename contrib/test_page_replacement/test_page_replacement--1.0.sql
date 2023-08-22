/* contrib/test_page_replacement/test_page_replacement--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_page_replacement" to load this file. \quit

/* generic file access functions */

CREATE FUNCTION test_page_replacement(text, text, int)
RETURNS void
AS 'MODULE_PATHNAME', 'test_page_replacement'
LANGUAGE C;

