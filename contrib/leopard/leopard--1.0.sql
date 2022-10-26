/* leopard/leopard--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION leopard" to load this file. \quit

CREATE FUNCTION leopard_tableam_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access method
CREATE ACCESS METHOD leopard TYPE TABLE HANDLER leopard_tableam_handler;
COMMENT ON ACCESS METHOD leopard IS 'leopard Table Access Method';

--
-- Recently Dead Archive (RDA) using a Heap
--
-- Note that changing the order or datatype of columns, or adding/changing
-- indexes will cause ERRORs in the hardcoded table access paths.
--
DROP SCHEMA IF EXISTS rda;
CREATE SCHEMA rda;
DROP TABLE IF EXISTS rda.rda;
CREATE TABLE rda.rda
(toid       OID NOT NULL    /* reloid */
,root_tid   BIGINT NOT NULL /* only 6 bytes */
,row_xmin   OID NOT NULL    /* xid is 32-bit unsigned int, so not INT4 */
,row_xmax   OID NOT NULL    /* xid is 32-bit unsigned int, so not INT4 */
,next_tid   BIGINT NOT NULL /* only updated rows are stored, so always set */
,row_data   BYTEA           /* NULL if row_xmin == row_xmax */
)
PARTITION BY RANGE (row_xmax)
;

REVOKE ALL ON rda.rda FROM PUBLIC;
GRANT SELECT, INSERT ON rda.rda TO PUBLIC;

-- generate partitions using pattern
--  CREATE TABLE rda_pNNNN PARTITION OF rda FOR VALUES FROM (START) TO (END) WITH (...)
--  CREATE INDEX ON <table> (toid, root_tid, row_xmin, row_xmax) WITH (...)
-- since parameters cannot be added directly to partitioned tables
DO LANGUAGE PLPGSQL
$$
DECLARE
    i   integer;
    s   integer;
    n   integer;
    sql text;
	name text;
	qname text;
	hibound	text;
BEGIN
 /*
  * n is the Number of Partitions
  *
  * n is configurable, but if you change this value you must also change the
  * constants defined in access/rda_heap.c to match this.
  * This is not intended for user configuration.
  * Current assumptions is this will be configured in the range 1-8192, since
  * names are generated/searched for using 4 zero-padded digits.
  */
 n := 32;  /* Number of partitions */

 s := (2^31) / n;
 FOR i IN 0..(n-1) LOOP
	name:= 'rda_p' || to_char(i, 'FM0000');
	qname:= 'rda.' || name;
	IF i = (n-1) THEN
		hibound:= 'MAXVALUE';
	ELSE
		hibound:= ((s * i)+(s))::text;
	END IF;
    sql :=  'CREATE TABLE ' || qname ||
            ' PARTITION OF rda.rda FOR VALUES FROM (' ||
            (s * i)::text ||
            ') TO (' ||
			hibound ||
            ') WITH (' ||
			' autovacuum_enabled=off, toast.autovacuum_enabled=off' ||
			',log_autovacuum_min_duration=0, toast.log_autovacuum_min_duration=0' ||
			',vacuum_truncate=off,toast.vacuum_truncate=off' ||
			',fillfactor=100' ||
			',toast_tuple_target=8160' ||
			'); CREATE INDEX ' || name || '_idx ON ' || qname ||
			' (toid, root_tid, row_xmin DESC, row_xmax)' ||
			' WITH (fillfactor=100);';

    EXECUTE sql;
 END LOOP;
END;
$$;
