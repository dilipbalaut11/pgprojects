--
-- GLOBAL index tests
--
CREATE TABLE range_parted (
	a int,
	b int
) PARTITION BY RANGE (a);

--create some partitions and insert data
CREATE TABLE range_parted_1 PARTITION OF range_parted FOR VALUES FROM (1) TO (100000);
CREATE TABLE range_parted_2 PARTITION OF range_parted FOR VALUES FROM (100000) TO (200000);
CREATE TABLE range_parted_3 PARTITION OF range_parted FOR VALUES FROM (200000) TO (300000);
INSERT INTO range_parted SELECT i,i%100 FROM generate_series(1,299999) AS i;

--Create global index
CREATE INDEX global_idx ON range_parted(b) global;
INSERT INTO range_parted SELECT i,i%200 FROM generate_series(1,299999) AS i;
EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 7;
SELECT * FROM range_parted WHERE b = 7 LIMIT 10;

EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 110;
SELECT * FROM range_parted WHERE b = 110 LIMIT 10;
SELECT * FROM range_parted WHERE b = 250 LIMIT 10;

UPDATE range_parted SET b=b+100;
EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 250;
SELECT * FROM range_parted WHERE b = 250 LIMIT 10;

--attach partition
CREATE TABLE range_parted_4(a int, b int);
INSERT INTO range_parted_4 SELECT i,i%300 + 100 FROM generate_series(300000,300300) as i;
ALTER TABLE range_parted ATTACH PARTITION range_parted_4 FOR VALUES FROM (300000) to (400000);
EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 350;
SELECT * FROM range_parted WHERE b = 350 LIMIT 10;
-- Cleanup
DROP TABLE range_parted;
