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

CREATE TABLE range_parted_5(a int, b int) PARTITION BY RANGE (a);
CREATE TABLE range_parted_5_1 PARTITION OF range_parted_5 FOR VALUES FROM (400000) TO (450000);
CREATE TABLE range_parted_5_2 PARTITION OF range_parted_5 FOR VALUES FROM (450000) TO (460000);
INSERT INTO range_parted_5 SELECT i,i%100 + 500 FROM generate_series(400000,459999) AS i;
CREATE INDEX global_idx_1 ON range_parted_5(b) global;
EXPLAIN (COSTS OFF) SELECT * FROM range_parted_5 WHERE b = 550;
SELECT * FROM range_parted_5 WHERE b = 550 LIMIT 5;

--attach to the top partition
ALTER TABLE range_parted ATTACH PARTITION range_parted_5 FOR VALUES FROM (400000) to (500000);
EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 550;
SELECT * FROM range_parted WHERE b = 550 LIMIT 5;

-- Attach to level-1 partition (test with multi level global index)
CREATE TABLE range_parted_6(a int, b int);
INSERT INTO range_parted_6 SELECT i,i%100 + 600 FROM generate_series(460000,490000) AS i;
ALTER TABLE range_parted_5 ATTACH PARTITION range_parted_6 FOR VALUES FROM (460000) to (500000);
EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 650;
SELECT * FROM range_parted WHERE b = 650 LIMIT 5;

-- Update the leaf and check we are inserting that in multi-level global index
UPDATE range_parted SET b=b+1000;
EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 1650;
SELECT * FROM range_parted WHERE b = 1650 LIMIT 5;
EXPLAIN (COSTS OFF) SELECT * FROM range_parted_5 WHERE b = 1650;
SELECT * FROM range_parted_5 WHERE b = 1650 LIMIT 5;

-- Conditional update using global index
EXPLAIN (COSTS OFF) UPDATE range_parted SET b=b+1000 where b = 1650;
UPDATE range_parted SET b=b+1000 where b = 1650;
EXPLAIN (COSTS OFF) SELECT * FROM range_parted_5 WHERE b = 2650;
SELECT * FROM range_parted_5 WHERE b = 2650 LIMIT 5;

--Detach partition
ALTER TABLE range_parted DETACH PARTITION range_parted_5;
EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 550;
SELECT * FROM range_parted WHERE b = 550 LIMIT 5;

--Reattach the partition
ALTER TABLE range_parted ATTACH PARTITION range_parted_5 FOR VALUES FROM (400000) to (500000);
EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 550;
SELECT * FROM range_parted WHERE b = 550 LIMIT 5;

--Drop the partitioned table
DROP TABLE range_parted_5;
EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 550;
SELECT * FROM range_parted WHERE b = 550 LIMIT 5;

-- Test unique global index
TRUNCATE TABLE range_parted;
INSERT INTO range_parted VALUES(1,2);
INSERT INTO range_parted VALUES(2,2);
CREATE UNIQUE INDEX global_idx_unique ON range_parted(b) global; -- Fail with duplicate
TRUNCATE TABLE range_parted;
INSERT INTO range_parted VALUES(1,2);
CREATE UNIQUE INDEX global_idx_unique ON range_parted(b) global;
INSERT INTO range_parted VALUES(1,2); -- Fail with duplicate
DROP INDEX global_idx_unique;
INSERT INTO range_parted VALUES(1,2); -- Now this should pass
-- Cleanup
DROP TABLE range_parted;
