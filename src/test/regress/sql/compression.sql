-- test creating table with compression method
CREATE TABLE cmdata(f1 text COMPRESSION pglz);
CREATE INDEX idx ON cmdata(f1);
INSERT INTO cmdata VALUES(repeat('1234567890',1000));
\d+ cmdata

CREATE TABLE cmdata1(f1 TEXT COMPRESSION lz4);
INSERT INTO cmdata1 VALUES(repeat('1234567890',1004));
\d+ cmdata1

-- verify stored compression method
SELECT pg_column_compression(f1) FROM cmdata;
SELECT pg_column_compression(f1) FROM cmdata1;

-- decompress data slice
SELECT SUBSTR(f1, 200, 5) FROM cmdata;
SELECT SUBSTR(f1, 2000, 50) FROM cmdata1;

-- copy with table creation
SELECT * INTO cmmove1 FROM cmdata;
SELECT pg_column_compression(f1) FROM cmmove1;

-- update using datum from different table
CREATE TABLE cmmove2(f1 text COMPRESSION pglz);
INSERT INTO cmmove2 VALUES (repeat('1234567890',1004));
SELECT pg_column_compression(f1) FROM cmmove2;

UPDATE cmmove2 SET f1 = cmdata.f1 FROM cmdata;
SELECT pg_column_compression(f1) FROM cmmove2;
UPDATE cmmove2 SET f1 = cmdata1.f1 FROM cmdata1;
SELECT pg_column_compression(f1) FROM cmmove2;

-- copy to existing table
CREATE TABLE cmmove3(f1 text COMPRESSION pglz);
INSERT INTO cmmove3 SELECT * FROM cmdata;
INSERT INTO cmmove3 SELECT * FROM cmdata1;
SELECT pg_column_compression(f1) FROM cmmove2;

-- test LIKE INCLUDING COMPRESSION
CREATE TABLE cmdata2 (LIKE cmdata1 INCLUDING COMPRESSION);
\d+ cmdata2

-- drop original compression information
DROP TABLE cmdata;
DROP TABLE cmdata1;

-- check data is ok
SELECT length(f1) FROM cmmove1;
SELECT length(f1) FROM cmmove2;
SELECT length(f1) FROM cmmove3;

DROP TABLE cmmove1, cmmove2, cmmove3;
