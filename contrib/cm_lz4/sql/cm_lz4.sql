CREATE EXTENSION cm_lz4;

-- zlib compression
CREATE TABLE lz4test(f1 TEXT COMPRESSION lz4);
INSERT INTO lz4test VALUES(repeat('1234567890',1004));
INSERT INTO lz4test VALUES(repeat('1234567890 one two three',1004));
SELECT length(f1) FROM lz4test;

-- alter compression method with rewrite
ALTER TABLE lz4test ALTER COLUMN f1 SET COMPRESSION zlib;
\d+ lz4test
ALTER TABLE lz4test ALTER COLUMN f1 SET COMPRESSION lz4;
\d+ lz4test

-- preserve old compression method
ALTER TABLE lz4test ALTER COLUMN f1 SET COMPRESSION zlib PRESERVE (lz4);
INSERT INTO lz4test VALUES (repeat('1234567890',1004));
\d+ lz4test
SELECT length(f1) FROM lz4test;

DROP TABLE lz4test;
