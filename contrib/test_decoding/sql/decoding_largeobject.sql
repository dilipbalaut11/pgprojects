-- test that we can insert into the large objects and decode the changes

-- predictability
SET synchronous_commit = on;

SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');

-- slot works
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');

-- Create a new large object
CREATE TABLE lotest_stash_values (loid oid, fd integer);

INSERT INTO lotest_stash_values (loid) SELECT lo_creat(42);

-- NOTE: large objects require transactions
BEGIN;
UPDATE lotest_stash_values SET fd = lo_open(loid, CAST(x'20000' | x'40000' AS integer));
SELECT lowrite(fd, 'large object test data') FROM lotest_stash_values;
SELECT lo_close(fd) FROM lotest_stash_values;
END;
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');

BEGIN;
UPDATE lotest_stash_values SET fd=lo_open(loid, CAST(x'20000' | x'40000' AS integer));
SELECT lo_lseek(fd, 10, 0) FROM lotest_stash_values;
SELECT lowrite(fd, 'overwrite some data') FROM lotest_stash_values;
END;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');

BEGIN;
UPDATE lotest_stash_values SET fd=lo_open(loid, CAST(x'20000' | x'40000' AS integer));
SELECT lo_lseek(fd, 2048, 0) FROM lotest_stash_values;
SELECT lowrite(fd, 'write into new page') FROM lotest_stash_values;
END;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');

BEGIN;
UPDATE lotest_stash_values SET fd = lo_open(loid, CAST(x'20000' | x'40000' AS integer));
SELECT lowrite(fd, '
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
test large data more in 2048 test large data more in 2048 test large data more in 2048
') FROM lotest_stash_values;
SELECT lo_close(fd) FROM lotest_stash_values;
END;
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');


-- Clean up the slot
SELECT pg_drop_replication_slot('regression_slot');