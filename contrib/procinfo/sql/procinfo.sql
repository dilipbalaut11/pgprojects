CREATE EXTENSION procinfo;

CREATE TABLE test_procinfo(a int);
CREATE PROCEDURE test_proc1()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO test_procinfo VALUES (1);
EXCEPTION WHEN others THEN
    RAISE NOTICE 'exception';
END;
$$;

CREATE PROCEDURE test_proc(x int)
LANGUAGE plpgsql
AS $$
BEGIN
    FOR i IN 1..x LOOP
	CALL test_proc1();
    END LOOP;
END;
$$;

BEGIN;
CALL test_proc(100);	-- create overflow
SELECT subxact_count, isoverflow FROM pg_procinfo;  --check count
ROLLBACK;
SELECT subxact_count, isoverflow FROM pg_procinfo;  --check count again

BEGIN;
SAVEPOINT S1;
INSERT INTO test_procinfo VALUES (1);
SAVEPOINT S2;
INSERT INTO test_procinfo VALUES (1);
SELECT subxact_count, isoverflow FROM pg_procinfo;  --check count
COMMIT;
SELECT subxact_count, isoverflow FROM pg_procinfo;  --check count again
