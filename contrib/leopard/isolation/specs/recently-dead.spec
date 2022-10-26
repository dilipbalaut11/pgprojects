# Test the visibility of recently dead tuples in transactions
# which should and transactions which should not see them.

setup
{
	CREATE TABLE tst (
		id BIGINT NOT NULL
	) WITH (fillfactor = 100);

	INSERT INTO tst (id) (SELECT * FROM generate_series(1,5000));
}

teardown
{
	DROP TABLE tst;
}

session s1
step s1b			{ begin isolation level serializable; }
step s1m2			{ update tst set id = id+1 where id % 2 = 1; }
step s1m3			{ update tst set id = id+1 where id % 3 = 1; }
step s1m5			{ update tst set id = id+1 where id % 5 = 1; }
step s1c			{ commit; }

session s2
step s2b			{ begin isolation level serializable; }
step s2m2			{ select md5(array_agg(id order by id)::text) from tst where id % 2 = 1; }
step s2m3			{ select md5(array_agg(id order by id)::text) from tst where id % 3 = 1; }
step s2m5			{ select md5(array_agg(id order by id)::text) from tst where id % 5 = 1; }
step s2c			{ commit; }

session s3
step s3b			{ begin isolation level serializable; }
step s3m2			{ select md5(array_agg(id order by id)::text) from tst where id % 2 = 1; }
step s3m3			{ select md5(array_agg(id order by id)::text) from tst where id % 3 = 1; }
step s3m5			{ select md5(array_agg(id order by id)::text) from tst where id % 5 = 1; }
step s3c			{ commit; }

session s4
step s4b			{ begin isolation level serializable; }
step s4m2			{ select md5(array_agg(id order by id)::text) from tst where id % 2 = 1; }
step s4m3			{ select md5(array_agg(id order by id)::text) from tst where id % 3 = 1; }
step s4m5			{ select md5(array_agg(id order by id)::text) from tst where id % 5 = 1; }
step s4c			{ commit; }

permutation s2b s1b s1m2 s2m2 s1c s2c
permutation s3b s2b s1b s1m2 s2m2 s1m3 s2m3 s3m3 s1c s2c s3c
permutation s4b s3b s2b s1b s1m2 s2m2 s1m3 s2m3 s3m3 s1m5 s2m5 s3m5 s4m5 s1c s2c s3c s4c
permutation s4b s3b s2b s1b s1m2 s1m3 s1m5 s1c s2m2 s2m3 s2m5 s1b s1m2 s1c s2c s3m2 s3m3 s3m5 s3c s1b s1m5 s1c s4m2 s4m3 s4m5 s4c
