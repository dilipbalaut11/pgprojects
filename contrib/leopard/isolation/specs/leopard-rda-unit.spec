# Unit tests for RDA heap

setup
{
  CREATE TABLE foo (
	id int PRIMARY KEY,
	data text NOT NULL
  );
  INSERT INTO foo SELECT generate_series(100,900), 'o';
}

teardown
{
  DROP TABLE foo;
  TRUNCATE rda.rda;
}

session s1
setup		{ BEGIN; }
step s1s	{ SELECT * FROM foo WHERE id < 100; }
step s1c	{ COMMIT; }

session s2
step i1		{ INSERT INTO foo VALUES (1, 'x'); }
step i2		{ INSERT INTO foo VALUES (2, 'y'); }
step i3		{ INSERT INTO foo VALUES (3, 'z'); }
step u1		{ UPDATE foo SET data = 'a' WHERE id = 2; }
step u2		{ UPDATE foo SET data = 'b' WHERE id = 2; }
step u3		{ UPDATE foo SET data = 'c' WHERE id = 2; }

step rdaif	{
  CREATE FUNCTION rda_unit_test_rda_insert(int) RETURNS VOID
	AS '/Users/sriggs/pg/pg-git/pgREL_14_STABLE/contrib/leopard/leopard.so', 'rda_unit_test_rda_insert'
	LANGUAGE C;
}
step rdai	{ BEGIN; SELECT rda_unit_test_rda_insert(0); }
step rdagf	{
  CREATE FUNCTION rda_unit_test_rda_get(int) RETURNS VOID
	AS '/Users/sriggs/pg/pg-git/pgREL_14_STABLE/contrib/leopard/leopard.so', 'rda_unit_test_rda_get'
	LANGUAGE C;
}
step rdag	{ SELECT rda_unit_test_rda_get(0); }
step rdatf	{
  CREATE FUNCTION rda_unit_test_rda_trim(int) RETURNS VOID
	AS '/Users/sriggs/pg/pg-git/pgREL_14_STABLE/contrib/leopard/leopard.so', 'rda_unit_test_rda_trim'
	LANGUAGE C;
}
step rdat	{ BEGIN; SELECT rda_unit_test_rda_trim(0); }
step s2c	{ COMMIT; }

session s3
step srda	{ SELECT root_tid, next_tid FROM rda.rda; }

step srdai1	{ SET enable_seqscan = off; }
step srdai2	{ EXPLAIN (COSTS OFF) SELECT * FROM rda.rda_p0000 WHERE toid = 16585 AND root_tid = 196733 AND row_xmin < 741 AND row_xmax >= 741 ORDER BY row_xmin DESC LIMIT 1; }
step srdai3	{ SELECT 1 FROM rda.rda_p0000 WHERE toid = 16585 AND root_tid = 196733 AND row_xmin < 741 AND row_xmax >= 741 ORDER BY row_xmin DESC LIMIT 1; }

permutation i1 i2 i3 s1s u1 u2 u3 rdaif rdai srda s2c s1c srdai1 srdai2 srdai3 rdagf rdag
permutation rdatf rdat s2c
