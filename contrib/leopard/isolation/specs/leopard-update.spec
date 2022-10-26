# Update tests for Leopard

setup
{
 CREATE TABLE ltest
 (   id  bigint not null primary key 
 ,   val bigint not null
 ) WITH (autovacuum_enabled = off);
 INSERT INTO ltest SELECT generate_series(1,182), 0;
}

teardown
{
  DROP TABLE ltest;
}

session s1
setup		{ SET enable_seqscan = off; }
step s1b	{ BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM ltest; }
step s1s	{ SELECT * FROM ltest WHERE id IN (1,3) ORDER BY id; }
step s1c	{ COMMIT; }

session s2
setup		{ SET enable_seqscan = off; }
step s		{ SELECT * FROM ltest WHERE id IN (1,3) ORDER BY id; }
step u1		{ UPDATE ltest SET val = val + 1 WHERE id = 1; }
step u3		{ UPDATE ltest SET val = val + 1 WHERE id = 3; }
step srda	{ SELECT toid, root_tid, row_xmin, row_xmax, next_tid FROM rda.rda; }

# earlier updates have left space that can be cleared
permutation s1b u3 u3 s u1 s u1 s srda s1s u1 s s1c

# earlier updates in same chain may be cleared
permutation s1b u3 u3 s u3 s u1 s srda s1s u1 s s1c

permutation     u3 u3 s u3 s u3 s u3 s
