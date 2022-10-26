# Test the visibility of recently dead tuples in transactions
# which should and transactions which should not see them.

setup
{
	CREATE FUNCTION mkjunk(id BIGINT) RETURNS TEXT AS $$
		SELECT string_agg(md5(gs::text), '-')
			FROM generate_series(1, $1) AS gs
	$$ LANGUAGE sql IMMUTABLE STRICT;

	CREATE TABLE a (
		aid BIGINT NOT NULL PRIMARY KEY,
		junk TEXT
	) WITH (fillfactor = 20);

	CREATE TABLE b (
		bid BIGINT NOT NULL PRIMARY KEY,
		aid BIGINT NOT NULL REFERENCES a(aid),
		junk TEXT
	) WITH (fillfactor = 20);

	INSERT INTO a (aid, junk)
		(SELECT gs, mkjunk(gs) FROM generate_series(1,200) AS gs);
	INSERT INTO b (aid, bid, junk)
		(SELECT gs, gs, mkjunk(gs) FROM generate_series(1,200) AS gs);
}

teardown
{
	DROP TABLE b, a;
	DROP FUNCTION mkjunk(BIGINT);
}

# We want the two sessions to update and delete rows spanning the table, but
# not the same rows, as that would lead to them blocking each other.  So,
# we trust that all IDs in the table are in the range [1..1000] and choose
# prime numbers which have no multiples in common within that range, and use
# strides of that much to choose which rows we update and delete.

session s1
step s1b			{ begin isolation level serializable; }
step s1svpt         { savepoint f; }
step s1rollback		{ rollback to f; }
step s1release		{ release savepoint f; }
step s1c			{ commit; }
step s1d1			{ declare d1 cursor for select a.aid, substring(a.junk FROM 1 for 40) from a order by 1, 2; }
step s1d2			{ declare d2 cursor for select a.aid, substring(a.junk FROM 1 for 40) from a natural join b order by 1, 2; }
step s1d3			{ declare d3 cursor for select a.aid, substring(a.junk FROM 1 for 40) from a join b on a.aid = b.aid order by 1, 2; }
step s1d4			{ declare d4 cursor for select a.aid, substring(a.junk FROM 1 for 40) from a where a.aid % 31 = 0 order by 1, 2; }
step s1d5			{ declare d5 cursor for select a.aid, substring(a.junk FROM 1 for 40) from a natural join b where a.aid % 31 = 0 order by 1, 2; }
step s1d6			{ declare d6 cursor for select a.aid, substring(a.junk FROM 1 for 40) from a join b on a.aid = b.aid where a.aid % 31 = 0 order by 1, 2; }
step s1upa			{ update a set junk = mkjunk(aid+2) where a.aid % 31 = 0; }
step s1upb			{ update b set junk = mkjunk(bid+2) where b.bid % 31 = 0; }
step s1dela			{ delete from a where a.aid % 41 = 0 and a.aid not in (select b.aid from b); }
step s1delb			{ delete from b where b.aid % 41 = 0; }
step s1f1			{ fetch next from d1; }
step s1f2			{ fetch forward 0 from d2; }
step s1f3			{ fetch first from d3; }
step s1f4			{ fetch forward 20 from d4; }
step s1f5			{ fetch absolute 20 from d5; }
step s1f6			{ fetch relative 20 from d6; }
step s1va			{ vacuum a; }
step s1vb			{ vacuum b; }

session s2
step s2b			{ begin isolation level read committed; }
step s2svpt         { savepoint f; }
step s2rollback     { rollback to f; }
step s2release		{ release savepoint f; }
step s2c            { commit; }
step s2d1			{ declare d1 cursor for select a.aid, substring(a.junk FROM 1 for 40) from a order by 1, 2; }
step s2d2			{ declare d2 cursor for select a.aid, substring(a.junk FROM 1 for 40) from a natural join b order by 1, 2; }
step s2d3			{ declare d3 cursor for select a.aid, substring(a.junk FROM 1 for 40) from a join b on a.aid = b.aid order by 1, 2; }
step s2d4			{ declare d4 cursor for select a.aid, substring(a.junk FROM 1 for 40) from a where a.aid % 37 = 0 order by 1, 2; }
step s2d5			{ declare d5 cursor for select a.aid, substring(a.junk FROM 1 for 40) from a natural join b where a.aid % 37 = 0 order by 1, 2; }
step s2d6			{ declare d6 cursor for select a.aid, substring(a.junk FROM 1 for 40) from a join b on a.aid = b.aid where a.aid % 37 = 0 order by 1, 2; }
step s2upa			{ update a set junk = mkjunk(aid+2) where a.aid % 37 = 0; }
step s2upb			{ update b set junk = mkjunk(bid+2) where b.bid % 37 = 0; }
step s2dela			{ delete from a where a.aid % 43 = 0 and a.aid not in (select b.aid from b); }
step s2delb			{ delete from b where b.aid % 43 = 0; }
step s2f1			{ fetch next from d1; }
step s2f2			{ fetch forward 0 from d2; }
step s2f3			{ fetch first from d3; }
step s2f4			{ fetch forward 20 from d4; }
step s2f5			{ fetch absolute 20 from d5; }
step s2f6			{ fetch relative 20 from d6; }
step s2va			{ vacuum a; }
step s2vb			{ vacuum b; }

permutation s1va s2b s1delb s2svpt s1b s1d1 s2upb s2d2 s1d5 s1f5 s2dela s1dela s2f2 s2d3 s2rollback s1svpt s2delb s1d3 s1d4 s1upb s1c s2c
permutation s2upb s2b s2d4 s2d2 s1upa s2d1 s2svpt s2delb s1dela s2d5 s1b s1d2 s2f2 s2f4 s2upa s1d3 s1upb s1d5 s1d4 s1delb s2c s1c
permutation s1va s1b s1d6 s2vb s1d2 s1d4 s1f4 s1f2 s1f6 s2b s1upa s1svpt s2d5 s2upa s1rollback s2d2 s1d3 s1d5 s2f2 s1c s2c
permutation s1b s1d3 s2va s1d4 s2delb s1f4 s1d2 s2b s1d5 s2d1 s2upa s2d5 s2dela s1upa s1svpt s1d1 s1dela s1upb s2f5 s1f1 s1release s2d6 s1c s2c
permutation s2vb s1b s2b s2d2 s1d4 s2upb s1upb s1svpt s2d3 s2d1 s1delb s1d6 s1d1 s1d5 s1dela s2upa s2d4 s2svpt s1d2 s2release s2dela s1c s2c
permutation s1b s2b s1d6 s1delb s1d1 s2d5 s2d2 s2d3 s2f5 s2dela s1d4 s2f2 s1upb s2c s1f6 s2delb s2va s1upa s2upa s2vb s1c
permutation s1vb s2dela s1b s2upa s1d1 s2b s1d4 s1svpt s1d6 s2delb s2d2 s1delb s1f6 s1upb s1f4 s2upb s1upa s1f1 s2d4 s1rollback s2d3 s1c s2c
permutation s2vb s1upa s1b s1d3 s1d1 s2b s1d6 s2upa s2upb s2svpt s2d3 s2delb s1d2 s2dela s1svpt s1release s1dela s2d1 s1f1 s1c s2c
permutation s2b s1b s1d3 s1svpt s1d4 s2d4 s1d6 s1rollback s1d1 s1f1 s2d6 s1d2 s2f6 s2d1 s1upb s2d2 s2f2 s1f2 s1upa s2f4 s1c s2c
permutation s2va s2upb s1b s2upa s1d3 s1d1 s1f1 s1svpt s1d6 s2b s1delb s1upa s1f6 s1d4 s2delb s2d2 s1release s2f2 s2d5 s1c s2c
permutation s1upa s2dela s1vb s1delb s2b s1va s2d1 s2d3 s2upb s1upb s2d5 s1dela s2svpt s2release s1b s1svpt s1release s2f1 s2d4 s1c s2c
permutation s2vb s1dela s1b s2dela s1d6 s1delb s1upa s2b s2d6 s2d2 s2delb s1svpt s1d5 s2upa s2d4 s1d3 s2f6 s2upb s1c s2d5 s2c
permutation s2va s1b s2b s2d6 s1upa s1svpt s2f6 s1delb s1dela s1release s2d1 s1upb s2d4 s1d4 s2delb s2svpt s2d5 s1d1 s1d6 s1f1 s1c s2c
permutation s1va s2vb s2b s1b s2d3 s2d2 s2f3 s1delb s2upa s1d6 s1d5 s2d1 s2f2 s1f6 s1f5 s1d4 s2d5 s1svpt s2f5 s1d2 s1c s2c
permutation s2upa s1va s1upa s1vb s2b s1b s2d1 s1d5 s2d6 s1f5 s2upb s1d2 s2svpt s2delb s2d3 s1dela s1c s2f1 s2release s1upb s2c
permutation s2b s2delb s2d2 s2svpt s1b s2release s1d4 s2upb s1svpt s1dela s2d3 s2d1 s2upa s1d2 s1rollback s2dela s2d4 s1upa s1upb s1c s2c
permutation s2dela s1dela s1b s1d1 s1delb s2b s1d6 s2upb s2d4 s1d5 s2d3 s1svpt s2d6 s2delb s1upa s2f3 s1upb s1release s1f6 s1c s2c
permutation s1b s1d6 s2upb s2b s2delb s2d6 s1svpt s2svpt s1dela s2rollback s2d3 s1d4 s1upa s2f6 s2c s1d1 s1upb s1d5 s1f6 s2vb s1c
permutation s1dela s2va s1delb s2vb s1b s2upb s2b s1d5 s1d2 s2d2 s2svpt s2d5 s2dela s2d4 s2release s2d6 s1upa s1upb s2f6 s1c s2c
permutation s2delb s2upa s1vb s1delb s2upb s2va s1va s1b s2b s1d1 s2dela s2d1 s1d5 s2d2 s1d2 s2svpt s2d3 s2release s1dela s2f2 s1upa s2c s1c
permutation s1delb s2b s1va s1b s1d5 s1d3 s1upa s2d1 s2d4 s2f1 s2d6 s2dela s2d3 s1upb s1svpt s1release s2svpt s1d6 s1d2 s2f6 s1c s2c
permutation s2upb s2upa s1upa s2b s2d3 s1b s2d5 s1d5 s1d4 s1dela s2d2 s1d6 s2d6 s1upb s1d1 s1d2 s1f2 s2d1 s1c s2f1 s2c
permutation s2dela s2b s1delb s1upa s2svpt s2upa s2d3 s1b s2f3 s2delb s1d6 s1dela s1d4 s2d4 s2release s1d1 s1svpt s1upb s1rollback s1c s2c
permutation s2upb s1upb s1delb s1dela s1b s2dela s2delb s1d3 s1d2 s2va s1d6 s2vb s2upa s2b s1d5 s2d6 s2f6 s2d3 s1svpt s1f6 s2d5 s1c s2c
permutation s2va s2b s2dela s1va s2d5 s2upa s2d3 s1upa s1b s2d6 s1upb s2d4 s2f3 s2f6 s2f5 s2d1 s2f1 s2d2 s1d4 s1c s2c
permutation s1dela s2b s2delb s1b s1d2 s1d6 s1d5 s1upb s2d1 s1delb s2d2 s2d5 s2f5 s1upa s1d4 s2upb s1svpt s2d3 s1d3 s1d1 s2c s1c
permutation s2b s2d5 s1b s1upb s1d3 s1d1 s1delb s2f5 s1f1 s2d1 s2d4 s2svpt s2c s1dela s1svpt s1release s1d6 s1d5 s2dela s1f6 s2upb s1c
permutation s1va s2dela s2va s2b s2delb s1dela s2svpt s1b s1d4 s1d2 s1d3 s1svpt s1d1 s1f2 s2d5 s2rollback s1d5 s1upb s2upb s1c s2c
permutation s1b s2vb s1d1 s2va s1f1 s1d6 s1d2 s1d4 s1upb s1delb s1d5 s1f4 s2delb s2upb s1dela s2dela s1f2 s1d3 s2b s1c s2c
permutation s1delb s1va s2vb s2dela s1vb s2va s1b s2delb s1upb s2upb s1d3 s2b s2d6 s1dela s2svpt s2rollback s2d4 s2d2 s2f2 s1c s2c
permutation s1va s2vb s2delb s1dela s1b s1d3 s1upb s2b s1upa s1f3 s2d4 s1d6 s2d2 s2d6 s1d2 s2f6 s1delb s1d5 s2d3 s1c s2c
permutation s1b s1delb s1svpt s1d3 s1d4 s1release s2upa s1d2 s2b s1upa s2d5 s2d6 s1d1 s1d6 s1f6 s2upb s2delb s2d1 s2d2 s1f4 s2f2 s1c s2c
permutation s1b s2vb s1d2 s1d4 s1d6 s2va s1dela s2b s2d5 s1f6 s1delb s2svpt s2release s2d3 s2upa s1f4 s1f2 s2d4 s1upa s1c s2c
permutation s2b s1upa s2d4 s2delb s2d1 s2svpt s1dela s1va s2f4 s2upb s2d2 s1upb s2upa s1b s1d3 s1d2 s2d3 s1d4 s1d6 s1f6 s1d1 s2d6 s1c s2c
permutation s1delb s2vb s1b s2dela s2b s1d5 s1upa s2d2 s2d4 s2d3 s1d3 s2d1 s2f1 s1upb s1svpt s1dela s2f2 s2d5 s1f5 s1f3 s2f5 s2upa s1rollback s2f3 s1c s2c
permutation s2upb s2b s1upa s2d1 s2d3 s2upa s2svpt s1upb s1b s1d6 s1dela s2d4 s1d2 s2d5 s1d1 s2release s2f5 s2dela s1d4 s1c s2c
permutation s2delb s2b s1dela s1delb s2d6 s2dela s1b s1d2 s2d4 s2d2 s1svpt s2d3 s1d5 s1f5 s1d3 s1upb s1d1 s2f2 s2d1 s2upa s1c s2c
permutation s1va s2upa s2vb s2delb s2va s2upb s1upa s2b s1b s2d3 s2svpt s2d5 s2d4 s2f4 s1d1 s2c s1d5 s1svpt s1delb s1f1 s1c
permutation s2b s1b s1svpt s1upa s1d5 s2d3 s2d5 s2d2 s1f5 s1delb s1rollback s2delb s2d6 s2f5 s2svpt s1d3 s2d1 s2dela s2f1 s2f3 s1c s2c
permutation s1b s1d6 s1svpt s2upa s2va s1f6 s1delb s2vb s1release s1d5 s1d2 s2b s2d5 s2d4 s1upa s2f4 s2svpt s2d6 s2dela s1d4 s2f5 s2d1 s1c s2c
permutation s1b s1upb s1d3 s1f3 s1d1 s1dela s2delb s1upa s2dela s2upa s1d6 s2vb s2b s1f1 s1d5 s2d3 s1d2 s2upb s1f2 s1c s2c
permutation s2upb s1upa s2b s2d4 s2d6 s2d1 s2f1 s2upa s2d5 s2d3 s2f6 s2dela s2f3 s2f4 s2delb s1va s1dela s2svpt s1b s1c s2c
permutation s2vb s2delb s1b s2va s1d2 s1d4 s2b s1d1 s2d2 s1f1 s1f4 s1d3 s1svpt s1f3 s1delb s1c s1dela s2d5 s1upa s2f2 s2c
permutation s2b s2d1 s1b s1upb s2d4 s1delb s1d5 s2dela s2d5 s2d2 s2d3 s2f2 s2f5 s2upa s1d4 s1svpt s1d1 s1upa s1rollback s1c s2c
permutation s1upa s2upa s1b s1d6 s2b s2upb s1delb s2d2 s1d3 s2d4 s2d3 s1svpt s2d1 s1dela s2delb s1d5 s2f3 s1release s1f5 s2f4 s1c s2c
permutation s2vb s1vb s2b s2upb s2d1 s1upa s2svpt s2upa s2d6 s1upb s2dela s1b s2delb s1d3 s2release s2d2 s2f1 s1d4 s1f4 s1delb s1c s2c
permutation s2b s1vb s1b s2svpt s2upa s2delb s2d6 s2upb s2release s1svpt s1d1 s2d2 s2d3 s2d1 s2f2 s2d4 s1d2 s2dela s1d6 s1upa s1release s1f1 s2f3 s2d5 s1c s2c
permutation s1vb s2b s2d2 s2upa s2svpt s1va s1delb s2dela s2d3 s1b s1d3 s1dela s2d6 s2delb s2f2 s1d2 s1d4 s2f3 s1upa s1d5 s1upb s1c s2release s2c
permutation s2vb s2dela s2upa s2va s2upb s1vb s1va s1b s1delb s2b s2d5 s2d6 s2svpt s1d3 s1d5 s2d3 s1d4 s2f5 s2release s1c s2c
permutation s1b s1d2 s2delb s1f2 s2dela s1svpt s1d5 s1d4 s1d1 s1d6 s1dela s1f4 s2b s2d4 s2f4 s2upa s1upa s2d3 s2svpt s1c s2d2 s2c
permutation s2upb s1upb s2b s1vb s2d3 s1dela s2d4 s2f4 s2d1 s2svpt s1upa s1b s1d6 s2rollback s1d4 s1f4 s2dela s2f1 s2c s1d2 s1c
permutation s1b s1upa s1delb s2upb s1d1 s2b s1upb s2d6 s1d2 s2svpt s1svpt s2dela s2d3 s2f3 s1d5 s1f5 s1d3 s1f1 s2d2 s2d4 s1c s2c
permutation s1upa s2vb s2b s2d5 s1dela s1va s2upb s2svpt s1upb s1delb s1b s2rollback s2delb s2d4 s2d2 s1d1 s1d2 s2d3 s1f1 s1c s2c
permutation s1b s1d5 s2b s1svpt s1d2 s2d4 s2upb s1rollback s2d6 s1dela s2d1 s2d5 s1d6 s2svpt s1upa s2f6 s2d2 s1f5 s2upa s1d1 s1c s2c
permutation s2va s2b s1dela s2dela s1vb s2d2 s1delb s2svpt s2rollback s2d1 s2d4 s2upb s2d3 s2f2 s2delb s1va s2upa s2f1 s1upa s2d6 s2c
permutation s1upb s2b s1va s2d5 s2d6 s2dela s2f6 s2d4 s2upa s1b s2d2 s1upa s1delb s2d3 s2d1 s1d2 s2f1 s1d5 s2f2 s1f5 s1c s2c
permutation s1b s2upa s1d6 s1upa s1svpt s1d4 s2b s1d1 s2d2 s1d5 s1d2 s1f1 s2d6 s1upb s2upb s1f2 s1delb s1f4 s1d3 s1c s2c
permutation s1delb s1dela s1vb s2b s1b s2d1 s2d6 s2d2 s2f1 s1d5 s1d1 s2d5 s1f1 s1upa s1d4 s2dela s2d3 s2d4 s1d6 s1c s2c
permutation s1delb s1b s2upa s1svpt s2dela s2delb s1d3 s1dela s1d5 s2va s1d6 s1d1 s1rollback s1f1 s1upa s2b s2upb s1d4 s1f4 s1c s2c
permutation s2b s1upa s1vb s1b s1d5 s1f5 s1d6 s2d1 s2d3 s2d5 s1d3 s1d2 s2f1 s2f5 s1svpt s1f6 s1d1 s1f3 s2d6 s2d2 s1c s2c
permutation s1va s1b s1d6 s2b s2d4 s2delb s1upb s1f6 s2upb s1d5 s1upa s2upa s1svpt s2d5 s2f4 s1d2 s2dela s2svpt s2release s1c s2c
permutation s1vb s1va s1b s1d2 s2upa s1d1 s1delb s1f2 s2b s2d1 s2d6 s1d5 s1d6 s2f6 s2d4 s2upb s1d4 s1upb s1c s2f1 s2c
permutation s2va s2upb s1b s2b s1d2 s1d5 s2d6 s2d5 s1d6 s2d3 s1dela s1d3 s1upa s2f3 s2f5 s1d4 s1c s2dela s2f6 s2d2 s2c
permutation s2upa s1b s1d3 s2b s1d4 s2upb s1d6 s1d5 s2delb s2d3 s1f5 s1delb s2d1 s1f6 s2d6 s1upa s1dela s1svpt s2d4 s1d2 s1c s2c
permutation s2vb s2b s2d5 s2dela s2d1 s1b s1d6 s1d2 s2d2 s2d3 s2f3 s1d3 s1dela s1d4 s1upb s2delb s1f4 s2f1 s1svpt s1c s2c
permutation s1vb s2delb s1b s2b s1delb s1d5 s2d4 s1d4 s2d3 s1d3 s1d1 s2d2 s2f4 s2f3 s1f5 s1f3 s2upa s1f1 s1f4 s1svpt s1upb s1c s2c
permutation s2b s1b s2d5 s2delb s1d3 s2d4 s2d1 s2dela s2upb s2d3 s1dela s1f3 s1delb s2f1 s2d6 s2upa s2f4 s2f3 s2c s1svpt s1c
permutation s2b s2d2 s1upb s2upb s2d5 s2d3 s2dela s1b s1d6 s1delb s1upa s1f6 s2d6 s2d1 s2f5 s2svpt s2c s2upa s2delb s1d5 s1c
permutation s1va s1delb s1b s1d3 s1d6 s1d1 s1f6 s2b s1svpt s2d5 s2svpt s1rollback s2release s2c s1d2 s1d4 s2delb s1upb s1f3 s1f1 s1c
permutation s1dela s2delb s2va s1delb s1b s2upa s1d4 s2upb s1upb s1svpt s2b s2d4 s1d2 s2svpt s2dela s1f2 s1d1 s2rollback s2d3 s1c s2c
permutation s2b s2svpt s2d1 s2f1 s1va s1dela s2d3 s1upb s2release s1b s1delb s2d5 s2d4 s2dela s2upa s2f5 s1d5 s1d3 s1f5 s1c s2c
permutation s1delb s1va s2vb s1upb s1b s2b s2d3 s1d2 s2delb s1svpt s2d5 s1d3 s1upa s1rollback s2upa s1d1 s2f5 s2d4 s1f1 s2f4 s1c s2c
permutation s2delb s1b s1svpt s1d3 s1d2 s2vb s1f3 s2b s1d6 s2d5 s1upa s2upa s2d2 s1release s2d6 s2d1 s1upb s2d3 s2f1 s2f5 s1c s1delb s2f6 s2c
permutation s1va s2upa s1b s1d3 s2b s2d1 s2d6 s1svpt s1upa s2f1 s1d2 s1d5 s2d4 s1f5 s1d1 s1d6 s2d2 s1f3 s1rollback s1c s2c
permutation s1b s2upa s2b s1delb s1d5 s2dela s1d6 s2delb s2d6 s1d1 s2d3 s1dela s2d2 s2f3 s1f1 s1d2 s2d4 s2f6 s2d5 s2svpt s1f6 s1f2 s2f2 s1d3 s1c s2c
permutation s1va s1b s2b s1d5 s1upa s1f5 s1svpt s2d4 s1release s1d6 s1d3 s1dela s2d5 s2delb s1d2 s2svpt s2d3 s2c s2va s2dela s1c
permutation s1delb s1vb s2b s1upb s2dela s2d3 s1upa s2d2 s2d1 s2svpt s2upb s2d4 s1b s1d4 s1d1 s2f4 s2f2 s1d6 s2f3 s2f1 s1c s2c
permutation s1delb s1upa s1upb s2va s2dela s1vb s1b s1svpt s2b s1d3 s2upb s2d5 s2svpt s1rollback s2d3 s2d4 s1f3 s2d6 s1d5 s1d6 s2d1 s1c s2c
permutation s1delb s2upb s2dela s1b s1svpt s1d1 s1d6 s1release s1d5 s1d2 s2va s1d3 s2b s2d6 s1upb s1dela s2d3 s2upa s2f3 s1c s2c
permutation s2b s1dela s2upb s2d3 s1upb s1b s1d6 s2d2 s1d3 s1d5 s2d4 s1svpt s2svpt s2d5 s2f3 s1d4 s1delb s2f4 s1rollback s1c s2c
permutation s2va s2b s1va s2d3 s1b s1d1 s1d3 s1dela s2d6 s1f1 s2f6 s1d4 s1d2 s2upb s1f3 s2d1 s1f4 s2svpt s2d4 s2f3 s1c s2c
permutation s2delb s2va s2vb s2dela s1upb s1dela s2upa s1va s2b s2upb s1b s1d4 s2d1 s2d3 s2svpt s1f4 s1d2 s1upa s1f2 s2d5 s1c s2c
permutation s1delb s2delb s2vb s1va s2dela s1upb s2b s2d6 s1b s2d3 s1upa s2f3 s2svpt s2f6 s2release s1d5 s1d4 s2d5 s2upa s1c s2c
permutation s1b s1delb s1upa s1d3 s1f3 s2upb s2delb s1upb s2vb s2b s2d3 s1svpt s1rollback s2d2 s2f2 s2d4 s2d1 s2dela s1d4 s2f1 s1c s2c
permutation s2upa s1dela s2b s2d3 s1vb s1b s2dela s2d2 s1upb s1d5 s2delb s2svpt s1d2 s2f2 s2d4 s2d6 s1upa s2f4 s2upb s1svpt s1c s2c
permutation s1b s2delb s1d4 s1d2 s1d6 s1upa s1d1 s1dela s1f2 s2b s1d3 s2d4 s1delb s1f1 s2svpt s2release s2d1 s2upa s2f4 s1c s2c
permutation s1upb s1upa s1vb s1b s2b s2d2 s1dela s2d4 s2f2 s1delb s2d3 s1d2 s1f2 s2upa s1d6 s1f6 s2c s2dela s1d4 s2upb s1c
permutation s2upa s1upb s2upb s1va s1upa s1b s2b s2delb s1d4 s2d5 s1svpt s2dela s2d6 s1d5 s1d2 s1f5 s2d3 s1release s1delb s1c s2c
permutation s2upb s2delb s1b s2vb s2b s1d4 s1d6 s1upb s1svpt s1d5 s1dela s2d4 s2svpt s2release s2d2 s1upa s1d2 s2c s1d3 s1c s2dela s2upa s2va
permutation s2dela s1b s1upb s1d3 s2b s1d1 s1svpt s2upb s1d6 s2d4 s2d5 s1rollback s2d6 s2d1 s1dela s2f1 s1f6 s1f3 s1f1 s2delb s2f5 s1d2 s1d4 s1c s2c
permutation s2vb s1b s1d3 s1d5 s2b s1dela s1svpt s2d4 s1f5 s1release s1upa s2f4 s2d5 s2upb s1d1 s1f3 s1f1 s1c s1vb s2d2 s2c
permutation s1b s1dela s1upa s1d1 s2b s1upb s2d4 s2d6 s2upb s1d4 s1d3 s2d2 s2f6 s1d5 s2d5 s2f4 s1d6 s2svpt s2release s1c s2c
permutation s1delb s1upa s2b s2d6 s2upb s2upa s2svpt s2d4 s1b s2f6 s1d4 s1d3 s2dela s2rollback s1d6 s2f4 s1d5 s1f6 s2d5 s1c s2c
permutation s2upa s2delb s1b s1d3 s1d1 s2va s1d5 s1delb s1f3 s1svpt s1upb s1d2 s2b s2c s1d4 s2vb s1d6 s2dela s1f2 s1upa s1c
permutation s2b s2d5 s1b s1svpt s2d6 s2f5 s1d3 s1d5 s2svpt s2release s1d6 s2d2 s1f6 s2f6 s2upb s2d3 s1delb s1d1 s2f2 s2c s1c
permutation s1upb s2upb s2dela s2b s1b s2d3 s2d1 s1d1 s1d4 s1d5 s2d6 s2svpt s2rollback s2d5 s2f3 s1d2 s2f6 s1upa s2delb s2f1 s2upa s1c s2c
permutation s1va s2b s1upb s1upa s1dela s2delb s2upa s2d5 s2svpt s1b s1d4 s2d3 s2rollback s2upb s2f3 s2dela s1d5 s2d2 s1d3 s1f5 s1c s2c
permutation s1b s2upa s1upa s2vb s1d5 s2b s2dela s1d1 s2d1 s2upb s1f5 s2d6 s1dela s2d4 s2c s1upb s1d4 s1delb s1svpt s2delb s2va s1c
permutation s2upb s1b s1d4 s1d2 s1dela s1delb s2b s2d5 s1d6 s2dela s1d5 s1upa s1upb s2f5 s1svpt s2upa s1f4 s1f5 s1f2 s1c s2c
permutation s1upb s2dela s2upb s2vb s2delb s1dela s2upa s1b s2b s2d3 s2d1 s2d2 s1d4 s1svpt s1delb s1d2 s1d5 s2d5 s2f5 s1c s2c
