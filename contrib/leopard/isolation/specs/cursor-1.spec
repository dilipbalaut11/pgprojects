setup
{
	CREATE SEQUENCE tbl_seq;
	CREATE TABLE tbl (
		a int NOT NULL PRIMARY KEY DEFAULT NEXTVAL('tbl_seq'),
		b int NOT NULL,
		c int NOT NULL
	) USING leopard;
	CREATE SEQUENCE s1seq;
	CREATE SEQUENCE s2seq;
	CREATE SEQUENCE s3seq;
}

teardown
{
	DROP TABLE tbl;
	DROP SEQUENCE tbl_seq, s1seq, s2seq, s3seq;
}

session s1
step s1begin			{ BEGIN; }
step s1abort			{ ROLLBACK; }
step s1commit			{ COMMIT; }
step s1a_sel_dec		{ DECLARE s1a_sel INSENSITIVE SCROLL CURSOR WITH HOLD FOR SELECT a, b, c FROM tbl; }
step s1b_sel_dec		{ DECLARE s1b_sel INSENSITIVE SCROLL CURSOR WITHOUT HOLD FOR SELECT a, b, c FROM tbl; }
step s1c_sel_dec		{ DECLARE s1c_sel INSENSITIVE NO SCROLL CURSOR WITH HOLD FOR SELECT a, b, c FROM tbl; }
step s1d_sel_dec		{ DECLARE s1d_sel INSENSITIVE NO SCROLL CURSOR WITHOUT HOLD FOR SELECT a, b, c FROM tbl; }
step s1a_sel_end		{ CLOSE s1a_sel; }
step s1b_sel_end		{ CLOSE s1b_sel; }
step s1c_sel_end		{ CLOSE s1c_sel; }
step s1d_sel_end		{ CLOSE s1d_sel; }
step s1_insert			{ INSERT INTO tbl (b, c) (SELECT 1, NEXTVAL('s1seq') FROM generate_series(1,10) gs); }
step s1a_sel_next		{ FETCH NEXT FROM s1a_sel; }
step s1b_sel_next		{ FETCH NEXT FROM s1b_sel; }
step s1c_sel_next		{ FETCH NEXT FROM s1c_sel; }
step s1d_sel_next		{ FETCH NEXT FROM s1d_sel; }
step s1a_sel_prior		{ FETCH PRIOR FROM s1a_sel; }
step s1b_sel_prior		{ FETCH PRIOR FROM s1b_sel; }
step s1a_sel_forward	{ FETCH FORWARD ALL FROM s1a_sel; }
step s1b_sel_forward	{ FETCH FORWARD ALL FROM s1b_sel; }
step s1c_sel_forward	{ FETCH FORWARD ALL FROM s1c_sel; }
step s1d_sel_forward	{ FETCH FORWARD ALL FROM s1d_sel; }
step s1a_sel_backward	{ FETCH BACKWARD ALL FROM s1a_sel; }
step s1b_sel_backward	{ FETCH BACKWARD ALL FROM s1b_sel; }

session s2
step s2begin			{ BEGIN; }
step s2abort			{ ROLLBACK; }
step s2commit			{ COMMIT; }
step s2a_sel_dec		{ DECLARE s2a_sel INSENSITIVE SCROLL CURSOR WITH HOLD FOR SELECT a, b, c FROM tbl; }
step s2b_sel_dec		{ DECLARE s2b_sel INSENSITIVE SCROLL CURSOR WITHOUT HOLD FOR SELECT a, b, c FROM tbl; }
step s2c_sel_dec		{ DECLARE s2c_sel INSENSITIVE NO SCROLL CURSOR WITH HOLD FOR SELECT a, b, c FROM tbl; }
step s2d_sel_dec		{ DECLARE s2d_sel INSENSITIVE NO SCROLL CURSOR WITHOUT HOLD FOR SELECT a, b, c FROM tbl; }
step s2a_sel_end		{ CLOSE s2a_sel; }
step s2b_sel_end		{ CLOSE s2b_sel; }
step s2c_sel_end		{ CLOSE s2c_sel; }
step s2d_sel_end		{ CLOSE s2d_sel; }
step s2_insert			{ INSERT INTO tbl (b, c) (SELECT 2, NEXTVAL('s2seq') FROM generate_series(2,20) gs); }
step s2a_sel_next		{ FETCH NEXT FROM s2a_sel; }
step s2b_sel_next		{ FETCH NEXT FROM s2b_sel; }
step s2c_sel_next		{ FETCH NEXT FROM s2c_sel; }
step s2d_sel_next		{ FETCH NEXT FROM s2d_sel; }
step s2a_sel_prior		{ FETCH PRIOR FROM s2a_sel; }
step s2b_sel_prior		{ FETCH PRIOR FROM s2b_sel; }
step s2a_sel_forward	{ FETCH FORWARD ALL FROM s2a_sel; }
step s2b_sel_forward	{ FETCH FORWARD ALL FROM s2b_sel; }
step s2c_sel_forward	{ FETCH FORWARD ALL FROM s2c_sel; }
step s2d_sel_forward	{ FETCH FORWARD ALL FROM s2d_sel; }
step s2a_sel_backward	{ FETCH BACKWARD ALL FROM s2a_sel; }
step s2b_sel_backward	{ FETCH BACKWARD ALL FROM s2b_sel; }

session s3
step s3begin			{ BEGIN; }
step s3abort			{ ROLLBACK; }
step s3commit			{ COMMIT; }
step s3a_sel_dec		{ DECLARE s3a_sel INSENSITIVE SCROLL CURSOR WITH HOLD FOR SELECT a, b, c FROM tbl; }
step s3b_sel_dec		{ DECLARE s3b_sel INSENSITIVE SCROLL CURSOR WITHOUT HOLD FOR SELECT a, b, c FROM tbl; }
step s3c_sel_dec		{ DECLARE s3c_sel INSENSITIVE NO SCROLL CURSOR WITH HOLD FOR SELECT a, b, c FROM tbl; }
step s3d_sel_dec		{ DECLARE s3d_sel INSENSITIVE NO SCROLL CURSOR WITHOUT HOLD FOR SELECT a, b, c FROM tbl; }
step s3a_sel_end		{ CLOSE s3a_sel; }
step s3b_sel_end		{ CLOSE s3b_sel; }
step s3c_sel_end		{ CLOSE s3c_sel; }
step s3d_sel_end		{ CLOSE s3d_sel; }
step s3_insert			{ INSERT INTO tbl (b, c) (SELECT 3, NEXTVAL('s3seq') FROM generate_series(3,30) gs); }
step s3a_sel_next		{ FETCH NEXT FROM s3a_sel; }
step s3b_sel_next		{ FETCH NEXT FROM s3b_sel; }
step s3c_sel_next		{ FETCH NEXT FROM s3c_sel; }
step s3d_sel_next		{ FETCH NEXT FROM s3d_sel; }
step s3a_sel_prior		{ FETCH PRIOR FROM s3a_sel; }
step s3b_sel_prior		{ FETCH PRIOR FROM s3b_sel; }
step s3a_sel_forward	{ FETCH FORWARD ALL FROM s3a_sel; }
step s3b_sel_forward	{ FETCH FORWARD ALL FROM s3b_sel; }
step s3c_sel_forward	{ FETCH FORWARD ALL FROM s3c_sel; }
step s3d_sel_forward	{ FETCH FORWARD ALL FROM s3d_sel; }
step s3a_sel_backward	{ FETCH BACKWARD ALL FROM s3a_sel; }
step s3b_sel_backward	{ FETCH BACKWARD ALL FROM s3b_sel; }

permutation
	 s1_insert s2_insert s2_insert s2_insert s1_insert s2_insert s1begin s1c_sel_dec s1a_sel_dec s1c_sel_end
	 s2_insert s2_insert s1c_sel_dec s1commit s2begin s1_insert s1c_sel_next s2b_sel_dec s1a_sel_next s1a_sel_prior
	 s1_insert s2_insert s2b_sel_prior s2a_sel_dec s1c_sel_next s1c_sel_forward s1_insert s2a_sel_forward s3begin s2a_sel_prior
	 s2b_sel_end s2a_sel_end s1c_sel_end s1a_sel_end s3abort s2commit
permutation
	 s1_insert s1_insert s3_insert s3_insert s3_insert s3begin s2begin s1_insert s1begin s3commit
	 s2d_sel_dec s1b_sel_dec s2c_sel_dec s2_insert s2abort s1a_sel_dec s2c_sel_next s1a_sel_end s1abort s2d_sel_forward
	 s1b_sel_prior s1b_sel_end s2c_sel_next s2c_sel_end s2d_sel_forward s1_insert s1begin s1d_sel_dec s2begin s1a_sel_dec
	 s1d_sel_end s1a_sel_end s2d_sel_end s2commit s1abort
permutation
	 s1_insert s1begin s1d_sel_dec s1c_sel_dec s1c_sel_forward s1b_sel_dec s1d_sel_end s1b_sel_end s2begin s3begin
	 s3d_sel_dec s2commit s1c_sel_forward s1abort s1c_sel_forward s1c_sel_forward s3c_sel_dec s2_insert s1_insert s3c_sel_end
	 s3_insert s1begin s1_insert s3_insert s1d_sel_dec s1commit s3abort s2begin s2_insert s1begin
	 s1d_sel_end s1c_sel_end s3d_sel_end s2commit s1abort
permutation
	 s3begin s1_insert s2_insert s3a_sel_dec s3a_sel_end s3commit s1begin s1a_sel_dec s1abort s2_insert
	 s2_insert s2begin s1a_sel_backward s2b_sel_dec s3begin s3abort s2_insert s2b_sel_forward s2b_sel_end s1a_sel_end
	 s2abort s2_insert s3_insert s2_insert s1begin s1c_sel_dec s1c_sel_end s1a_sel_dec s2_insert s3_insert
	 s1a_sel_end s1commit
permutation
	 s3begin s2begin s2abort s1_insert s1begin s2begin s1_insert s3c_sel_dec s1b_sel_dec s3b_sel_dec
	 s3b_sel_forward s3b_sel_end s1_insert s3a_sel_dec s3abort s1d_sel_dec s1a_sel_dec s1b_sel_backward s3c_sel_forward s2c_sel_dec
	 s1abort s1a_sel_prior s1d_sel_forward s1b_sel_forward s3a_sel_prior s1d_sel_next s3a_sel_next s1b_sel_backward s2d_sel_dec s1begin
	 s1a_sel_end s2c_sel_end s2d_sel_end s3a_sel_end s3c_sel_end s1b_sel_end s1d_sel_end s2abort s1commit
permutation
	 s1_insert s1begin s1c_sel_dec s1abort s1c_sel_forward s3begin s1c_sel_next s2begin s3_insert s1c_sel_forward
	 s2c_sel_dec s3b_sel_dec s2c_sel_end s1c_sel_end s2d_sel_dec s3c_sel_dec s2_insert s3c_sel_end s3b_sel_prior s1_insert
	 s1begin s2a_sel_dec s1commit s2commit s3b_sel_backward s2d_sel_end s2a_sel_backward s2a_sel_backward s2_insert s3b_sel_backward
	 s2a_sel_end s3b_sel_end s3commit
permutation
	 s1_insert s2begin s3_insert s2commit s2_insert s1_insert s1begin s2begin s1_insert s1_insert
	 s2c_sel_dec s2c_sel_next s1d_sel_dec s1b_sel_dec s2_insert s3_insert s3_insert s1c_sel_dec s2_insert s2abort
	 s2c_sel_forward s2_insert s1b_sel_forward s1d_sel_next s1c_sel_end s1commit s2c_sel_forward s1b_sel_prior s3begin s1d_sel_next
	 s1b_sel_end s1d_sel_end s2c_sel_end s3commit
permutation
	 s3_insert s1_insert s1begin s1b_sel_dec s2_insert s1b_sel_end s3begin s3abort s1a_sel_dec s1abort
	 s3begin s1a_sel_end s1begin s1d_sel_dec s1d_sel_end s3a_sel_dec s3abort s3a_sel_forward s2begin s1b_sel_dec
	 s2abort s1_insert s2_insert s1b_sel_end s2_insert s1abort s3a_sel_forward s3a_sel_prior s2begin s1begin
	 s3a_sel_end s1commit s2commit
permutation
	 s3begin s3b_sel_dec s1_insert s3b_sel_next s3_insert s3a_sel_dec s3_insert s2begin s3commit s1begin
	 s2b_sel_dec s2b_sel_next s3b_sel_prior s3a_sel_forward s2c_sel_dec s1commit s3a_sel_forward s3b_sel_forward s2d_sel_dec s2b_sel_next
	 s2b_sel_end s3b_sel_end s1_insert s2abort s2d_sel_next s3a_sel_forward s3a_sel_prior s3_insert s3a_sel_prior s2c_sel_end
	 s2d_sel_end s3a_sel_end
permutation
	 s1_insert s2_insert s1_insert s2begin s2b_sel_dec s2b_sel_prior s2abort s3_insert s2b_sel_prior s3_insert
	 s2b_sel_forward s2b_sel_prior s2b_sel_prior s1begin s1d_sel_dec s2b_sel_prior s2b_sel_forward s1d_sel_next s1c_sel_dec s1c_sel_forward
	 s1_insert s1c_sel_next s1c_sel_forward s1b_sel_dec s2b_sel_forward s2begin s1d_sel_forward s1d_sel_end s1d_sel_dec s2b_sel_backward
	 s1c_sel_end s1d_sel_end s2b_sel_end s1b_sel_end s2abort s1commit
permutation
	 s2begin s1begin s2d_sel_dec s2d_sel_next s1b_sel_dec s2d_sel_next s1b_sel_next s1b_sel_forward s1c_sel_dec s2d_sel_forward
	 s1b_sel_backward s2abort s1b_sel_backward s1c_sel_forward s1_insert s1b_sel_next s2_insert s1a_sel_dec s1_insert s1c_sel_end
	 s1a_sel_forward s2begin s1c_sel_dec s1a_sel_end s1c_sel_next s2d_sel_forward s1abort s2_insert s2_insert s2commit
	 s1b_sel_end s1c_sel_end s2d_sel_end
permutation
	 s1_insert s3_insert s2begin s3_insert s1_insert s2abort s3_insert s1_insert s3begin s3abort
	 s2begin s2commit s2begin s2abort s2_insert s2begin s2d_sel_dec s2c_sel_dec s2commit s2c_sel_next
	 s2c_sel_forward s3begin s3b_sel_dec s2d_sel_next s2c_sel_end s1_insert s3a_sel_dec s1begin s2d_sel_next s1_insert
	 s3a_sel_end s3b_sel_end s2d_sel_end s1commit s3commit
permutation
	 s3_insert s2begin s3begin s3commit s2commit s1begin s1abort s3begin s2_insert s3commit
	 s3_insert s3begin s3d_sel_dec s3commit s3begin s3d_sel_next s1begin s3d_sel_end s2_insert s1abort
	 s3c_sel_dec s3_insert s3b_sel_dec s3b_sel_end s3a_sel_dec s3c_sel_end s1_insert s3_insert s3a_sel_forward s3abort
	 s3a_sel_end
permutation
	 s3begin s3c_sel_dec s3abort s3_insert s1_insert s2begin s3_insert s1begin s1a_sel_dec s2a_sel_dec
	 s2a_sel_backward s1a_sel_end s2a_sel_prior s2a_sel_prior s1c_sel_dec s2d_sel_dec s2a_sel_backward s3_insert s3c_sel_end s2_insert
	 s2a_sel_next s1c_sel_forward s2d_sel_next s1b_sel_dec s1c_sel_next s1c_sel_next s1abort s1b_sel_forward s2d_sel_next s1begin
	 s1b_sel_end s1c_sel_end s2a_sel_end s2d_sel_end s2abort s1commit
permutation
	 s3_insert s1begin s2_insert s2_insert s1a_sel_dec s1c_sel_dec s3begin s1d_sel_dec s1a_sel_end s1d_sel_next
	 s1c_sel_forward s3abort s1_insert s1commit s2begin s1d_sel_forward s1_insert s1_insert s1d_sel_forward s2abort
	 s1d_sel_end s2begin s2abort s1c_sel_end s2begin s2commit s1begin s3begin s3_insert s1a_sel_dec
	 s1a_sel_end s3abort s1commit
permutation
	 s2_insert s2_insert s2begin s2d_sel_dec s3_insert s3_insert s2c_sel_dec s2a_sel_dec s2c_sel_forward s2c_sel_forward
	 s2c_sel_end s3_insert s2commit s2a_sel_prior s1_insert s2begin s2d_sel_end s2a_sel_next s2a_sel_next s1begin
	 s3begin s2abort s3d_sel_dec s2_insert s1_insert s2a_sel_prior s2_insert s3b_sel_dec s3abort s2a_sel_prior
	 s3d_sel_end s2a_sel_end s3b_sel_end s1commit
permutation
	 s1_insert s1_insert s1begin s2_insert s3_insert s3_insert s2_insert s2begin s1d_sel_dec s1b_sel_dec
	 s1d_sel_end s1d_sel_dec s1d_sel_end s1abort s2abort s1_insert s2_insert s1b_sel_forward s3_insert s1b_sel_next
	 s1b_sel_prior s3begin s1b_sel_backward s3commit s1begin s1d_sel_dec s1b_sel_prior s2_insert s1a_sel_dec s1a_sel_backward
	 s1a_sel_end s1b_sel_end s1d_sel_end s1commit
permutation
	 s1begin s1commit s1begin s1_insert s1a_sel_dec s1a_sel_next s3_insert s2begin s1abort s2c_sel_dec
	 s2abort s1a_sel_end s2_insert s3_insert s1begin s1a_sel_dec s1b_sel_dec s1abort s1a_sel_forward s1begin
	 s2c_sel_end s3begin s1d_sel_dec s3a_sel_dec s1a_sel_forward s3a_sel_prior s3d_sel_dec s1b_sel_prior s1b_sel_end s1a_sel_backward
	 s1a_sel_end s3d_sel_end s3a_sel_end s1d_sel_end s3abort s1abort
permutation
	 s2_insert s3_insert s2begin s2d_sel_dec s2_insert s2a_sel_dec s2a_sel_backward s2_insert s2a_sel_backward s2d_sel_forward
	 s3begin s3a_sel_dec s3b_sel_dec s3abort s1_insert s1_insert s2_insert s3_insert s2commit s2a_sel_next
	 s3b_sel_backward s3b_sel_backward s3b_sel_next s3b_sel_next s2d_sel_next s2d_sel_next s3b_sel_prior s3a_sel_end s1_insert s3b_sel_backward
	 s2d_sel_end s2a_sel_end s3b_sel_end
permutation
	 s2_insert s3_insert s2_insert s3begin s3d_sel_dec s3commit s1_insert s3begin s1begin s3d_sel_forward
	 s1c_sel_dec s3d_sel_forward s3_insert s3d_sel_next s3c_sel_dec s3abort s3c_sel_end s1a_sel_dec s1c_sel_forward s1a_sel_next
	 s1c_sel_forward s3d_sel_end s1a_sel_forward s2begin s1_insert s1c_sel_forward s2abort s1_insert s1c_sel_forward s1a_sel_forward
	 s1a_sel_end s1c_sel_end s1commit
permutation
	 s1_insert s1begin s2begin s1b_sel_dec s1d_sel_dec s1_insert s2commit s1commit s1b_sel_next s2_insert
	 s1d_sel_forward s1b_sel_backward s1begin s1abort s2_insert s1b_sel_next s3_insert s1d_sel_end s1begin s1c_sel_dec
	 s1abort s1b_sel_next s1c_sel_forward s3begin s1c_sel_next s1b_sel_next s1begin s1commit s3commit s1_insert
	 s1c_sel_end s1b_sel_end
permutation
	 s3_insert s2begin s3begin s2_insert s3b_sel_dec s2b_sel_dec s3b_sel_end s2abort s2b_sel_prior s3commit
	 s2b_sel_end s1begin s1a_sel_dec s1a_sel_prior s1a_sel_backward s1a_sel_forward s1b_sel_dec s1_insert s1_insert s2begin
	 s2b_sel_dec s2c_sel_dec s1a_sel_prior s2c_sel_next s2b_sel_backward s1b_sel_next s1a_sel_backward s1d_sel_dec s2d_sel_dec s2c_sel_forward
	 s2d_sel_end s2c_sel_end s1a_sel_end s2b_sel_end s1d_sel_end s1b_sel_end s1commit s2abort
permutation
	 s1_insert s1_insert s3begin s3abort s2_insert s1_insert s3_insert s2begin s2_insert s3_insert
	 s3_insert s2d_sel_dec s2d_sel_forward s2a_sel_dec s2a_sel_backward s2b_sel_dec s1begin s2b_sel_next s1abort s2commit
	 s3begin s3c_sel_dec s2b_sel_prior s3c_sel_forward s3c_sel_next s2a_sel_end s3c_sel_forward s2b_sel_forward s2b_sel_forward s3c_sel_end
	 s2b_sel_end s2d_sel_end s3commit
permutation
	 s3_insert s2begin s2commit s3_insert s3_insert s1begin s1abort s2_insert s2begin s1_insert
	 s2d_sel_dec s2abort s2begin s2_insert s2commit s2d_sel_forward s1begin s1d_sel_dec s1d_sel_end s1c_sel_dec
	 s1a_sel_dec s1c_sel_end s1a_sel_forward s2d_sel_next s1d_sel_dec s1a_sel_prior s2d_sel_end s1d_sel_next s1abort s1d_sel_end
	 s1a_sel_end
permutation
	 s2begin s2a_sel_dec s2abort s2a_sel_backward s2a_sel_forward s3begin s1begin s2a_sel_end s2_insert s1a_sel_dec
	 s1a_sel_prior s3c_sel_dec s1_insert s3d_sel_dec s3d_sel_end s1a_sel_prior s3_insert s1a_sel_end s3abort s1commit
	 s3c_sel_end s2begin s2commit s3begin s3abort s1_insert s2_insert s2begin s3_insert s1_insert
	 s2commit
permutation
	 s1begin s1abort s3_insert s1_insert s2begin s3begin s2b_sel_dec s3b_sel_dec s1_insert s3d_sel_dec
	 s2b_sel_next s2b_sel_prior s2abort s2begin s2_insert s3b_sel_forward s3b_sel_forward s2b_sel_backward s2abort s3c_sel_dec
	 s3d_sel_end s3b_sel_backward s2b_sel_forward s3c_sel_forward s3abort s3b_sel_forward s3b_sel_prior s3b_sel_backward s2b_sel_backward s2b_sel_forward
	 s3b_sel_end s2b_sel_end s3c_sel_end
permutation
	 s2begin s2a_sel_dec s2a_sel_backward s2a_sel_forward s2_insert s2a_sel_backward s2d_sel_dec s3_insert s3_insert s2a_sel_next
	 s2d_sel_next s1begin s1_insert s2d_sel_end s2abort s3begin s1b_sel_dec s3abort s2a_sel_prior s3_insert
	 s2a_sel_forward s2a_sel_next s1b_sel_backward s2a_sel_next s1abort s2a_sel_backward s2a_sel_end s1b_sel_backward s3begin s3commit
	 s1b_sel_end
permutation
	 s3begin s3commit s3_insert s1_insert s1_insert s1_insert s2_insert s1_insert s2begin s1begin
	 s2abort s2_insert s3begin s3commit s1d_sel_dec s1abort s1d_sel_forward s1begin s2_insert s1d_sel_end
	 s1commit s3_insert s2begin s2_insert s3_insert s2a_sel_dec s2a_sel_end s2a_sel_dec s2a_sel_next s2c_sel_dec
	 s2a_sel_end s2c_sel_end s2abort
permutation
	 s1begin s3begin s3c_sel_dec s3d_sel_dec s3_insert s3c_sel_next s3d_sel_forward s3_insert s3c_sel_end s3d_sel_forward
	 s1b_sel_dec s1d_sel_dec s1d_sel_forward s1d_sel_forward s3c_sel_dec s3c_sel_end s1_insert s1b_sel_prior s1b_sel_next s1commit
	 s1b_sel_end s1d_sel_end s1_insert s3d_sel_next s3c_sel_dec s3a_sel_dec s2begin s3c_sel_next s3c_sel_forward s3commit
	 s3c_sel_end s3a_sel_end s3d_sel_end s2abort
permutation
	 s1_insert s2_insert s2_insert s2begin s2_insert s1begin s2c_sel_dec s2d_sel_dec s2d_sel_end s2c_sel_end
	 s1b_sel_dec s2d_sel_dec s2a_sel_dec s2d_sel_forward s2_insert s2b_sel_dec s2abort s2a_sel_next s2b_sel_backward s1b_sel_backward
	 s1b_sel_end s1abort s2d_sel_next s2b_sel_backward s1begin s2a_sel_backward s2d_sel_next s2b_sel_prior s1a_sel_dec s1b_sel_dec
	 s2b_sel_end s1b_sel_end s2a_sel_end s2d_sel_end s1a_sel_end s1commit
permutation
	 s3_insert s2begin s1_insert s2c_sel_dec s1_insert s2c_sel_forward s2a_sel_dec s2a_sel_backward s3begin s2a_sel_backward
	 s2abort s2_insert s1begin s2a_sel_forward s2a_sel_end s3_insert s2c_sel_next s3commit s1_insert s1abort
	 s2begin s2c_sel_forward s1_insert s2c_sel_next s2c_sel_end s2c_sel_dec s2abort s1begin s1b_sel_dec s1b_sel_backward
	 s1b_sel_end s2c_sel_end s1abort
permutation
	 s2_insert s3begin s3d_sel_dec s1_insert s2_insert s1begin s3commit s1_insert s3d_sel_forward s2begin
	 s1c_sel_dec s1a_sel_dec s2d_sel_dec s1b_sel_dec s2commit s1commit s2_insert s1b_sel_forward s3d_sel_end s3_insert
	 s1a_sel_forward s2d_sel_end s1a_sel_next s1_insert s1_insert s1b_sel_end s3_insert s1c_sel_next s1begin s1a_sel_next
	 s1c_sel_end s1a_sel_end s1commit
permutation
	 s2_insert s2begin s1_insert s3_insert s2c_sel_dec s3begin s2c_sel_end s1_insert s3a_sel_dec s1_insert
	 s3a_sel_end s3c_sel_dec s3c_sel_next s1_insert s3_insert s3c_sel_end s3c_sel_dec s3d_sel_dec s2b_sel_dec s2c_sel_dec
	 s3commit s3c_sel_end s2b_sel_backward s1begin s2d_sel_dec s2c_sel_end s3d_sel_next s3d_sel_next s3d_sel_end s1b_sel_dec
	 s2b_sel_end s1b_sel_end s2d_sel_end s2commit s1commit
permutation
	 s3_insert s2begin s2commit s3_insert s3begin s3_insert s3_insert s3commit s3_insert s3_insert
	 s2_insert s1begin s2_insert s1_insert s3begin s3commit s3_insert s1_insert s2_insert s2_insert
	 s1b_sel_dec s1a_sel_dec s1b_sel_forward s2begin s1_insert s1b_sel_forward s1abort s2commit s1begin s1b_sel_next
	 s1b_sel_end s1a_sel_end s1commit
permutation
	 s3begin s3c_sel_dec s3commit s3c_sel_forward s2_insert s3c_sel_forward s1_insert s3c_sel_next s2_insert s1begin
	 s2_insert s3begin s3a_sel_dec s3b_sel_dec s3b_sel_next s1b_sel_dec s1commit s3a_sel_next s3c_sel_end s3b_sel_end
	 s3a_sel_backward s1b_sel_forward s1b_sel_prior s2_insert s3_insert s2begin s3abort s1_insert s2commit s3a_sel_next
	 s3a_sel_end s1b_sel_end
permutation
	 s1_insert s2begin s3begin s3a_sel_dec s2a_sel_dec s3a_sel_backward s2a_sel_end s3a_sel_next s2abort s2begin
	 s3d_sel_dec s3a_sel_forward s3b_sel_dec s3d_sel_next s3d_sel_next s2a_sel_dec s1begin s3d_sel_end s3a_sel_backward s3commit
	 s3_insert s3b_sel_end s1d_sel_dec s3a_sel_end s2_insert s1d_sel_next s3_insert s3_insert s2a_sel_next s1d_sel_forward
	 s1d_sel_end s2a_sel_end s1commit s2abort
permutation
	 s1_insert s3_insert s3_insert s3begin s2_insert s1_insert s3commit s2_insert s1_insert s3begin
	 s3a_sel_dec s3abort s1_insert s2begin s3a_sel_forward s2b_sel_dec s3begin s3a_sel_prior s2b_sel_backward s3a_sel_end
	 s2abort s2b_sel_backward s1begin s1commit s1begin s2b_sel_forward s3c_sel_dec s2b_sel_forward s2begin s1b_sel_dec
	 s2b_sel_end s3c_sel_end s1b_sel_end s1abort s2abort s3commit
permutation
	 s1begin s1c_sel_dec s1a_sel_dec s1commit s1c_sel_next s1_insert s2begin s1a_sel_next s1a_sel_prior s1a_sel_forward
	 s2abort s1_insert s1c_sel_next s1a_sel_prior s1a_sel_backward s1a_sel_forward s3_insert s1a_sel_prior s2begin s1a_sel_backward
	 s1a_sel_end s2_insert s2d_sel_dec s3begin s1c_sel_end s2_insert s3abort s1_insert s2_insert s2abort
	 s2d_sel_end
permutation
	 s3_insert s1_insert s2_insert s1begin s3_insert s2_insert s3begin s1commit s3c_sel_dec s3a_sel_dec
	 s3a_sel_next s3d_sel_dec s3_insert s3d_sel_forward s3d_sel_end s2begin s2a_sel_dec s2d_sel_dec s2a_sel_next s2a_sel_forward
	 s3b_sel_dec s2_insert s3a_sel_backward s2abort s3d_sel_dec s2a_sel_backward s3b_sel_next s3d_sel_next s3c_sel_end s2a_sel_end
	 s3b_sel_end s3a_sel_end s3d_sel_end s2d_sel_end s3commit
permutation
	 s1begin s1a_sel_dec s1commit s3begin s1a_sel_backward s3b_sel_dec s1a_sel_end s3b_sel_prior s3commit s3begin
	 s3b_sel_backward s2begin s1_insert s2a_sel_dec s2abort s2a_sel_next s3b_sel_backward s3abort s2a_sel_backward s2a_sel_end
	 s3b_sel_forward s2_insert s3b_sel_backward s2_insert s3begin s1_insert s1begin s3commit s3b_sel_forward s1commit
	 s3b_sel_end
permutation
	 s2_insert s3_insert s3begin s3b_sel_dec s3_insert s1begin s3commit s3b_sel_prior s2_insert s3b_sel_prior
	 s3_insert s3b_sel_forward s3b_sel_end s1d_sel_dec s1a_sel_dec s2begin s1d_sel_forward s2c_sel_dec s2_insert s1a_sel_end
	 s2b_sel_dec s1d_sel_next s1a_sel_dec s1a_sel_end s1b_sel_dec s1b_sel_prior s2b_sel_next s1_insert s1b_sel_prior s1d_sel_forward
	 s1b_sel_end s1d_sel_end s2b_sel_end s2c_sel_end s2commit s1commit
permutation
	 s2_insert s2_insert s3_insert s1begin s3begin s2_insert s1a_sel_dec s3abort s1a_sel_end s1c_sel_dec
	 s3_insert s1b_sel_dec s3begin s3_insert s1b_sel_next s2_insert s1c_sel_forward s3d_sel_dec s3c_sel_dec s3d_sel_forward
	 s1c_sel_forward s3commit s1_insert s3c_sel_forward s3c_sel_end s1d_sel_dec s1b_sel_next s1c_sel_next s1b_sel_prior s1b_sel_backward
	 s1d_sel_end s1b_sel_end s1c_sel_end s3d_sel_end s1abort
permutation
	 s3begin s3b_sel_dec s1_insert s2_insert s3b_sel_forward s3c_sel_dec s3b_sel_prior s3c_sel_next s3abort s3b_sel_end
	 s2begin s2b_sel_dec s2c_sel_dec s3begin s3c_sel_end s2abort s2_insert s2c_sel_forward s3d_sel_dec s3d_sel_end
	 s2b_sel_backward s3a_sel_dec s3b_sel_dec s1begin s3d_sel_dec s1commit s2b_sel_backward s3b_sel_forward s2b_sel_next s3b_sel_end
	 s2b_sel_end s3a_sel_end s3d_sel_end s2c_sel_end s3abort
permutation
	 s2_insert s3_insert s3_insert s3_insert s1_insert s2_insert s1_insert s3_insert s1begin s3begin
	 s1b_sel_dec s1b_sel_forward s2_insert s1b_sel_prior s2_insert s1b_sel_next s1b_sel_next s2_insert s1_insert s1b_sel_prior
	 s1commit s3c_sel_dec s3commit s3c_sel_forward s1b_sel_backward s1b_sel_forward s1b_sel_backward s1b_sel_backward s3c_sel_end s1b_sel_end
permutation
	 s1_insert s3_insert s2_insert s2_insert s3begin s3c_sel_dec s3abort s2_insert s1_insert s3c_sel_forward
	 s2_insert s3begin s1_insert s3_insert s3b_sel_dec s1_insert s3a_sel_dec s2_insert s3_insert s3c_sel_forward
	 s3b_sel_end s3a_sel_end s3_insert s3abort s1_insert s1_insert s3c_sel_forward s2_insert s2begin s2commit
	 s3c_sel_end
permutation
	 s1begin s2_insert s1_insert s1d_sel_dec s1d_sel_end s1a_sel_dec s1commit s1a_sel_backward s1a_sel_end s1_insert
	 s1_insert s2begin s3_insert s1begin s2abort s1c_sel_dec s1abort s1c_sel_end s2begin s2c_sel_dec
	 s2commit s2c_sel_forward s3begin s3b_sel_dec s3d_sel_dec s3b_sel_backward s2c_sel_next s3b_sel_forward s3b_sel_forward s3_insert
	 s2c_sel_end s3d_sel_end s3b_sel_end s3commit
permutation
	 s2begin s2d_sel_dec s2d_sel_end s1begin s1commit s1_insert s2d_sel_dec s3_insert s1_insert s2d_sel_next
	 s2_insert s1_insert s2c_sel_dec s2c_sel_next s2abort s2c_sel_forward s2begin s2commit s2_insert s2d_sel_next
	 s1_insert s1_insert s1begin s2_insert s2c_sel_next s3_insert s3_insert s2_insert s2begin s2abort
	 s2d_sel_end s2c_sel_end s1abort
permutation
	 s1begin s1abort s1_insert s3begin s3a_sel_dec s1begin s3a_sel_prior s3a_sel_backward s2_insert s2begin
	 s3a_sel_backward s3_insert s1abort s3a_sel_prior s3c_sel_dec s2abort s1_insert s3d_sel_dec s3_insert s1_insert
	 s3d_sel_forward s2_insert s3d_sel_forward s3d_sel_end s2_insert s3_insert s3a_sel_next s3commit s1_insert s3a_sel_forward
	 s3c_sel_end s3a_sel_end
permutation
	 s1begin s3begin s2_insert s1a_sel_dec s1a_sel_end s1c_sel_dec s1c_sel_forward s2begin s1a_sel_dec s2b_sel_dec
	 s1b_sel_dec s1a_sel_end s1d_sel_dec s3_insert s1_insert s1_insert s2a_sel_dec s2a_sel_next s1_insert s3a_sel_dec
	 s3_insert s2b_sel_end s1c_sel_next s1c_sel_next s2b_sel_dec s1c_sel_end s2abort s2_insert s1d_sel_forward s1b_sel_end
	 s2a_sel_end s3a_sel_end s1d_sel_end s2b_sel_end s1abort s3commit
permutation
	 s3_insert s2begin s2b_sel_dec s2abort s2_insert s3_insert s3_insert s2b_sel_backward s2b_sel_forward s3_insert
	 s2b_sel_prior s2b_sel_prior s1_insert s1_insert s2_insert s1_insert s1begin s2b_sel_forward s2b_sel_forward s2b_sel_backward
	 s2_insert s1a_sel_dec s1a_sel_forward s1abort s1_insert s3begin s1_insert s3d_sel_dec s1a_sel_forward s3b_sel_dec
	 s1a_sel_end s3d_sel_end s3b_sel_end s2b_sel_end s3commit
permutation
	 s3begin s3b_sel_dec s1begin s1_insert s3b_sel_next s3b_sel_backward s1c_sel_dec s2_insert s3_insert s1_insert
	 s1c_sel_next s1commit s3b_sel_prior s3d_sel_dec s3b_sel_forward s1begin s1abort s1c_sel_forward s3c_sel_dec s3abort
	 s2begin s1c_sel_forward s1_insert s2a_sel_dec s3b_sel_prior s1begin s2a_sel_next s3b_sel_forward s2commit s2a_sel_end
	 s1c_sel_end s3d_sel_end s3c_sel_end s3b_sel_end s1abort
permutation
	 s3_insert s3begin s1begin s3_insert s3c_sel_dec s3c_sel_next s2_insert s1a_sel_dec s3c_sel_forward s3c_sel_end
	 s1a_sel_backward s1commit s2begin s2_insert s1a_sel_forward s2commit s1_insert s3c_sel_dec s1begin s3d_sel_dec
	 s1a_sel_end s3d_sel_next s3c_sel_end s1b_sel_dec s2_insert s1b_sel_forward s1a_sel_dec s1a_sel_end s1b_sel_forward s3d_sel_end
	 s1b_sel_end s3abort s1abort
permutation
	 s3_insert s3_insert s2begin s2commit s2begin s2commit s1_insert s2_insert s3_insert s3_insert
	 s1begin s1_insert s2begin s1a_sel_dec s1_insert s2abort s1commit s1begin s1b_sel_dec s1b_sel_forward
	 s1abort s2_insert s2begin s1a_sel_backward s1b_sel_forward s2a_sel_dec s2commit s1b_sel_next s2begin s1a_sel_forward
	 s1a_sel_end s2a_sel_end s1b_sel_end s2abort
permutation
	 s1begin s1a_sel_dec s1c_sel_dec s1c_sel_end s1a_sel_backward s1a_sel_backward s1commit s1a_sel_next s1_insert s1begin
	 s1c_sel_dec s1abort s1a_sel_end s3begin s1begin s1c_sel_end s3commit s1abort s2_insert s2_insert
	 s3begin s3c_sel_dec s3a_sel_dec s2begin s2c_sel_dec s3d_sel_dec s2d_sel_dec s2c_sel_forward s3a_sel_prior s3a_sel_end
	 s2d_sel_end s2c_sel_end s3d_sel_end s3c_sel_end s2commit s3commit
permutation
	 s1begin s1_insert s1abort s2_insert s2begin s2abort s3_insert s1_insert s3begin s3abort
	 s2begin s2d_sel_dec s2b_sel_dec s1begin s3begin s3_insert s3_insert s2b_sel_end s2a_sel_dec s2b_sel_dec
	 s2a_sel_prior s2b_sel_end s2a_sel_end s3c_sel_dec s1b_sel_dec s1b_sel_end s3b_sel_dec s2abort s3b_sel_next s2begin
	 s2d_sel_end s3c_sel_end s3b_sel_end s2commit s1abort s3commit
permutation
	 s2begin s2c_sel_dec s2b_sel_dec s1_insert s2_insert s2d_sel_dec s2commit s2b_sel_forward s1_insert s2d_sel_end
	 s2c_sel_next s2_insert s2b_sel_prior s2c_sel_forward s2b_sel_backward s2b_sel_prior s2c_sel_forward s2b_sel_backward s2c_sel_forward s2_insert
	 s1_insert s1_insert s3_insert s2begin s1_insert s2_insert s2commit s2b_sel_end s2c_sel_end s1begin
	 s1commit
permutation
	 s3begin s1_insert s3d_sel_dec s1begin s1c_sel_dec s1_insert s3abort s2begin s1c_sel_end s2abort
	 s2_insert s1d_sel_dec s3begin s3commit s3begin s3d_sel_end s3d_sel_dec s3abort s2begin s2_insert
	 s3d_sel_forward s1abort s1begin s1commit s3_insert s2c_sel_dec s3d_sel_next s3_insert s3d_sel_next s2c_sel_next
	 s1d_sel_end s3d_sel_end s2c_sel_end s2commit
permutation
	 s1_insert s3_insert s1begin s2_insert s1abort s1begin s1d_sel_dec s1d_sel_end s3_insert s1c_sel_dec
	 s3begin s3d_sel_dec s1commit s1begin s1c_sel_forward s3d_sel_next s1_insert s2begin s1commit s3a_sel_dec
	 s2commit s3d_sel_next s3d_sel_forward s1c_sel_next s3d_sel_end s3a_sel_end s1c_sel_end s1begin s3_insert s1c_sel_dec
	 s1c_sel_end s1commit s3commit
permutation
	 s3_insert s2begin s2c_sel_dec s2c_sel_forward s3begin s1begin s1b_sel_dec s3_insert s3c_sel_dec s2a_sel_dec
	 s2c_sel_forward s2a_sel_backward s2a_sel_forward s3c_sel_end s3d_sel_dec s1b_sel_forward s3commit s1b_sel_forward s1a_sel_dec s2b_sel_dec
	 s1b_sel_next s1a_sel_prior s3_insert s1b_sel_forward s1b_sel_end s2a_sel_end s1a_sel_prior s2b_sel_prior s2c_sel_forward s3begin
	 s2b_sel_end s1a_sel_end s2c_sel_end s3d_sel_end s3abort s2abort s1abort
permutation
	 s1begin s2_insert s3_insert s1c_sel_dec s1c_sel_forward s1b_sel_dec s3begin s1b_sel_forward s1b_sel_end s1c_sel_end
	 s3abort s3begin s3d_sel_dec s3commit s1d_sel_dec s1d_sel_end s1_insert s3d_sel_next s1d_sel_dec s1abort
	 s3begin s3b_sel_dec s3a_sel_dec s3b_sel_forward s3commit s3a_sel_prior s3begin s3a_sel_prior s3a_sel_forward s3d_sel_end
	 s3b_sel_end s1d_sel_end s3a_sel_end s3commit
permutation
	 s3_insert s2begin s2b_sel_dec s2commit s1_insert s2b_sel_forward s2b_sel_forward s1_insert s3_insert s1_insert
	 s1begin s1b_sel_dec s1commit s2b_sel_next s1b_sel_next s3begin s1b_sel_prior s3_insert s1begin s2b_sel_end
	 s3abort s1c_sel_dec s1b_sel_next s1c_sel_end s1b_sel_forward s1b_sel_next s1b_sel_next s1b_sel_next s1a_sel_dec s1commit
	 s1a_sel_end s1b_sel_end
permutation
	 s3begin s3a_sel_dec s3d_sel_dec s3b_sel_dec s3d_sel_end s3a_sel_forward s2_insert s3b_sel_end s3a_sel_forward s3a_sel_forward
	 s3c_sel_dec s3a_sel_forward s2begin s1_insert s3c_sel_end s2d_sel_dec s3a_sel_backward s3_insert s3a_sel_backward s2d_sel_forward
	 s2c_sel_dec s2d_sel_forward s1begin s3commit s2d_sel_end s2b_sel_dec s3a_sel_end s1c_sel_dec s1_insert s1commit
	 s1c_sel_end s2c_sel_end s2b_sel_end s2abort
permutation
	 s3begin s3d_sel_dec s3d_sel_forward s3d_sel_end s3commit s1begin s2begin s1c_sel_dec s2_insert s1_insert
	 s1c_sel_end s1commit s2commit s2_insert s2_insert s2begin s2_insert s2b_sel_dec s3_insert s1begin
	 s1c_sel_dec s2b_sel_end s1a_sel_dec s1abort s1a_sel_forward s1a_sel_prior s3_insert s1_insert s1_insert s1a_sel_next
	 s1c_sel_end s1a_sel_end s2abort
permutation
	 s3_insert s3_insert s1_insert s3begin s3commit s1_insert s1_insert s2begin s1_insert s1begin
	 s1abort s2abort s2begin s2c_sel_dec s2c_sel_forward s2a_sel_dec s3begin s2c_sel_forward s3b_sel_dec s2a_sel_forward
	 s2a_sel_prior s2_insert s3a_sel_dec s3a_sel_forward s2a_sel_end s2c_sel_next s3commit s1begin s1commit s1begin
	 s2c_sel_end s3a_sel_end s3b_sel_end s2commit s1abort
permutation
	 s1begin s1d_sel_dec s1d_sel_end s1d_sel_dec s2begin s1d_sel_end s3_insert s2d_sel_dec s1b_sel_dec s1c_sel_dec
	 s3_insert s1c_sel_forward s3_insert s1d_sel_dec s2d_sel_end s1commit s2c_sel_dec s2abort s2_insert s2c_sel_next
	 s1begin s1a_sel_dec s1b_sel_prior s1c_sel_end s2_insert s1_insert s2_insert s1b_sel_forward s1a_sel_backward s1b_sel_forward
	 s2c_sel_end s1a_sel_end s1d_sel_end s1b_sel_end s1abort
permutation
	 s2begin s2d_sel_dec s2c_sel_dec s2c_sel_next s3_insert s2d_sel_end s2c_sel_forward s1_insert s2c_sel_next s2c_sel_end
	 s2_insert s1_insert s2commit s3_insert s1_insert s3begin s3b_sel_dec s3b_sel_backward s3b_sel_next s3commit
	 s1_insert s3b_sel_end s1_insert s2begin s2c_sel_dec s3begin s2c_sel_forward s2c_sel_forward s2b_sel_dec s2d_sel_dec
	 s2c_sel_end s2d_sel_end s2b_sel_end s3abort s2commit
permutation
	 s2begin s2commit s2_insert s1begin s1b_sel_dec s1c_sel_dec s1a_sel_dec s1b_sel_next s1c_sel_end s1a_sel_next
	 s1b_sel_backward s1d_sel_dec s1d_sel_next s1b_sel_backward s1a_sel_next s1a_sel_backward s2_insert s1b_sel_backward s1abort s1b_sel_forward
	 s1a_sel_forward s2_insert s2begin s1a_sel_backward s1b_sel_next s2c_sel_dec s3begin s2d_sel_dec s2c_sel_end s1d_sel_forward
	 s1d_sel_end s1b_sel_end s2d_sel_end s1a_sel_end s3commit s2commit
permutation
	 s1begin s3_insert s2_insert s2begin s1a_sel_dec s1a_sel_end s2b_sel_dec s1abort s2commit s3begin
	 s2begin s3commit s3begin s2b_sel_forward s3abort s2a_sel_dec s2b_sel_forward s2a_sel_next s2d_sel_dec s2_insert
	 s2b_sel_next s2_insert s2c_sel_dec s2a_sel_backward s2b_sel_forward s2b_sel_forward s2a_sel_next s3_insert s2commit s1begin
	 s2b_sel_end s2a_sel_end s2c_sel_end s2d_sel_end s1commit
permutation
	 s3_insert s3begin s1_insert s3a_sel_dec s3abort s3a_sel_end s1begin s1commit s1begin s1a_sel_dec
	 s1a_sel_backward s1commit s1a_sel_forward s1begin s1a_sel_end s1b_sel_dec s3_insert s3begin s1b_sel_backward s2_insert
	 s3_insert s3abort s3_insert s1abort s1b_sel_next s1b_sel_prior s1b_sel_next s1b_sel_next s3begin s3b_sel_dec
	 s3b_sel_end s1b_sel_end s3abort
permutation
	 s3begin s3b_sel_dec s3b_sel_end s2_insert s3a_sel_dec s3a_sel_forward s3abort s2begin s3a_sel_next s1begin
	 s2b_sel_dec s3a_sel_next s3begin s3a_sel_prior s2b_sel_end s3b_sel_dec s3b_sel_forward s1abort s3a_sel_forward s3a_sel_prior
	 s3b_sel_prior s3b_sel_end s2abort s3commit s3_insert s3_insert s2_insert s2_insert s3begin s2begin
	 s3a_sel_end s3commit s2abort
permutation
	 s2_insert s3_insert s3begin s1begin s3b_sel_dec s1b_sel_dec s3a_sel_dec s1a_sel_dec s1commit s3b_sel_backward
	 s3abort s3a_sel_forward s3a_sel_prior s1b_sel_prior s1b_sel_forward s1begin s1d_sel_dec s1b_sel_prior s3b_sel_next s3a_sel_forward
	 s1b_sel_end s3b_sel_prior s3b_sel_next s1d_sel_end s1commit s3b_sel_forward s3b_sel_end s3a_sel_backward s3begin s1a_sel_forward
	 s1a_sel_end s3a_sel_end s3commit
permutation
	 s2begin s2a_sel_dec s3begin s3_insert s2commit s2a_sel_backward s3b_sel_dec s3abort s3b_sel_prior s3b_sel_backward
	 s3b_sel_forward s3b_sel_prior s3b_sel_forward s3b_sel_prior s3begin s3b_sel_next s3b_sel_backward s3b_sel_backward s1begin s3b_sel_backward
	 s3_insert s1_insert s2_insert s3b_sel_forward s1b_sel_dec s2begin s1c_sel_dec s1b_sel_prior s1b_sel_next s1b_sel_prior
	 s1b_sel_end s3b_sel_end s1c_sel_end s2a_sel_end s2commit s1commit s3commit
permutation
	 s2_insert s3begin s2begin s1begin s1_insert s3b_sel_dec s2abort s1commit s3_insert s2_insert
	 s1_insert s3_insert s3b_sel_next s2_insert s3b_sel_prior s1_insert s3b_sel_backward s3b_sel_next s3b_sel_forward s3d_sel_dec
	 s3a_sel_dec s3c_sel_dec s3abort s3b_sel_prior s2begin s3b_sel_prior s3c_sel_end s3d_sel_forward s3b_sel_forward s1begin
	 s3a_sel_end s3b_sel_end s3d_sel_end s1commit s2abort
permutation
	 s2begin s2abort s1begin s1b_sel_dec s3_insert s2begin s1b_sel_prior s2_insert s2b_sel_dec s2b_sel_end
	 s2a_sel_dec s1b_sel_prior s2a_sel_end s1d_sel_dec s1_insert s1b_sel_end s1d_sel_end s1_insert s2d_sel_dec s2d_sel_end
	 s2b_sel_dec s1c_sel_dec s2_insert s1_insert s1c_sel_forward s2c_sel_dec s1abort s1c_sel_next s2b_sel_end s2abort
	 s1c_sel_end s2c_sel_end
permutation
	 s3begin s3a_sel_dec s3_insert s3a_sel_next s1_insert s2begin s3a_sel_backward s1_insert s3c_sel_dec s2a_sel_dec
	 s3b_sel_dec s2c_sel_dec s2a_sel_next s3b_sel_backward s2c_sel_next s3commit s3begin s3b_sel_forward s3b_sel_end s3a_sel_forward
	 s2d_sel_dec s3commit s2_insert s2d_sel_forward s2d_sel_forward s2a_sel_backward s2a_sel_end s3c_sel_end s3begin s2b_sel_dec
	 s2d_sel_end s2c_sel_end s2b_sel_end s3a_sel_end s3abort s2abort
permutation
	 s2begin s2_insert s1begin s2c_sel_dec s1a_sel_dec s2c_sel_end s1commit s3_insert s1a_sel_backward s1begin
	 s1c_sel_dec s1a_sel_prior s2a_sel_dec s1a_sel_end s2abort s1c_sel_end s1c_sel_dec s1_insert s1_insert s1commit
	 s2a_sel_next s2begin s2abort s2begin s2c_sel_dec s1c_sel_end s2a_sel_next s2a_sel_backward s2a_sel_prior s2a_sel_end
	 s2c_sel_end s2commit
permutation
	 s2_insert s1begin s1d_sel_dec s3_insert s2_insert s1b_sel_dec s1d_sel_end s1b_sel_end s2_insert s1a_sel_dec
	 s3begin s1commit s1a_sel_prior s2begin s3abort s2d_sel_dec s2b_sel_dec s1a_sel_next s2a_sel_dec s1a_sel_prior
	 s2d_sel_next s2b_sel_end s2d_sel_end s1a_sel_prior s1a_sel_end s2d_sel_dec s2_insert s3begin s2a_sel_forward s3_insert
	 s2d_sel_end s2a_sel_end s2commit s3commit
permutation
	 s3begin s3_insert s3commit s1_insert s1begin s1b_sel_dec s1d_sel_dec s1b_sel_end s1a_sel_dec s1a_sel_next
	 s2_insert s1a_sel_backward s1a_sel_prior s1_insert s1a_sel_backward s1d_sel_end s1b_sel_dec s1b_sel_forward s1commit s1b_sel_forward
	 s1b_sel_backward s1_insert s3begin s3abort s3_insert s3begin s1b_sel_backward s1b_sel_end s3abort s1_insert
	 s1a_sel_end
permutation
	 s3_insert s2begin s2commit s1_insert s2_insert s1begin s1c_sel_dec s1b_sel_dec s1_insert s2begin
	 s1b_sel_prior s2abort s3_insert s1b_sel_next s1b_sel_end s3begin s2_insert s3d_sel_dec s2begin s3d_sel_forward
	 s2a_sel_dec s3_insert s2c_sel_dec s1c_sel_end s2_insert s1a_sel_dec s2_insert s2d_sel_dec s3d_sel_next s2_insert
	 s3d_sel_end s2d_sel_end s2c_sel_end s2a_sel_end s1a_sel_end s3abort s1abort s2commit
permutation
	 s2begin s2a_sel_dec s2b_sel_dec s2c_sel_dec s2a_sel_next s2b_sel_next s2a_sel_prior s3_insert s2c_sel_next s2commit
	 s2b_sel_next s2a_sel_forward s2a_sel_next s1begin s2a_sel_prior s3_insert s2a_sel_next s2b_sel_backward s2a_sel_forward s2c_sel_forward
	 s2a_sel_prior s2b_sel_prior s2a_sel_forward s2c_sel_end s1commit s2begin s1begin s2a_sel_forward s2b_sel_backward s2abort
	 s2b_sel_end s2a_sel_end s1commit
permutation
	 s1begin s1c_sel_dec s3begin s2_insert s3commit s3_insert s1a_sel_dec s1c_sel_forward s3_insert s1a_sel_next
	 s3begin s1abort s3commit s1begin s1c_sel_end s2begin s2c_sel_dec s1d_sel_dec s2c_sel_forward s1a_sel_backward
	 s1c_sel_dec s2c_sel_next s1c_sel_end s1_insert s3begin s1a_sel_next s2c_sel_next s2d_sel_dec s3_insert s2c_sel_forward
	 s2d_sel_end s2c_sel_end s1a_sel_end s1d_sel_end s3commit s2abort s1commit
permutation
	 s3begin s3b_sel_dec s3b_sel_end s3_insert s2begin s3d_sel_dec s2abort s3_insert s3_insert s3a_sel_dec
	 s2_insert s2_insert s3a_sel_next s3_insert s3_insert s3a_sel_prior s2begin s3_insert s2b_sel_dec s3b_sel_dec
	 s1begin s1commit s3a_sel_backward s2b_sel_next s2commit s3a_sel_prior s3a_sel_prior s3abort s2begin s3d_sel_forward
	 s3d_sel_end s3a_sel_end s2b_sel_end s3b_sel_end s2commit
permutation
	 s3_insert s1_insert s2begin s3begin s3commit s2abort s2_insert s3begin s3_insert s3a_sel_dec
	 s3a_sel_end s3a_sel_dec s3d_sel_dec s2begin s2_insert s2commit s2begin s3abort s3d_sel_next s2d_sel_dec
	 s2_insert s3a_sel_end s3d_sel_end s2c_sel_dec s2c_sel_forward s2c_sel_end s3_insert s1begin s1_insert s2abort
	 s2d_sel_end s1commit
permutation
	 s2begin s2commit s3_insert s2begin s1_insert s2c_sel_dec s2c_sel_end s1begin s2a_sel_dec s3begin
	 s3b_sel_dec s2a_sel_next s1d_sel_dec s3a_sel_dec s1commit s3b_sel_next s1d_sel_next s3a_sel_backward s2_insert s2a_sel_prior
	 s2c_sel_dec s2d_sel_dec s3a_sel_prior s3c_sel_dec s2c_sel_forward s2a_sel_end s2c_sel_forward s3b_sel_next s3_insert s3b_sel_end
	 s2c_sel_end s2d_sel_end s3a_sel_end s3c_sel_end s1d_sel_end s3commit s2abort
permutation
	 s1begin s3begin s1abort s2begin s3abort s1begin s2d_sel_dec s1abort s3begin s2abort
	 s3d_sel_dec s3_insert s2d_sel_next s1_insert s3d_sel_next s3d_sel_forward s3commit s2_insert s2d_sel_next s3_insert
	 s3d_sel_next s2d_sel_next s1_insert s3d_sel_next s1begin s1commit s1_insert s2d_sel_forward s3d_sel_end s1begin
	 s2d_sel_end s1abort
permutation
	 s2begin s1begin s3begin s1d_sel_dec s1c_sel_dec s1_insert s3_insert s1d_sel_next s3a_sel_dec s1d_sel_forward
	 s1c_sel_next s1d_sel_forward s3commit s1c_sel_forward s1_insert s2a_sel_dec s3_insert s1d_sel_end s3begin s3a_sel_backward
	 s1c_sel_forward s1c_sel_next s3d_sel_dec s2b_sel_dec s2a_sel_forward s1abort s1c_sel_forward s3d_sel_forward s3a_sel_prior s2a_sel_end
	 s3a_sel_end s2b_sel_end s1c_sel_end s3d_sel_end s2abort s3commit
permutation
	 s1begin s1a_sel_dec s1d_sel_dec s1b_sel_dec s1b_sel_forward s1a_sel_forward s2_insert s1c_sel_dec s1c_sel_next s1d_sel_forward
	 s1b_sel_end s1d_sel_forward s1c_sel_next s3_insert s1abort s1c_sel_forward s1begin s1b_sel_dec s1a_sel_forward s1abort
	 s1a_sel_prior s1d_sel_end s1a_sel_end s1c_sel_end s2begin s1b_sel_prior s1b_sel_forward s1b_sel_next s1b_sel_end s2b_sel_dec
	 s2b_sel_end s2commit
permutation
	 s2begin s2commit s2_insert s3_insert s1begin s2_insert s1d_sel_dec s1abort s1d_sel_next s1d_sel_next
	 s1d_sel_next s3begin s3d_sel_dec s3c_sel_dec s1d_sel_forward s3a_sel_dec s3a_sel_end s1d_sel_forward s1d_sel_forward s2_insert
	 s2_insert s3b_sel_dec s1begin s3b_sel_backward s3b_sel_forward s3d_sel_forward s3c_sel_end s3d_sel_next s3d_sel_end s1c_sel_dec
	 s1d_sel_end s3b_sel_end s1c_sel_end s3commit s1abort
permutation
	 s2begin s2_insert s2_insert s2b_sel_dec s2a_sel_dec s2c_sel_dec s1begin s1c_sel_dec s1commit s2commit
	 s3begin s3b_sel_dec s2c_sel_next s2_insert s2c_sel_forward s3a_sel_dec s3b_sel_end s2c_sel_next s3_insert s3c_sel_dec
	 s1c_sel_end s1_insert s3commit s3c_sel_forward s2a_sel_end s1_insert s2b_sel_next s3a_sel_forward s3a_sel_forward s2b_sel_forward
	 s2c_sel_end s3a_sel_end s3c_sel_end s2b_sel_end
permutation
	 s1begin s1d_sel_dec s1c_sel_dec s1c_sel_forward s1d_sel_next s3_insert s1b_sel_dec s1d_sel_forward s1b_sel_next s1b_sel_prior
	 s1c_sel_next s1d_sel_forward s1c_sel_end s1abort s1b_sel_prior s3_insert s3begin s1begin s1d_sel_end s1abort
	 s3a_sel_dec s1b_sel_backward s3a_sel_end s1b_sel_prior s3commit s1b_sel_end s2begin s2_insert s2commit s3begin
	 s3abort
permutation
	 s1begin s2_insert s1c_sel_dec s1_insert s3_insert s3_insert s1b_sel_dec s2_insert s3_insert s3_insert
	 s1b_sel_next s1b_sel_next s1abort s1_insert s1_insert s1b_sel_prior s1c_sel_forward s1c_sel_forward s1b_sel_prior s1b_sel_prior
	 s1_insert s3begin s3b_sel_dec s3a_sel_dec s1b_sel_end s3c_sel_dec s3a_sel_forward s3b_sel_next s1_insert s3b_sel_backward
	 s3b_sel_end s3c_sel_end s3a_sel_end s1c_sel_end s3commit
permutation
	 s2_insert s1_insert s3_insert s1_insert s3_insert s2begin s2abort s2begin s2_insert s2c_sel_dec
	 s3begin s1_insert s2c_sel_forward s1_insert s3b_sel_dec s3c_sel_dec s3commit s2a_sel_dec s2b_sel_dec s3c_sel_next
	 s2c_sel_end s2a_sel_next s2_insert s1_insert s2a_sel_forward s3c_sel_forward s2abort s2a_sel_end s3c_sel_forward s3begin
	 s3c_sel_end s2b_sel_end s3b_sel_end s3commit
permutation
	 s1_insert s1begin s2begin s2commit s1a_sel_dec s3_insert s1a_sel_forward s3_insert s1a_sel_next s1abort
	 s3_insert s3begin s1a_sel_backward s2begin s2_insert s1begin s3c_sel_dec s1a_sel_backward s2b_sel_dec s3a_sel_dec
	 s1b_sel_dec s2b_sel_end s3a_sel_prior s1b_sel_forward s1b_sel_end s2abort s3commit s1a_sel_backward s3c_sel_forward s1_insert
	 s3c_sel_end s3a_sel_end s1a_sel_end s1commit
permutation
	 s1begin s1c_sel_dec s1_insert s1abort s2_insert s1begin s1c_sel_next s2begin s1b_sel_dec s1b_sel_forward
	 s1_insert s2c_sel_dec s2c_sel_next s1b_sel_forward s2b_sel_dec s2c_sel_next s1d_sel_dec s2b_sel_end s1b_sel_end s2_insert
	 s1c_sel_end s1d_sel_forward s2c_sel_end s2_insert s2c_sel_dec s1b_sel_dec s1d_sel_end s1a_sel_dec s1commit s2c_sel_forward
	 s1a_sel_end s2c_sel_end s1b_sel_end s2abort
permutation
	 s2begin s1_insert s1begin s3_insert s2_insert s2b_sel_dec s2b_sel_end s2c_sel_dec s1abort s2b_sel_dec
	 s2c_sel_end s1_insert s2b_sel_backward s1_insert s3begin s1_insert s3_insert s2a_sel_dec s2b_sel_next s1_insert
	 s3abort s2commit s2begin s2_insert s1begin s2a_sel_end s2c_sel_dec s2commit s2_insert s1abort
	 s2b_sel_end s2c_sel_end
permutation
	 s1begin s1commit s3begin s2begin s2abort s3_insert s2begin s3b_sel_dec s3b_sel_backward s3b_sel_backward
	 s3_insert s2a_sel_dec s3b_sel_forward s3a_sel_dec s2d_sel_dec s2a_sel_backward s3a_sel_prior s2b_sel_dec s2b_sel_backward s2a_sel_forward
	 s3c_sel_dec s3a_sel_forward s3b_sel_backward s3c_sel_next s3a_sel_prior s2d_sel_end s1begin s1abort s2commit s2b_sel_prior
	 s2b_sel_end s3b_sel_end s3c_sel_end s3a_sel_end s2a_sel_end s3commit
permutation
	 s3begin s3a_sel_dec s3a_sel_forward s1_insert s3a_sel_next s2begin s2b_sel_dec s2a_sel_dec s2b_sel_forward s3_insert
	 s3c_sel_dec s3d_sel_dec s3a_sel_forward s3c_sel_next s2d_sel_dec s2_insert s3a_sel_prior s3abort s3d_sel_end s2d_sel_end
	 s2a_sel_prior s2abort s3a_sel_next s3a_sel_forward s2a_sel_backward s3begin s2a_sel_forward s1begin s3c_sel_forward s3commit
	 s2a_sel_end s3c_sel_end s3a_sel_end s2b_sel_end s1abort
permutation
	 s3_insert s1begin s1d_sel_dec s1d_sel_next s1b_sel_dec s1_insert s1_insert s1commit s1b_sel_next s3_insert
	 s1b_sel_end s3_insert s3begin s3_insert s1d_sel_next s3d_sel_dec s3abort s1d_sel_forward s2begin s3d_sel_next
	 s3d_sel_next s1_insert s2b_sel_dec s2b_sel_prior s3d_sel_next s1d_sel_forward s3_insert s1d_sel_forward s2b_sel_next s2b_sel_prior
	 s3d_sel_end s2b_sel_end s1d_sel_end s2abort
permutation
	 s2_insert s1_insert s1_insert s3begin s3abort s2_insert s2begin s3_insert s2commit s3_insert
	 s1_insert s2_insert s1begin s1b_sel_dec s1abort s2_insert s3begin s3c_sel_dec s1b_sel_end s3b_sel_dec
	 s2begin s2_insert s3b_sel_next s3b_sel_backward s3b_sel_backward s1begin s3b_sel_next s3b_sel_next s3abort s3b_sel_backward
	 s3b_sel_end s3c_sel_end s1commit s2abort
permutation
	 s1_insert s1begin s3begin s2begin s1abort s2b_sel_dec s3b_sel_dec s3_insert s2b_sel_prior s2d_sel_dec
	 s2c_sel_dec s2abort s2b_sel_prior s2_insert s3a_sel_dec s2c_sel_forward s2c_sel_forward s2c_sel_forward s3c_sel_dec s1_insert
	 s2d_sel_end s3c_sel_forward s2b_sel_backward s3d_sel_dec s3a_sel_backward s2c_sel_end s3a_sel_next s3b_sel_forward s1begin s3c_sel_end
	 s3a_sel_end s3b_sel_end s2b_sel_end s3d_sel_end s3abort s1commit
permutation
	 s2begin s2c_sel_dec s2c_sel_end s2c_sel_dec s1begin s2d_sel_dec s1d_sel_dec s2d_sel_end s2c_sel_forward s2a_sel_dec
	 s1commit s2c_sel_next s2a_sel_forward s2c_sel_forward s2a_sel_end s2abort s2c_sel_next s1d_sel_next s2c_sel_end s3begin
	 s1_insert s3d_sel_dec s3commit s3d_sel_end s1d_sel_forward s1d_sel_forward s1_insert s3_insert s2begin s2_insert
	 s1d_sel_end s2abort
permutation
	 s1_insert s1_insert s2begin s2b_sel_dec s2_insert s2b_sel_prior s2b_sel_prior s1begin s2b_sel_backward s2commit
	 s3begin s1_insert s2_insert s3c_sel_dec s1c_sel_dec s2b_sel_prior s2b_sel_end s3a_sel_dec s3a_sel_next s3c_sel_forward
	 s1a_sel_dec s1a_sel_backward s3b_sel_dec s1a_sel_next s3b_sel_backward s1a_sel_prior s3a_sel_prior s3d_sel_dec s3a_sel_backward s3b_sel_forward
	 s3a_sel_end s3c_sel_end s3b_sel_end s1a_sel_end s1c_sel_end s3d_sel_end s3abort s1abort
permutation
	 s2_insert s3_insert s1begin s2_insert s2begin s2commit s1abort s3begin s3commit s2_insert
	 s3begin s2begin s1_insert s2abort s1_insert s3b_sel_dec s3a_sel_dec s1_insert s2begin s2c_sel_dec
	 s3b_sel_forward s2commit s3abort s2c_sel_end s3a_sel_backward s3b_sel_next s2_insert s3b_sel_end s3a_sel_next s1_insert
	 s3a_sel_end
permutation
	 s3begin s1_insert s3d_sel_dec s3b_sel_dec s3b_sel_forward s3b_sel_end s3d_sel_next s2begin s3c_sel_dec s2a_sel_dec
	 s2a_sel_next s3a_sel_dec s3c_sel_end s3_insert s3_insert s3b_sel_dec s2_insert s3d_sel_next s3d_sel_forward s3d_sel_next
	 s3b_sel_prior s2abort s3b_sel_backward s2_insert s2begin s1begin s1commit s3b_sel_next s1begin s2_insert
	 s3b_sel_end s3a_sel_end s2a_sel_end s3d_sel_end s3abort s2commit s1abort
permutation
	 s3begin s3a_sel_dec s3abort s3a_sel_prior s3_insert s3a_sel_backward s2_insert s1_insert s3a_sel_backward s1_insert
	 s3a_sel_forward s3a_sel_end s1begin s1_insert s3_insert s1b_sel_dec s1b_sel_backward s1d_sel_dec s2begin s2b_sel_dec
	 s2abort s1b_sel_next s1b_sel_forward s1b_sel_next s1b_sel_backward s1b_sel_backward s2begin s2b_sel_next s2d_sel_dec s1c_sel_dec
	 s1d_sel_end s2b_sel_end s1b_sel_end s2d_sel_end s1c_sel_end s1abort s2abort
permutation
	 s3_insert s1begin s1c_sel_dec s2begin s1c_sel_next s2abort s1d_sel_dec s3begin s1d_sel_forward s3c_sel_dec
	 s3c_sel_next s1commit s1begin s3a_sel_dec s1c_sel_forward s3c_sel_next s1_insert s3a_sel_prior s1d_sel_next s3abort
	 s3begin s1c_sel_end s3_insert s3c_sel_forward s3a_sel_prior s2_insert s1d_sel_end s2_insert s1c_sel_dec s3a_sel_forward
	 s3c_sel_end s3a_sel_end s1c_sel_end s3commit s1abort
permutation
	 s1begin s2begin s1d_sel_dec s1commit s1begin s1d_sel_end s1a_sel_dec s1commit s3_insert s3begin
	 s1a_sel_end s1begin s3a_sel_dec s3a_sel_forward s2c_sel_dec s1d_sel_dec s1abort s1begin s2c_sel_next s2abort
	 s1a_sel_dec s1a_sel_forward s1a_sel_next s3c_sel_dec s1b_sel_dec s1a_sel_prior s1b_sel_prior s1a_sel_next s1b_sel_forward s1b_sel_backward
	 s1d_sel_end s3c_sel_end s3a_sel_end s1b_sel_end s2c_sel_end s1a_sel_end s3commit s1abort
permutation
	 s3begin s2_insert s3commit s1begin s1d_sel_dec s3_insert s1abort s1d_sel_next s3begin s2_insert
	 s3d_sel_dec s3_insert s1d_sel_end s3commit s2_insert s3d_sel_next s2begin s2b_sel_dec s2_insert s2b_sel_forward
	 s2a_sel_dec s2a_sel_end s2_insert s2abort s3begin s3a_sel_dec s3d_sel_next s1begin s3a_sel_prior s3commit
	 s3a_sel_end s2b_sel_end s3d_sel_end s1commit
permutation
	 s3_insert s2_insert s1begin s1c_sel_dec s1c_sel_end s1_insert s1c_sel_dec s1c_sel_forward s1commit s2_insert
	 s1begin s1c_sel_end s1commit s2begin s1begin s2c_sel_dec s1d_sel_dec s2c_sel_end s1d_sel_next s1c_sel_dec
	 s2a_sel_dec s1abort s3begin s1d_sel_end s1_insert s2a_sel_backward s2abort s2_insert s3commit s1c_sel_forward
	 s1c_sel_end s2a_sel_end
permutation
	 s3_insert s1begin s3begin s3abort s2begin s1d_sel_dec s3begin s3_insert s1d_sel_forward s2a_sel_dec
	 s2commit s2a_sel_backward s1d_sel_next s2a_sel_prior s1a_sel_dec s1d_sel_end s1commit s2a_sel_prior s3a_sel_dec s1a_sel_forward
	 s2begin s3d_sel_dec s2a_sel_end s3b_sel_dec s3a_sel_prior s3a_sel_forward s2_insert s2a_sel_dec s1a_sel_backward s2a_sel_next
	 s2a_sel_end s3d_sel_end s1a_sel_end s3b_sel_end s3a_sel_end s2abort s3commit
permutation
	 s3_insert s2begin s2d_sel_dec s2d_sel_forward s2a_sel_dec s2commit s2d_sel_next s2d_sel_forward s2a_sel_prior s2begin
	 s2d_sel_forward s2d_sel_end s2_insert s2commit s2a_sel_end s3begin s3c_sel_dec s1_insert s2begin s3c_sel_next
	 s3_insert s2commit s2begin s2d_sel_dec s1_insert s3d_sel_dec s3d_sel_forward s3c_sel_forward s2_insert s2d_sel_forward
	 s2d_sel_end s3d_sel_end s3c_sel_end s2abort s3abort
permutation
	 s2_insert s3_insert s2_insert s3begin s3_insert s3abort s2_insert s1_insert s1_insert s2_insert
	 s1begin s2begin s2b_sel_dec s1commit s2b_sel_prior s3begin s3_insert s3commit s3_insert s1begin
	 s1d_sel_dec s2a_sel_dec s1d_sel_forward s2commit s1b_sel_dec s3begin s2b_sel_backward s2begin s2b_sel_forward s1b_sel_backward
	 s2a_sel_end s2b_sel_end s1d_sel_end s1b_sel_end s1abort s2abort s3commit
permutation
	 s1_insert s3_insert s1begin s1_insert s1a_sel_dec s1a_sel_prior s1a_sel_prior s1_insert s1c_sel_dec s3_insert
	 s1a_sel_end s1c_sel_forward s1d_sel_dec s1c_sel_next s1c_sel_end s1_insert s1commit s2begin s1d_sel_end s1begin
	 s2commit s1_insert s3_insert s2begin s1commit s1begin s3_insert s1_insert s1c_sel_dec s2commit
	 s1c_sel_end s1commit
permutation
	 s2begin s1_insert s2a_sel_dec s2b_sel_dec s2b_sel_next s1begin s2_insert s3begin s3c_sel_dec s1abort
	 s2a_sel_backward s2a_sel_forward s3c_sel_end s2_insert s2b_sel_backward s3a_sel_dec s3d_sel_dec s1_insert s2d_sel_dec s1_insert
	 s3a_sel_prior s3a_sel_next s2commit s2_insert s2a_sel_backward s3a_sel_end s3commit s2d_sel_next s2_insert s2a_sel_end
	 s2b_sel_end s3d_sel_end s2d_sel_end
permutation
	 s3_insert s3begin s3d_sel_dec s1_insert s3a_sel_dec s3a_sel_next s3d_sel_forward s3commit s2begin s1_insert
	 s3_insert s3_insert s2c_sel_dec s3a_sel_end s2_insert s2b_sel_dec s3_insert s1_insert s3d_sel_forward s2b_sel_end
	 s2abort s2c_sel_end s3d_sel_end s2begin s2d_sel_dec s2_insert s2a_sel_dec s2d_sel_next s3_insert s2a_sel_prior
	 s2a_sel_end s2d_sel_end s2commit
permutation
	 s1begin s1a_sel_dec s1a_sel_backward s1commit s3begin s1begin s1a_sel_backward s1a_sel_forward s3a_sel_dec s3a_sel_forward
	 s2_insert s1d_sel_dec s1a_sel_forward s3a_sel_next s2_insert s3c_sel_dec s3d_sel_dec s3abort s3a_sel_next s2begin
	 s3begin s3a_sel_next s3a_sel_prior s2abort s3commit s2begin s1a_sel_forward s2_insert s2abort s1abort
	 s1a_sel_end s3d_sel_end s3c_sel_end s3a_sel_end s1d_sel_end
permutation
	 s1begin s2begin s1d_sel_dec s1_insert s1abort s1_insert s1_insert s2a_sel_dec s3_insert s2a_sel_backward
	 s1_insert s2b_sel_dec s2b_sel_prior s2c_sel_dec s2c_sel_forward s1d_sel_forward s2abort s2begin s2b_sel_prior s2b_sel_end
	 s2d_sel_dec s2c_sel_next s2d_sel_forward s1d_sel_next s2a_sel_forward s2a_sel_next s1d_sel_forward s2a_sel_prior s3_insert s2c_sel_next
	 s1d_sel_end s2d_sel_end s2a_sel_end s2c_sel_end s2commit
permutation
	 s3_insert s2_insert s3_insert s2_insert s3_insert s2_insert s3begin s3commit s3_insert s2begin
	 s3_insert s3begin s2a_sel_dec s3c_sel_dec s2c_sel_dec s3c_sel_forward s2d_sel_dec s2commit s2a_sel_backward s2c_sel_forward
	 s3commit s2a_sel_forward s2d_sel_next s3c_sel_forward s1begin s2c_sel_forward s2c_sel_next s2c_sel_forward s2d_sel_next s2d_sel_forward
	 s2d_sel_end s2a_sel_end s2c_sel_end s3c_sel_end s1commit
permutation
	 s1begin s1abort s2begin s2commit s1begin s1abort s1begin s1commit s3_insert s3_insert
	 s2_insert s2_insert s3_insert s2_insert s1begin s1_insert s3_insert s1b_sel_dec s1_insert s1b_sel_end
	 s3begin s3commit s1a_sel_dec s1d_sel_dec s3begin s3b_sel_dec s1a_sel_next s2begin s1d_sel_end s2abort
	 s3b_sel_end s1a_sel_end s3abort s1abort
permutation
	 s2begin s2abort s3begin s3b_sel_dec s3abort s3b_sel_next s3_insert s3_insert s3begin s3b_sel_prior
	 s2begin s2a_sel_dec s2_insert s2d_sel_dec s3c_sel_dec s3b_sel_next s3commit s2abort s2a_sel_next s1begin
	 s2_insert s2d_sel_next s2a_sel_next s3begin s3c_sel_end s2d_sel_next s2a_sel_prior s3a_sel_dec s2d_sel_end s3b_sel_backward
	 s3b_sel_end s3a_sel_end s2a_sel_end s3commit s1commit
permutation
	 s2_insert s2_insert s3_insert s3_insert s1_insert s1_insert s2_insert s2_insert s1_insert s3begin
	 s2begin s2abort s3c_sel_dec s1_insert s2begin s2_insert s2a_sel_dec s2a_sel_next s2a_sel_end s3c_sel_end
	 s3commit s1_insert s3_insert s2b_sel_dec s2b_sel_backward s2c_sel_dec s2_insert s2b_sel_backward s2abort s1_insert
	 s2c_sel_end s2b_sel_end
permutation
	 s3_insert s3begin s1begin s2begin s2_insert s2d_sel_dec s2b_sel_dec s2commit s3d_sel_dec s3abort
	 s2d_sel_forward s3_insert s2b_sel_backward s3d_sel_next s1c_sel_dec s1a_sel_dec s3d_sel_next s1c_sel_next s2b_sel_prior s1c_sel_next
	 s1_insert s2b_sel_backward s1a_sel_end s2b_sel_backward s2_insert s1c_sel_end s1c_sel_dec s3d_sel_end s2d_sel_next s2d_sel_next
	 s2d_sel_end s1c_sel_end s2b_sel_end s1abort
permutation
	 s3begin s3commit s2begin s2d_sel_dec s2a_sel_dec s2a_sel_prior s2a_sel_end s2d_sel_forward s1begin s3begin
	 s2commit s1abort s3a_sel_dec s3_insert s3d_sel_dec s3c_sel_dec s2_insert s3d_sel_end s1_insert s2d_sel_next
	 s3d_sel_dec s3a_sel_forward s3a_sel_prior s2d_sel_forward s1begin s1a_sel_dec s3abort s1a_sel_end s1b_sel_dec s1b_sel_end
	 s3d_sel_end s2d_sel_end s3a_sel_end s3c_sel_end s1abort
permutation
	 s3begin s1_insert s3d_sel_dec s3d_sel_end s3commit s2_insert s3_insert s2begin s2_insert s2d_sel_dec
	 s2_insert s2d_sel_end s2d_sel_dec s2d_sel_next s2_insert s2d_sel_next s2c_sel_dec s3_insert s1begin s1a_sel_dec
	 s2commit s1a_sel_forward s2c_sel_end s1a_sel_backward s1commit s2d_sel_forward s3begin s3commit s1a_sel_backward s2d_sel_end
	 s1a_sel_end
permutation
	 s1_insert s2_insert s1_insert s2_insert s3begin s3abort s3begin s2begin s3abort s2_insert
	 s2abort s1_insert s1_insert s1begin s1c_sel_dec s3_insert s1d_sel_dec s1c_sel_end s3_insert s1d_sel_forward
	 s1d_sel_forward s1d_sel_end s1a_sel_dec s3begin s3_insert s3abort s3begin s1a_sel_prior s3d_sel_dec s3a_sel_dec
	 s3a_sel_end s3d_sel_end s1a_sel_end s1abort s3commit
permutation
	 s1_insert s3_insert s2begin s3begin s1_insert s1begin s3commit s3_insert s1d_sel_dec s2d_sel_dec
	 s2a_sel_dec s2a_sel_backward s3begin s3a_sel_dec s3a_sel_end s1a_sel_dec s2a_sel_backward s1commit s3b_sel_dec s1a_sel_end
	 s1d_sel_end s3a_sel_dec s3b_sel_prior s1_insert s3_insert s2commit s3_insert s3b_sel_forward s1_insert s1_insert
	 s3a_sel_end s3b_sel_end s2d_sel_end s2a_sel_end s3abort
permutation
	 s3begin s3abort s3begin s3d_sel_dec s3a_sel_dec s3commit s3a_sel_end s1_insert s3d_sel_end s2_insert
	 s2_insert s2begin s2b_sel_dec s2a_sel_dec s2a_sel_next s2a_sel_end s2_insert s1begin s2b_sel_forward s3_insert
	 s2b_sel_prior s2b_sel_backward s2commit s2b_sel_backward s3begin s2_insert s2b_sel_next s1abort s3_insert s3abort
	 s2b_sel_end
permutation
	 s3_insert s2_insert s3begin s1begin s3b_sel_dec s1_insert s1abort s3a_sel_dec s3d_sel_dec s3a_sel_end
	 s3a_sel_dec s3b_sel_end s3_insert s2_insert s3commit s3a_sel_next s3begin s3a_sel_forward s3d_sel_end s3a_sel_next
	 s3abort s3_insert s3begin s3commit s3a_sel_forward s3a_sel_backward s3a_sel_forward s3a_sel_end s1_insert s2begin
	 s2commit
permutation
	 s2_insert s1_insert s1_insert s2begin s2commit s1begin s2begin s1_insert s1commit s2a_sel_dec
	 s2a_sel_backward s3begin s3abort s2b_sel_dec s2a_sel_next s2b_sel_forward s2c_sel_dec s2b_sel_forward s2b_sel_backward s2a_sel_backward
	 s3begin s2c_sel_next s2b_sel_end s2a_sel_prior s2commit s2c_sel_next s3_insert s2c_sel_forward s3_insert s2c_sel_next
	 s2c_sel_end s2a_sel_end s3abort
permutation
	 s3begin s3commit s2begin s3_insert s3begin s3b_sel_dec s3commit s2c_sel_dec s2b_sel_dec s2b_sel_next
	 s1begin s3b_sel_prior s3begin s3b_sel_forward s2commit s3a_sel_dec s3abort s3a_sel_forward s3a_sel_next s2c_sel_next
	 s2b_sel_backward s3a_sel_end s1c_sel_dec s3begin s3b_sel_forward s1_insert s1b_sel_dec s3_insert s1c_sel_next s2_insert
	 s2c_sel_end s1c_sel_end s1b_sel_end s3b_sel_end s2b_sel_end s3commit s1abort
permutation
	 s3begin s3abort s3begin s3a_sel_dec s3c_sel_dec s3abort s3a_sel_forward s3begin s3a_sel_backward s3a_sel_next
	 s2begin s3_insert s2a_sel_dec s3b_sel_dec s3b_sel_next s3a_sel_forward s2commit s3a_sel_backward s2a_sel_prior s2a_sel_prior
	 s3b_sel_end s2a_sel_prior s2a_sel_forward s2a_sel_prior s2a_sel_prior s3d_sel_dec s2a_sel_end s3a_sel_next s3commit s3a_sel_backward
	 s3c_sel_end s3a_sel_end s3d_sel_end
permutation
	 s1begin s3begin s3d_sel_dec s3b_sel_dec s3b_sel_end s3abort s3d_sel_next s1_insert s3d_sel_end s1b_sel_dec
	 s1commit s1_insert s2_insert s1b_sel_backward s1begin s3_insert s3begin s1_insert s3b_sel_dec s3b_sel_prior
	 s1commit s1b_sel_prior s1b_sel_next s1_insert s1begin s1_insert s1b_sel_forward s3abort s1c_sel_dec s1abort
	 s3b_sel_end s1b_sel_end s1c_sel_end
permutation
	 s1begin s1commit s3_insert s1begin s1commit s1begin s1_insert s1_insert s1_insert s1b_sel_dec
	 s3_insert s3begin s3c_sel_dec s3c_sel_forward s1b_sel_end s2begin s3c_sel_next s3commit s1abort s3c_sel_forward
	 s3begin s1begin s2a_sel_dec s2a_sel_end s1d_sel_dec s1d_sel_end s2commit s1b_sel_dec s3abort s3c_sel_next
	 s1b_sel_end s3c_sel_end s1abort
permutation
	 s1begin s2begin s3_insert s1c_sel_dec s2b_sel_dec s1c_sel_next s2abort s3_insert s1c_sel_next s1a_sel_dec
	 s2b_sel_backward s1a_sel_backward s2b_sel_prior s1d_sel_dec s1b_sel_dec s2b_sel_backward s1a_sel_prior s3_insert s1b_sel_prior s1a_sel_end
	 s2b_sel_next s2b_sel_next s2b_sel_prior s2_insert s1c_sel_end s1d_sel_end s1c_sel_dec s1b_sel_prior s1b_sel_forward s2b_sel_backward
	 s1b_sel_end s2b_sel_end s1c_sel_end s1commit
permutation
	 s2begin s3begin s3a_sel_dec s2_insert s2a_sel_dec s2b_sel_dec s3commit s3a_sel_end s2a_sel_prior s3_insert
	 s2a_sel_end s2b_sel_next s1_insert s3_insert s2commit s1_insert s2b_sel_end s1_insert s1begin s3_insert
	 s1b_sel_dec s1d_sel_dec s1commit s1d_sel_forward s2begin s2commit s3begin s3c_sel_dec s3d_sel_dec s3d_sel_forward
	 s1d_sel_end s1b_sel_end s3c_sel_end s3d_sel_end s3commit
permutation
	 s2begin s1_insert s1begin s2commit s1c_sel_dec s1commit s2begin s2abort s1begin s1c_sel_forward
	 s2_insert s1c_sel_forward s1c_sel_forward s2begin s1a_sel_dec s2c_sel_dec s1c_sel_forward s1_insert s2c_sel_forward s2abort
	 s1abort s3begin s2begin s1c_sel_end s2a_sel_dec s3b_sel_dec s1_insert s3c_sel_dec s2c_sel_forward s3b_sel_forward
	 s3c_sel_end s3b_sel_end s1a_sel_end s2a_sel_end s2c_sel_end s2abort s3abort
permutation
	 s3begin s3abort s2begin s2d_sel_dec s2b_sel_dec s2b_sel_backward s2b_sel_end s2d_sel_next s2d_sel_forward s2d_sel_next
	 s2d_sel_next s2_insert s2d_sel_end s2c_sel_dec s2c_sel_forward s1begin s2d_sel_dec s1_insert s1commit s1_insert
	 s2d_sel_next s1_insert s3begin s2a_sel_dec s2a_sel_backward s3c_sel_dec s2d_sel_forward s3_insert s2a_sel_next s2d_sel_forward
	 s3c_sel_end s2d_sel_end s2a_sel_end s2c_sel_end s3abort s2abort
permutation
	 s3begin s1begin s1c_sel_dec s1c_sel_end s3b_sel_dec s1c_sel_dec s2begin s2c_sel_dec s2b_sel_dec s3b_sel_forward
	 s2commit s2c_sel_next s2b_sel_backward s3b_sel_backward s2c_sel_end s3b_sel_forward s1c_sel_next s3b_sel_prior s1c_sel_next s2b_sel_backward
	 s3b_sel_backward s1c_sel_next s2b_sel_prior s2b_sel_backward s3b_sel_prior s1_insert s3abort s2_insert s3b_sel_next s1_insert
	 s3b_sel_end s2b_sel_end s1c_sel_end s1abort
permutation
	 s1begin s1a_sel_dec s1commit s3_insert s3_insert s3_insert s3begin s1a_sel_backward s3c_sel_dec s1begin
	 s2begin s1a_sel_forward s1a_sel_prior s3abort s1a_sel_end s1abort s1begin s1abort s2commit s3begin
	 s3d_sel_dec s3_insert s3c_sel_forward s2begin s3commit s3d_sel_forward s2d_sel_dec s3c_sel_forward s2c_sel_dec s1_insert
	 s2c_sel_end s2d_sel_end s3d_sel_end s3c_sel_end s2commit
permutation
	 s3_insert s1_insert s2begin s1_insert s2abort s1begin s3_insert s1b_sel_dec s1b_sel_prior s1b_sel_prior
	 s3_insert s1b_sel_prior s1c_sel_dec s1a_sel_dec s1a_sel_backward s1d_sel_dec s1c_sel_next s1commit s1b_sel_forward s1c_sel_end
	 s1b_sel_backward s1a_sel_end s1_insert s3begin s3_insert s3_insert s1b_sel_next s3abort s2begin s2abort
	 s1b_sel_end s1d_sel_end
permutation
	 s2_insert s1begin s1commit s1_insert s3_insert s1_insert s1begin s3_insert s1commit s1begin
	 s3begin s3c_sel_dec s1b_sel_dec s2_insert s1b_sel_next s3c_sel_next s1d_sel_dec s3abort s1b_sel_next s3begin
	 s3c_sel_next s1c_sel_dec s1b_sel_next s3a_sel_dec s1a_sel_dec s1d_sel_forward s3d_sel_dec s1_insert s1c_sel_forward s1b_sel_end
	 s1a_sel_end s1c_sel_end s3d_sel_end s3c_sel_end s3a_sel_end s1d_sel_end s1commit s3commit
permutation
	 s1_insert s1_insert s1_insert s3begin s3abort s3begin s3abort s1begin s1commit s2begin
	 s2_insert s3begin s3abort s3_insert s2a_sel_dec s2a_sel_prior s2_insert s2abort s3_insert s2a_sel_prior
	 s2a_sel_prior s2_insert s3begin s3_insert s1begin s2a_sel_end s3d_sel_dec s1c_sel_dec s1c_sel_next s1d_sel_dec
	 s1c_sel_end s3d_sel_end s1d_sel_end s1commit s3abort
permutation
	 s3begin s2_insert s3d_sel_dec s3commit s3_insert s2begin s1_insert s2c_sel_dec s2d_sel_dec s1_insert
	 s2c_sel_end s2c_sel_dec s2a_sel_dec s3begin s2_insert s3d_sel_next s2_insert s2d_sel_end s3d_sel_end s2c_sel_forward
	 s2a_sel_end s3d_sel_dec s2c_sel_forward s2commit s3abort s2_insert s2_insert s3d_sel_next s1begin s2c_sel_end
	 s3d_sel_end s1commit
permutation
	 s3_insert s2begin s2_insert s2c_sel_dec s2a_sel_dec s2c_sel_end s2a_sel_forward s3begin s3a_sel_dec s3d_sel_dec
	 s2d_sel_dec s2a_sel_prior s3a_sel_end s3abort s2a_sel_forward s3d_sel_forward s3begin s2d_sel_next s3d_sel_end s3a_sel_dec
	 s1_insert s3a_sel_forward s2b_sel_dec s3_insert s3a_sel_end s3b_sel_dec s2b_sel_end s1_insert s3_insert s3d_sel_dec
	 s3b_sel_end s3d_sel_end s2a_sel_end s2d_sel_end s2abort s3abort
permutation
	 s2_insert s3_insert s3_insert s1begin s1c_sel_dec s1commit s1c_sel_next s1c_sel_end s2begin s1begin
	 s2commit s1_insert s1c_sel_dec s1c_sel_forward s1b_sel_dec s2begin s1b_sel_backward s1c_sel_next s2_insert s2a_sel_dec
	 s1b_sel_end s2d_sel_dec s2a_sel_backward s2_insert s2a_sel_prior s2a_sel_prior s2a_sel_forward s1b_sel_dec s2_insert s1c_sel_forward
	 s2a_sel_end s2d_sel_end s1c_sel_end s1b_sel_end s2abort s1commit
permutation
	 s3begin s3_insert s1_insert s3b_sel_dec s3abort s3b_sel_forward s3begin s3b_sel_prior s3b_sel_end s2_insert
	 s3commit s3_insert s1begin s3begin s3abort s3begin s1_insert s1commit s1_insert s3d_sel_dec
	 s2begin s2_insert s1begin s3a_sel_dec s1_insert s3b_sel_dec s3abort s1abort s3begin s3b_sel_next
	 s3b_sel_end s3a_sel_end s3d_sel_end s3abort s2commit
permutation
	 s3begin s1begin s1c_sel_dec s1c_sel_next s3a_sel_dec s1c_sel_end s3_insert s1a_sel_dec s2begin s3d_sel_dec
	 s2_insert s3a_sel_end s3a_sel_dec s3_insert s2commit s3d_sel_forward s2_insert s1commit s3commit s3d_sel_end
	 s3_insert s3a_sel_next s1a_sel_prior s1a_sel_forward s1a_sel_prior s3a_sel_forward s3a_sel_prior s3a_sel_forward s3_insert s3a_sel_next
	 s3a_sel_end s1a_sel_end
permutation
	 s1_insert s1begin s1d_sel_dec s1d_sel_forward s1abort s1d_sel_next s3begin s3abort s1begin s1d_sel_next
	 s1commit s1_insert s3_insert s3begin s1_insert s3d_sel_dec s3b_sel_dec s1_insert s3b_sel_forward s3abort
	 s3b_sel_next s2_insert s3b_sel_end s2begin s2commit s1begin s2begin s1d_sel_next s1commit s3d_sel_end
	 s1d_sel_end s2commit
permutation
	 s2begin s1_insert s2abort s1_insert s2_insert s3begin s3b_sel_dec s1_insert s3b_sel_next s3c_sel_dec
	 s3b_sel_end s3c_sel_next s3_insert s3c_sel_next s3c_sel_end s3_insert s3_insert s2begin s3commit s2_insert
	 s3begin s2d_sel_dec s3commit s2d_sel_forward s2d_sel_end s3begin s2a_sel_dec s2abort s3commit s2a_sel_forward
	 s2a_sel_end
permutation
	 s1_insert s1begin s1d_sel_dec s1_insert s1a_sel_dec s1abort s1a_sel_forward s1_insert s1a_sel_prior s1d_sel_forward
	 s2begin s1a_sel_backward s1d_sel_end s1a_sel_forward s1begin s1commit s1a_sel_backward s3begin s3d_sel_dec s3c_sel_dec
	 s3d_sel_end s2commit s1a_sel_next s1a_sel_forward s1_insert s1a_sel_end s3b_sel_dec s1_insert s3c_sel_next s3_insert
	 s3c_sel_end s3b_sel_end s3commit
permutation
	 s1_insert s1begin s1c_sel_dec s1d_sel_dec s2_insert s1d_sel_forward s3_insert s1a_sel_dec s1b_sel_dec s1b_sel_end
	 s1d_sel_end s1c_sel_forward s1abort s1a_sel_next s1begin s1_insert s1a_sel_backward s2_insert s1a_sel_forward s1c_sel_forward
	 s3_insert s1b_sel_dec s1d_sel_dec s1a_sel_forward s1d_sel_end s1c_sel_end s3_insert s1c_sel_dec s1c_sel_forward s2_insert
	 s1b_sel_end s1a_sel_end s1c_sel_end s1commit
permutation
	 s2_insert s3begin s3commit s2begin s2commit s1begin s1_insert s3begin s3commit s2begin
	 s2b_sel_dec s1abort s2b_sel_backward s2commit s1begin s2b_sel_forward s1b_sel_dec s1c_sel_dec s2b_sel_backward s1b_sel_end
	 s2b_sel_backward s1c_sel_next s3begin s1d_sel_dec s3abort s2b_sel_backward s1c_sel_forward s2b_sel_backward s2_insert s1a_sel_dec
	 s1c_sel_end s1a_sel_end s2b_sel_end s1d_sel_end s1abort
permutation
	 s3_insert s1_insert s3_insert s2_insert s2_insert s1_insert s3_insert s1_insert s3_insert s1_insert
	 s1_insert s1_insert s2begin s2abort s3_insert s3begin s3c_sel_dec s3c_sel_forward s2begin s2d_sel_dec
	 s3c_sel_forward s2b_sel_dec s1begin s1commit s2b_sel_prior s2d_sel_next s1begin s2b_sel_backward s1c_sel_dec s1commit
	 s2b_sel_end s3c_sel_end s2d_sel_end s1c_sel_end s3abort s2abort
permutation
	 s2_insert s3begin s3c_sel_dec s3d_sel_dec s3d_sel_next s1begin s3d_sel_forward s3a_sel_dec s3a_sel_end s1b_sel_dec
	 s1_insert s3_insert s1_insert s3abort s1abort s1b_sel_forward s1_insert s1b_sel_backward s3c_sel_end s2begin
	 s2commit s3d_sel_next s1b_sel_backward s3begin s1_insert s2begin s2b_sel_dec s1b_sel_backward s3abort s2b_sel_prior
	 s3d_sel_end s1b_sel_end s2b_sel_end s2commit
permutation
	 s3_insert s3_insert s3begin s2_insert s3abort s3_insert s1_insert s2begin s2_insert s3begin
	 s3a_sel_dec s3a_sel_prior s3d_sel_dec s3d_sel_forward s3a_sel_prior s3d_sel_end s2c_sel_dec s3a_sel_end s2_insert s3c_sel_dec
	 s3b_sel_dec s3commit s3begin s3_insert s3c_sel_end s2b_sel_dec s2abort s2b_sel_prior s3_insert s3b_sel_next
	 s2c_sel_end s2b_sel_end s3b_sel_end s3abort
permutation
	 s2begin s2a_sel_dec s2a_sel_forward s2a_sel_backward s2a_sel_next s1begin s2c_sel_dec s1a_sel_dec s3begin s1a_sel_backward
	 s2a_sel_next s1b_sel_dec s2a_sel_forward s1d_sel_dec s2a_sel_end s3a_sel_dec s1b_sel_backward s1a_sel_forward s3d_sel_dec s2c_sel_forward
	 s1d_sel_forward s2commit s3_insert s2c_sel_next s3a_sel_end s2_insert s1c_sel_dec s1a_sel_next s1a_sel_forward s1a_sel_backward
	 s1d_sel_end s1b_sel_end s1c_sel_end s3d_sel_end s2c_sel_end s1a_sel_end s1abort s3abort
permutation
	 s2begin s2commit s2begin s2abort s2_insert s3begin s2begin s1begin s1commit s2b_sel_dec
	 s3commit s2d_sel_dec s3begin s3abort s2b_sel_end s1begin s3begin s3c_sel_dec s2d_sel_next s3d_sel_dec
	 s2abort s3b_sel_dec s1_insert s3d_sel_end s1commit s3b_sel_forward s3c_sel_forward s3c_sel_end s3b_sel_backward s2d_sel_forward
	 s3b_sel_end s2d_sel_end s3abort
permutation
	 s3_insert s3begin s2_insert s3b_sel_dec s3b_sel_prior s3b_sel_next s3_insert s3b_sel_prior s3_insert s3a_sel_dec
	 s3abort s3begin s3_insert s3b_sel_next s2begin s2abort s3b_sel_backward s3_insert s3a_sel_forward s3a_sel_next
	 s2begin s2_insert s3a_sel_prior s3a_sel_forward s2commit s3a_sel_end s2_insert s3b_sel_forward s3b_sel_prior s3a_sel_dec
	 s3a_sel_end s3b_sel_end s3commit
permutation
	 s3_insert s3begin s2begin s2b_sel_dec s2b_sel_next s2_insert s2abort s3a_sel_dec s3d_sel_dec s3a_sel_backward
	 s3a_sel_forward s3a_sel_forward s3a_sel_next s3c_sel_dec s2b_sel_prior s2b_sel_next s3a_sel_next s3a_sel_prior s3a_sel_forward s2b_sel_end
	 s3commit s3d_sel_end s3c_sel_next s3a_sel_end s3c_sel_end s2begin s2d_sel_dec s2d_sel_next s1_insert s2d_sel_forward
	 s2d_sel_end s2commit
permutation
	 s1begin s3begin s3d_sel_dec s2_insert s3d_sel_end s1d_sel_dec s1b_sel_dec s1d_sel_forward s1b_sel_forward s3commit
	 s1b_sel_forward s2_insert s1a_sel_dec s1d_sel_next s1d_sel_end s1a_sel_prior s2begin s1b_sel_end s2abort s1_insert
	 s3begin s1abort s1a_sel_forward s3c_sel_dec s1a_sel_end s1begin s2_insert s1b_sel_dec s1d_sel_dec s3commit
	 s3c_sel_end s1b_sel_end s1d_sel_end s1abort
permutation
	 s3_insert s3_insert s2begin s2c_sel_dec s2c_sel_next s1begin s2c_sel_forward s3begin s2c_sel_forward s3_insert
	 s3b_sel_dec s3b_sel_backward s3a_sel_dec s3b_sel_next s2b_sel_dec s1a_sel_dec s2c_sel_end s2d_sel_dec s2_insert s3_insert
	 s3a_sel_forward s3abort s3b_sel_prior s3b_sel_next s2_insert s2b_sel_next s2d_sel_forward s1a_sel_backward s2commit s2d_sel_forward
	 s2b_sel_end s3b_sel_end s3a_sel_end s2d_sel_end s1a_sel_end s1abort
permutation
	 s3_insert s3_insert s3_insert s3_insert s3begin s3commit s2_insert s2_insert s2begin s3_insert
	 s2c_sel_dec s1_insert s3begin s2abort s2c_sel_forward s2c_sel_end s3d_sel_dec s2begin s2c_sel_dec s3abort
	 s3_insert s2abort s2c_sel_next s3d_sel_next s1_insert s1_insert s3d_sel_forward s2c_sel_next s2begin s2c_sel_end
	 s3d_sel_end s2abort
permutation
	 s2_insert s3begin s3a_sel_dec s3a_sel_prior s3a_sel_prior s3a_sel_next s3abort s2begin s2b_sel_dec s2b_sel_prior
	 s2b_sel_end s3a_sel_next s2_insert s3_insert s2commit s3a_sel_backward s3_insert s1_insert s2_insert s1begin
	 s3a_sel_end s1a_sel_dec s1b_sel_dec s1a_sel_forward s1b_sel_prior s1a_sel_end s2_insert s1b_sel_backward s2begin s2d_sel_dec
	 s1b_sel_end s2d_sel_end s2abort s1abort
permutation
	 s3_insert s1_insert s1begin s1a_sel_dec s1a_sel_end s2begin s2abort s1a_sel_dec s1abort s3_insert
	 s3begin s1a_sel_next s1a_sel_backward s3c_sel_dec s3c_sel_forward s3c_sel_next s1_insert s3commit s3_insert s3_insert
	 s1a_sel_backward s1a_sel_next s2_insert s1a_sel_next s2_insert s1a_sel_prior s1begin s1d_sel_dec s1d_sel_forward s1a_sel_forward
	 s1d_sel_end s3c_sel_end s1a_sel_end s1abort
permutation
	 s2begin s3begin s3d_sel_dec s2a_sel_dec s1begin s1a_sel_dec s1commit s2a_sel_end s1a_sel_next s1a_sel_next
	 s3d_sel_next s3_insert s2a_sel_dec s3c_sel_dec s2_insert s3abort s2a_sel_next s1begin s3d_sel_forward s3_insert
	 s2d_sel_dec s3d_sel_end s2a_sel_forward s1d_sel_dec s2a_sel_backward s2a_sel_next s3c_sel_next s1commit s1d_sel_end s1_insert
	 s3c_sel_end s1a_sel_end s2a_sel_end s2d_sel_end s2abort
permutation
	 s3begin s2begin s3a_sel_dec s3commit s2c_sel_dec s3a_sel_backward s2commit s1begin s1abort s1begin
	 s3a_sel_forward s1c_sel_dec s1b_sel_dec s1b_sel_forward s2c_sel_forward s1_insert s1d_sel_dec s1d_sel_end s3a_sel_next s1commit
	 s3a_sel_end s1c_sel_forward s3begin s1c_sel_end s3d_sel_dec s3d_sel_next s1b_sel_forward s3commit s1b_sel_end s1_insert
	 s2c_sel_end s3d_sel_end
permutation
	 s2begin s2a_sel_dec s3_insert s2abort s2begin s2a_sel_end s2c_sel_dec s3begin s3c_sel_dec s2commit
	 s2c_sel_next s2c_sel_forward s2c_sel_forward s3commit s3_insert s2c_sel_forward s2c_sel_end s3begin s3commit s3begin
	 s2_insert s3a_sel_dec s3c_sel_next s3c_sel_next s3abort s3c_sel_next s1begin s3a_sel_backward s3_insert s3a_sel_forward
	 s3a_sel_end s3c_sel_end s1commit
permutation
	 s3_insert s2_insert s3_insert s1_insert s2begin s2c_sel_dec s2c_sel_forward s3_insert s2abort s2_insert
	 s2c_sel_next s2c_sel_next s2_insert s2c_sel_forward s2c_sel_forward s1begin s1commit s2begin s1begin s2a_sel_dec
	 s2b_sel_dec s2c_sel_next s3begin s1commit s3commit s1_insert s2a_sel_end s3_insert s2b_sel_prior s3_insert
	 s2c_sel_end s2b_sel_end s2commit
permutation
	 s3_insert s1begin s3_insert s1b_sel_dec s1c_sel_dec s2_insert s1c_sel_end s1b_sel_next s1b_sel_backward s1_insert
	 s1commit s3_insert s1b_sel_backward s1b_sel_forward s1b_sel_end s2begin s1begin s1b_sel_dec s2a_sel_dec s1a_sel_dec
	 s1a_sel_backward s2_insert s2a_sel_end s2b_sel_dec s2_insert s2a_sel_dec s2b_sel_end s1commit s1b_sel_next s2c_sel_dec
	 s1b_sel_end s2c_sel_end s2a_sel_end s1a_sel_end s2abort
permutation
	 s1begin s1_insert s3_insert s1_insert s1abort s1_insert s2begin s1begin s1_insert s2abort
	 s3begin s1b_sel_dec s3b_sel_dec s3b_sel_end s1d_sel_dec s1a_sel_dec s1b_sel_next s1b_sel_end s1d_sel_next s1a_sel_next
	 s3abort s1b_sel_dec s1b_sel_end s2begin s3_insert s1a_sel_next s2b_sel_dec s2c_sel_dec s2b_sel_next s1c_sel_dec
	 s2b_sel_end s1d_sel_end s1a_sel_end s2c_sel_end s1c_sel_end s1commit s2commit
permutation
	 s1_insert s2begin s2d_sel_dec s2c_sel_dec s2d_sel_end s2d_sel_dec s3_insert s2c_sel_end s3begin s3_insert
	 s2a_sel_dec s3commit s2_insert s2d_sel_end s2commit s2_insert s2a_sel_forward s2a_sel_end s2_insert s2_insert
	 s2begin s1begin s1b_sel_dec s1b_sel_forward s2_insert s3begin s2commit s1d_sel_dec s3commit s1d_sel_end
	 s1b_sel_end s1abort
permutation
	 s3begin s3commit s1_insert s3_insert s3begin s1begin s3abort s1b_sel_dec s1b_sel_backward s1b_sel_backward
	 s3begin s3d_sel_dec s2_insert s1c_sel_dec s3a_sel_dec s3b_sel_dec s3a_sel_backward s1b_sel_end s2_insert s3a_sel_end
	 s3commit s1c_sel_end s3b_sel_end s1c_sel_dec s3d_sel_next s2begin s2_insert s1abort s2c_sel_dec s2d_sel_dec
	 s2d_sel_end s2c_sel_end s3d_sel_end s1c_sel_end s2abort
permutation
	 s3_insert s3_insert s3_insert s2begin s2abort s2begin s2abort s2_insert s3begin s3_insert
	 s1begin s1d_sel_dec s1_insert s3abort s1abort s1_insert s1d_sel_next s1d_sel_end s3_insert s2_insert
	 s3_insert s2begin s2c_sel_dec s3begin s3abort s2a_sel_dec s2a_sel_next s2a_sel_next s2a_sel_backward s3begin
	 s2a_sel_end s2c_sel_end s3abort s2abort
permutation
	 s1begin s1c_sel_dec s1c_sel_forward s1commit s3_insert s1c_sel_forward s2_insert s1begin s1c_sel_forward s1abort
	 s1c_sel_next s2begin s1begin s2_insert s2b_sel_dec s1c_sel_forward s2b_sel_backward s1d_sel_dec s1c_sel_forward s1commit
	 s2b_sel_end s1d_sel_forward s1c_sel_forward s1begin s1c_sel_end s1d_sel_end s3begin s2commit s3commit s3_insert
	 s1commit
permutation
	 s3_insert s3_insert s2_insert s2_insert s2_insert s3_insert s2_insert s1_insert s3_insert s1begin
	 s1commit s1_insert s3_insert s3_insert s1_insert s1_insert s1begin s1commit s2_insert s1begin
	 s2begin s2commit s2begin s2a_sel_dec s2a_sel_forward s1commit s2abort s2_insert s2a_sel_backward s2a_sel_prior
	 s2a_sel_end
permutation
	 s1begin s1b_sel_dec s1b_sel_prior s1abort s2_insert s1b_sel_forward s1b_sel_forward s1b_sel_forward s2_insert s1b_sel_next
	 s3_insert s1begin s1a_sel_dec s2_insert s1a_sel_prior s1_insert s2_insert s1a_sel_prior s1c_sel_dec s1_insert
	 s1c_sel_next s1c_sel_end s1commit s1b_sel_prior s1b_sel_next s1a_sel_next s1b_sel_prior s1b_sel_forward s1b_sel_prior s1begin
	 s1b_sel_end s1a_sel_end s1abort
permutation
	 s1_insert s1begin s1a_sel_dec s1abort s1a_sel_prior s1a_sel_backward s1a_sel_next s1begin s1a_sel_next s1a_sel_forward
	 s1c_sel_dec s1a_sel_end s1a_sel_dec s1a_sel_forward s1a_sel_forward s1c_sel_forward s2_insert s1a_sel_forward s1c_sel_next s3begin
	 s1c_sel_forward s1_insert s1_insert s1a_sel_next s1c_sel_forward s3abort s1d_sel_dec s2begin s3begin s2b_sel_dec
	 s1a_sel_end s1c_sel_end s1d_sel_end s2b_sel_end s1abort s2commit s3commit
permutation
	 s1begin s1abort s2_insert s2begin s2b_sel_dec s3_insert s1begin s1commit s2d_sel_dec s2a_sel_dec
	 s2b_sel_next s2a_sel_prior s2a_sel_forward s2d_sel_forward s2b_sel_forward s2b_sel_forward s2a_sel_next s2b_sel_end s2d_sel_end s2b_sel_dec
	 s2a_sel_backward s2commit s2a_sel_forward s2b_sel_next s2a_sel_prior s2a_sel_backward s2_insert s3begin s3_insert s2_insert
	 s2b_sel_end s2a_sel_end s3abort
permutation
	 s1_insert s2begin s2commit s3begin s3abort s2_insert s2begin s2d_sel_dec s2a_sel_dec s2a_sel_forward
	 s2a_sel_backward s2d_sel_next s2abort s2d_sel_end s2a_sel_next s2a_sel_forward s3_insert s1begin s3_insert s2begin
	 s1d_sel_dec s1a_sel_dec s3begin s2a_sel_end s2a_sel_dec s3a_sel_dec s2a_sel_forward s1d_sel_next s3a_sel_end s1a_sel_forward
	 s1a_sel_end s2a_sel_end s1d_sel_end s1abort s2commit s3commit
permutation
	 s3begin s2_insert s1begin s2begin s1d_sel_dec s3c_sel_dec s2abort s3c_sel_end s1b_sel_dec s1b_sel_end
	 s3a_sel_dec s1c_sel_dec s1c_sel_forward s3a_sel_forward s3d_sel_dec s3commit s2_insert s1commit s2_insert s3begin
	 s1d_sel_forward s3d_sel_next s1c_sel_forward s2begin s3commit s3d_sel_forward s3begin s2a_sel_dec s2a_sel_end s2b_sel_dec
	 s2b_sel_end s1d_sel_end s3a_sel_end s3d_sel_end s1c_sel_end s3abort s2commit
permutation
	 s2_insert s2_insert s1begin s1_insert s3_insert s1_insert s1_insert s2_insert s1c_sel_dec s1_insert
	 s3_insert s1_insert s1a_sel_dec s1c_sel_end s3begin s3abort s1a_sel_backward s1a_sel_prior s1b_sel_dec s1d_sel_dec
	 s1a_sel_end s1commit s1b_sel_end s1d_sel_end s3_insert s1begin s1_insert s2_insert s1a_sel_dec s1b_sel_dec
	 s1a_sel_end s1b_sel_end s1abort
permutation
	 s3begin s2_insert s2_insert s3a_sel_dec s3b_sel_dec s3a_sel_backward s3c_sel_dec s3b_sel_next s3b_sel_next s3c_sel_forward
	 s3_insert s3b_sel_next s2_insert s3b_sel_prior s2begin s1_insert s3b_sel_next s3b_sel_end s3c_sel_forward s2commit
	 s3d_sel_dec s3a_sel_forward s3a_sel_prior s3abort s3a_sel_backward s3c_sel_next s2_insert s3d_sel_end s3a_sel_next s3a_sel_end
	 s3c_sel_end
permutation
	 s2begin s2_insert s2d_sel_dec s3_insert s2abort s2begin s2a_sel_dec s1_insert s2d_sel_forward s2a_sel_forward
	 s2a_sel_end s2abort s1begin s1abort s2d_sel_next s2_insert s2d_sel_next s2d_sel_end s3_insert s3begin
	 s2begin s2b_sel_dec s2b_sel_backward s3c_sel_dec s3a_sel_dec s3c_sel_end s2a_sel_dec s2a_sel_end s3a_sel_forward s3abort
	 s2b_sel_end s3a_sel_end s2abort
permutation
	 s3_insert s2_insert s3begin s2_insert s3abort s2begin s1begin s2b_sel_dec s1d_sel_dec s2b_sel_backward
	 s1_insert s2c_sel_dec s2abort s2b_sel_end s2c_sel_next s2c_sel_end s1c_sel_dec s1c_sel_forward s1d_sel_next s1c_sel_forward
	 s1d_sel_next s1b_sel_dec s1abort s2begin s3_insert s1d_sel_forward s1c_sel_forward s2_insert s1_insert s2abort
	 s1b_sel_end s1d_sel_end s1c_sel_end
permutation
	 s3_insert s1begin s1_insert s1commit s1begin s3_insert s2_insert s3_insert s1d_sel_dec s3begin
	 s3c_sel_dec s1d_sel_forward s3_insert s3_insert s1d_sel_end s3c_sel_next s2begin s1commit s3c_sel_next s3c_sel_next
	 s2d_sel_dec s3b_sel_dec s2d_sel_next s2a_sel_dec s3a_sel_dec s3c_sel_end s3a_sel_forward s2a_sel_prior s3b_sel_next s3a_sel_forward
	 s3b_sel_end s3a_sel_end s2a_sel_end s2d_sel_end s3abort s2commit
permutation
	 s3_insert s1begin s3begin s3a_sel_dec s1d_sel_dec s3a_sel_next s3a_sel_next s2_insert s3b_sel_dec s3b_sel_forward
	 s1b_sel_dec s3a_sel_next s1b_sel_prior s1_insert s3a_sel_backward s1b_sel_end s3d_sel_dec s3c_sel_dec s3a_sel_prior s3c_sel_end
	 s1d_sel_next s3abort s1_insert s3begin s3a_sel_next s1c_sel_dec s1c_sel_next s1_insert s3b_sel_prior s3commit
	 s3b_sel_end s1d_sel_end s3a_sel_end s1c_sel_end s3d_sel_end s1commit
permutation
	 s2begin s3_insert s2c_sel_dec s2a_sel_dec s2d_sel_dec s2commit s1_insert s2c_sel_end s2d_sel_forward s3begin
	 s1begin s3c_sel_dec s3abort s2begin s1abort s2d_sel_next s1_insert s3_insert s1_insert s3c_sel_forward
	 s2a_sel_forward s1_insert s2d_sel_next s2abort s2d_sel_next s2begin s2d_sel_forward s2commit s3c_sel_forward s2a_sel_end
	 s3c_sel_end s2d_sel_end
permutation
	 s1_insert s1begin s3begin s3a_sel_dec s3commit s3a_sel_forward s3a_sel_backward s1a_sel_dec s3a_sel_end s1abort
	 s1begin s1d_sel_dec s1a_sel_prior s1d_sel_forward s3_insert s1a_sel_next s2_insert s1a_sel_forward s1a_sel_prior s1abort
	 s1_insert s1begin s1d_sel_end s1d_sel_dec s1a_sel_next s1a_sel_prior s2begin s1abort s2_insert s1a_sel_prior
	 s1a_sel_end s1d_sel_end s2commit
permutation
	 s1_insert s3_insert s1begin s1a_sel_dec s2begin s2c_sel_dec s3begin s2b_sel_dec s1a_sel_backward s1d_sel_dec
	 s1a_sel_end s3commit s2c_sel_forward s2b_sel_backward s2c_sel_forward s2d_sel_dec s2b_sel_next s2b_sel_next s1abort s1d_sel_next
	 s2_insert s2_insert s2c_sel_next s1d_sel_next s2commit s2b_sel_end s1d_sel_next s1d_sel_end s3begin s3b_sel_dec
	 s2d_sel_end s2c_sel_end s3b_sel_end s3commit
permutation
	 s1begin s2_insert s1commit s3begin s3a_sel_dec s1begin s3abort s2begin s2a_sel_dec s2a_sel_backward
	 s2c_sel_dec s3begin s3a_sel_end s2a_sel_next s3b_sel_dec s1d_sel_dec s3_insert s2abort s3d_sel_dec s2c_sel_next
	 s3b_sel_forward s1b_sel_dec s3d_sel_next s2a_sel_forward s1b_sel_backward s1b_sel_backward s3b_sel_forward s1_insert s1_insert s2a_sel_end
	 s1d_sel_end s3b_sel_end s1b_sel_end s3d_sel_end s2c_sel_end s1commit s3commit
permutation
	 s3_insert s2_insert s1_insert s3_insert s1_insert s3begin s2_insert s3a_sel_dec s1_insert s1begin
	 s3a_sel_end s3b_sel_dec s2_insert s3b_sel_backward s2begin s1c_sel_dec s3d_sel_dec s3d_sel_end s2abort s1_insert
	 s1c_sel_forward s3b_sel_forward s3a_sel_dec s3b_sel_backward s3abort s3a_sel_end s3b_sel_next s1abort s3b_sel_end s2_insert
	 s1c_sel_end
permutation
	 s2begin s3begin s1_insert s3_insert s2commit s1_insert s3abort s3begin s1begin s3abort
	 s2_insert s1d_sel_dec s1d_sel_end s2_insert s1commit s2begin s2_insert s1_insert s2a_sel_dec s2a_sel_end
	 s3_insert s3begin s1begin s2c_sel_dec s3commit s2d_sel_dec s2b_sel_dec s2b_sel_next s3_insert s3_insert
	 s2c_sel_end s2d_sel_end s2b_sel_end s2abort s1commit
permutation
	 s1begin s3begin s1b_sel_dec s1c_sel_dec s2begin s2d_sel_dec s1c_sel_end s2abort s2d_sel_next s3commit
	 s1commit s2d_sel_forward s2begin s2abort s2d_sel_next s3begin s3_insert s1b_sel_backward s3d_sel_dec s1b_sel_prior
	 s3c_sel_dec s2d_sel_end s2begin s3c_sel_forward s3d_sel_end s2d_sel_dec s3c_sel_next s2_insert s3abort s3c_sel_forward
	 s2d_sel_end s3c_sel_end s1b_sel_end s2abort
permutation
	 s3_insert s3begin s3a_sel_dec s3a_sel_next s3d_sel_dec s3commit s3d_sel_next s3d_sel_end s1begin s3a_sel_backward
	 s3a_sel_prior s1b_sel_dec s1commit s1b_sel_forward s1b_sel_backward s3a_sel_next s3_insert s3a_sel_prior s3a_sel_prior s1b_sel_next
	 s3a_sel_next s1b_sel_forward s1begin s1c_sel_dec s1c_sel_end s1b_sel_backward s3a_sel_forward s1a_sel_dec s1a_sel_backward s3a_sel_backward
	 s1a_sel_end s3a_sel_end s1b_sel_end s1abort
permutation
	 s3_insert s1begin s3_insert s2begin s3begin s1c_sel_dec s2d_sel_dec s1a_sel_dec s1a_sel_backward s1c_sel_forward
	 s1_insert s2_insert s2a_sel_dec s2a_sel_forward s3_insert s1c_sel_next s2d_sel_forward s1a_sel_backward s2_insert s2a_sel_next
	 s2a_sel_backward s2d_sel_end s2c_sel_dec s2a_sel_forward s2a_sel_forward s1a_sel_prior s2abort s2a_sel_next s1_insert s2c_sel_forward
	 s2c_sel_end s2a_sel_end s1c_sel_end s1a_sel_end s3abort s1commit
permutation
	 s1begin s1d_sel_dec s2begin s1a_sel_dec s2a_sel_dec s1a_sel_forward s2_insert s2c_sel_dec s1c_sel_dec s2a_sel_forward
	 s1c_sel_end s2a_sel_next s3begin s3abort s2c_sel_next s1a_sel_end s1c_sel_dec s2d_sel_dec s2d_sel_forward s3begin
	 s2d_sel_next s1c_sel_next s2d_sel_next s1abort s1_insert s3a_sel_dec s3d_sel_dec s3a_sel_prior s1begin s2_insert
	 s1c_sel_end s3d_sel_end s2d_sel_end s2c_sel_end s2a_sel_end s3a_sel_end s1d_sel_end s1commit s2commit s3commit
permutation
	 s2_insert s1_insert s3_insert s1_insert s2_insert s2_insert s3begin s3d_sel_dec s3d_sel_end s2_insert
	 s3abort s2_insert s3begin s1_insert s1begin s1a_sel_dec s3commit s3begin s3abort s3begin
	 s1_insert s3commit s1a_sel_next s3begin s3commit s1a_sel_backward s1d_sel_dec s1d_sel_next s1_insert s1d_sel_next
	 s1a_sel_end s1d_sel_end s1abort
permutation
	 s3begin s3a_sel_dec s3c_sel_dec s3b_sel_dec s1begin s3a_sel_end s1b_sel_dec s3b_sel_forward s3b_sel_end s2_insert
	 s3d_sel_dec s1b_sel_forward s3abort s1a_sel_dec s3d_sel_forward s3_insert s3c_sel_forward s1d_sel_dec s1b_sel_backward s1a_sel_prior
	 s1b_sel_forward s3c_sel_next s1abort s1a_sel_next s3d_sel_end s1b_sel_backward s3_insert s1_insert s1d_sel_next s3begin
	 s1a_sel_end s1d_sel_end s1b_sel_end s3c_sel_end s3abort
permutation
	 s2begin s3_insert s1begin s1commit s2d_sel_dec s1_insert s2d_sel_end s2c_sel_dec s1_insert s2d_sel_dec
	 s2c_sel_end s2abort s2d_sel_end s3begin s3_insert s3b_sel_dec s1_insert s3b_sel_end s3b_sel_dec s3b_sel_end
	 s3d_sel_dec s3commit s3d_sel_end s1begin s1a_sel_dec s1commit s1a_sel_prior s2_insert s1begin s1a_sel_next
	 s1a_sel_end s1abort
permutation
	 s2begin s2b_sel_dec s1_insert s2b_sel_backward s2_insert s2b_sel_forward s2b_sel_next s2commit s2_insert s2b_sel_end
	 s3_insert s2_insert s2begin s1begin s2d_sel_dec s2c_sel_dec s2c_sel_end s2d_sel_forward s2abort s1commit
	 s1begin s3_insert s1abort s2begin s2b_sel_dec s2d_sel_end s2b_sel_forward s2c_sel_dec s2b_sel_forward s2c_sel_next
	 s2c_sel_end s2b_sel_end s2abort
