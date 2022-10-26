# Verify that cloning a foreign key constraint to a partition ensures
# that referenced values exist, even if they're being concurrently
# deleted.
setup {
  -- Department table, partitioned by hash into three partitions
  create table dep (id int primary key, department text) partition by hash (id);
  create table dep0 partition of dep for values with (modulus 3, remainder 0) using leopard;
  create table dep2 partition of dep for values with (modulus 3, remainder 1) using leopard;
  create table dep3 partition of dep for values with (modulus 3, remainder 2) using leopard;

  -- Employee table, partitioned by hash into two partitions, themselves partially partitioned
  create table emp (id int, depid int references dep(id), title text, primary key(id,title) ) partition by hash (id);

  -- Direct partition of emp
  create table emp0 partition of emp for values with (modulus 3, remainder 0) using leopard;

  -- Partitions of emp with further partitioning
  create table emp1 partition of emp for values with (modulus 3, remainder 1) partition by range (id);
  create table emp2 partition of emp for values with (modulus 3, remainder 2) partition by list (title);

  -- Subpartitions
  create table emp1neg partition of emp1 for values from (minvalue) to (0) using leopard;
  create table emp1pos partition of emp1 for values from (0) to (maxvalue) using leopard;
  create table emp2eng partition of emp2 for values in ('Surface Warfare Engineer', 'Torpedo Engineer', 'Propulsion Engineer', 'Nuclear Engineer') using leopard;
  create table emp2tec partition of emp2 for values in ('Aegis Technician', 'Combat Radar Technician', 'Sonabouy Technician', 'Sonar Technician') using leopard;
  create table emp2mgr partition of emp2 for values in ('Admiral', 'Flight Operations Officer', 'Petty Pay Officer') using leopard;
}

session s1
step s1b	{	begin; }
step s1s	{	savepoint s1; }
step s1eng	{	insert into dep values (1, 'Engineering'); }
step s1tec	{	insert into dep values (2, 'Repairs'); }
step s1mgr	{	insert into dep values (3, 'Management'); }
step s1c	{	commit; }

session s2
step s2b	{	begin; }
step s2s	{	savepoint s2; }
step s2e1	{	insert into emp values (1, 1, 'Surface Warfare Engineer'); }
step s2e2	{	insert into emp values (2, 1, 'Torpedo Engineer'); }
step s2t1	{	insert into emp values (3, 2, 'Aegis Technician'); }
step s2t2	{	insert into emp values (4, 2, 'Combat Radar Technician'); }
step s2m1	{	insert into emp values (5, 3, 'Admiral'); }
step s2m2	{	insert into emp values (6, 3, 'Admiral'); }
step s2c	{	commit; }

session s3
step s3b	{	begin; }
step s3s	{	savepoint s3; }
step s3e1	{	insert into emp values (7, 1, 'Propulsion Engineer'); }
step s3e2	{	insert into emp values (8, 1, 'Nuclear Engineer'); }
step s3t1	{	insert into emp values (9, 2, 'Sonabouy Technician'); }
step s3t2	{	insert into emp values (10, 2, 'Sonar Technician'); }
step s3m1	{	insert into emp values (11, 3, 'Flight Operations Officer'); }
step s3m2	{	insert into emp values (12, 3, 'Petty Pay Officer'); }
step s3c	{	commit; }

teardown {
  drop table emp cascade;
  drop table dep cascade;
}

# All in order, serialized
permutation s1b s1eng s1tec s1mgr s1c s2b s2e1 s2e2 s2t1 s2t2 s2m1 s2m2 s2c s3b s3e1 s3e2 s3t1 s3t2 s3m1 s3m2 s3c

# All in order, concurrent
permutation s1b s2b s3b s1eng s1tec s1mgr s2e1 s2e2 s2t1 s2t2 s2m1 s2m2 s3e1 s3e2 s3t1 s3t2 s3m1 s3m2 s1c s2c s3c

# Interleaved, concurrent
permutation s1b s2b s3b s1eng s2e1 s2e2 s3e1 s3e2 s1tec s2t1 s2t2 s3t1 s3t2 s1mgr s2m1 s2m2 s3m1 s3m2 s1c s2c s3c

# Dep first, then interleaved concurrent emp
permutation s1b s1eng s1tec s1mgr s1c s2b s3b s2e1 s2e2 s3e1 s3e2 s2t1 s2t2 s3t1 s3t2 s2m1 s2m2 s3m1 s3m2 s2c s3c

# All in order, serialized, with savepoints
permutation s1b s1s s1eng s1s s1tec s1s s1mgr s1s s1c s2b s2s s2e1 s2s s2e2 s2s s2t1 s2s s2t2 s2s s2m1 s2s s2m2 s2s s2c s3b s3s s3e1 s3s s3e2 s3s s3t1 s3s s3t2 s3s s3m1 s3s s3m2 s3s s3c

# All in order, concurrent, with savepoints
permutation s1b s2b s3b s1s s1eng s1s s1tec s1s s1mgr s2s s2e1 s2s s2e2 s2s s2t1 s2s s2t2 s2s s2m1 s2s s2m2 s3s s3e1 s3s s3e2 s3s s3t1 s3s s3t2 s3s s3m1 s3s s3m2 s1s s1c s2c s3c

# Interleaved, concurrent, with savepoints
permutation s1b s2b s3b s1s s1eng s2s s2e1 s2s s2e2 s3s s3e1 s3s s3e2 s1s s1tec s2s s2t1 s2s s2t2 s3s s3t1 s3s s3t2 s1s s1mgr s2s s2m1 s2s s2m2 s3s s3m1 s3s s3m2 s1s s1c s2c s3c

# Dep first, then interleaved concurrent emp, with savepoints
permutation s1b s1s s1eng s1s s1tec s1s s1mgr s1s s1c s2b s3b s2e1 s2s s2e2 s2s s3e1 s2s s3e2 s2t1 s2s s2t2 s2s s3t1 s2s s3t2 s2m1 s2s s2m2 s2s s3m1 s2s s3m2 s2c s3c
