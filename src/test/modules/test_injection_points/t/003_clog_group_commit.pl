# Test consistent of initial snapshot data.

# This requires a node with wal_level=logical combined with an injection
# point that forces a failure when a snapshot is initially built with a
# logical slot created.
#
# See bug https://postgr.es/m/CAFiTN-s0zA1Kj0ozGHwkYkHwa5U0zUE94RSc_g81WrpcETB5=w@mail.gmail.com.

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init(allows_streaming => 'logical');
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION test_injection_points;');
$node->safe_psql('postgres', 'CREATE TABLE test(a int);');

# Consume multiple xids so that next xids get generated in new banks
$node->safe_psql(
	'postgres', q{
do $$
begin
  for i in 1..128001 loop
    -- use an exception block so that each iteration eats an XID
    begin
      insert into test values (i);
    exception
      when division_by_zero then null;
    end;
  end loop;
end$$;
});

my $result = $node->safe_psql('postgres',
	"SELECT txid_current();");
is($result, qq(128740),
	'check column trigger applied even on update for other column');

$node->safe_psql('postgres',
  "SELECT test_injection_points_attach('ClogGroupCommit', 'wait');");


# First session will get the slru lock and will wait on injection point
my $session1 = $node->background_psql('postgres');

$session1->query_until(
	qr/start/, q(
\echo start
INSERT INTO test VALUES(1);
));

#create another 4 session which will not get the lock as first session is holding that lock
#so these all will go for group update
my $session2 = $node->background_psql('postgres');

$session2->query_until(
	qr/start/, q(
\echo start
INSERT INTO test VALUES(2);
));

my $session3 = $node->background_psql('postgres');

$session3->query_until(
	qr/start/, q(
\echo start
INSERT INTO test VALUES(3);
));

my $session4 = $node->background_psql('postgres');

$session4->query_until(
	qr/start/, q(
\echo start
INSERT INTO test VALUES(4);
));

my $session5 = $node->background_psql('postgres');

$session5->query_until(
	qr/start/, q(
\echo start
INSERT INTO test VALUES(5);
));

# Now wake up the first session and let next 4 session perform the group update
$node->safe_psql('postgres',
  "SELECT test_injection_points_wake();");
$node->safe_psql('postgres',
  "SELECT test_injection_points_detach('ClogGroupCommit');");

done_testing();
