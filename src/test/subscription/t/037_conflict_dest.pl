# Copyright (c) 2025, PostgreSQL Global Development Group

# Test conflicts in logical replication
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

###############################
# Setup
###############################

# Create a publisher node
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

# Create a subscriber node
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

# Create a table on publisher
$node_publisher->safe_psql('postgres',
	"CREATE TABLE conf_tab (a int PRIMARY KEY, b int UNIQUE, c int UNIQUE);");

$node_publisher->safe_psql('postgres',
	"CREATE TABLE conf_tab_2 (a int PRIMARY KEY, b int UNIQUE, c int UNIQUE);"
);

# Create same table on subscriber
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE conf_tab (a int PRIMARY key, b int UNIQUE, c int UNIQUE);");

$node_subscriber->safe_psql(
	'postgres', qq[
	 CREATE TABLE conf_tab_2 (a int PRIMARY KEY, b int, c int, unique(a,b)) PARTITION BY RANGE (a);
	 CREATE TABLE conf_tab_2_p1 PARTITION OF conf_tab_2 FOR VALUES FROM (MINVALUE) TO (100);
]);

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION pub_tab FOR TABLE conf_tab, conf_tab_2");

# Create the subscription
my $appname = 'sub_tab';
$node_subscriber->safe_psql(
	'postgres',
	"CREATE SUBSCRIPTION sub_tab
	 CONNECTION '$publisher_connstr application_name=$appname'
	 PUBLICATION pub_tab WITH (conflict_log_destination=table)");

# Wait for initial table sync to finish
$node_subscriber->wait_for_subscription_sync($node_publisher, $appname);

##################################################
# INSERT data on Pub and Sub
##################################################

# Insert data in the publisher table
$node_publisher->safe_psql('postgres',
	"INSERT INTO conf_tab VALUES (1,1,1);");

# Insert data in the subscriber table
$node_subscriber->safe_psql('postgres',
	"INSERT INTO conf_tab VALUES (2,2,2), (3,3,3), (4,4,4);");

###############################################################################
# Test conflict insertion into the internal conflict log table
###############################################################################

$node_subscriber->safe_psql('postgres',
	"INSERT INTO conf_tab VALUES (10, 10, 10);");

# Get the internally generated table name
my $subid = $node_subscriber->safe_psql('postgres',
	"SELECT oid FROM pg_subscription WHERE subname = 'sub_tab';");
my $conflict_table = "pg_conflict.pg_conflict_$subid";

$node_publisher->safe_psql('postgres',
	"INSERT INTO conf_tab VALUES (10, 20, 30);");

# Wait for the conflict to be logged
my $log_check = $node_subscriber->poll_query_until(
    'postgres',
    "SELECT count(*) > 0 FROM $conflict_table;"
);

is($log_check, 1, 'Conflict was successfully logged to the internal table');

my $json_query = qq[
    SELECT string_agg((unnested.j::json)->'key'->>'a', ',')
    FROM (
        SELECT unnest(local_conflicts) AS j
        FROM $conflict_table
    ) AS unnested;
];

my $all_keys = $node_subscriber->safe_psql('postgres', $json_query);

# Verify that '10' is present in the resulting string
like($all_keys, qr/10/, 'Verified that key 10 exists in the local_conflicts log');

pass('Conflict type and data successfully validated in internal table');

# Final cleanup for subsequent bidirectional tests in the script
$node_subscriber->safe_psql('postgres', "TRUNCATE conf_tab;");

###############################################################################
# Test Case: update_missing
###############################################################################

# Sync a row, then delete it locally on subscriber
$node_publisher->safe_psql('postgres', "INSERT INTO conf_tab VALUES (50, 50, 50);");
$node_publisher->wait_for_catchup($appname);
$node_subscriber->safe_psql('postgres', "DELETE FROM conf_tab WHERE a = 50;");

# Trigger conflict by updating that row on publisher
$node_publisher->safe_psql('postgres', "UPDATE conf_tab SET b = 500 WHERE a = 50;");

# Wait for the apply worker to detect the missing row and log it
$node_subscriber->poll_query_until('postgres',
    "SELECT count(*) > 0 FROM $conflict_table WHERE conflict_type = 'update_missing';"
) or die "Timed out waiting for update_missing conflict";

my $upd_miss_check = $node_subscriber->safe_psql('postgres',
    "SELECT count(*) FROM $conflict_table WHERE conflict_type = 'update_missing';");
is($upd_miss_check, 1, 'Verified update_missing conflict logged to internal table');

$node_subscriber->safe_psql('postgres', "TRUNCATE conf_tab;");

###############################################################################
# Test Case: insert_exists (via secondary unique index)
###############################################################################

# 1. Subscriber has a row with b=100
$node_subscriber->safe_psql('postgres', "INSERT INTO conf_tab VALUES (100, 100, 100);");

# 2. Publisher inserts a NEW PK (101) but a DUPLICATE 'b' (100)
$node_publisher->safe_psql('postgres', "INSERT INTO conf_tab VALUES (101, 100, 101);");

# 3. Verify it appears as 'insert_exists' in your log table
$node_subscriber->poll_query_until('postgres',
    "SELECT count(*) > 0 FROM $conflict_table WHERE conflict_type = 'insert_exists' AND local_conflicts::text LIKE '%100%';"
) or die "Timed out waiting for secondary index insert_exists conflict";

pass('Logged insert_exists triggered by secondary unique index violation');

$node_subscriber->safe_psql('postgres', "TRUNCATE conf_tab;");

###############################################################################
# CASE 3: Switching Destination to 'log' (Server Log Verification)
###############################################################################

# Switch destination
$node_subscriber->safe_psql('postgres',
    "ALTER SUBSCRIPTION sub_tab SET (conflict_log_destination = 'all');");

$node_subscriber->safe_psql('postgres', "DELETE FROM $conflict_table;");
# Trigger a conflict for server log (insert_exists)
$node_subscriber->safe_psql('postgres', "INSERT INTO conf_tab VALUES (600, 600, 600);");
$node_publisher->safe_psql('postgres', "INSERT INTO conf_tab VALUES (600, 700, 700);");

# Wait for table log
$node_subscriber->poll_query_until('postgres', "SELECT count(*) > 0 FROM $conflict_table;")
    or die "Timed out waiting for insert_exists conflict";

# Check subscriber server log
my $log_found = $node_subscriber->wait_for_log(
    qr/conflict detected on relation "public.conf_tab": conflict=insert_exists/
);
ok($log_found, 'Conflict correctly directed to server stderr log');

# Verify table count DID NOT increase for this conflict
my $table_check = $node_subscriber->safe_psql('postgres',
    "SELECT count(*) FROM $conflict_table WHERE local_conflicts::text LIKE '%600%';");
is($table_check, 1, 'Table log was bypassed when destination set to log');

done_testing();
