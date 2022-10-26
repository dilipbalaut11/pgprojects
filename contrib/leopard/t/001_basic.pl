
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

# Basic test that the leopard module can be loaded
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use Test::More tests => 1;

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('test');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'leopard'");
$node->start;
$node->safe_psql('postgres', "CREATE EXTENSION leopard WITH SCHEMA public");
$node->stop;
$node->append_conf('postgresql.conf', "default_table_access_method = 'leopard'");
$node->start;
my $default_version = $node->safe_psql('postgres',
"SELECT default_version FROM pg_available_extensions WHERE name = 'leopard'");
is($default_version, '1.0', "Check default version of leopard extension");
