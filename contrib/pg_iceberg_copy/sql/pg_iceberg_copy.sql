CREATE EXTENSION pg_iceberg_copy;
SELECT pg_iceberg_copy_run('/tmp/test_iceberg_export');
