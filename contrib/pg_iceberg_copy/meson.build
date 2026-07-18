# Copyright (c) 2026, PostgreSQL Global Development Group

pg_iceberg_copy_sources = files(
  'pg_iceberg_copy.c',
)

if host_system == 'windows'
  pg_iceberg_copy_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_iceberg_copy',
    '--FILEDESC', 'pg_iceberg_copy - copy PostgreSQL data blocks, WAL, and SLRU in Iceberg format',])
endif

pg_iceberg_copy = shared_module('pg_iceberg_copy',
  pg_iceberg_copy_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_iceberg_copy

install_data(
  'pg_iceberg_copy--1.0.sql',
  'pg_iceberg_copy.control',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_iceberg_copy',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'pg_iceberg_copy',
    ],
  },
}
