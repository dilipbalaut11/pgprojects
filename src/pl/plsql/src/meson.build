# Copyright (c) 2022-2025, PostgreSQL Global Development Group

plsql_sources = files(
  'pl_comp.c',
  'pl_exec.c',
  'pl_funcs.c',
  'pl_handler.c',
  'pl_scanner.c',
)

pl_gram = custom_target('gram',
  input: 'pl_gram.y',
  kwargs: bison_kw,
)
generated_sources += pl_gram.to_list()
plsql_sources += pl_gram

gen_plerrcodes = files('generate-plerrcodes.pl')
pl_errcodes = custom_target('plerrcodes',
  input: ['../../../../src/backend/utils/errcodes.txt'],
  output: ['plerrcodes.h'],
  command: [perl, gen_plerrcodes, '@INPUT0@'],
  capture: true,
)
generated_sources += pl_errcodes
plsql_sources += pl_errcodes

pl_reserved = custom_target('pl_reserved_kwlist',
  input: ['pl_reserved_kwlist.h'],
  output: ['pl_reserved_kwlist_d.h'],
  depend_files: gen_kwlist_deps,
  command: [gen_kwlist_cmd, '--varname', 'ReservedPLKeywords'],
)
generated_sources += pl_reserved
plsql_sources += pl_reserved

pl_unreserved = custom_target('pl_unreserved_kwlist',
  input: ['pl_unreserved_kwlist.h'],
  output: ['pl_unreserved_kwlist_d.h'],
  depend_files: gen_kwlist_deps,
  command: [gen_kwlist_cmd, '--varname', 'UnreservedPLKeywords'],
)
generated_sources += pl_unreserved
plsql_sources += pl_unreserved

if host_system == 'windows'
  plsql_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'plsql',
    '--FILEDESC', 'PL/pgSQL - procedural language',])
endif

plsql = shared_module('plsql',
  plsql_sources,
  c_pch: pch_postgres_h,
  include_directories: include_directories('.'),
  kwargs: pg_mod_args,
)
pl_targets += plsql

install_data(
  'plsql.control',
  'plsql--1.0.sql',
  install_dir: dir_data_extension,
)

install_headers(
  'plsql.h',
  install_dir: dir_include_server
)


tests += {
  'name': 'plsql',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'plsql_array',
      'plsql_cache',
      'plsql_call',
      'plsql_control',
      'plsql_copy',
      'plsql_domain',
      'plsql_misc',
      'plsql_record',
      'plsql_simple',
      'plsql_transaction',
      'plsql_trap',
      'plsql_trigger',
      'plsql_varprops',
    ],
  },
}

subdir('po', if_found: libintl)
