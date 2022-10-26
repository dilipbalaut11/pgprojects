# leopard/Makefile

EXTRA_INSTALL=contrib/hstore contrib/pg_prewarm contrib/test_decoding

REGRESS_SHLIB=$(abs_top_builddir)/src/test/regress/regress$(DLSUFFIX)
export REGRESS_SHLIB

MODULE_big = leopard
OBJS = \
	$(WIN32RES) \
	access/leopardam.o \
	access/leopardam_handler.o \
	access/leoparddecode.o \
	access/leoparddesc.o \
	access/leopardio.o \
	access/leopardsnapbuild.o \
	access/leopardvacuum.o \
	access/leopardam_visibility.o \
	access/leopardrewrite.o \
	access/leopardprune.o \
	access/leopardtoast.o \
	access/leopardtuple.o \
	access/leopardvismap.o \
	access/rda_heap.o \
	module.o

SUBDIRS = \
	amcheck \
	isolation \
	pageinspect \
	snapshot_too_old

EXTENSION = leopard
DATA = leopard--1.0.sql
PGFILEDESC = "leopard Table Access Method"

LDFLAGS_SL += $(filter -lm, $(LIBS))

# Export the --with-icu option for TAP tests
export with_icu

REGRESS_OPTS = --dlpath=$(top_builddir)/src/test/regress
REGRESS_OPTS += --load-extension=leopard

# removed type_sanity
REGRESS_OPTS += --temp-config $(top_srcdir)/contrib/leopard/leopard.conf
REGRESS_OPTS += --temp-config $(top_srcdir)/contrib/leopard/testing.conf
#REGRESS = tablespace boolean char name varchar text int2 int4 int8 oid float4 float8 bit numeric txid uuid enum money rangetypes pg_lsn regproc strings numerology point lseg line box path polygon circle date time timetz timestamp timestamptz interval inet macaddr macaddr8 multirangetypes create_function_0 geometry horology tstypes regex opr_sanity misc_sanity comments expressions unicode xid mvcc create_function_1 create_type create_table create_function_2 copy copyselect copydml insert insert_conflict create_misc create_operator create_procedure create_index_spgist create_view index_including index_including_gist create_aggregate create_function_3 create_cast constraints triggers typed_table vacuum drop_if_exists roleattributes hash_func errors infinite_recurse select_implicit select_having case transactions random btree_index hash_index update delete namespace prepared_xacts gin gist spgist privileges init_privs collate matview lock replica_identity rowsecurity object_address tablesample drop_operator password identity generated join_hash guc bitmapops combocid tsearch tsdicts window functional_deps advisory_lock indirect_toast equivclass json jsonb json_encoding jsonpath jsonpath_encoding jsonb_jsonpath plancache plpgsql copy2 temp domain rangefuncs prepare conversion truncate reloptions hash_part indexing partition_aggregate partition_info tuplesort explain compression oidjoins fast_default

ISOLATION_OPTS = --load-extension=leopard

ISOLATION_OPTS += --inputdir=isolation
ISOLATION_OPTS += --temp-config $(top_srcdir)/contrib/leopard/leopard.conf
ISOLATION_OPTS += --temp-config $(top_srcdir)/contrib/leopard/testing.conf
# removed temp-schema-cleanup
ISOLATION = leopard-rda-unit leopard-update nowait-5 ri-trigger vacuum-reltuples lock-update-delete vacuum-conflict aborted-keyrevoke read-write-unique-2 total-cash simple-write-skew tuplelock-update insert-conflict-do-update-3 nowait partition-drop-index-locking lock-committed-update nowait-3 insert-conflict-do-nothing receipt-report read-write-unique-3 lock-update-traversal sequence-ddl partial-index serializable-parallel temporal-range-integrity fk-deadlock2 propagate-lock-delete alter-table-3 eval-plan-qual-trigger lock-committed-keyupdate detach-partition-concurrently-2 predicate-hash prepared-transactions predicate-lock-hot-tuple prepared-transactions-cic update-conflict-out tuplelock-upgrade-no-deadlock recently-dead partition-concurrent-attach multixact-no-forget reindex-concurrently read-only-anomaly-3 recently-dead2 update-locked-tuple horizons read-only-anomaly detach-partition-concurrently-3 multixact-no-deadlock tuplelock-conflict delete-abort-savept fk-deadlock reindex-concurrently-toast create-trigger deadlock-soft-2 delete-abort-savept-2 freeze-the-dead truncate-conflict nowait-4 serializable-parallel-2 multiple-row-versions alter-table-4 fk-partitioned predicate-gist partition-key-update-1 fk-contention alter-table-2 skip-locked-2 read-only-anomaly-2 deadlock-simple partition-key-update-2 skip-locked fk-partitioned-1 classroom-scheduling tuplelock-partition timeouts predicate-gin two-ids nowait-2 async-notify alter-table-1 inherit-temp fk-partitioned-2 skip-locked-3 deadlock-parallel vacuum-concurrent-drop project-manager read-write-unique-4 drop-index-concurrently-1 cursor-2 partition-key-update-4 detach-partition-concurrently-1 vacuum-skip-locked fk-partitioned-savepoints referential-integrity reindex-schema detach-partition-concurrently-4 cursor-1 multiple-cic deadlock-soft partition-key-update-3 deadlock-hard insert-conflict-do-update-2 skip-locked-4 eval-plan-qual read-write-unique insert-conflict-do-update index-only-scan insert-conflict-specconflict insert-conflict-do-nothing-2 plpgsql-toast

TAP_TESTS = 1

# Disabled because these tests require "shared_preload_libraries=leopard",
# which typical installcheck users do not have (e.g. buildfarm clients).
NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/leopard
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
