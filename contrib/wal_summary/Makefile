# contrib/wal_summary/Makefile

MODULE_big	= wal_summary
OBJS = \
	$(WIN32RES) \
	wal_summary.o \
	blkref_table.o

EXTENSION = wal_summary
DATA = wal_summary--1.0.sql
PGFILEDESC = "wal_summary - wal summarization utility for incremental backup"

REGRESS = check

TAP_TESTS = 0

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/wal_summary
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
