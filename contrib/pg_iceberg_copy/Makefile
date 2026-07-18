# contrib/pg_iceberg_copy/Makefile

MODULE_big = pg_iceberg_copy
OBJS = pg_iceberg_copy.o

EXTENSION = pg_iceberg_copy
DATA = pg_iceberg_copy--1.0.sql
PGFILEDESC = "pg_iceberg_copy - copy PostgreSQL data blocks, WAL, and SLRU in Iceberg format"

REGRESS = pg_iceberg_copy

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_iceberg_copy
top_builddir = ../..
top_srcdir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
