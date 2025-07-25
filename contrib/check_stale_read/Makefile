# contrib/check_stale_read/Makefile

MODULE_big = check_stale_read
OBJS = \
	$(WIN32RES) \
	check_stale_read.o

EXTENSION = check_stale_read
DATA = check_stale_read--1.0.sql
PGFILEDESC = "check_stale_read - check stale page read"

TAP_TESTS = 0

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/check_stale_read
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
