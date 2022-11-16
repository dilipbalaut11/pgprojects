# contrib/procinfo/Makefile

MODULE_big	= procinfo
OBJS = \
	$(WIN32RES) \
	procinfo.o

EXTENSION = procinfo
DATA = procinfo--1.0.sql
PGFILEDESC = "procinfo - function for getting backend proinfo"

REGRESS = procinfo

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/procinfo
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
