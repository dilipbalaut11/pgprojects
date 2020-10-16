# contrib/cm_lz4/Makefile

MODULE_big = cm_lz4
OBJS = \
	$(WIN32RES) \
	cm_lz4.o

EXTENSION = cm_lz4
DATA = cm_lz4--1.0.sql
PGFILEDESC = "lz4 compression method "

SHLIB_LINK += $(filter -llz4, $(LIBS))

REGRESS = cm_lz4


ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/cm_lz4
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
