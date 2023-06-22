# contrib/edb_wait_states/Makefile

MODULE_big = edb_wait_states
OBJS = edb_wait_states_reader.o $(WIN32RES) \
	   edb_wait_states_utils.o \
	   edb_wait_states_worker.o

EXTENSION = edb_wait_states
DATA = edb_wait_states--1.0.sql
PGFILEDESC = "edb_wait_states - wait events statistics of SQL statements"

LDFLAGS_SL += $(filter -lm, $(LIBS))

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/edb_wait_states
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
