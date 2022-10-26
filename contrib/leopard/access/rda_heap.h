/*-------------------------------------------------------------------------
 *
 * rda_heap.h
 *
 * Portions Copyright (c) 2022, EnterpriseDB Corporation. All Rights Reserved.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef LEOPARD_RDA_HEAP_H
#define LEOPARD_RDA_HEAP_H

extern void rda_insert(Oid toid, HeapTuple tuple, ItemPointerData root_tid);
extern bool rda_get(Oid toid, ItemPointerData root_tid, Snapshot snapshot, bool debug, LeopardTuple leopardTuple);
extern void rda_trim(void);

extern Datum rda_unit_test_rda_insert(PG_FUNCTION_ARGS);
extern Datum rda_unit_test_rda_get(PG_FUNCTION_ARGS);
extern Datum rda_unit_test_rda_trim(PG_FUNCTION_ARGS);

#endif							/* LEOPARD_RDA_HEAP_H */
