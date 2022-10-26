/*-------------------------------------------------------------------------
 *
 * leopardtup.h
 *	  POSTGRES leopard tuple definitions.
 *
 *
 * Portions Copyright (c) 2022, EnterpriseDB Corporation. All Rights Reserved.
 *
 *
 *
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/leopardtup.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LEOPARDTUP_H
#define LEOPARDTUP_H

#include "storage/itemptr.h"

/* typedefs and forward declarations for structs defined in leopardtup_details.h */

typedef struct LeopardTupleHeaderData LeopardTupleHeaderData;

typedef LeopardTupleHeaderData *LeopardTupleHeader;

typedef struct MinimalLeopardTupleData MinimalLeopardTupleData;

typedef MinimalLeopardTupleData *MinimalLeopardTuple;


/*
 * LeopardTupleData is an in-memory data structure that points to a tuple.
 *
 * There are several ways in which this data structure is used:
 *
 * * Pointer to a tuple in a disk buffer: t_data points directly into the
 *	 buffer (which the code had better be holding a pin on, but this is not
 *	 reflected in LeopardTupleData itself).
 *
 * * Pointer to nothing: t_data is NULL.  This is used as a failure indication
 *	 in some functions.
 *
 * * Part of a palloc'd tuple: the LeopardTupleData itself and the tuple
 *	 form a single palloc'd chunk.  t_data points to the memory location
 *	 immediately following the LeopardTupleData struct (at offset LEOPARDTUPLESIZE).
 *	 This is the output format of leopard_form_tuple and related routines.
 *
 * * Separately allocated tuple: t_data points to a palloc'd chunk that
 *	 is not adjacent to the LeopardTupleData.  (This case is deprecated since
 *	 it's difficult to tell apart from case #1.  It should be used only in
 *	 limited contexts where the code knows that case #1 will never apply.)
 *
 * * Separately allocated minimal tuple: t_data points MINIMAL_LEOPARD_TUPLE_OFFSET
 *	 bytes before the start of a MinimalLeopardTuple.  As with the previous case,
 *	 this can't be told apart from case #1 by inspection; code setting up
 *	 or destroying this representation has to know what it's doing.
 *
 * t_len should always be valid, except in the pointer-to-nothing case.
 * t_self and t_tableOid should be valid if the LeopardTupleData points to
 * a disk buffer, or if it represents a copy of a tuple on disk.  They
 * should be explicitly set invalid in manufactured tuples.
 */
typedef struct LeopardTupleData
{
	uint32		t_len;			/* length of *t_data */
	ItemPointerData t_self;		/* SelfItemPointer */
	Oid			t_tableOid;		/* table the tuple came from */
#define FIELDNO_HEAPTUPLEDATA_DATA 3
	LeopardTupleHeader t_data;		/* -> tuple header and data */
} LeopardTupleData;

typedef LeopardTupleData *LeopardTuple;

#define LEOPARDTUPLESIZE	MAXALIGN(sizeof(LeopardTupleData))

/*
 * Accessor macros to be used with LeopardTuple pointers.
 */
#define HeapTupleIsValid(tuple) PointerIsValid(tuple)

/* LeopardTupleHeader functions implemented in utils/time/combocid.c */




/* Prototype for LeopardTupleHeader accessors in leopardam.c */
extern TransactionId LeopardTupleGetUpdateXid(LeopardTupleHeader tuple);

#endif							/* LEOPARDTUP_H */
