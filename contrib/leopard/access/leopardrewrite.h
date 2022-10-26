/*-------------------------------------------------------------------------
 *
 * leopardrewrite.h
 *	  Declarations for leopard rewrite support functions
 *
 * Portions Copyright (c) 2022, EnterpriseDB Corporation. All Rights Reserved.
 *
 *
 *
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * src/include/access/leopardrewrite.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LEOPARDREWRITE_H
#define LEOPARDREWRITE_H

#include "access/leopardtup.h"
#include "storage/itemptr.h"
#include "storage/relfilenode.h"
#include "utils/relcache.h"

/* struct definition is private to leopardrewrite.c */
typedef struct RewriteStateData *RewriteState;

extern RewriteState begin_leopard_rewrite(Relation OldLeopard, Relation NewLeopard,
									   TransactionId OldestXmin, TransactionId FreezeXid,
									   MultiXactId MultiXactCutoff);
extern void end_leopard_rewrite(RewriteState state);
extern void rewrite_leopard_tuple(RewriteState state, LeopardTuple oldTuple,
							   LeopardTuple newTuple);
extern bool rewrite_leopard_dead_tuple(RewriteState state, LeopardTuple oldTuple);

/*
 * On-Disk data format for an individual logical rewrite mapping.
 */
typedef struct LogicalRewriteMappingData
{
	RelFileNode old_node;
	RelFileNode new_node;
	ItemPointerData old_tid;
	ItemPointerData new_tid;
} LogicalRewriteMappingData;

/* ---
 * The filename consists of the following, dash separated,
 * components:
 * 1) database oid or InvalidOid for shared relations
 * 2) the oid of the relation
 * 3) upper 32bit of the LSN at which a rewrite started
 * 4) lower 32bit of the LSN at which a rewrite started
 * 5) xid we are mapping for
 * 6) xid of the xact performing the mapping
 * ---
 */
#define LOGICAL_REWRITE_FORMAT "map-%x-%x-%X_%X-%x-%x"
void		CheckPointLogicalRewriteLeopard(void);

#endif							/* LEOPARDREWRITE_H */
