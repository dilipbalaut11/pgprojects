/*-------------------------------------------------------------------------
 *
 * pg_qualstats.h: 
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * The implementation is heavily inspired by pg_stat_statements
 *
 * Copyright (c) 2014,2017 Ronan Dunklau
 * Copyright (c) 2018-2022, The Powa-Team *
 *-------------------------------------------------------------------------
*/
#ifndef _PG_QUALSTATS_H_
#define _PG_QUALSTATS_H_

#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#endif
#include "catalog/catalog.h"
#include "commands/explain.h"
#include "lib/dshash.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/plancat.h"
#include "utils/memutils.h"

/* Since cff440d368, queryid becomes a uint64 internally. */

#define PGQS_CONSTANT_SIZE 80	/* Truncate constant representation at 80 */

#if PG_VERSION_NUM >= 110000
typedef uint64 pgqs_queryid;
#else
typedef uint32 pgqs_queryid;
#endif

/*---- Data structures declarations ----*/
typedef struct pgqsSharedState
{
#if PG_VERSION_NUM >= 90400
	LWLock	   *lock;			/* protects counters hashtable
								 * search/modification */
	LWLock	   *querylock;		/* protects query/update hashtable
								 * search/modification */
#else
	LWLockId	lock;			/* protects counters hashtable
								 * search/modification */
	LWLockId	querylock;		/* protects query hashtable
								 * search/modification */
#endif
	/* Hash table holding last start times of subscriptions' apply workers. */
	int			tranche_id;
	dsa_handle	dsa;
	dshash_table_handle pgqs_dsh;
	dshash_table_handle pgqs_querydsh;
	dshash_table_handle pgqs_updatedsh;
	dsa_pointer	qualdata;
	dsa_pointer	querydata;
	dsa_pointer	updatedata;
	int			qualentindex;
	int			qryentindex;
	int			updateentindex;
	int			qualmaxent;
	int			qrymaxent;
	int			updatemaxent;
#if PG_VERSION_NUM >= 90600
	LWLock	   *sampledlock;	/* protects sampled array search/modification */
	bool		sampled[FLEXIBLE_ARRAY_MEMBER]; /* should we sample this
												 * query? */
#endif
} pgqsSharedState;

/* 
 * Actual data segment, this contains the data and also the dsa_pointer to the
 * next segment or an InvalidDsaPointer if this is a last segment.
 */
typedef struct pgqsHashSegment
{
	dsa_pointer	nextsegment;
	char		data[FLEXIBLE_ARRAY_MEMBER];
} pgqsHashSegment;

#define PGQSHASHSEGHDRSZ offsetof(pgqsHashSegment, data)

/*
 * In older version sequence serach was not implemented for dshash so in hash
 * we just store the key and an index to the actual entry but the actual
 * entries are stored outside of the hash in form of segments of data.
 */
typedef struct pgqsHashDataHeader
{
	int			index;			/* index where to insert next entry */
	int			maxentries;		/* max allocated entries */
	dsa_pointer firstsegment;	/* dsa pointer to the first pgqsHashSegment */
	dsa_pointer activesegment;	/* dsa pointer to the first pgqsHashSegment */
} pgqsHashDataHeader;

typedef struct pgqsHashDataInfo
{
	pgqsHashDataHeader	*header;
	pgqsHashSegment		*activeseg;
} pgqsHashDataInfo;

typedef struct pgqsHashKey
{
	Oid			userid;			/* user OID */
	Oid			dbid;			/* database OID */
	pgqs_queryid queryid;		/* query identifier (if set by another plugin */
	uint32		uniquequalnodeid;	/* Hash of the const */
	uint32		uniquequalid;	/* Hash of the parent, including the consts */
	char		evaltype;		/* Evaluation type. Can be 'f' to mean a qual
								 * executed after a scan, or 'i' for an
								 * indexqual */
} pgqsHashKey;

typedef struct pgqsNames
{
	NameData	rolname;
	NameData	datname;
	NameData	lrelname;
	NameData	lattname;
	NameData	opname;
	NameData	rrelname;
	NameData	rattname;
} pgqsNames;

typedef struct pgqsEntry
{
	pgqsHashKey key;
	Oid			lrelid;			/* LHS relation OID or NULL if not var */
	AttrNumber	lattnum;		/* LHS attribute Number or NULL if not var */
	Oid			opoid;			/* Operator OID */
	Oid			rrelid;			/* RHS relation OID or NULL if not var */
	AttrNumber	rattnum;		/* RHS attribute Number or NULL if not var */
	char		constvalue[PGQS_CONSTANT_SIZE]; /* Textual representation of
												 * the right hand constant, if
												 * any */
	uint32		qualid;			/* Hash of the parent AND expression if any, 0
								 * otherwise. */
	uint32		qualnodeid;		/* Hash of the node itself */

	int64		count;			/* # of operator execution */
	int64		nbfiltered;		/* # of lines discarded by the operator */
	int			position;		/* content position in query text */
	double		usage;			/* # of qual execution, used for deallocation */
	double		min_err_estim[2];	/* min estimation error ratio and num */
	double		max_err_estim[2];	/* max estimation error ratio and num */
	double		mean_err_estim[2];	/* mean estimation error ratio and num */
	double		sum_err_estim[2];	/* sum of variances in estimation error
									 * ratio and num */
	int64		occurences;		/* # of qual execution, 1 per query */
} pgqsEntry;

typedef struct pgqsQualEntry
{
	pgqsHashKey key;
	Oid			lrelid;			/* LHS relation OID or NULL if not var */
	AttrNumber	lattnum;		/* LHS attribute Number or NULL if not var */
	Oid			opoid;			/* Operator OID */
	Oid			rrelid;			/* RHS relation OID or NULL if not var */
	AttrNumber	rattnum;		/* RHS attribute Number or NULL if not var */
	char		constvalue[PGQS_CONSTANT_SIZE]; /* Textual representation of
												 * the right hand constant, if
												 * any */
	uint32		qualid;			/* Hash of the parent AND expression if any, 0
								 * otherwise. */
	uint32		qualnodeid;		/* Hash of the node itself */

	int64		count;			/* # of operator execution */
	int64		nbfiltered;		/* # of lines discarded by the operator */
	int			position;		/* content position in query text */
	double		usage;			/* # of qual execution, used for deallocation */
	double		min_err_estim[2];	/* min estimation error ratio and num */
	double		max_err_estim[2];	/* max estimation error ratio and num */
	double		mean_err_estim[2];	/* mean estimation error ratio and num */
	double		sum_err_estim[2];	/* sum of variances in estimation error
									 * ratio and num */
	int64		occurences;		/* # of qual execution, 1 per query */
} pgqsQualEntry;

typedef struct pgqsEntryWithNames
{
	pgqsEntry	entry;
	pgqsNames	names;
} pgqsEntryWithNames;

typedef struct pgqsQueryStringHashKey
{
	pgqs_queryid queryid;
} pgqsQueryStringHashKey;

typedef struct pgqsQueryStringEntry
{
	pgqsQueryStringHashKey key;
	int					   index;
} pgqsQueryStringEntry;

typedef struct pgqsQueryEntry
{
	pgqs_queryid queryid;
	int			frequency;
	bool		isExplain;		
	int			qrylen;

	/*
	 * Imperatively at the end of the struct This is actually of length
	 * query_size, which is track_activity_query_size
	 */
	char		querytext[1];
} pgqsQueryEntry;

typedef struct pgqsUpdateHashKey
{
	pgqs_queryid 	queryid;	/* query identifier (if set by another plugin */
	Oid				dbid;		/* database oid. */
	Oid				relid;		/* relation OID of the updated column */
	AttrNumber		attnum;		/* attribute Number of the updated column */	
} pgqsUpdateHashKey;

typedef struct pgqsUpdateHashEntry
{
	pgqsUpdateHashKey	key;
	int		 			frequency;		/* frequency of execution */
	int64				updated_rows;	/* total commulative updated rows */
} pgqsUpdateHashEntry;

typedef struct pgqsUpdateEntry
{
	pgqsUpdateHashKey	key;
	int		 			frequency;		/* frequency of execution */
	int64				updated_rows;	/* total commulative updated rows */
} pgqsUpdateEntry;

/* Globals */
extern pgqsSharedState *pgqs;
extern dsa_area *pgqs_dsa;
extern dshash_table *pgqs_dshash;
extern dshash_table *pgqs_query_dshash;
extern dshash_table *pgqs_update_dshash;
extern pgqsHashDataInfo pgqsqualdata;
extern pgqsHashDataInfo pgqsquerydata;
extern pgqsHashDataInfo pgqsupdatedata;
extern int pgqs_query_size;

#define	PGQS_SEG_ENTRIES	10000
#define PGQS_SEG_SZ(entsz)	(PGQSHASHSEGHDRSZ + PGQS_SEG_ENTRIES * (entsz))
#define	PGQS_QRY_ENTSZ		sizeof(pgqsQueryEntry) + pgqs_query_size

extern void *pgqs_data_get_entry(pgqsHashDataInfo *pgqsdata, int index,
								 int entrysz);
#endif
