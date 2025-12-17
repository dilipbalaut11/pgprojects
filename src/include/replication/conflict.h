/*-------------------------------------------------------------------------
 * conflict.h
 *	   Exports for conflicts logging.
 *
 * Copyright (c) 2024-2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONFLICT_H
#define CONFLICT_H

#include "access/xlogdefs.h"
#include "catalog/pg_type.h"
#include "nodes/pg_list.h"
#include "utils/timestamp.h"

/* Avoid including execnodes.h here */
typedef struct EState EState;
typedef struct ResultRelInfo ResultRelInfo;
typedef struct TupleTableSlot TupleTableSlot;


/*
 * Conflict types that could occur while applying remote changes.
 *
 * This enum is used in statistics collection (see
 * PgStat_StatSubEntry::conflict_count and
 * PgStat_BackendSubEntry::conflict_count) as well, therefore, when adding new
 * values or reordering existing ones, ensure to review and potentially adjust
 * the corresponding statistics collection codes.
 */
typedef enum
{
	/* The row to be inserted violates unique constraint */
	CT_INSERT_EXISTS,

	/* The row to be updated was modified by a different origin */
	CT_UPDATE_ORIGIN_DIFFERS,

	/* The updated row value violates unique constraint */
	CT_UPDATE_EXISTS,

	/* The row to be updated was concurrently deleted by a different origin */
	CT_UPDATE_DELETED,

	/* The row to be updated is missing */
	CT_UPDATE_MISSING,

	/* The row to be deleted was modified by a different origin */
	CT_DELETE_ORIGIN_DIFFERS,

	/* The row to be deleted is missing */
	CT_DELETE_MISSING,

	/* The row to be inserted/updated violates multiple unique constraint */
	CT_MULTIPLE_UNIQUE_CONFLICTS,

	/*
	 * Other conflicts, such as exclusion constraint violations, involve more
	 * complex rules than simple equality checks. These conflicts are left for
	 * future improvements.
	 */
} ConflictType;

#define CONFLICT_NUM_TYPES (CT_MULTIPLE_UNIQUE_CONFLICTS + 1)

/*
 * Information for the existing local row that caused the conflict.
 */
typedef struct ConflictTupleInfo
{
	TupleTableSlot *slot;		/* tuple slot holding the conflicting local
								 * tuple */
	Oid			indexoid;		/* OID of the index where the conflict
								 * occurred */
	TransactionId xmin;			/* transaction ID of the modification causing
								 * the conflict */
	RepOriginId origin;			/* origin identifier of the modification */
	TimestampTz ts;				/* timestamp of when the modification on the
								 * conflicting local row occurred */
} ConflictTupleInfo;

/*
 * Conflict log destination types.
 *
 * These values are defined as bitmask flags to allow for multiple simultaneous
 * logging destinations (e.g., logging to both system logs and a table).
 * Internally, we use these for bitwise comparisons (IsSet), but the string
 * representation is stored in pg_subscription.subconflictlogdest.
 */
typedef enum ConflictLogDest
{
	/* Log conflicts to the server logs */
	CONFLICT_LOG_DEST_LOG   = 1 << 0,   /* 0x01 */

	/* Log conflicts to an internally managed table */
	CONFLICT_LOG_DEST_TABLE = 1 << 1,   /* 0x02 */

	/* Convenience flag for all supported destinations */
	CONFLICT_LOG_DEST_ALL   = (CONFLICT_LOG_DEST_LOG | CONFLICT_LOG_DEST_TABLE)
} ConflictLogDest;

/*
 * Array mapping for converting internal enum to string.
 */
static const char *const ConflictLogDestNames[] = {
	[CONFLICT_LOG_DEST_LOG] = "log",
	[CONFLICT_LOG_DEST_TABLE] = "table",
	[CONFLICT_LOG_DEST_ALL] = "all"
};

/* Structure to hold metadata for one column of the conflict log table */
typedef struct ConflictLogColumnDef
{
	const char *attname;    /* Column name */
	Oid         atttypid;   /* Data type OID */
} ConflictLogColumnDef;

/* The single source of truth for the conflict log table schema */
static const ConflictLogColumnDef ConflictLogSchema[] =
{
	{ .attname = "relid",            .atttypid = OIDOID },
	{ .attname = "schemaname",       .atttypid = TEXTOID },
	{ .attname = "relname",          .atttypid = TEXTOID },
	{ .attname = "conflict_type",    .atttypid = TEXTOID },
	{ .attname = "remote_xid",       .atttypid = XIDOID },
	{ .attname = "remote_commit_lsn",.atttypid = LSNOID },
	{ .attname = "remote_commit_ts", .atttypid = TIMESTAMPTZOID },
	{ .attname = "remote_origin",    .atttypid = TEXTOID },
	{ .attname = "replica_identity", .atttypid = JSONOID },
	{ .attname = "remote_tuple",     .atttypid = JSONOID },
	{ .attname = "local_conflicts",  .atttypid = JSONARRAYOID }
};

#define MAX_CONFLICT_ATTR_NUM lengthof(ConflictLogSchema)

extern bool GetTupleTransactionInfo(TupleTableSlot *localslot,
									TransactionId *xmin,
									RepOriginId *localorigin,
									TimestampTz *localts);
extern void ReportApplyConflict(EState *estate, ResultRelInfo *relinfo,
								int elevel, ConflictType type,
								TupleTableSlot *searchslot,
								TupleTableSlot *remoteslot,
								List *conflicttuples);
extern void InitConflictIndexes(ResultRelInfo *relInfo);
#endif
