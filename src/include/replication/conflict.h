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
#include "datatype/timestamp.h"
#include "nodes/pg_list.h"

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
 * Information for the local row that caused the conflict.
 */
typedef struct ConflictTupleInfo
{
	TupleTableSlot *slot;		/* tuple slot holding the conflicting local
								 * tuple */
	Oid			indexoid;		/* OID of the index where the conflict
								 * occurred */
	TransactionId xmin;			/* transaction ID of the modification causing
								 * the conflict */
	ReplOriginId origin;		/* origin identifier of the modification */
	TimestampTz ts;				/* timestamp of when the modification on the
								 * conflicting local row occurred */
} ConflictTupleInfo;

/*
 * Defines where logical replication conflict details are recorded.
 *
 * While stored as a text-based array/string in
 * pg_subscription.subconflictlogdest for user readability and extensibility,
 * we map these to an internal enum to allow for efficient checks.
 */
typedef enum ConflictLogDest
{
	CONFLICT_LOG_DEST_LOG = 0,	/* Emit to server logs */
	CONFLICT_LOG_DEST_TABLE,	/* Insert into the conflict log table */
	CONFLICT_LOG_DEST_ALL		/* Both log and table */
} ConflictLogDest;

/*
 * Array mapping for converting internal enum to string.
 */
extern PGDLLIMPORT const char *const ConflictLogDestNames[];

/* Structure to hold metadata for one column of the conflict log table */
typedef struct ConflictLogColumnDef
{
	const char *attname;    /* Column name */
	Oid         atttypid;   /* Data type OID */
} ConflictLogColumnDef;

/* The single source of truth for the conflict log table schema */
extern PGDLLIMPORT const ConflictLogColumnDef ConflictLogSchema[];

#define MAX_CONFLICT_ATTR_NUM 11

extern bool GetTupleTransactionInfo(TupleTableSlot *localslot,
									TransactionId *xmin,
									ReplOriginId *localorigin,
									TimestampTz *localts);
extern void ReportApplyConflict(EState *estate, ResultRelInfo *relinfo,
								int elevel, ConflictType type,
								TupleTableSlot *searchslot,
								TupleTableSlot *remoteslot,
								List *conflicttuples);
extern void InitConflictIndexes(ResultRelInfo *relInfo);
extern Relation GetConflictLogDestAndTable(ConflictLogDest *log_dest);
extern void InsertConflictLogTuple(Relation conflictlogrel);
#endif
