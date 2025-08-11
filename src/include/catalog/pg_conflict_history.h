/*-------------------------------------------------------------------------
 *
 * pg_conflict_history.h
 *	  logical replication conflict history information
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_conflict_history.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CONFLICT_HISTORT_H
#define PG_CONFLICT_HISTORT_H

#include "catalog/genbki.h"
#include "catalog/pg_conflict_history_d.h"	/* IWYU pragma: export */
#include "utils/snapshot.h"

/* ----------------
 *		pg_conflict_history definition.  cpp turns this into
 *		typedef struct FormData_pg_conflict_history
 * ----------------
 */
CATALOG(pg_conflict_history,1566,ConflictHistoryRelationId)
{
	Oid			subid BKI_LOOKUP(pg_subscription);	/* Oid of subscription */
	Oid			relid BKI_LOOKUP(pg_class);	/* Oid of relation */
	xid			local_xid;	/* local xid at the time of conflict */
	xid			remote_xid; /* remote node xid that produced the conflicting change */
	pg_lsn		local_lsn  BKI_FORCE_NOT_NULL;	/* local lsn at the time of conflict */
	pg_lsn		remote_commit_lsn  BKI_FORCE_NOT_NULL; /* commit lsn of the remote transaction */
#ifdef CATALOG_VARLEN
	timestamptz	local_commit_ts;	/* commit ts of the local tuple */
	timestamptz	remote_commit_ts;	/* commit ts of the remote tuple */
	text		table_schema;		/* name of the schema */
	text		table_name;			/* name of the table */
	text		conflict_type BKI_FORCE_NOT_NULL;	/* conflict type */
	text		origin BKI_DEFAULT(LOGICALREP_ORIGIN_ANY); /* origin of remote tuple */
	json		key_tuple;	/* json representation of the key used for searching */
	json		local_tuple; /* json representation of the local tuple */
	json		remote_tuple; /* json representation of the remote tuple */
#endif
} FormData_pg_conflict_history;

/* ----------------
 *		FormData_pg_conflict_history corresponds to a pointer to a tuple with
 *		the format of pg_conflict_history relation.
 * ----------------
 */
typedef FormData_pg_conflict_history *Form_pg_conflict_history;

DECLARE_INDEX(pg_conflict_history_subid_index, 1568, ConflictHistorySubIndexId, pg_conflict_history, btree(subid oid_ops));

#endif							/* PG_CONFLICT_HISTORT_H */
