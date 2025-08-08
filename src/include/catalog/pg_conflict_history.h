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
#ifdef CATALOG_VARLEN
	timestamptz	remote_commit_ts;
	timestamptz	local_commit_ts;
	text		conflict_type BKI_FORCE_NOT_NULL;
	text		suborigin BKI_DEFAULT(LOGICALREP_ORIGIN_ANY);
	json		ri_key;
	json		remote_tuple;
	json		local_tuple;
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
