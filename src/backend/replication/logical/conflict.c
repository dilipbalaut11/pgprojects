/*-------------------------------------------------------------------------
 * conflict.c
 *	   Support routines for logging conflicts.
 *
 * Copyright (c) 2024-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/conflict.c
 *
 * This file contains the code for logging conflicts on the subscriber during
 * logical replication.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/commit_ts.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/table.h"
#include "catalog/pg_namespace_d.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "pgstat.h"
#include "replication/conflict.h"
#include "replication/worker_internal.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

static const char *const ConflictTypeNames[] = {
	[CT_INSERT_EXISTS] = "insert_exists",
	[CT_UPDATE_ORIGIN_DIFFERS] = "update_origin_differs",
	[CT_UPDATE_EXISTS] = "update_exists",
	[CT_UPDATE_MISSING] = "update_missing",
	[CT_DELETE_ORIGIN_DIFFERS] = "delete_origin_differs",
	[CT_DELETE_MISSING] = "delete_missing",
	[CT_MULTIPLE_UNIQUE_CONFLICTS] = "multiple_unique_conflicts"
};

static int	errcode_apply_conflict(ConflictType type);
static void errdetail_apply_conflict(EState *estate,
									 ResultRelInfo *relinfo,
									 ConflictType type,
									 TupleTableSlot *searchslot,
									 TupleTableSlot *localslot,
									 TupleTableSlot *remoteslot,
									 Oid indexoid, TransactionId localxmin,
									 RepOriginId localorigin,
									 TimestampTz localts, StringInfo err_msg);
static char *build_tuple_value_details(EState *estate, ResultRelInfo *relinfo,
									   ConflictType type,
									   TupleTableSlot *searchslot,
									   TupleTableSlot *localslot,
									   TupleTableSlot *remoteslot,
									   Oid indexoid);
static char *build_index_value_desc(EState *estate, Relation localrel,
									TupleTableSlot *slot, Oid indexoid);

/*
 * Get the xmin and commit timestamp data (origin and timestamp) associated
 * with the provided local tuple.
 *
 * Return true if the commit timestamp data was found, false otherwise.
 */
bool
GetTupleTransactionInfo(TupleTableSlot *localslot, TransactionId *xmin,
						RepOriginId *localorigin, TimestampTz *localts)
{
	Datum		xminDatum;
	bool		isnull;

	xminDatum = slot_getsysattr(localslot, MinTransactionIdAttributeNumber,
								&isnull);
	*xmin = DatumGetTransactionId(xminDatum);
	Assert(!isnull);

	/*
	 * The commit timestamp data is not available if track_commit_timestamp is
	 * disabled.
	 */
	if (!track_commit_timestamp)
	{
		*localorigin = InvalidRepOriginId;
		*localts = 0;
		return false;
	}

	return TransactionIdGetCommitTsData(*xmin, localts, localorigin);
}


/*
 * Assuming these are defined globally or in an appropriate context for the apply worker
 */
static SPIPlanPtr conflict_log_insert_plan = NULL;
static Oid          conflict_log_insert_argtypes[13];


/*
 * Helper function to convert a TupleTableSlot to Jsonb
 *
 * This would be a new internal helper function for logical replication
 * Needs to handle various data types and potentially TOASTed data
 */
static Datum
TupleTableSlotToJsonDatum(TupleTableSlot *slot)
{
	HeapTuple	tuple = ExecCopySlotHeapTuple(slot);
	Datum		datum = heap_copy_tuple_as_datum(tuple, slot->tts_tupleDescriptor);
	Datum		json;

	if (TupIsNull(slot))
		return 0;

	json = DirectFunctionCall1(row_to_json, datum);
	heap_freetuple(tuple);

	return json;
}

/*
 * Function to initialize/get the prepared plan for conflict logging INSERT
 */
static SPIPlanPtr
GetConflictLogInsertPlan(void)
{
	if (conflict_log_insert_plan == NULL)
	{
		const char *command = "INSERT INTO public.conflict_log_table "
				"(subscription_name, schema_name, table_name, conflict_type, "
				"operation_type, replication_origin, "
				"publisher_commit_time, replica_identity_key, "
				"old_data, new_data, conflict_details) "
				"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";

		int			nargs = 11;

		/* Define argument types */
		conflict_log_insert_argtypes[0] = TEXTOID;
		conflict_log_insert_argtypes[1] = TEXTOID;
		conflict_log_insert_argtypes[2] = TEXTOID;
		conflict_log_insert_argtypes[3] = TEXTOID;
		conflict_log_insert_argtypes[4] = TEXTOID;
		conflict_log_insert_argtypes[5] = TEXTOID;
		conflict_log_insert_argtypes[6] = TIMESTAMPTZOID;
		conflict_log_insert_argtypes[7] = JSONOID;  /* replica_identity_key */
		conflict_log_insert_argtypes[8] = JSONOID; /* old_data */
		conflict_log_insert_argtypes[9] = JSONOID; /* new_data */
		conflict_log_insert_argtypes[10] = TEXTOID;  /* conflict_details */

		/* Prepare the plan */
		if (SPI_connect() != SPI_OK_CONNECT)
		{
			ereport(WARNING, (errmsg("could not connect to SPI for "
									 "conflict logging")));
			return NULL;
		}

		conflict_log_insert_plan = SPI_prepare(command, nargs,
											   conflict_log_insert_argtypes);
		if (conflict_log_insert_plan == NULL)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("could not prepare plan for conflict logging: %s",
							SPI_result_code_string(SPI_result))));
			SPI_finish();
			return NULL;
		}
		SPI_keepplan(conflict_log_insert_plan); /* Keep the plan for reuse */
		SPI_finish();
	}
	return conflict_log_insert_plan;
}


/*
 * LogApplyConflictToTable
 *
 * Logs details about a logical replication conflict to a user-defined table.
 *
 * Parameters:
 * subid: OID of the subscription.
 * subname: Name of the subscription.
 * reloid: OID of the conflicted table.
 * schemaname: Schema name of the conflicted table.
 * relname: Relation name of the conflicted table.
 * conflict_type: Type of conflict (e.g., 'duplicate_key').
 * operation_type: DML operation type ('INSERT', 'UPDATE', 'DELETE').
 * origin_name: Name of the replication origin.
 * publisher_lsn: LSN from the publisher.
 * publisher_commit_ts: Commit timestamp from the publisher.
 * local_xid: Local transaction ID of the apply worker.
 * searchslot: Tuple found on subscriber during search
 * (local old data for UPDATE/DELETE, existing row for INSERT).
 * localslot: The original local tuple state
 * (before remote change attempt).
 * remoteslot: The incoming tuple from the publisher
 * (remote new data for INSERT/UPDATE, old for DELETE).
 */
static void
LogApplyConflictToTable(Oid subid, const char *subname, Oid reloid,
						const char *schemaname, const char *relname,
						ConflictType conflict_type, CmdType operation_type,
						const char *origin_name, XLogRecPtr publisher_lsn,
						TimestampTz commit_ts, TupleTableSlot *searchslot,
						TupleTableSlot *localslot, TupleTableSlot *remoteslot)
{
	SPIPlanPtr	plan;
	int			ret;
	int			i;
	Oid			relid;
	Datum		values[11];
	//char		nulls[11];
	bool		nulls[11];
	char	   *conflict_type_str;
	char	   *operation_type_str;
	char	   *conflict_details_str;
	TransactionId local_xid;
	Datum		replica_identity;
	Datum		old_data;
	Datum		new_data;
	Relation	rel;
	HeapTuple	tup;
	ParamListInfoData params_data;
	SPIExecuteOptions options;

	/* Get the prepared plan */
	plan = GetConflictLogInsertPlan();
	if (plan == NULL)
	{
		/* Error already reported in GetConflictLogInsertPlan */
		return;
	}

	/* Map enums/types to text strings */
	//conflict_type_str = GetConflictTypeString(conflict_type);  /* Assume this helper exists */
	//operation_type_str = GetOperationTypeString(operation_type); /* Assume this helper exists */

	/* Convert TupleTableSlots to JSONB */
	/* The logic here will depend on the conflict type and operation type */

	/* Populate values and nulls arrays */
	memset(nulls, 0, sizeof(bool) * 11);
	memset(values, 0, sizeof(Datum) * 11);

	values[0] = CStringGetTextDatum(subname);
	values[1] = CStringGetTextDatum(schemaname);
	values[2] = CStringGetTextDatum(relname);
	values[3] = CStringGetTextDatum(conflict_type_str);
	values[4] = CStringGetTextDatum(operation_type_str);
	if (origin_name)
		values[5] = CStringGetTextDatum(origin_name);
	else
		nulls[5] = true;
	//values[6] = LSNGetDatum(publisher_lsn);
	values[6] = TimestampTzGetDatum(commit_ts);
	nulls[6] = true;

	if (searchslot != NULL)
		values[7] = TupleTableSlotToJsonDatum(searchslot);
	else
		nulls[7] = true;

	if (localslot != NULL)
		values[8] = TupleTableSlotToJsonDatum(localslot);
	else
		nulls[8] = true;
	if (remoteslot != NULL)
		values[9] = TupleTableSlotToJsonDatum(remoteslot);
	else
		nulls[9] = true;

	values[10] = CStringGetTextDatum(conflict_details_str);

	/* Build conflict_details_str */
	/*
	 * This part should be more sophisticated, using the contents of the slots
	 * to provide a rich description.
	 */
	//local_xid = 100; //FIXME
	conflict_details_str = "test_string";
/*	psprintf("Conflict type: %s, Operation: %s, "
									"Table: %s.%s. Local XID: %u. "
									"Publisher LSN: %X/%X.",
									conflict_type_str, operation_type_str,
									schemaname, relname, local_xid,
									(uint32)(publisher_lsn >> 32),
									(uint32)publisher_lsn); */

	relid = get_relname_relid("conflict_log_table", PG_PUBLIC_NAMESPACE);
	rel = table_open(relid, RowExclusiveLock);
	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	simple_heap_insert(rel, tup);
	table_close(rel, RowExclusiveLock);

#if 0
	/* Use SPI to execute INSERT */
	if (SPI_connect() != SPI_OK_CONNECT)
	{
		ereport(WARNING, (errmsg("could not connect to SPI for "
								 "conflict logging")));
		goto cleanup;
	}

	/* Prepare ParamListInfo for SPI_execute_plan_extended */

#if 0
	memset(&params_data, 0, sizeof(params_data));

	params_data.numParams = 11;
	for (i = 0; i < 11; i++)
	{
		params_data.params[i].ptype = conflict_log_insert_argtypes[i];
		params_data.params[i].value = values[i];
		params_data.params[i].isnull = nulls[i];
	}

	memset(&options, 0, sizeof(options));
	options.params = &params_data; /* Pass the ParamListInfo */
	options.read_only = false;
	options.tcount = 0; /* No limit on rows to return (for INSERT, typically 0/1) */

	ret = SPI_execute_plan_extended(plan, &options);
#endif
	ret = SPI_execp(plan, values, nulls, 0);

	if (ret != SPI_OK_INSERT)
	{
		ereport(WARNING, (errmsg("could not insert into conflict_log_table: %s",
								 SPI_result_code_string(ret))));
	}

	SPI_finish();

cleanup:
	/* Free palloc'd memory: strings, Jsonb objects */
//	if (conflict_type_str)
//		pfree(conflict_type_str);
//	if (operation_type_str)
//		pfree(operation_type_str);
//	if (conflict_details_str)
//		pfree(conflict_details_str);
//	if (params_array->params) /* Free dynamically allocated params */
//		pfree(params_array->params);
#endif
}

/*
 * This function is used to report a conflict while applying replication
 * changes.
 *
 * 'searchslot' should contain the tuple used to search the local tuple to be
 * updated or deleted.
 *
 * 'remoteslot' should contain the remote new tuple, if any.
 *
 * conflicttuples is a list of local tuples that caused the conflict and the
 * conflict related information. See ConflictTupleInfo.
 *
 * The caller must ensure that all the indexes passed in ConflictTupleInfo are
 * locked so that we can fetch and display the conflicting key values.
 */
void
ReportApplyConflict(EState *estate, ResultRelInfo *relinfo, int elevel,
					ConflictType type, TupleTableSlot *searchslot,
					TupleTableSlot *remoteslot, List *conflicttuples)
{
	Relation	localrel = relinfo->ri_RelationDesc;
	StringInfoData err_detail;

	initStringInfo(&err_detail);

	/* Form errdetail message by combining conflicting tuples information. */
	foreach_ptr(ConflictTupleInfo, conflicttuple, conflicttuples)
		errdetail_apply_conflict(estate, relinfo, type, searchslot,
								 conflicttuple->slot, remoteslot,
								 conflicttuple->indexoid,
								 conflicttuple->xmin,
								 conflicttuple->origin,
								 conflicttuple->ts,
								 &err_detail);

	pgstat_report_subscription_conflict(MySubscription->oid, type);

	ereport(elevel,
			errcode_apply_conflict(type),
			errmsg("conflict detected on relation \"%s.%s\": conflict=%s",
				   get_namespace_name(RelationGetNamespace(localrel)),
				   RelationGetRelationName(localrel),
				   ConflictTypeNames[type]),
			errdetail_internal("%s", err_detail.data));

	{
		int i=1;
//		while(i);
	}
	LogApplyConflictToTable(1, "test_sub", RelationGetRelid(relinfo->ri_RelationDesc),
							"schema", relinfo->ri_RelationDesc->rd_rel->relname.data, type,
							CMD_INSERT, "origin_name_1", 0, 0, searchslot,
							NULL, remoteslot);
}

/*
 * Find all unique indexes to check for a conflict and store them into
 * ResultRelInfo.
 */
void
InitConflictIndexes(ResultRelInfo *relInfo)
{
	List	   *uniqueIndexes = NIL;

	for (int i = 0; i < relInfo->ri_NumIndices; i++)
	{
		Relation	indexRelation = relInfo->ri_IndexRelationDescs[i];

		if (indexRelation == NULL)
			continue;

		/* Detect conflict only for unique indexes */
		if (!relInfo->ri_IndexRelationInfo[i]->ii_Unique)
			continue;

		/* Don't support conflict detection for deferrable index */
		if (!indexRelation->rd_index->indimmediate)
			continue;

		uniqueIndexes = lappend_oid(uniqueIndexes,
									RelationGetRelid(indexRelation));
	}

	relInfo->ri_onConflictArbiterIndexes = uniqueIndexes;
}

/*
 * Add SQLSTATE error code to the current conflict report.
 */
static int
errcode_apply_conflict(ConflictType type)
{
	switch (type)
	{
		case CT_INSERT_EXISTS:
		case CT_UPDATE_EXISTS:
		case CT_MULTIPLE_UNIQUE_CONFLICTS:
			return errcode(ERRCODE_UNIQUE_VIOLATION);
		case CT_UPDATE_ORIGIN_DIFFERS:
		case CT_UPDATE_MISSING:
		case CT_DELETE_ORIGIN_DIFFERS:
		case CT_DELETE_MISSING:
			return errcode(ERRCODE_T_R_SERIALIZATION_FAILURE);
	}

	Assert(false);
	return 0;					/* silence compiler warning */
}

/*
 * Add an errdetail() line showing conflict detail.
 *
 * The DETAIL line comprises of two parts:
 * 1. Explanation of the conflict type, including the origin and commit
 *    timestamp of the existing local tuple.
 * 2. Display of conflicting key, existing local tuple, remote new tuple, and
 *    replica identity columns, if any. The remote old tuple is excluded as its
 *    information is covered in the replica identity columns.
 */
static void
errdetail_apply_conflict(EState *estate, ResultRelInfo *relinfo,
						 ConflictType type, TupleTableSlot *searchslot,
						 TupleTableSlot *localslot, TupleTableSlot *remoteslot,
						 Oid indexoid, TransactionId localxmin,
						 RepOriginId localorigin, TimestampTz localts,
						 StringInfo err_msg)
{
	StringInfoData err_detail;
	char	   *val_desc;
	char	   *origin_name;

	initStringInfo(&err_detail);

	/* First, construct a detailed message describing the type of conflict */
	switch (type)
	{
		case CT_INSERT_EXISTS:
		case CT_UPDATE_EXISTS:
		case CT_MULTIPLE_UNIQUE_CONFLICTS:
			Assert(OidIsValid(indexoid) &&
				   CheckRelationOidLockedByMe(indexoid, RowExclusiveLock, true));

			if (localts)
			{
				if (localorigin == InvalidRepOriginId)
					appendStringInfo(&err_detail, _("Key already exists in unique index \"%s\", modified locally in transaction %u at %s."),
									 get_rel_name(indexoid),
									 localxmin, timestamptz_to_str(localts));
				else if (replorigin_by_oid(localorigin, true, &origin_name))
					appendStringInfo(&err_detail, _("Key already exists in unique index \"%s\", modified by origin \"%s\" in transaction %u at %s."),
									 get_rel_name(indexoid), origin_name,
									 localxmin, timestamptz_to_str(localts));

				/*
				 * The origin that modified this row has been removed. This
				 * can happen if the origin was created by a different apply
				 * worker and its associated subscription and origin were
				 * dropped after updating the row, or if the origin was
				 * manually dropped by the user.
				 */
				else
					appendStringInfo(&err_detail, _("Key already exists in unique index \"%s\", modified by a non-existent origin in transaction %u at %s."),
									 get_rel_name(indexoid),
									 localxmin, timestamptz_to_str(localts));
			}
			else
				appendStringInfo(&err_detail, _("Key already exists in unique index \"%s\", modified in transaction %u."),
								 get_rel_name(indexoid), localxmin);

			break;

		case CT_UPDATE_ORIGIN_DIFFERS:
			if (localorigin == InvalidRepOriginId)
				appendStringInfo(&err_detail, _("Updating the row that was modified locally in transaction %u at %s."),
								 localxmin, timestamptz_to_str(localts));
			else if (replorigin_by_oid(localorigin, true, &origin_name))
				appendStringInfo(&err_detail, _("Updating the row that was modified by a different origin \"%s\" in transaction %u at %s."),
								 origin_name, localxmin, timestamptz_to_str(localts));

			/* The origin that modified this row has been removed. */
			else
				appendStringInfo(&err_detail, _("Updating the row that was modified by a non-existent origin in transaction %u at %s."),
								 localxmin, timestamptz_to_str(localts));

			break;

		case CT_UPDATE_MISSING:
			appendStringInfoString(&err_detail, _("Could not find the row to be updated."));
			break;

		case CT_DELETE_ORIGIN_DIFFERS:
			if (localorigin == InvalidRepOriginId)
				appendStringInfo(&err_detail, _("Deleting the row that was modified locally in transaction %u at %s."),
								 localxmin, timestamptz_to_str(localts));
			else if (replorigin_by_oid(localorigin, true, &origin_name))
				appendStringInfo(&err_detail, _("Deleting the row that was modified by a different origin \"%s\" in transaction %u at %s."),
								 origin_name, localxmin, timestamptz_to_str(localts));

			/* The origin that modified this row has been removed. */
			else
				appendStringInfo(&err_detail, _("Deleting the row that was modified by a non-existent origin in transaction %u at %s."),
								 localxmin, timestamptz_to_str(localts));

			break;

		case CT_DELETE_MISSING:
			appendStringInfoString(&err_detail, _("Could not find the row to be deleted."));
			break;
	}

	Assert(err_detail.len > 0);

	val_desc = build_tuple_value_details(estate, relinfo, type, searchslot,
										 localslot, remoteslot, indexoid);

	/*
	 * Next, append the key values, existing local tuple, remote tuple and
	 * replica identity columns after the message.
	 */
	if (val_desc)
		appendStringInfo(&err_detail, "\n%s", val_desc);

	/*
	 * Insert a blank line to visually separate the new detail line from the
	 * existing ones.
	 */
	if (err_msg->len > 0)
		appendStringInfoChar(err_msg, '\n');

	appendStringInfoString(err_msg, err_detail.data);
}

/*
 * Helper function to build the additional details for conflicting key,
 * existing local tuple, remote tuple, and replica identity columns.
 *
 * If the return value is NULL, it indicates that the current user lacks
 * permissions to view the columns involved.
 */
static char *
build_tuple_value_details(EState *estate, ResultRelInfo *relinfo,
						  ConflictType type,
						  TupleTableSlot *searchslot,
						  TupleTableSlot *localslot,
						  TupleTableSlot *remoteslot,
						  Oid indexoid)
{
	Relation	localrel = relinfo->ri_RelationDesc;
	Oid			relid = RelationGetRelid(localrel);
	TupleDesc	tupdesc = RelationGetDescr(localrel);
	StringInfoData tuple_value;
	char	   *desc = NULL;

	Assert(searchslot || localslot || remoteslot);

	initStringInfo(&tuple_value);

	/*
	 * Report the conflicting key values in the case of a unique constraint
	 * violation.
	 */
	if (type == CT_INSERT_EXISTS || type == CT_UPDATE_EXISTS ||
		type == CT_MULTIPLE_UNIQUE_CONFLICTS)
	{
		Assert(OidIsValid(indexoid) && localslot);

		desc = build_index_value_desc(estate, localrel, localslot, indexoid);

		if (desc)
			appendStringInfo(&tuple_value, _("Key %s"), desc);
	}

	if (localslot)
	{
		/*
		 * The 'modifiedCols' only applies to the new tuple, hence we pass
		 * NULL for the existing local tuple.
		 */
		desc = ExecBuildSlotValueDescription(relid, localslot, tupdesc,
											 NULL, 64);

		if (desc)
		{
			if (tuple_value.len > 0)
			{
				appendStringInfoString(&tuple_value, "; ");
				appendStringInfo(&tuple_value, _("existing local tuple %s"),
								 desc);
			}
			else
			{
				appendStringInfo(&tuple_value, _("Existing local tuple %s"),
								 desc);
			}
		}
	}

	if (remoteslot)
	{
		Bitmapset  *modifiedCols;

		/*
		 * Although logical replication doesn't maintain the bitmap for the
		 * columns being inserted, we still use it to create 'modifiedCols'
		 * for consistency with other calls to ExecBuildSlotValueDescription.
		 *
		 * Note that generated columns are formed locally on the subscriber.
		 */
		modifiedCols = bms_union(ExecGetInsertedCols(relinfo, estate),
								 ExecGetUpdatedCols(relinfo, estate));
		desc = ExecBuildSlotValueDescription(relid, remoteslot, tupdesc,
											 modifiedCols, 64);

		if (desc)
		{
			if (tuple_value.len > 0)
			{
				appendStringInfoString(&tuple_value, "; ");
				appendStringInfo(&tuple_value, _("remote tuple %s"), desc);
			}
			else
			{
				appendStringInfo(&tuple_value, _("Remote tuple %s"), desc);
			}
		}
	}

	if (searchslot)
	{
		/*
		 * Note that while index other than replica identity may be used (see
		 * IsIndexUsableForReplicaIdentityFull for details) to find the tuple
		 * when applying update or delete, such an index scan may not result
		 * in a unique tuple and we still compare the complete tuple in such
		 * cases, thus such indexes are not used here.
		 */
		Oid			replica_index = GetRelationIdentityOrPK(localrel);

		Assert(type != CT_INSERT_EXISTS);

		/*
		 * If the table has a valid replica identity index, build the index
		 * key value string. Otherwise, construct the full tuple value for
		 * REPLICA IDENTITY FULL cases.
		 */
		if (OidIsValid(replica_index))
			desc = build_index_value_desc(estate, localrel, searchslot, replica_index);
		else
			desc = ExecBuildSlotValueDescription(relid, searchslot, tupdesc, NULL, 64);

		if (desc)
		{
			if (tuple_value.len > 0)
			{
				appendStringInfoString(&tuple_value, "; ");
				appendStringInfo(&tuple_value, OidIsValid(replica_index)
								 ? _("replica identity %s")
								 : _("replica identity full %s"), desc);
			}
			else
			{
				appendStringInfo(&tuple_value, OidIsValid(replica_index)
								 ? _("Replica identity %s")
								 : _("Replica identity full %s"), desc);
			}
		}
	}

	if (tuple_value.len == 0)
		return NULL;

	appendStringInfoChar(&tuple_value, '.');
	return tuple_value.data;
}

/*
 * Helper functions to construct a string describing the contents of an index
 * entry. See BuildIndexValueDescription for details.
 *
 * The caller must ensure that the index with the OID 'indexoid' is locked so
 * that we can fetch and display the conflicting key value.
 */
static char *
build_index_value_desc(EState *estate, Relation localrel, TupleTableSlot *slot,
					   Oid indexoid)
{
	char	   *index_value;
	Relation	indexDesc;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	TupleTableSlot *tableslot = slot;

	if (!tableslot)
		return NULL;

	Assert(CheckRelationOidLockedByMe(indexoid, RowExclusiveLock, true));

	indexDesc = index_open(indexoid, NoLock);

	/*
	 * If the slot is a virtual slot, copy it into a heap tuple slot as
	 * FormIndexDatum only works with heap tuple slots.
	 */
	if (TTS_IS_VIRTUAL(slot))
	{
		tableslot = table_slot_create(localrel, &estate->es_tupleTable);
		tableslot = ExecCopySlot(tableslot, slot);
	}

	/*
	 * Initialize ecxt_scantuple for potential use in FormIndexDatum when
	 * index expressions are present.
	 */
	GetPerTupleExprContext(estate)->ecxt_scantuple = tableslot;

	/*
	 * The values/nulls arrays passed to BuildIndexValueDescription should be
	 * the results of FormIndexDatum, which are the "raw" input to the index
	 * AM.
	 */
	FormIndexDatum(BuildIndexInfo(indexDesc), tableslot, estate, values, isnull);

	index_value = BuildIndexValueDescription(indexDesc, values, isnull);

	index_close(indexDesc, NoLock);

	return index_value;
}
