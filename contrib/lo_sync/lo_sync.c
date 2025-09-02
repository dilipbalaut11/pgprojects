/*
 *	PostgreSQL definitions for managed Large Objects.
 *
 *	contrib/losync/lo_sync.c
 *
 */

#include "postgres.h"

#include "catalog/pg_subscription_rel.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "utils/fmgrprotos.h"
#include "utils/rel.h"

PG_MODULE_MAGIC_EXT(
					.name = "lo_sync",
					.version = PG_VERSION
);


/*
 * This is the trigger that protects us from orphaned large objects
 */
PG_FUNCTION_INFO_V1(lo_get_info);

Datum
lo_get_info(PG_FUNCTION_ARGS)
{
	
}

#if 0
static void
LogicalRepSyncSequences(void)
{
	Oid			subid = PG_GETARG_OID(0);
	StringInfoData app_name;
	Relation	rel;

	StartTransactionCommand();

	rel = table_open(SubscriptionRelRelationId, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_pg_subscription_rel_srsubid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));

	ScanKeyInit(&skey[1],
				Anum_pg_subscription_rel_srsubstate,
				BTEqualStrategyNumber, F_CHAREQ,
				CharGetDatum(SUBREL_STATE_INIT));

	scan = systable_beginscan(rel, InvalidOid, false,
							  NULL, 2, skey);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_subscription_rel subrel;
		char		relkind;
		Relation	sequence_rel;
		LogicalRepSequenceInfo *seq_info;
		char	   *nspname;
		char	   *seqname;
		MemoryContext oldctx;

		CHECK_FOR_INTERRUPTS();

		subrel = (Form_pg_subscription_rel) GETSTRUCT(tup);

		/* Relation is either a sequence or a table */
		relkind = get_rel_relkind(subrel->srrelid);
		if (relkind != RELKIND_SEQUENCE)
			continue;

		/* Skip if sequence was dropped concurrently */
		sequence_rel = try_table_open(subrel->srrelid, RowExclusiveLock);
		if (!sequence_rel)
			continue;

		seqname = RelationGetRelationName(sequence_rel);
		nspname = get_namespace_name(RelationGetNamespace(sequence_rel));

		/* Allocate the tracking info in a permanent memory context. */
		oldctx = MemoryContextSwitchTo(CacheMemoryContext);

		seq_info = (LogicalRepSequenceInfo *) palloc(sizeof(LogicalRepSequenceInfo));
		seq_info->seqname = pstrdup(seqname);
		seq_info->nspname = pstrdup(nspname);
		seq_info->localrelid = subrel->srrelid;
		seq_info->remote_seq_fetched = false;
		seq_info->seqowner = sequence_rel->rd_rel->relowner;
		sequences_to_copy = lappend(sequences_to_copy, seq_info);

		MemoryContextSwitchTo(oldctx);

		table_close(sequence_rel, RowExclusiveLock);
	}

	/* Cleanup */
	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	CommitTransactionCommand();

	/* Is the use of a password mandatory? */
	must_use_password = MySubscription->passwordrequired &&
		!MySubscription->ownersuperuser;

	initStringInfo(&app_name);
	appendStringInfo(&app_name, "%s_%s", MySubscription->name, "sequencesync worker");

	/*
	 * Establish the connection to the publisher for sequence synchronization.
	 */
	LogRepWorkerWalRcvConn =
		walrcv_connect(MySubscription->conninfo, true, true,
					   must_use_password,
					   app_name.data, &err);
	if (LogRepWorkerWalRcvConn == NULL)
		ereport(ERROR,
				errcode(ERRCODE_CONNECTION_FAILURE),
				errmsg("sequencesync worker for subscription \"%s\" could not connect to the publisher: %s",
					   MySubscription->name, err));

	pfree(app_name.data);

	copy_sequences(LogRepWorkerWalRcvConn, sequences_to_copy, subid);

	foreach_ptr(LogicalRepSequenceInfo, seq_info, sequences_to_copy)
	{
		pfree(seq_info->seqname);
		pfree(seq_info->nspname);

		sequences_to_copy = foreach_delete_current(sequences_to_copy, seq_info);
	}

	list_free(sequences_to_copy);
}
}
#endif
