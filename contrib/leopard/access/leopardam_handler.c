/*-------------------------------------------------------------------------
 *
 * leopardam_handler.c
 *	  leopard table access method code
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
 *
 * IDENTIFICATION
 *	  src/backend/access/leopard/leopardam_handler.c
 *
 *
 * NOTES
 *	  This files wires up the lower level leopardam.c et al routines with the
 *	  tableam abstraction.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/leopardam.h"
#include "access/leopardtoast.h"
#include "access/multixact.h"
#include "access/leopardrewrite.h"
#include "access/syncscan.h"
#include "access/tableam.h"
#include "access/tsmapi.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/progress.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "access/leopard_tableam.h"

static void reform_and_rewrite_tuple(LeopardTuple tuple,
									 Relation OldLeopard, Relation NewLeopard,
									 Datum *values, bool *isnull, RewriteState rwstate);

static bool SampleLeopardTupleVisible(TableScanDesc scan, Buffer buffer,
								   LeopardTuple tuple,
								   OffsetNumber tupoffset);

static BlockNumber leopardam_scan_get_blocks_done(LeopardScanDesc hscan);

static const TableAmRoutine leopardam_methods;


/* ------------------------------------------------------------------------
 * Slot related callbacks for leopard AM
 * ------------------------------------------------------------------------
 */

static const TupleTableSlotOps *
leopardam_slot_callbacks(Relation relation)
{
	return &TTSOpsBufferLeopardTuple;
}


/* ------------------------------------------------------------------------
 * Index Scan Callbacks for leopard AM
 * ------------------------------------------------------------------------
 */

static IndexFetchTableData *
leopardam_index_fetch_begin(Relation rel)
{
	IndexFetchLeopardData *hscan = palloc0(sizeof(IndexFetchLeopardData));

	hscan->xs_base.rel = rel;
	hscan->xs_cbuf = InvalidBuffer;

	return &hscan->xs_base;
}

static void
leopardam_index_fetch_reset(IndexFetchTableData *scan)
{
	IndexFetchLeopardData *hscan = (IndexFetchLeopardData *) scan;

	if (BufferIsValid(hscan->xs_cbuf))
	{
		ReleaseBuffer(hscan->xs_cbuf);
		hscan->xs_cbuf = InvalidBuffer;
	}
}

static void
leopardam_index_fetch_end(IndexFetchTableData *scan)
{
	IndexFetchLeopardData *hscan = (IndexFetchLeopardData *) scan;

	leopardam_index_fetch_reset(scan);

	pfree(hscan);
}

static bool
leopardam_index_fetch_tuple(struct IndexFetchTableData *scan,
						 ItemPointer tid,
						 Snapshot snapshot,
						 TupleTableSlot *slot,
						 bool *call_again, bool *all_dead)
{
	IndexFetchLeopardData *hscan = (IndexFetchLeopardData *) scan;
	BufferLeopardTupleTableSlot *bslot = (BufferLeopardTupleTableSlot *) slot;
	LeopardTupleResponseType	got_leopard_tuple = LEOPARD_TUPLE_NOT_FOUND;

	Assert(TTS_IS_BUFFERTUPLE(slot));

	/* We can skip the buffer-switching logic if we're in mid-HOT chain. */
	if (!*call_again)
	{
		/* Switch to correct buffer if we don't have it already */
		Buffer		prev_buf = hscan->xs_cbuf;

		hscan->xs_cbuf = ReleaseAndReadBuffer(hscan->xs_cbuf,
											  hscan->xs_base.rel,
											  ItemPointerGetBlockNumber(tid));

		/*
		 * Prune page, but only if we weren't already on this page
		 */
		if (prev_buf != hscan->xs_cbuf)
			leopard_page_prune_opt(hscan->xs_base.rel, hscan->xs_cbuf);
	}

	/* Obtain share-lock on the buffer so we can examine visibility */
	LockBuffer(hscan->xs_cbuf, BUFFER_LOCK_SHARE);
	got_leopard_tuple = leopard_hot_search_buffer(tid,
											hscan->xs_base.rel,
											hscan->xs_cbuf,
											snapshot,
											&bslot->base.tupdata,
											all_dead,
											!*call_again);
	bslot->base.tupdata.t_self = *tid;
	LockBuffer(hscan->xs_cbuf, BUFFER_LOCK_UNLOCK);

	if (got_leopard_tuple == LEOPARD_TUPLE_FROM_BUFFER)
	{
		/*
		 * Only in a non-MVCC snapshot can more than one member of the HOT
		 * chain be visible.
		 */
		*call_again = !IsMVCCSnapshot(snapshot);

		slot->tts_tableOid = RelationGetRelid(scan->rel);
		ExecStoreBufferLeopardTuple(&bslot->base.tupdata, slot, hscan->xs_cbuf);
	}
	else if (got_leopard_tuple == LEOPARD_TUPLE_FROM_RDA)
	{
		if (LEOPARD_DEBUG)
			elog(NOTICE, "forcing RDA tuple into slot");
		*call_again = false;

		slot->tts_tableOid = RelationGetRelid(scan->rel);

		/*
		 * Force our manufactured tuple into the TTSOpsBufferHeapTuple slot,
		 * freeing the tuple from the RDA and letting the slot own it,
		 * to be freed later by caller.
		 */
//		ExecForceStoreHeapTuple((HeapTuple) &bslot->base.tupdata, slot, false);
//		if (LEOPARD_DEBUG)
//			elog(NOTICE, "forced RDA tuple into slot");
	}
	else
	{
		/* We've reached the end of the HOT chain. */
		*call_again = false;
	}

	return (got_leopard_tuple == LEOPARD_TUPLE_FROM_RDA ||
			got_leopard_tuple == LEOPARD_TUPLE_FROM_BUFFER);
}


/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples for leopard AM
 * ------------------------------------------------------------------------
 */

static bool
leopardam_fetch_row_version(Relation relation,
						 ItemPointer tid,
						 Snapshot snapshot,
						 TupleTableSlot *slot)
{
	BufferLeopardTupleTableSlot *bslot = (BufferLeopardTupleTableSlot *) slot;
	Buffer		buffer;

	Assert(TTS_IS_BUFFERTUPLE(slot));

	bslot->base.tupdata.t_self = *tid;
	if (leopard_fetch(relation, snapshot, &bslot->base.tupdata, &buffer))
	{
		/* store in slot, transferring existing pin */
		ExecStorePinnedBufferLeopardTuple(&bslot->base.tupdata, slot, buffer);
		slot->tts_tableOid = RelationGetRelid(relation);

		return true;
	}

	return false;
}

static bool
leopardam_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	LeopardScanDesc hscan = (LeopardScanDesc) scan;

	return ItemPointerIsValid(tid) &&
		ItemPointerGetBlockNumber(tid) < hscan->rs_nblocks;
}

static bool
leopardam_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
								Snapshot snapshot)
{
	BufferLeopardTupleTableSlot *bslot = (BufferLeopardTupleTableSlot *) slot;
	bool		res;

	Assert(TTS_IS_BUFFERTUPLE(slot));
	Assert(BufferIsValid(bslot->buffer));

	/*
	 * We need buffer pin and lock to call LeopardTupleSatisfiesVisibility.
	 * Caller should be holding pin, but not lock.
	 */
	LockBuffer(bslot->buffer, BUFFER_LOCK_SHARE);
	res = LeopardTupleSatisfiesVisibility(bslot->base.tuple, snapshot,
									   bslot->buffer);
	LockBuffer(bslot->buffer, BUFFER_LOCK_UNLOCK);

	return res;
}


/* ----------------------------------------------------------------------------
 *  Functions for manipulations of physical tuples for leopard AM.
 * ----------------------------------------------------------------------------
 */

static void
leopardam_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
					int options, BulkInsertState bistate)
{
	bool		shouldFree = true;
	LeopardTuple	tuple = ExecFetchSlotLeopardTuple(slot, true, &shouldFree);

	/* Update the tuple with table oid */
	slot->tts_tableOid = RelationGetRelid(relation);
	tuple->t_tableOid = slot->tts_tableOid;

	/* Perform the insertion, and copy the resulting ItemPointer */
	leopard_insert(relation, tuple, cid, options, bistate);
	ItemPointerCopy(&tuple->t_self, &slot->tts_tid);

	if (shouldFree)
		pfree(tuple);
}

static void
leopardam_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
								CommandId cid, int options,
								BulkInsertState bistate, uint32 specToken)
{
	bool		shouldFree = true;
	LeopardTuple	tuple = ExecFetchSlotLeopardTuple(slot, true, &shouldFree);

	/* Update the tuple with table oid */
	slot->tts_tableOid = RelationGetRelid(relation);
	tuple->t_tableOid = slot->tts_tableOid;

	LeopardTupleHeaderSetSpeculativeToken(tuple->t_data, specToken);
	options |= LEOPARD_INSERT_SPECULATIVE;

	/* Perform the insertion, and copy the resulting ItemPointer */
	leopard_insert(relation, tuple, cid, options, bistate);
	ItemPointerCopy(&tuple->t_self, &slot->tts_tid);

	if (shouldFree)
		pfree(tuple);
}

static void
leopardam_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
								  uint32 specToken, bool succeeded)
{
	bool		shouldFree = true;
	LeopardTuple	tuple = ExecFetchSlotLeopardTuple(slot, true, &shouldFree);

	/* adjust the tuple's state accordingly */
	if (succeeded)
		leopard_finish_speculative(relation, &slot->tts_tid);
	else
		leopard_abort_speculative(relation, &slot->tts_tid);

	if (shouldFree)
		pfree(tuple);
}

static TM_Result
leopardam_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
					Snapshot snapshot, Snapshot crosscheck, bool wait,
					TM_FailureData *tmfd, bool changingPart)
{
	/*
	 * Currently Deleting of index tuples are handled at vacuum, in case if
	 * the storage itself is cleaning the dead tuples by itself, it is the
	 * time to call the index tuple deletion also.
	 */
	return leopard_delete(relation, tid, cid, crosscheck, wait, tmfd, changingPart);
}


static TM_Result
leopardam_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
					CommandId cid, Snapshot snapshot, Snapshot crosscheck,
					bool wait, TM_FailureData *tmfd,
					LockTupleMode *lockmode, bool *update_indexes)
{
	bool		shouldFree = true;
	LeopardTuple	tuple = ExecFetchSlotLeopardTuple(slot, true, &shouldFree);
	TM_Result	result;

	/* Update the tuple with table oid */
	slot->tts_tableOid = RelationGetRelid(relation);
	tuple->t_tableOid = slot->tts_tableOid;

	result = leopard_update(relation, otid, tuple, cid, crosscheck, wait,
						 tmfd, lockmode);
	ItemPointerCopy(&tuple->t_self, &slot->tts_tid);

	/*
	 * Decide whether new index entries are needed for the tuple
	 *
	 * Note: leopard_update returns the tid (location) of the new tuple in the
	 * t_self field.
	 *
	 * If it's a HOT update, we mustn't insert new index entries.
	 */
	*update_indexes = result == TM_Ok && !LeopardTupleIsLeopardOnly(tuple);

	if (shouldFree)
		pfree(tuple);

	return result;
}

static TM_Result
leopardam_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
				  TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
				  LockWaitPolicy wait_policy, uint8 flags,
				  TM_FailureData *tmfd)
{
	BufferLeopardTupleTableSlot *bslot = (BufferLeopardTupleTableSlot *) slot;
	TM_Result	result;
	Buffer		buffer;
	LeopardTuple	tuple = &bslot->base.tupdata;
	bool		follow_updates;

	follow_updates = (flags & TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS) != 0;
	tmfd->traversed = false;

	Assert(TTS_IS_BUFFERTUPLE(slot));

tuple_lock_retry:
	tuple->t_self = *tid;
	result = leopard_lock_tuple(relation, tuple, cid, mode, wait_policy,
							 follow_updates, &buffer, tmfd);

	if (result == TM_Updated &&
		(flags & TUPLE_LOCK_FLAG_FIND_LAST_VERSION))
	{
		/* Should not encounter speculative tuple on recheck */
		Assert(!LeopardTupleHeaderIsSpeculative(tuple->t_data));

		ReleaseBuffer(buffer);

		if (!ItemPointerEquals(&tmfd->ctid, &tuple->t_self))
		{
			SnapshotData SnapshotDirty;
			TransactionId priorXmax;

			/* it was updated, so look at the updated version */
			*tid = tmfd->ctid;
			/* updated row should have xmin matching this xmax */
			priorXmax = tmfd->xmax;

			/* signal that a tuple later in the chain is getting locked */
			tmfd->traversed = true;

			/*
			 * fetch target tuple
			 *
			 * Loop here to deal with updated or busy tuples
			 */
			InitDirtySnapshot(SnapshotDirty);
			for (;;)
			{
				if (ItemPointerIndicatesMovedPartitions(tid))
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("tuple to be locked was already moved to another partition due to concurrent update")));

				tuple->t_self = *tid;
				if (leopard_fetch_extended(relation, &SnapshotDirty, tuple,
										&buffer, true))
				{
					/*
					 * If xmin isn't what we're expecting, the slot must have
					 * been recycled and reused for an unrelated tuple.  This
					 * implies that the latest version of the row was deleted,
					 * so we need do nothing.  (Should be safe to examine xmin
					 * without getting buffer's content lock.  We assume
					 * reading a TransactionId to be atomic, and Xmin never
					 * changes in an existing tuple, except to invalid or
					 * frozen, and neither of those can match priorXmax.)
					 */
					if (!TransactionIdEquals(LeopardTupleHeaderGetXmin(tuple->t_data),
											 priorXmax))
					{
						ReleaseBuffer(buffer);
						return TM_Deleted;
					}

					/* otherwise xmin should not be dirty... */
					if (TransactionIdIsValid(SnapshotDirty.xmin))
						ereport(ERROR,
								(errcode(ERRCODE_DATA_CORRUPTED),
								 errmsg_internal("t_xmin %u is uncommitted in tuple (%u,%u) to be updated in table \"%s\"",
												 SnapshotDirty.xmin,
												 ItemPointerGetBlockNumber(&tuple->t_self),
												 ItemPointerGetOffsetNumber(&tuple->t_self),
												 RelationGetRelationName(relation))));

					/*
					 * If tuple is being updated by other transaction then we
					 * have to wait for its commit/abort, or die trying.
					 */
					if (TransactionIdIsValid(SnapshotDirty.xmax))
					{
						ReleaseBuffer(buffer);
						switch (wait_policy)
						{
							case LockWaitBlock:
								XactLockTableWait(SnapshotDirty.xmax,
												  relation, &tuple->t_self,
												  XLTW_FetchUpdated);
								break;
							case LockWaitSkip:
								if (!ConditionalXactLockTableWait(SnapshotDirty.xmax))
									/* skip instead of waiting */
									return TM_WouldBlock;
								break;
							case LockWaitError:
								if (!ConditionalXactLockTableWait(SnapshotDirty.xmax))
									ereport(ERROR,
											(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
											 errmsg("could not obtain lock on row in relation \"%s\"",
													RelationGetRelationName(relation))));
								break;
						}
						continue;	/* loop back to repeat leopard_fetch */
					}

					/*
					 * If tuple was inserted by our own transaction, we have
					 * to check cmin against cid: cmin >= current CID means
					 * our command cannot see the tuple, so we should ignore
					 * it. Otherwise leopard_lock_tuple() will throw an error,
					 * and so would any later attempt to update or delete the
					 * tuple.  (We need not check cmax because
					 * LeopardTupleSatisfiesDirty will consider a tuple deleted
					 * by our transaction dead, regardless of cmax.)  We just
					 * checked that priorXmax == xmin, so we can test that
					 * variable instead of doing LeopardTupleHeaderGetXmin again.
					 */
					if (TransactionIdIsCurrentTransactionId(priorXmax) &&
						LeopardTupleHeaderGetCmin(tuple->t_data) >= cid)
					{
						tmfd->xmax = priorXmax;

						/*
						 * Cmin is the problematic value, so store that. See
						 * above.
						 */
						tmfd->cmax = LeopardTupleHeaderGetCmin(tuple->t_data);
						ReleaseBuffer(buffer);
						return TM_SelfModified;
					}

					/*
					 * This is a live tuple, so try to lock it again.
					 */
					ReleaseBuffer(buffer);
					goto tuple_lock_retry;
				}

				/*
				 * If the referenced slot was actually empty, the latest
				 * version of the row must have been deleted, so we need do
				 * nothing.
				 */
				if (tuple->t_data == NULL)
				{
					Assert(!BufferIsValid(buffer));
					return TM_Deleted;
				}

				/*
				 * As above, if xmin isn't what we're expecting, do nothing.
				 */
				if (!TransactionIdEquals(LeopardTupleHeaderGetXmin(tuple->t_data),
										 priorXmax))
				{
					ReleaseBuffer(buffer);
					return TM_Deleted;
				}

				/*
				 * If we get here, the tuple was found but failed
				 * SnapshotDirty. Assuming the xmin is either a committed xact
				 * or our own xact (as it certainly should be if we're trying
				 * to modify the tuple), this must mean that the row was
				 * updated or deleted by either a committed xact or our own
				 * xact.  If it was deleted, we can ignore it; if it was
				 * updated then chain up to the next version and repeat the
				 * whole process.
				 *
				 * As above, it should be safe to examine xmax and t_ctid
				 * without the buffer content lock, because they can't be
				 * changing.  We'd better hold a buffer pin though.
				 */
				if (ItemPointerEquals(&tuple->t_self, &tuple->t_data->t_ctid))
				{
					/* deleted, so forget about it */
					ReleaseBuffer(buffer);
					return TM_Deleted;
				}

				/* updated, so look at the updated row */
				*tid = tuple->t_data->t_ctid;
				/* updated row should have xmin matching this xmax */
				priorXmax = LeopardTupleHeaderGetUpdateXid(tuple->t_data);
				ReleaseBuffer(buffer);
				/* loop back to fetch next in chain */
			}
		}
		else
		{
			/* tuple was deleted, so give up */
			return TM_Deleted;
		}
	}

	slot->tts_tableOid = RelationGetRelid(relation);
	tuple->t_tableOid = slot->tts_tableOid;

	/* store in slot, transferring existing pin */
	ExecStorePinnedBufferLeopardTuple(tuple, slot, buffer);

	return result;
}


/* ------------------------------------------------------------------------
 * DDL related callbacks for leopard AM.
 * ------------------------------------------------------------------------
 */

static void
leopardam_relation_set_new_filenode(Relation rel,
								 const RelFileNode *newrnode,
								 char persistence,
								 TransactionId *freezeXid,
								 MultiXactId *minmulti)
{
	SMgrRelation srel;

	/*
	 * Initialize to the minimum XID that could put tuples in the table. We
	 * know that no xacts older than RecentXmin are still running, so that
	 * will do.
	 */
	*freezeXid = RecentXmin;

	/*
	 * Similarly, initialize the minimum Multixact to the first value that
	 * could possibly be stored in tuples in the table.  Running transactions
	 * could reuse values from their local cache, so we are careful to
	 * consider all currently running multis.
	 *
	 * XXX this could be refined further, but is it worth the hassle?
	 */
	*minmulti = GetOldestMultiXactId();

	srel = RelationCreateStorage(*newrnode, persistence);

	/*
	 * If required, set up an init fork for an unlogged table so that it can
	 * be correctly reinitialized on restart.  An immediate sync is required
	 * even if the page has been logged, because the write did not go through
	 * shared_buffers and therefore a concurrent checkpoint may have moved the
	 * redo pointer past our xlog record.  Recovery may as well remove it
	 * while replaying, for example, XLOG_DBASE_CREATE or XLOG_TBLSPC_CREATE
	 * record. Therefore, logging is necessary even if wal_level=minimal.
	 */
	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		Assert(rel->rd_rel->relkind == RELKIND_RELATION ||
			   rel->rd_rel->relkind == RELKIND_MATVIEW ||
			   rel->rd_rel->relkind == RELKIND_TOASTVALUE);
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(newrnode, INIT_FORKNUM);
		smgrimmedsync(srel, INIT_FORKNUM);
	}

	smgrclose(srel);
}

static void
leopardam_relation_nontransactional_truncate(Relation rel)
{
	RelationTruncate(rel, 0);
}

static void
leopardam_relation_copy_data(Relation rel, const RelFileNode *newrnode)
{
	SMgrRelation dstrel;

	dstrel = smgropen(*newrnode, rel->rd_backend);
	RelationOpenSmgr(rel);

	/*
	 * Since we copy the file directly without looking at the shared buffers,
	 * we'd better first flush out any pages of the source relation that are
	 * in shared buffers.  We assume no new changes will be made while we are
	 * holding exclusive lock on the rel.
	 */
	FlushRelationBuffers(rel);

	/*
	 * Create and copy all forks of the relation, and schedule unlinking of
	 * old physical files.
	 *
	 * NOTE: any conflict in relfilenode value will be caught in
	 * RelationCreateStorage().
	 */
	RelationCreateStorage(*newrnode, rel->rd_rel->relpersistence);

	/* copy main fork */
	RelationCopyStorage(rel->rd_smgr, dstrel, MAIN_FORKNUM,
						rel->rd_rel->relpersistence);

	/* copy those extra forks that exist */
	for (ForkNumber forkNum = MAIN_FORKNUM + 1;
		 forkNum <= MAX_FORKNUM; forkNum++)
	{
		if (smgrexists(rel->rd_smgr, forkNum))
		{
			smgrcreate(dstrel, forkNum, false);

			/*
			 * WAL log creation if the relation is persistent, or this is the
			 * init fork of an unlogged relation.
			 */
			if (RelationIsPermanent(rel) ||
				(rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED &&
				 forkNum == INIT_FORKNUM))
				log_smgrcreate(newrnode, forkNum);
			RelationCopyStorage(rel->rd_smgr, dstrel, forkNum,
								rel->rd_rel->relpersistence);
		}
	}


	/* drop old relation, and close new one */
	RelationDropStorage(rel);
	smgrclose(dstrel);
}

static void
leopardam_relation_copy_for_cluster(Relation OldLeopard, Relation NewLeopard,
								 Relation OldIndex, bool use_sort,
								 TransactionId OldestXmin,
								 TransactionId *xid_cutoff,
								 MultiXactId *multi_cutoff,
								 double *num_tuples,
								 double *tups_vacuumed,
								 double *tups_recently_dead)
{
	RewriteState rwstate;
	IndexScanDesc indexScan;
	TableScanDesc tableScan;
	LeopardScanDesc leopardScan;
	bool		is_system_catalog;
	Tuplesortstate *tuplesort;
	TupleDesc	oldTupDesc = RelationGetDescr(OldLeopard);
	TupleDesc	newTupDesc = RelationGetDescr(NewLeopard);
	TupleTableSlot *slot;
	int			natts;
	Datum	   *values;
	bool	   *isnull;
	BufferLeopardTupleTableSlot *hslot;
	BlockNumber prev_cblock = InvalidBlockNumber;

	/* Remember if it's a system catalog */
	is_system_catalog = IsSystemRelation(OldLeopard);

	/*
	 * Valid smgr_targblock implies something already wrote to the relation.
	 * This may be harmless, but this function hasn't planned for it.
	 */
	Assert(RelationGetTargetBlock(NewLeopard) == InvalidBlockNumber);

	/* Preallocate values/isnull arrays */
	natts = newTupDesc->natts;
	values = (Datum *) palloc(natts * sizeof(Datum));
	isnull = (bool *) palloc(natts * sizeof(bool));

	/* Initialize the rewrite operation */
	rwstate = begin_leopard_rewrite(OldLeopard, NewLeopard, OldestXmin, *xid_cutoff,
								 *multi_cutoff);


	/* Set up sorting if wanted */
	if (use_sort)
		tuplesort = tuplesort_begin_cluster(oldTupDesc, OldIndex,
											maintenance_work_mem,
											NULL, false);
	else
		tuplesort = NULL;

	/*
	 * Prepare to scan the OldLeopard.  To ensure we see recently-dead tuples
	 * that still need to be copied, we scan with SnapshotAny and use
	 * LeopardTupleSatisfiesVacuum for the visibility test.
	 */
	if (OldIndex != NULL && !use_sort)
	{
		const int	ci_index[] = {
			PROGRESS_CLUSTER_PHASE,
			PROGRESS_CLUSTER_INDEX_RELID
		};
		int64		ci_val[2];

		/* Set phase and OIDOldIndex to columns */
		ci_val[0] = PROGRESS_CLUSTER_PHASE_INDEX_SCAN_LEOPARD;
		ci_val[1] = RelationGetRelid(OldIndex);
		pgstat_progress_update_multi_param(2, ci_index, ci_val);

		tableScan = NULL;
		leopardScan = NULL;
		indexScan = index_beginscan(OldLeopard, OldIndex, SnapshotAny, 0, 0);
		index_rescan(indexScan, NULL, 0, NULL, 0);
	}
	else
	{
		/* In scan-and-sort mode and also VACUUM FULL, set phase */
		pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE,
									 PROGRESS_CLUSTER_PHASE_SEQ_SCAN_LEOPARD);

		tableScan = table_beginscan(OldLeopard, SnapshotAny, 0, (ScanKey) NULL);
		leopardScan = (LeopardScanDesc) tableScan;
		indexScan = NULL;

		/* Set total leopard blocks */
		pgstat_progress_update_param(PROGRESS_CLUSTER_TOTAL_LEOPARD_BLKS,
									 leopardScan->rs_nblocks);
	}

	slot = table_slot_create(OldLeopard, NULL);
	hslot = (BufferLeopardTupleTableSlot *) slot;

	/*
	 * Scan through the OldLeopard, either in OldIndex order or sequentially;
	 * copy each tuple into the NewLeopard, or transiently to the tuplesort
	 * module.  Note that we don't bother sorting dead tuples (they won't get
	 * to the new table anyway).
	 */
	for (;;)
	{
		LeopardTuple	tuple;
		Buffer		buf;
		bool		isdead;

		CHECK_FOR_INTERRUPTS();

		if (indexScan != NULL)
		{
			if (!index_getnext_slot(indexScan, ForwardScanDirection, slot))
				break;

			/* Since we used no scan keys, should never need to recheck */
			if (indexScan->xs_recheck)
				elog(ERROR, "CLUSTER does not support lossy index conditions");
		}
		else
		{
			if (!table_scan_getnextslot(tableScan, ForwardScanDirection, slot))
			{
				/*
				 * If the last pages of the scan were empty, we would go to
				 * the next phase while leopard_blks_scanned != leopard_blks_total.
				 * Instead, to ensure that leopard_blks_scanned is equivalent to
				 * total_leopard_blks after the table scan phase, this parameter
				 * is manually updated to the correct value when the table
				 * scan finishes.
				 */
				pgstat_progress_update_param(PROGRESS_CLUSTER_LEOPARD_BLKS_SCANNED,
											 leopardScan->rs_nblocks);
				break;
			}

			/*
			 * In scan-and-sort mode and also VACUUM FULL, set leopard blocks
			 * scanned
			 *
			 * Note that leopardScan may start at an offset and wrap around, i.e.
			 * rs_startblock may be >0, and rs_cblock may end with a number
			 * below rs_startblock. To prevent showing this wraparound to the
			 * user, we offset rs_cblock by rs_startblock (modulo rs_nblocks).
			 */
			if (prev_cblock != leopardScan->rs_cblock)
			{
				pgstat_progress_update_param(PROGRESS_CLUSTER_LEOPARD_BLKS_SCANNED,
											 (leopardScan->rs_cblock +
											  leopardScan->rs_nblocks -
											  leopardScan->rs_startblock
											  ) % leopardScan->rs_nblocks + 1);
				prev_cblock = leopardScan->rs_cblock;
			}
		}

		tuple = ExecFetchSlotLeopardTuple(slot, false, NULL);
		buf = hslot->buffer;

		LockBuffer(buf, BUFFER_LOCK_SHARE);

		switch (LeopardTupleSatisfiesVacuum(tuple, OldestXmin, buf))
		{
			case LEOPARDTUPLE_DEAD:
				/* Definitely dead */
				isdead = true;
				break;
			case LEOPARDTUPLE_RECENTLY_DEAD:
				*tups_recently_dead += 1;
				/* fall through */
			case LEOPARDTUPLE_LIVE:
				/* Live or recently dead, must copy it */
				isdead = false;
				break;
			case LEOPARDTUPLE_INSERT_IN_PROGRESS:

				/*
				 * Since we hold exclusive lock on the relation, normally the
				 * only way to see this is if it was inserted earlier in our
				 * own transaction.  However, it can happen in system
				 * catalogs, since we tend to release write lock before commit
				 * there.  Give a warning if neither case applies; but in any
				 * case we had better copy it.
				 */
				if (!is_system_catalog &&
					!TransactionIdIsCurrentTransactionId(LeopardTupleHeaderGetXmin(tuple->t_data)))
					elog(WARNING, "concurrent insert in progress within table \"%s\"",
						 RelationGetRelationName(OldLeopard));
				/* treat as live */
				isdead = false;
				break;
			case LEOPARDTUPLE_DELETE_IN_PROGRESS:

				/*
				 * Similar situation to INSERT_IN_PROGRESS case.
				 */
				if (!is_system_catalog &&
					!TransactionIdIsCurrentTransactionId(LeopardTupleHeaderGetUpdateXid(tuple->t_data)))
					elog(WARNING, "concurrent delete in progress within table \"%s\"",
						 RelationGetRelationName(OldLeopard));
				/* treat as recently dead */
				*tups_recently_dead += 1;
				isdead = false;
				break;
			default:
				elog(ERROR, "unexpected LeopardTupleSatisfiesVacuum result");
				isdead = false; /* keep compiler quiet */
				break;
		}

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);

		if (isdead)
		{
			*tups_vacuumed += 1;
			/* leopard rewrite module still needs to see it... */
			if (rewrite_leopard_dead_tuple(rwstate, tuple))
			{
				/* A previous recently-dead tuple is now known dead */
				*tups_vacuumed += 1;
				*tups_recently_dead -= 1;
			}
			continue;
		}

		*num_tuples += 1;
		if (tuplesort != NULL)
		{
			tuplesort_putleopardtuple(tuplesort, tuple);

			/*
			 * In scan-and-sort mode, report increase in number of tuples
			 * scanned
			 */
			pgstat_progress_update_param(PROGRESS_CLUSTER_LEOPARD_TUPLES_SCANNED,
										 *num_tuples);
		}
		else
		{
			const int	ct_index[] = {
				PROGRESS_CLUSTER_LEOPARD_TUPLES_SCANNED,
				PROGRESS_CLUSTER_LEOPARD_TUPLES_WRITTEN
			};
			int64		ct_val[2];

			reform_and_rewrite_tuple(tuple, OldLeopard, NewLeopard,
									 values, isnull, rwstate);

			/*
			 * In indexscan mode and also VACUUM FULL, report increase in
			 * number of tuples scanned and written
			 */
			ct_val[0] = *num_tuples;
			ct_val[1] = *num_tuples;
			pgstat_progress_update_multi_param(2, ct_index, ct_val);
		}
	}

	if (indexScan != NULL)
		index_endscan(indexScan);
	if (tableScan != NULL)
		table_endscan(tableScan);
	if (slot)
		ExecDropSingleTupleTableSlot(slot);

	/*
	 * In scan-and-sort mode, complete the sort, then read out all live tuples
	 * from the tuplestore and write them to the new relation.
	 */
	if (tuplesort != NULL)
	{
		double		n_tuples = 0;

		/* Report that we are now sorting tuples */
		pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE,
									 PROGRESS_CLUSTER_PHASE_SORT_TUPLES);

		tuplesort_performsort(tuplesort);

		/* Report that we are now writing new leopard */
		pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE,
									 PROGRESS_CLUSTER_PHASE_WRITE_NEW_LEOPARD);

		for (;;)
		{
			LeopardTuple	tuple;

			CHECK_FOR_INTERRUPTS();

			tuple = tuplesort_getleopardtuple(tuplesort, true);
			if (tuple == NULL)
				break;

			n_tuples += 1;
			reform_and_rewrite_tuple(tuple,
									 OldLeopard, NewLeopard,
									 values, isnull,
									 rwstate);
			/* Report n_tuples */
			pgstat_progress_update_param(PROGRESS_CLUSTER_LEOPARD_TUPLES_WRITTEN,
										 n_tuples);
		}

		tuplesort_end(tuplesort);
	}

	/* Write out any remaining tuples, and fsync if needed */
	end_leopard_rewrite(rwstate);

	/* Clean up */
	pfree(values);
	pfree(isnull);
}

static bool
leopardam_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
							   BufferAccessStrategy bstrategy)
{
	LeopardScanDesc hscan = (LeopardScanDesc) scan;

	/*
	 * We must maintain a pin on the target page's buffer to ensure that
	 * concurrent activity - e.g. HOT pruning - doesn't delete tuples out from
	 * under us.  Hence, pin the page until we are done looking at it.  We
	 * also choose to hold sharelock on the buffer throughout --- we could
	 * release and re-acquire sharelock for each tuple, but since we aren't
	 * doing much work per tuple, the extra lock traffic is probably better
	 * avoided.
	 */
	hscan->rs_cblock = blockno;
	hscan->rs_cindex = FirstOffsetNumber;
	hscan->rs_cbuf = ReadBufferExtended(scan->rs_rd, MAIN_FORKNUM,
										blockno, RBM_NORMAL, bstrategy);
	LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_SHARE);

	/* in leopard all blocks can contain tuples, so always return true */
	return true;
}

static bool
leopardam_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
							   double *liverows, double *deadrows,
							   TupleTableSlot *slot)
{
	LeopardScanDesc hscan = (LeopardScanDesc) scan;
	Page		targpage;
	OffsetNumber maxoffset;
	BufferLeopardTupleTableSlot *hslot;

	Assert(TTS_IS_BUFFERTUPLE(slot));

	hslot = (BufferLeopardTupleTableSlot *) slot;
	targpage = BufferGetPage(hscan->rs_cbuf);
	maxoffset = PageGetMaxOffsetNumber(targpage);

	/* Inner loop over all tuples on the selected page */
	for (; hscan->rs_cindex <= maxoffset; hscan->rs_cindex++)
	{
		ItemId		itemid;
		LeopardTuple	targtuple = &hslot->base.tupdata;
		bool		sample_it = false;

		itemid = PageGetItemId(targpage, hscan->rs_cindex);

		/*
		 * We ignore unused and redirect line pointers.  DEAD line pointers
		 * should be counted as dead, because we need vacuum to run to get rid
		 * of them.  Note that this rule agrees with the way that
		 * leopard_page_prune() counts things.
		 */
		if (!ItemIdIsNormal(itemid))
		{
			if (ItemIdIsDead(itemid))
				*deadrows += 1;
			continue;
		}

		ItemPointerSet(&targtuple->t_self, hscan->rs_cblock, hscan->rs_cindex);

		targtuple->t_tableOid = RelationGetRelid(scan->rs_rd);
		targtuple->t_data = (LeopardTupleHeader) PageGetItem(targpage, itemid);
		targtuple->t_len = ItemIdGetLength(itemid);

		switch (LeopardTupleSatisfiesVacuum(targtuple, OldestXmin,
										 hscan->rs_cbuf))
		{
			case LEOPARDTUPLE_LIVE:
				sample_it = true;
				*liverows += 1;
				break;

			case LEOPARDTUPLE_DEAD:
			case LEOPARDTUPLE_RECENTLY_DEAD:
				/* Count dead and recently-dead rows */
				*deadrows += 1;
				break;

			case LEOPARDTUPLE_INSERT_IN_PROGRESS:

				/*
				 * Insert-in-progress rows are not counted.  We assume that
				 * when the inserting transaction commits or aborts, it will
				 * send a stats message to increment the proper count.  This
				 * works right only if that transaction ends after we finish
				 * analyzing the table; if things happen in the other order,
				 * its stats update will be overwritten by ours.  However, the
				 * error will be large only if the other transaction runs long
				 * enough to insert many tuples, so assuming it will finish
				 * after us is the safer option.
				 *
				 * A special case is that the inserting transaction might be
				 * our own.  In this case we should count and sample the row,
				 * to accommodate users who load a table and analyze it in one
				 * transaction.  (pgstat_report_analyze has to adjust the
				 * numbers we send to the stats collector to make this come
				 * out right.)
				 */
				if (TransactionIdIsCurrentTransactionId(LeopardTupleHeaderGetXmin(targtuple->t_data)))
				{
					sample_it = true;
					*liverows += 1;
				}
				break;

			case LEOPARDTUPLE_DELETE_IN_PROGRESS:

				/*
				 * We count and sample delete-in-progress rows the same as
				 * live ones, so that the stats counters come out right if the
				 * deleting transaction commits after us, per the same
				 * reasoning given above.
				 *
				 * If the delete was done by our own transaction, however, we
				 * must count the row as dead to make pgstat_report_analyze's
				 * stats adjustments come out right.  (Note: this works out
				 * properly when the row was both inserted and deleted in our
				 * xact.)
				 *
				 * The net effect of these choices is that we act as though an
				 * IN_PROGRESS transaction hasn't happened yet, except if it
				 * is our own transaction, which we assume has happened.
				 *
				 * This approach ensures that we behave sanely if we see both
				 * the pre-image and post-image rows for a row being updated
				 * by a concurrent transaction: we will sample the pre-image
				 * but not the post-image.  We also get sane results if the
				 * concurrent transaction never commits.
				 */
				if (TransactionIdIsCurrentTransactionId(LeopardTupleHeaderGetUpdateXid(targtuple->t_data)))
					*deadrows += 1;
				else
				{
					sample_it = true;
					*liverows += 1;
				}
				break;

			default:
				elog(ERROR, "unexpected LeopardTupleSatisfiesVacuum result");
				break;
		}

		if (sample_it)
		{
			ExecStoreBufferLeopardTuple(targtuple, slot, hscan->rs_cbuf);
			hscan->rs_cindex++;

			/* note that we leave the buffer locked here! */
			return true;
		}
	}

	/* Now release the lock and pin on the page */
	UnlockReleaseBuffer(hscan->rs_cbuf);
	hscan->rs_cbuf = InvalidBuffer;

	/* also prevent old slot contents from having pin on page */
	ExecClearTuple(slot);

	return false;
}

static double
leopardam_index_build_range_scan(Relation leopardRelation,
							  Relation indexRelation,
							  IndexInfo *indexInfo,
							  bool allow_sync,
							  bool anyvisible,
							  bool progress,
							  BlockNumber start_blockno,
							  BlockNumber numblocks,
							  IndexBuildCallback callback,
							  void *callback_state,
							  TableScanDesc scan)
{
	LeopardScanDesc hscan;
	bool		is_system_catalog;
	bool		checking_uniqueness;
	LeopardTuple	leopardTuple;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	double		reltuples;
	ExprState  *predicate;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	Snapshot	snapshot;
	bool		need_unregister_snapshot = false;
	TransactionId OldestXmin;
	BlockNumber previous_blkno = InvalidBlockNumber;
	BlockNumber root_blkno = InvalidBlockNumber;
	OffsetNumber root_offsets[MaxLeopardTuplesPerPage];

	/*
	 * Leopard tables may have entries in the RDA that should be indexed.
	 * XXX add later check to see if any entries exist according the the
	 * snapshot, but for now, just set indexInfo->ii_BrokenHotChain = true
	 * always.
	 */
	indexInfo->ii_BrokenHotChain = true;

	/*
	 * sanity checks
	 */
	Assert(OidIsValid(indexRelation->rd_rel->relam));

	/* Remember if it's a system catalog */
	is_system_catalog = IsSystemRelation(leopardRelation);

	/* See whether we're verifying uniqueness/exclusion properties */
	checking_uniqueness = (indexInfo->ii_Unique ||
						   indexInfo->ii_ExclusionOps != NULL);

	/*
	 * "Any visible" mode is not compatible with uniqueness checks; make sure
	 * only one of those is requested.
	 */
	Assert(!(anyvisible && checking_uniqueness));

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.  Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = table_slot_create(leopardRelation, NULL);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* Set up execution state for predicate, if any. */
	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	/*
	 * Prepare for scan of the base relation.  In a normal index build, we use
	 * SnapshotAny because we must retrieve all tuples and do our own time
	 * qual checks (because we have to index RECENTLY_DEAD tuples). In a
	 * concurrent build, or during bootstrap, we take a regular MVCC snapshot
	 * and index whatever's live according to that.
	 */
	OldestXmin = InvalidTransactionId;

	/* okay to ignore lazy VACUUMs here */
	if (!IsBootstrapProcessingMode() && !indexInfo->ii_Concurrent)
		OldestXmin = GetOldestNonRemovableTransactionId(leopardRelation);

	if (!scan)
	{
		/*
		 * Serial index build.
		 *
		 * Must begin our own leopard scan in this case.  We may also need to
		 * register a snapshot whose lifetime is under our direct control.
		 */
		if (!TransactionIdIsValid(OldestXmin))
		{
			snapshot = RegisterSnapshot(GetTransactionSnapshot());
			need_unregister_snapshot = true;
		}
		else
			snapshot = SnapshotAny;

		scan = table_beginscan_strat(leopardRelation,	/* relation */
									 snapshot,	/* snapshot */
									 0, /* number of keys */
									 NULL,	/* scan key */
									 true,	/* buffer access strategy OK */
									 allow_sync);	/* syncscan OK? */
	}
	else
	{
		/*
		 * Parallel index build.
		 *
		 * Parallel case never registers/unregisters own snapshot.  Snapshot
		 * is taken from parallel leopard scan, and is SnapshotAny or an MVCC
		 * snapshot, based on same criteria as serial case.
		 */
		Assert(!IsBootstrapProcessingMode());
		Assert(allow_sync);
		snapshot = scan->rs_snapshot;
	}

	hscan = (LeopardScanDesc) scan;

	/*
	 * Must have called GetOldestNonRemovableTransactionId() if using
	 * SnapshotAny.  Shouldn't have for an MVCC snapshot. (It's especially
	 * worth checking this for parallel builds, since ambuild routines that
	 * support parallel builds must work these details out for themselves.)
	 */
	Assert(snapshot == SnapshotAny || IsMVCCSnapshot(snapshot));
	Assert(snapshot == SnapshotAny ? TransactionIdIsValid(OldestXmin) :
		   !TransactionIdIsValid(OldestXmin));
	Assert(snapshot == SnapshotAny || !anyvisible);

	/* Publish number of blocks to scan */
	if (progress)
	{
		BlockNumber nblocks;

		if (hscan->rs_base.rs_parallel != NULL)
		{
			ParallelBlockTableScanDesc pbscan;

			pbscan = (ParallelBlockTableScanDesc) hscan->rs_base.rs_parallel;
			nblocks = pbscan->phs_nblocks;
		}
		else
			nblocks = hscan->rs_nblocks;

		pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_TOTAL,
									 nblocks);
	}

	/* set our scan endpoints */
	if (!allow_sync)
		leopard_setscanlimits(scan, start_blockno, numblocks);
	else
	{
		/* syncscan can only be requested on whole relation */
		Assert(start_blockno == 0);
		Assert(numblocks == InvalidBlockNumber);
	}

	reltuples = 0;

	/*
	 * Scan all tuples in the base relation.
	 */
	while ((leopardTuple = leopard_getnext(scan, ForwardScanDirection)) != NULL)
	{
		bool		tupleIsAlive;

		CHECK_FOR_INTERRUPTS();

		/* Report scan progress, if asked to. */
		if (progress)
		{
			BlockNumber blocks_done = leopardam_scan_get_blocks_done(hscan);

			if (blocks_done != previous_blkno)
			{
				pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_DONE,
											 blocks_done);
				previous_blkno = blocks_done;
			}
		}

		/*
		 * When dealing with a HOT-chain of updated tuples, we want to index
		 * the values of the live tuple (if any), but index it under the TID
		 * of the chain's root tuple.  This approach is necessary to preserve
		 * the HOT-chain structure in the leopard. So we need to be able to find
		 * the root item offset for every tuple that's in a HOT-chain.  When
		 * first reaching a new page of the relation, call
		 * leopard_get_root_tuples() to build a map of root item offsets on the
		 * page.
		 *
		 * It might look unsafe to use this information across buffer
		 * lock/unlock.  However, we hold ShareLock on the table so no
		 * ordinary insert/update/delete should occur; and we hold pin on the
		 * buffer continuously while visiting the page, so no pruning
		 * operation can occur either.
		 *
		 * In cases with only ShareUpdateExclusiveLock on the table, it's
		 * possible for some HOT tuples to appear that we didn't know about
		 * when we first read the page.  To handle that case, we re-obtain the
		 * list of root offsets when a HOT tuple points to a root item that we
		 * don't know about.
		 *
		 * Also, although our opinions about tuple liveness could change while
		 * we scan the page (due to concurrent transaction commits/aborts),
		 * the chain root locations won't, so this info doesn't need to be
		 * rebuilt after waiting for another transaction.
		 *
		 * Note the implied assumption that there is no more than one live
		 * tuple per HOT-chain --- else we could create more than one index
		 * entry pointing to the same root tuple.
		 */
		if (hscan->rs_cblock != root_blkno)
		{
			Page		page = BufferGetPage(hscan->rs_cbuf);

			LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_SHARE);
			leopard_get_root_tuples(page, root_offsets);
			LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK);

			root_blkno = hscan->rs_cblock;
		}

		if (snapshot == SnapshotAny)
		{
			/* do our own time qual check */
			bool		indexIt;
			TransactionId xwait;

	recheck:

			/*
			 * We could possibly get away with not locking the buffer here,
			 * since caller should hold ShareLock on the relation, but let's
			 * be conservative about it.  (This remark is still correct even
			 * with HOT-pruning: our pin on the buffer prevents pruning.)
			 */
			LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_SHARE);

			/*
			 * The criteria for counting a tuple as live in this block need to
			 * match what analyze.c's leopardam_scan_analyze_next_tuple() does,
			 * otherwise CREATE INDEX and ANALYZE may produce wildly different
			 * reltuples values, e.g. when there are many recently-dead
			 * tuples.
			 */
			switch (LeopardTupleSatisfiesVacuum(leopardTuple, OldestXmin,
											 hscan->rs_cbuf))
			{
				case LEOPARDTUPLE_DEAD:
					/* Definitely dead, we can ignore it */
					indexIt = false;
					tupleIsAlive = false;
					break;
				case LEOPARDTUPLE_LIVE:
					/* Normal case, index and unique-check it */
					indexIt = true;
					tupleIsAlive = true;
					/* Count it as live, too */
					reltuples += 1;
					break;
				case LEOPARDTUPLE_RECENTLY_DEAD:

					/*
					 * If tuple is recently deleted then we must index it
					 * anyway to preserve MVCC semantics.  (Pre-existing
					 * transactions could try to use the index after we finish
					 * building it, and may need to see such tuples.)
					 *
					 * However, if it was HOT-updated then we must only index
					 * the live tuple at the end of the HOT-chain.  Since this
					 * breaks semantics for pre-existing snapshots, mark the
					 * index as unusable for them.
					 *
					 * We don't count recently-dead tuples in reltuples, even
					 * if we index them; see leopardam_scan_analyze_next_tuple().
					 */
					if (LeopardTupleIsHotUpdated(leopardTuple))
					{
						indexIt = false;
						/* mark the index as unsafe for old snapshots */
						indexInfo->ii_BrokenHotChain = true;
					}
					else
						indexIt = true;
					/* In any case, exclude the tuple from unique-checking */
					tupleIsAlive = false;
					break;
				case LEOPARDTUPLE_INSERT_IN_PROGRESS:

					/*
					 * In "anyvisible" mode, this tuple is visible and we
					 * don't need any further checks.
					 */
					if (anyvisible)
					{
						indexIt = true;
						tupleIsAlive = true;
						reltuples += 1;
						break;
					}

					/*
					 * Since caller should hold ShareLock or better, normally
					 * the only way to see this is if it was inserted earlier
					 * in our own transaction.  However, it can happen in
					 * system catalogs, since we tend to release write lock
					 * before commit there.  Give a warning if neither case
					 * applies.
					 */
					xwait = LeopardTupleHeaderGetXmin(leopardTuple->t_data);
					if (!TransactionIdIsCurrentTransactionId(xwait))
					{
						if (!is_system_catalog)
							elog(WARNING, "concurrent insert in progress within table \"%s\"",
								 RelationGetRelationName(leopardRelation));

						/*
						 * If we are performing uniqueness checks, indexing
						 * such a tuple could lead to a bogus uniqueness
						 * failure.  In that case we wait for the inserting
						 * transaction to finish and check again.
						 */
						if (checking_uniqueness)
						{
							/*
							 * Must drop the lock on the buffer before we wait
							 */
							LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK);
							XactLockTableWait(xwait, leopardRelation,
											  &leopardTuple->t_self,
											  XLTW_InsertIndexUnique);
							CHECK_FOR_INTERRUPTS();
							goto recheck;
						}
					}
					else
					{
						/*
						 * For consistency with
						 * leopardam_scan_analyze_next_tuple(), count
						 * LEOPARDTUPLE_INSERT_IN_PROGRESS tuples as live only
						 * when inserted by our own transaction.
						 */
						reltuples += 1;
					}

					/*
					 * We must index such tuples, since if the index build
					 * commits then they're good.
					 */
					indexIt = true;
					tupleIsAlive = true;
					break;
				case LEOPARDTUPLE_DELETE_IN_PROGRESS:

					/*
					 * As with INSERT_IN_PROGRESS case, this is unexpected
					 * unless it's our own deletion or a system catalog; but
					 * in anyvisible mode, this tuple is visible.
					 */
					if (anyvisible)
					{
						indexIt = true;
						tupleIsAlive = false;
						reltuples += 1;
						break;
					}

					xwait = LeopardTupleHeaderGetUpdateXid(leopardTuple->t_data);
					if (!TransactionIdIsCurrentTransactionId(xwait))
					{
						if (!is_system_catalog)
							elog(WARNING, "concurrent delete in progress within table \"%s\"",
								 RelationGetRelationName(leopardRelation));

						/*
						 * If we are performing uniqueness checks, assuming
						 * the tuple is dead could lead to missing a
						 * uniqueness violation.  In that case we wait for the
						 * deleting transaction to finish and check again.
						 *
						 * Also, if it's a HOT-updated tuple, we should not
						 * index it but rather the live tuple at the end of
						 * the HOT-chain.  However, the deleting transaction
						 * could abort, possibly leaving this tuple as live
						 * after all, in which case it has to be indexed. The
						 * only way to know what to do is to wait for the
						 * deleting transaction to finish and check again.
						 */
						if (checking_uniqueness ||
							LeopardTupleIsHotUpdated(leopardTuple))
						{
							/*
							 * Must drop the lock on the buffer before we wait
							 */
							LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK);
							XactLockTableWait(xwait, leopardRelation,
											  &leopardTuple->t_self,
											  XLTW_InsertIndexUnique);
							CHECK_FOR_INTERRUPTS();
							goto recheck;
						}

						/*
						 * Otherwise index it but don't check for uniqueness,
						 * the same as a RECENTLY_DEAD tuple.
						 */
						indexIt = true;

						/*
						 * Count LEOPARDTUPLE_DELETE_IN_PROGRESS tuples as live,
						 * if they were not deleted by the current
						 * transaction.  That's what
						 * leopardam_scan_analyze_next_tuple() does, and we want
						 * the behavior to be consistent.
						 */
						reltuples += 1;
					}
					else if (LeopardTupleIsHotUpdated(leopardTuple))
					{
						/*
						 * It's a HOT-updated tuple deleted by our own xact.
						 * We can assume the deletion will commit (else the
						 * index contents don't matter), so treat the same as
						 * RECENTLY_DEAD HOT-updated tuples.
						 */
						indexIt = false;
						/* mark the index as unsafe for old snapshots */
						indexInfo->ii_BrokenHotChain = true;
					}
					else
					{
						/*
						 * It's a regular tuple deleted by our own xact. Index
						 * it, but don't check for uniqueness nor count in
						 * reltuples, the same as a RECENTLY_DEAD tuple.
						 */
						indexIt = true;
					}
					/* In any case, exclude the tuple from unique-checking */
					tupleIsAlive = false;
					break;
				default:
					elog(ERROR, "unexpected LeopardTupleSatisfiesVacuum result");
					indexIt = tupleIsAlive = false; /* keep compiler quiet */
					break;
			}

			LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK);

			if (!indexIt)
				continue;
		}
		else
		{
			/* leopard_getnext did the time qual check */
			tupleIsAlive = true;
			reltuples += 1;
		}

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		/* Set up for predicate or expression evaluation */
		ExecStoreBufferLeopardTuple(leopardTuple, slot, hscan->rs_cbuf);

		/*
		 * In a partial index, discard tuples that don't satisfy the
		 * predicate.
		 */
		if (predicate != NULL)
		{
			if (!ExecQual(predicate, econtext))
				continue;
		}

		/*
		 * For the current leopard tuple, extract all the attributes we use in
		 * this index, and note which are null.  This also performs evaluation
		 * of any expressions needed.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   estate,
					   values,
					   isnull);

		/*
		 * You'd think we should go ahead and build the index tuple here, but
		 * some index AMs want to do further processing on the data first.  So
		 * pass the values[] and isnull[] arrays, instead.
		 */

		if (LeopardTupleIsLeopardOnly(leopardTuple))
		{
			/*
			 * For a leopard-only tuple, pretend its TID is that of the root. See
			 * src/backend/access/leopard/README.HOT for discussion.
			 */
			ItemPointerData tid;
			OffsetNumber offnum;

			offnum = ItemPointerGetOffsetNumber(&leopardTuple->t_self);

			/*
			 * If a HOT tuple points to a root that we don't know about,
			 * obtain root items afresh.  If that still fails, report it as
			 * corruption.
			 */
			if (root_offsets[offnum - 1] == InvalidOffsetNumber)
			{
				Page		page = BufferGetPage(hscan->rs_cbuf);

				LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_SHARE);
				leopard_get_root_tuples(page, root_offsets);
				LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK);
			}

			if (!OffsetNumberIsValid(root_offsets[offnum - 1]))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg_internal("failed to find parent tuple for leopard-only tuple at (%u,%u) in table \"%s\"",
										 ItemPointerGetBlockNumber(&leopardTuple->t_self),
										 offnum,
										 RelationGetRelationName(leopardRelation))));

			ItemPointerSet(&tid, ItemPointerGetBlockNumber(&leopardTuple->t_self),
						   root_offsets[offnum - 1]);

			/* Call the AM's callback routine to process the tuple */
			callback(indexRelation, &tid, values, isnull, tupleIsAlive,
					 callback_state);
		}
		else
		{
			/* Call the AM's callback routine to process the tuple */
			callback(indexRelation, &leopardTuple->t_self, values, isnull,
					 tupleIsAlive, callback_state);
		}
	}

	/* Report scan progress one last time. */
	if (progress)
	{
		BlockNumber blks_done;

		if (hscan->rs_base.rs_parallel != NULL)
		{
			ParallelBlockTableScanDesc pbscan;

			pbscan = (ParallelBlockTableScanDesc) hscan->rs_base.rs_parallel;
			blks_done = pbscan->phs_nblocks;
		}
		else
			blks_done = hscan->rs_nblocks;

		pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_DONE,
									 blks_done);
	}

	table_endscan(scan);

	/* we can now forget our snapshot, if set and registered by us */
	if (need_unregister_snapshot)
		UnregisterSnapshot(snapshot);

	ExecDropSingleTupleTableSlot(slot);

	FreeExecutorState(estate);

	/* These may have been pointing to the now-gone estate */
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;

	return reltuples;
}

static void
leopardam_index_validate_scan(Relation leopardRelation,
						   Relation indexRelation,
						   IndexInfo *indexInfo,
						   Snapshot snapshot,
						   ValidateIndexState *state)
{
	TableScanDesc scan;
	LeopardScanDesc hscan;
	LeopardTuple	leopardTuple;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	ExprState  *predicate;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	BlockNumber root_blkno = InvalidBlockNumber;
	OffsetNumber root_offsets[MaxLeopardTuplesPerPage];
	bool		in_index[MaxLeopardTuplesPerPage];
	BlockNumber previous_blkno = InvalidBlockNumber;

	/* state variables for the merge */
	ItemPointer indexcursor = NULL;
	ItemPointerData decoded;
	bool		tuplesort_empty = false;

	/*
	 * sanity checks
	 */
	Assert(OidIsValid(indexRelation->rd_rel->relam));

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.  Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = MakeSingleTupleTableSlot(RelationGetDescr(leopardRelation),
									&TTSOpsLeopardTuple);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* Set up execution state for predicate, if any. */
	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	/*
	 * Prepare for scan of the base relation.  We need just those tuples
	 * satisfying the passed-in reference snapshot.  We must disable syncscan
	 * here, because it's critical that we read from block zero forward to
	 * match the sorted TIDs.
	 */
	scan = table_beginscan_strat(leopardRelation,	/* relation */
								 snapshot,	/* snapshot */
								 0, /* number of keys */
								 NULL,	/* scan key */
								 true,	/* buffer access strategy OK */
								 false);	/* syncscan not OK */
	hscan = (LeopardScanDesc) scan;

	pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_TOTAL,
								 hscan->rs_nblocks);

	/*
	 * Scan all tuples matching the snapshot.
	 */
	while ((leopardTuple = leopard_getnext(scan, ForwardScanDirection)) != NULL)
	{
		ItemPointer leopardcursor = &leopardTuple->t_self;
		ItemPointerData rootTuple;
		OffsetNumber root_offnum;

		CHECK_FOR_INTERRUPTS();

		state->htups += 1;

		if ((previous_blkno == InvalidBlockNumber) ||
			(hscan->rs_cblock != previous_blkno))
		{
			pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_DONE,
										 hscan->rs_cblock);
			previous_blkno = hscan->rs_cblock;
		}

		/*
		 * As commented in table_index_build_scan, we should index leopard-only
		 * tuples under the TIDs of their root tuples; so when we advance onto
		 * a new leopard page, build a map of root item offsets on the page.
		 *
		 * This complicates merging against the tuplesort output: we will
		 * visit the live tuples in order by their offsets, but the root
		 * offsets that we need to compare against the index contents might be
		 * ordered differently.  So we might have to "look back" within the
		 * tuplesort output, but only within the current page.  We handle that
		 * by keeping a bool array in_index[] showing all the
		 * already-passed-over tuplesort output TIDs of the current page. We
		 * clear that array here, when advancing onto a new leopard page.
		 */
		if (hscan->rs_cblock != root_blkno)
		{
			Page		page = BufferGetPage(hscan->rs_cbuf);

			LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_SHARE);
			leopard_get_root_tuples(page, root_offsets);
			LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK);

			memset(in_index, 0, sizeof(in_index));

			root_blkno = hscan->rs_cblock;
		}

		/* Convert actual tuple TID to root TID */
		rootTuple = *leopardcursor;
		root_offnum = ItemPointerGetOffsetNumber(leopardcursor);

		if (LeopardTupleIsLeopardOnly(leopardTuple))
		{
			root_offnum = root_offsets[root_offnum - 1];
			if (!OffsetNumberIsValid(root_offnum))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg_internal("failed to find parent tuple for leopard-only tuple at (%u,%u) in table \"%s\"",
										 ItemPointerGetBlockNumber(leopardcursor),
										 ItemPointerGetOffsetNumber(leopardcursor),
										 RelationGetRelationName(leopardRelation))));
			ItemPointerSetOffsetNumber(&rootTuple, root_offnum);
		}

		/*
		 * "merge" by skipping through the index tuples until we find or pass
		 * the current root tuple.
		 */
		while (!tuplesort_empty &&
			   (!indexcursor ||
				ItemPointerCompare(indexcursor, &rootTuple) < 0))
		{
			Datum		ts_val;
			bool		ts_isnull;

			if (indexcursor)
			{
				/*
				 * Remember index items seen earlier on the current leopard page
				 */
				if (ItemPointerGetBlockNumber(indexcursor) == root_blkno)
					in_index[ItemPointerGetOffsetNumber(indexcursor) - 1] = true;
			}

			tuplesort_empty = !tuplesort_getdatum(state->tuplesort, true,
												  &ts_val, &ts_isnull, NULL);
			Assert(tuplesort_empty || !ts_isnull);
			if (!tuplesort_empty)
			{
				itemptr_decode(&decoded, DatumGetInt64(ts_val));
				indexcursor = &decoded;

				/* If int8 is pass-by-ref, free (encoded) TID Datum memory */
#ifndef USE_FLOAT8_BYVAL
				pfree(DatumGetPointer(ts_val));
#endif
			}
			else
			{
				/* Be tidy */
				indexcursor = NULL;
			}
		}

		/*
		 * If the tuplesort has overshot *and* we didn't see a match earlier,
		 * then this tuple is missing from the index, so insert it.
		 */
		if ((tuplesort_empty ||
			 ItemPointerCompare(indexcursor, &rootTuple) > 0) &&
			!in_index[root_offnum - 1])
		{
			MemoryContextReset(econtext->ecxt_per_tuple_memory);

			/* Set up for predicate or expression evaluation */
			ExecStoreLeopardTuple(leopardTuple, slot, false);

			/*
			 * In a partial index, discard tuples that don't satisfy the
			 * predicate.
			 */
			if (predicate != NULL)
			{
				if (!ExecQual(predicate, econtext))
					continue;
			}

			/*
			 * For the current leopard tuple, extract all the attributes we use
			 * in this index, and note which are null.  This also performs
			 * evaluation of any expressions needed.
			 */
			FormIndexDatum(indexInfo,
						   slot,
						   estate,
						   values,
						   isnull);

			/*
			 * You'd think we should go ahead and build the index tuple here,
			 * but some index AMs want to do further processing on the data
			 * first. So pass the values[] and isnull[] arrays, instead.
			 */

			/*
			 * If the tuple is already committed dead, you might think we
			 * could suppress uniqueness checking, but this is no longer true
			 * in the presence of HOT, because the insert is actually a proxy
			 * for a uniqueness check on the whole HOT-chain.  That is, the
			 * tuple we have here could be dead because it was already
			 * HOT-updated, and if so the updating transaction will not have
			 * thought it should insert index entries.  The index AM will
			 * check the whole HOT-chain and correctly detect a conflict if
			 * there is one.
			 */

			index_insert(indexRelation,
						 values,
						 isnull,
						 &rootTuple,
						 leopardRelation,
						 indexInfo->ii_Unique ?
						 UNIQUE_CHECK_YES : UNIQUE_CHECK_NO,
						 false,
						 indexInfo);

			state->tups_inserted += 1;
		}
	}

	table_endscan(scan);

	ExecDropSingleTupleTableSlot(slot);

	FreeExecutorState(estate);

	/* These may have been pointing to the now-gone estate */
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;
}

/*
 * Return the number of blocks that have been read by this scan since
 * starting.  This is meant for progress reporting rather than be fully
 * accurate: in a parallel scan, workers can be concurrently reading blocks
 * further ahead than what we report.
 */
static BlockNumber
leopardam_scan_get_blocks_done(LeopardScanDesc hscan)
{
	ParallelBlockTableScanDesc bpscan = NULL;
	BlockNumber startblock;
	BlockNumber blocks_done;

	if (hscan->rs_base.rs_parallel != NULL)
	{
		bpscan = (ParallelBlockTableScanDesc) hscan->rs_base.rs_parallel;
		startblock = bpscan->phs_startblock;
	}
	else
		startblock = hscan->rs_startblock;

	/*
	 * Might have wrapped around the end of the relation, if startblock was
	 * not zero.
	 */
	if (hscan->rs_cblock > startblock)
		blocks_done = hscan->rs_cblock - startblock;
	else
	{
		BlockNumber nblocks;

		nblocks = bpscan != NULL ? bpscan->phs_nblocks : hscan->rs_nblocks;
		blocks_done = nblocks - startblock +
			hscan->rs_cblock;
	}

	return blocks_done;
}


/* ------------------------------------------------------------------------
 * Miscellaneous callbacks for the leopard AM
 * ------------------------------------------------------------------------
 */

/*
 * Check to see whether the table needs a TOAST table.  It does only if
 * (1) there are any toastable attributes, and (2) the maximum length
 * of a tuple could exceed TOAST_TUPLE_THRESHOLD.  (We don't want to
 * create a toast table for something like "f1 varchar(20)".)
 */
static bool
leopardam_relation_needs_toast_table(Relation rel)
{
	int32		data_length = 0;
	bool		maxlength_unknown = false;
	bool		has_toastable_attrs = false;
	TupleDesc	tupdesc = rel->rd_att;
	int32		tuple_length;
	int			i;

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			continue;
		data_length = att_align_nominal(data_length, att->attalign);
		if (att->attlen > 0)
		{
			/* Fixed-length types are never toastable */
			data_length += att->attlen;
		}
		else
		{
			int32		maxlen = type_maximum_size(att->atttypid,
												   att->atttypmod);

			if (maxlen < 0)
				maxlength_unknown = true;
			else
				data_length += maxlen;
			if (att->attstorage != TYPSTORAGE_PLAIN)
				has_toastable_attrs = true;
		}
	}
	if (!has_toastable_attrs)
		return false;			/* nothing to toast? */
	if (maxlength_unknown)
		return true;			/* any unlimited-length attrs? */
	tuple_length = MAXALIGN(SizeofLeopardTupleHeader +
							BITMAPLEN(tupdesc->natts)) +
		MAXALIGN(data_length);
	return (tuple_length > TOAST_TUPLE_THRESHOLD);
}

/*
 * TOAST tables for leopard relations are just leopard relations.
 */
static Oid
leopardam_relation_toast_am(Relation rel)
{
	return rel->rd_rel->relam;
}


/* ------------------------------------------------------------------------
 * Planner related callbacks for the leopard AM
 * ------------------------------------------------------------------------
 */

#define LEOPARD_OVERHEAD_BYTES_PER_TUPLE \
	(MAXALIGN(SizeofLeopardTupleHeader) + sizeof(ItemIdData))
#define LEOPARD_USABLE_BYTES_PER_PAGE \
	(BLCKSZ - SizeOfPageHeaderData)

static void
leopardam_estimate_rel_size(Relation rel, int32 *attr_widths,
						 BlockNumber *pages, double *tuples,
						 double *allvisfrac)
{
	table_block_relation_estimate_size(rel, attr_widths, pages,
									   tuples, allvisfrac,
									   LEOPARD_OVERHEAD_BYTES_PER_TUPLE,
									   LEOPARD_USABLE_BYTES_PER_PAGE);
}


/* ------------------------------------------------------------------------
 * Executor related callbacks for the leopard AM
 * ------------------------------------------------------------------------
 */

static bool
leopardam_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	LeopardScanDesc hscan = (LeopardScanDesc) scan;
	TsmRoutine *tsm = scanstate->tsmroutine;
	BlockNumber blockno;

	/* return false immediately if relation is empty */
	if (hscan->rs_nblocks == 0)
		return false;

	if (tsm->NextSampleBlock)
	{
		blockno = tsm->NextSampleBlock(scanstate, hscan->rs_nblocks);
		hscan->rs_cblock = blockno;
	}
	else
	{
		/* scanning table sequentially */

		if (hscan->rs_cblock == InvalidBlockNumber)
		{
			Assert(!hscan->rs_inited);
			blockno = hscan->rs_startblock;
		}
		else
		{
			Assert(hscan->rs_inited);

			blockno = hscan->rs_cblock + 1;

			if (blockno >= hscan->rs_nblocks)
			{
				/* wrap to beginning of rel, might not have started at 0 */
				blockno = 0;
			}

			/*
			 * Report our new scan position for synchronization purposes.
			 *
			 * Note: we do this before checking for end of scan so that the
			 * final state of the position hint is back at the start of the
			 * rel.  That's not strictly necessary, but otherwise when you run
			 * the same query multiple times the starting position would shift
			 * a little bit backwards on every invocation, which is confusing.
			 * We don't guarantee any specific ordering in general, though.
			 */
			if (scan->rs_flags & SO_ALLOW_SYNC)
				ss_report_location(scan->rs_rd, blockno);

			if (blockno == hscan->rs_startblock)
			{
				blockno = InvalidBlockNumber;
			}
		}
	}

	if (!BlockNumberIsValid(blockno))
	{
		if (BufferIsValid(hscan->rs_cbuf))
			ReleaseBuffer(hscan->rs_cbuf);
		hscan->rs_cbuf = InvalidBuffer;
		hscan->rs_cblock = InvalidBlockNumber;
		hscan->rs_inited = false;

		return false;
	}

	leopardgetpage(scan, blockno);
	hscan->rs_inited = true;

	return true;
}

static bool
leopardam_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
							  TupleTableSlot *slot)
{
	LeopardScanDesc hscan = (LeopardScanDesc) scan;
	TsmRoutine *tsm = scanstate->tsmroutine;
	BlockNumber blockno = hscan->rs_cblock;
	bool		pagemode = (scan->rs_flags & SO_ALLOW_PAGEMODE) != 0;

	Page		page;
	bool		all_visible;
	OffsetNumber maxoffset;

	/*
	 * When not using pagemode, we must lock the buffer during tuple
	 * visibility checks.
	 */
	if (!pagemode)
		LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_SHARE);

	page = (Page) BufferGetPage(hscan->rs_cbuf);
	all_visible = PageIsAllVisible(page) &&
		!scan->rs_snapshot->takenDuringRecovery;
	maxoffset = PageGetMaxOffsetNumber(page);

	for (;;)
	{
		OffsetNumber tupoffset;

		CHECK_FOR_INTERRUPTS();

		/* Ask the tablesample method which tuples to check on this page. */
		tupoffset = tsm->NextSampleTuple(scanstate,
										 blockno,
										 maxoffset);

		if (OffsetNumberIsValid(tupoffset))
		{
			ItemId		itemid;
			bool		visible;
			LeopardTuple	tuple = &(hscan->rs_ctup);

			/* Skip invalid tuple pointers. */
			itemid = PageGetItemId(page, tupoffset);
			if (!ItemIdIsNormal(itemid))
				continue;

			tuple->t_data = (LeopardTupleHeader) PageGetItem(page, itemid);
			tuple->t_len = ItemIdGetLength(itemid);
			ItemPointerSet(&(tuple->t_self), blockno, tupoffset);


			if (all_visible)
				visible = true;
			else
				visible = SampleLeopardTupleVisible(scan, hscan->rs_cbuf,
												 tuple, tupoffset);

			/* in pagemode, leopardgetpage did this for us */
			if (!pagemode)
				LeopardCheckForSerializableConflictOut(visible, scan->rs_rd, tuple,
													hscan->rs_cbuf, scan->rs_snapshot);

			/* Try next tuple from same page. */
			if (!visible)
				continue;

			/* Found visible tuple, return it. */
			if (!pagemode)
				LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK);

			ExecStoreBufferLeopardTuple(tuple, slot, hscan->rs_cbuf);

			/* Count successfully-fetched tuples as leopard fetches */
			pgstat_count_leopard_getnext(scan->rs_rd);

			return true;
		}
		else
		{
			/*
			 * If we get here, it means we've exhausted the items on this page
			 * and it's time to move to the next.
			 */
			if (!pagemode)
				LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK);

			ExecClearTuple(slot);
			return false;
		}
	}

	Assert(0);
}


/* ----------------------------------------------------------------------------
 *  Helper functions for the above.
 * ----------------------------------------------------------------------------
 */

/*
 * Reconstruct and rewrite the given tuple
 *
 * We cannot simply copy the tuple as-is, for several reasons:
 *
 * 1. We'd like to squeeze out the values of any dropped columns, both
 * to save space and to ensure we have no corner-case failures. (It's
 * possible for example that the new table hasn't got a TOAST table
 * and so is unable to store any large values of dropped cols.)
 *
 * 2. The tuple might not even be legal for the new table; this is
 * currently only known to happen as an after-effect of ALTER TABLE
 * SET WITHOUT OIDS.
 *
 * So, we must reconstruct the tuple from component Datums.
 */
static void
reform_and_rewrite_tuple(LeopardTuple tuple,
						 Relation OldLeopard, Relation NewLeopard,
						 Datum *values, bool *isnull, RewriteState rwstate)
{
	TupleDesc	oldTupDesc = RelationGetDescr(OldLeopard);
	TupleDesc	newTupDesc = RelationGetDescr(NewLeopard);
	LeopardTuple	copiedTuple;
	int			i;

	leopard_deform_tuple(tuple, oldTupDesc, values, isnull);

	/* Be sure to null out any dropped columns */
	for (i = 0; i < newTupDesc->natts; i++)
	{
		if (TupleDescAttr(newTupDesc, i)->attisdropped)
			isnull[i] = true;
	}

	copiedTuple = leopard_form_tuple(newTupDesc, values, isnull);

	/* The leopard rewrite module does the rest */
	rewrite_leopard_tuple(rwstate, tuple, copiedTuple);

	leopard_freetuple(copiedTuple);
}

/*
 * Check visibility of the tuple.
 */
static bool
SampleLeopardTupleVisible(TableScanDesc scan, Buffer buffer,
					   LeopardTuple tuple,
					   OffsetNumber tupoffset)
{
	LeopardScanDesc hscan = (LeopardScanDesc) scan;

	if (scan->rs_flags & SO_ALLOW_PAGEMODE)
	{
		/*
		 * In pageatatime mode, leopardgetpage() already did visibility checks,
		 * so just look at the info it left in rs_vistuples[].
		 *
		 * We use a binary search over the known-sorted array.  Note: we could
		 * save some effort if we insisted that NextSampleTuple select tuples
		 * in increasing order, but it's not clear that there would be enough
		 * gain to justify the restriction.
		 */
		int			start = 0,
					end = hscan->rs_ntuples - 1;

		while (start <= end)
		{
			int			mid = (start + end) / 2;
			OffsetNumber curoffset = hscan->rs_vistuples[mid];

			if (tupoffset == curoffset)
				return true;
			else if (tupoffset < curoffset)
				end = mid - 1;
			else
				start = mid + 1;
		}

		return false;
	}
	else
	{
		/* Otherwise, we have to check the tuple individually. */
		return LeopardTupleSatisfiesVisibility(tuple, scan->rs_snapshot,
											buffer);
	}
}


/* ------------------------------------------------------------------------
 * Definition of the leopard table access method.
 * ------------------------------------------------------------------------
 */

static const TableAmRoutine leopardam_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = leopardam_slot_callbacks,

	.scan_begin = leopard_beginscan,
	.scan_end = leopard_endscan,
	.scan_rescan = leopard_rescan,
	.scan_getnextslot = leopard_getnextslot,

	.scan_set_tidrange = NULL,
	.scan_getnextslot_tidrange = NULL,

	.parallelscan_estimate = table_block_parallelscan_estimate,
	.parallelscan_initialize = table_block_parallelscan_initialize,
	.parallelscan_reinitialize = table_block_parallelscan_reinitialize,

	.index_fetch_begin = leopardam_index_fetch_begin,
	.index_fetch_reset = leopardam_index_fetch_reset,
	.index_fetch_end = leopardam_index_fetch_end,
	.index_fetch_tuple = leopardam_index_fetch_tuple,

	.tuple_insert = leopardam_tuple_insert,
	.tuple_insert_speculative = leopardam_tuple_insert_speculative,
	.tuple_complete_speculative = leopardam_tuple_complete_speculative,
	.multi_insert = leopard_multi_insert,
	.tuple_delete = leopardam_tuple_delete,
	.tuple_update = leopardam_tuple_update,
	.tuple_lock = leopardam_tuple_lock,

	.tuple_fetch_row_version = leopardam_fetch_row_version,
	.tuple_get_latest_tid = leopard_get_latest_tid,
	.tuple_tid_valid = leopardam_tuple_tid_valid,
	.tuple_satisfies_snapshot = leopardam_tuple_satisfies_snapshot,
	.index_delete_tuples = leopard_index_delete_tuples,

	.relation_set_new_filenode = leopardam_relation_set_new_filenode,
	.relation_nontransactional_truncate = leopardam_relation_nontransactional_truncate,
	.relation_copy_data = leopardam_relation_copy_data,
	.relation_copy_for_cluster = leopardam_relation_copy_for_cluster,
	.relation_vacuum = leopard_vacuum_rel,
	.scan_analyze_next_block = leopardam_scan_analyze_next_block,
	.scan_analyze_next_tuple = leopardam_scan_analyze_next_tuple,
	.index_build_range_scan = leopardam_index_build_range_scan,
	.index_validate_scan = leopardam_index_validate_scan,

	.relation_size = table_block_relation_size,
	.relation_needs_toast_table = leopardam_relation_needs_toast_table,
	.relation_toast_am = leopardam_relation_toast_am,
	.relation_fetch_toast_slice = leopard_fetch_toast_slice,

	.relation_estimate_size = leopardam_estimate_rel_size,

	.scan_bitmap_next_block = NULL,
	.scan_bitmap_next_tuple = NULL,
	.scan_sample_next_block = leopardam_scan_sample_next_block,
	.scan_sample_next_tuple = leopardam_scan_sample_next_tuple
};


const TableAmRoutine *
GetLeopardamTableAmRoutine(void)
{
	return &leopardam_methods;
}

PG_FUNCTION_INFO_V1(leopard_tableam_handler);
Datum
leopard_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&leopardam_methods);
}
