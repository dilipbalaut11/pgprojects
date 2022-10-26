/*-------------------------------------------------------------------------
 *
 * pruneleopard.c
 *	  leopard page pruning and HOT-chain management code
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
 *	  src/backend/access/leopard/pruneleopard.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/leopardam.h"
#include "access/leopardam_xlog.h"
#include "access/leopardtup_details.h"
#include "access/rda_heap.h"

#include "access/transam.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "utils/snapmgr.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/* Working data for leopard_page_prune and subroutines */
typedef struct
{
	Relation	rel;

	/* tuple visibility test, initialized for the relation */
	GlobalVisState *vistest;

	/*
	 * Thresholds set by TransactionIdLimitedForOldSnapshots() if they have
	 * been computed (done on demand, and only if
	 * OldSnapshotThresholdActive()). The first time a tuple is about to be
	 * removed based on the limited horizon, old_snap_used is set to true, and
	 * SetOldSnapshotThresholdTimestamp() is called. See
	 * leopard_prune_satisfies_vacuum().
	 */
	TimestampTz old_snap_ts;
	TransactionId old_snap_xmin;
	bool		old_snap_used;

	TransactionId new_prune_xid;	/* new prune hint value for page */
	TransactionId latestRemovedXid; /* latest xid to be removed by this prune */
	int			nredirected;	/* numbers of entries in arrays below */
	int			ndead;
	int			nunused;
	/* arrays that accumulate indexes of items to be changed */
	OffsetNumber redirected[MaxLeopardTuplesPerPage * 2];
	OffsetNumber nowdead[MaxLeopardTuplesPerPage];
	OffsetNumber nowunused[MaxLeopardTuplesPerPage];

	/*
	 * marked[i] is true if item i is entered in one of the above arrays.
	 *
	 * This needs to be MaxLeopardTuplesPerPage + 1 long as FirstOffsetNumber is
	 * 1. Otherwise every access would need to subtract 1.
	 */
	bool		marked[MaxLeopardTuplesPerPage + 1];

	/*
	 * Tuple visibility is only computed once for each tuple, for correctness
	 * and efficiency reasons; see comment in leopard_page_prune() for
	 * details. This is of type int8[,] intead of HTSV_Result[], so we can use
	 * -1 to indicate no visibility has been computed, e.g. for LP_DEAD items.
	 *
	 * Same indexing as ->marked.
	 */
	int8		htsv[MaxLeopardTuplesPerPage + 1];

	/*
	 * Leopard additions
	 */
	bool		leopard_archive;	/* do we consider recently dead as well? */
	Size		dead_size;
	Size		recently_dead_size;

	int			narchive;		/* number of tuples that can be archived */
	OffsetNumber	archive[MaxLeopardTuplesPerPage + 1];		/* tuple */
	OffsetNumber	archive_root[MaxLeopardTuplesPerPage + 1];	/* root of tuple */
} PruneState;

/* Local functions */
static HTSV_Result leopard_prune_satisfies_vacuum(PruneState *prstate,
											   LeopardTuple tup,
											   Buffer buffer);
static int	leopard_prune_chain(Buffer buffer,
							 OffsetNumber rootoffnum,
							 PruneState *prstate);
static void leopard_prune_record_prunable(PruneState *prstate, TransactionId xid);
static void leopard_prune_record_redirect(PruneState *prstate,
									   OffsetNumber offnum, OffsetNumber rdoffnum);
static void leopard_prune_record_dead(PruneState *prstate, OffsetNumber offnum);
static void leopard_prune_record_unused(PruneState *prstate, OffsetNumber offnum);


/*
 * Optionally prune and repair fragmentation in the specified page.
 *
 * This is an opportunistic function.  It will perform housekeeping
 * only if the page heuristically looks like a candidate for pruning and we
 * can acquire buffer cleanup lock without blocking.
 *
 * Note: this is called quite often.  It's important that it fall out quickly
 * if there's not any use in pruning.
 *
 * Caller must have pin on the buffer, and must *not* have a lock on it.
 */
void
leopard_page_prune_opt(Relation relation, Buffer buffer)
{
	Page		page = BufferGetPage(buffer);
	TransactionId prune_xid;
	GlobalVisState *vistest;
	TransactionId limited_xmin = InvalidTransactionId;
	TimestampTz limited_ts = 0;
	Size		minfree;

	/*
	 * We can't write WAL in recovery mode, so there's no point trying to
	 * clean the page. The primary will likely issue a cleaning WAL record
	 * soon anyway, so this is no particular loss.
	 */
	if (RecoveryInProgress())
		return;

	/*
	 * XXX: Magic to keep old_snapshot_threshold tests appear "working". They
	 * currently are broken, and discussion of what to do about them is
	 * ongoing. See
	 * https://www.postgresql.org/message-id/20200403001235.e6jfdll3gh2ygbuc%40alap3.anarazel.de
	 */
	if (old_snapshot_threshold == 0)
		SnapshotTooOldMagicForTest();

	/*
	 * First check whether there's any chance there's something to prune,
	 * determining the appropriate horizon is a waste if there's no prune_xid
	 * (i.e. no updates/deletes left potentially dead tuples around).
	 */
	prune_xid = ((PageHeader) page)->pd_prune_xid;
	if (!TransactionIdIsValid(prune_xid))
		return;

	/*
	 * Check whether prune_xid indicates that there may be dead rows that can
	 * be cleaned up.
	 *
	 * It is OK to check the old snapshot limit before acquiring the cleanup
	 * lock because the worst that can happen is that we are not quite as
	 * aggressive about the cleanup (by however many transaction IDs are
	 * consumed between this point and acquiring the lock).  This allows us to
	 * save significant overhead in the case where the page is found not to be
	 * prunable.
	 *
	 * Even if old_snapshot_threshold is set, we first check whether the page
	 * can be pruned without. Both because
	 * TransactionIdLimitedForOldSnapshots() is not cheap, and because not
	 * unnecessarily relying on old_snapshot_threshold avoids causing
	 * conflicts.
	 */
	vistest = GlobalVisTestFor(relation);

	if (!GlobalVisTestIsRemovableXid(vistest, prune_xid))
	{
		if (!OldSnapshotThresholdActive())
			return;

		if (!TransactionIdLimitedForOldSnapshots(GlobalVisTestNonRemovableHorizon(vistest),
												 relation,
												 &limited_xmin, &limited_ts))
			return;

		if (!TransactionIdPrecedes(prune_xid, limited_xmin))
			return;
	}

	/*
	 * We prune when a previous UPDATE failed to find enough space on the page
	 * for a new tuple version, or when free space falls below the relation's
	 * fill-factor target (but not less than 10%).
	 *
	 * Checking free space here is questionable since we aren't holding any
	 * lock on the buffer; in the worst case we could get a bogus answer. It's
	 * unlikely to be *seriously* wrong, though, since reading either pd_lower
	 * or pd_upper is probably atomic.  Avoiding taking a lock seems more
	 * important than sometimes getting a wrong answer in what is after all
	 * just a heuristic estimate.
	 */
	minfree = RelationGetTargetPageFreeSpace(relation,
											 LEOPARD_DEFAULT_FILLFACTOR);
	minfree = Max(minfree, BLCKSZ / 10);

	if (PageIsFull(page) || PageGetHeapFreeSpace(page) < minfree)
	{
		/* OK, try to get exclusive buffer lock */
		if (!ConditionalLockBufferForCleanup(buffer))
			return;

		/*
		 * Now that we have buffer lock, get accurate information about the
		 * page's free space, and recheck the heuristic about whether to
		 * prune. (We needn't recheck PageIsPrunable, since no one else could
		 * have pruned while we hold pin.)
		 */
		if (PageIsFull(page) || PageGetHeapFreeSpace(page) < minfree)
		{
			/* OK to prune */
			(void) leopard_page_prune(relation, buffer, vistest,
								   limited_xmin, limited_ts,
								   true, NULL);
		}

		/* And release buffer lock */
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	}
}


/*
 * Prune and repair fragmentation in the specified page.
 *
 * Caller must have pin and buffer cleanup lock on the page.
 *
 * vistest is used to distinguish whether tuples are DEAD or RECENTLY_DEAD
 * (see leopard_prune_satisfies_vacuum and
 * LeopardTupleSatisfiesVacuum). old_snap_xmin / old_snap_ts need to
 * either have been set by TransactionIdLimitedForOldSnapshots, or
 * InvalidTransactionId/0 respectively.
 *
 * If report_stats is true then we send the number of reclaimed leopard-only
 * tuples to pgstats.  (This must be false during vacuum, since vacuum will
 * send its own new total to pgstats, and we don't want this delta applied
 * on top of that.)
 *
 * off_loc is the offset location required by the caller to use in error
 * callback.
 *
 * Returns the number of tuples deleted from the page during this call.
 */
int
leopard_page_prune(Relation relation, Buffer buffer,
				GlobalVisState *vistest,
				TransactionId old_snap_xmin,
				TimestampTz old_snap_ts,
				bool report_stats,
				OffsetNumber *off_loc)
{
	int			ndeleted = 0;
	Page		page = BufferGetPage(buffer);
	OffsetNumber offnum,
				maxoff;
	PruneState	prstate;
	LeopardTupleData tup;

	prstate.leopard_archive = false;
	prstate.narchive = 0;
	memset(prstate.archive, InvalidOffsetNumber, sizeof(prstate.archive));
	memset(prstate.archive_root, InvalidOffsetNumber, sizeof(prstate.archive_root));

	/*
	 * Our strategy is to scan the page and make lists of items to change,
	 * then apply the changes within a critical section.  This keeps as much
	 * logic as possible out of the critical section, and also ensures that
	 * WAL replay will work the same as the normal case.
	 *
	 * First, initialize the new pd_prune_xid value to zero (indicating no
	 * prunable tuples).  If we find any tuples which may soon become
	 * prunable, we will save the lowest relevant XID in new_prune_xid. Also
	 * initialize the rest of our working state.
	 */
	prstate.new_prune_xid = InvalidTransactionId;
	prstate.rel = relation;
	prstate.vistest = vistest;
	prstate.old_snap_xmin = old_snap_xmin;
	prstate.old_snap_ts = old_snap_ts;
	prstate.old_snap_used = false;
	prstate.latestRemovedXid = InvalidTransactionId;
	prstate.nredirected = prstate.ndead = prstate.nunused = 0;
	memset(prstate.marked, 0, sizeof(prstate.marked));

	maxoff = PageGetMaxOffsetNumber(page);
	tup.t_tableOid = RelationGetRelid(prstate.rel);

	/*
	 * Determine HTSV for all tuples.
	 *
	 * This is required for correctness to deal with cases where running HTSV
	 * twice could result in different results (e.g. RECENTLY_DEAD can turn to
	 * DEAD if another checked item causes GlobalVisTestIsRemovableFullXid()
	 * to update the horizon, INSERT_IN_PROGRESS can change to DEAD if the
	 * inserting transaction aborts, ...). That in turn could cause
	 * leopard_prune_chain() to behave incorrectly if a tuple is reached twice,
	 * once directly via a leopard_prune_chain() and once following a HOT chain.
	 *
	 * It's also good for performance. Most commonly tuples within a page are
	 * stored at decreasing offsets (while the items are stored at increasing
	 * offsets). When processing all tuples on a page this leads to reading
	 * memory at decreasing offsets within a page, with a variable stride.
	 * That's hard for CPU prefetchers to deal with. Processing the items in
	 * reverse order (and thus the tuples in increasing order) increases
	 * prefetching efficiency significantly / decreases the number of cache
	 * misses.
	 */
	for (offnum = maxoff;
		 offnum >= FirstOffsetNumber;
		 offnum = OffsetNumberPrev(offnum))
	{
		ItemId		itemid = PageGetItemId(page, offnum);
		LeopardTupleHeader leopardtup;

		/* Nothing to do if slot doesn't contain a tuple */
		if (!ItemIdIsNormal(itemid))
		{
			prstate.htsv[offnum] = -1;
			continue;
		}

		leopardtup = (LeopardTupleHeader) PageGetItem(page, itemid);
		tup.t_data = leopardtup;
		tup.t_len = ItemIdGetLength(itemid);
		ItemPointerSet(&(tup.t_self), BufferGetBlockNumber(buffer), offnum);

		/*
		 * Set the offset number so that we can display it along with any
		 * error that occurred while processing this tuple.
		 */
		if (off_loc)
			*off_loc = offnum;

		prstate.htsv[offnum] = leopard_prune_satisfies_vacuum(&prstate, &tup,
														   buffer);
	}

	/* Scan the page */
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid;

		/* Ignore items already processed as part of an earlier chain */
		if (prstate.marked[offnum])
			continue;

		/* see preceding loop */
		if (off_loc)
			*off_loc = offnum;

		/* Nothing to do if slot is empty or already dead */
		itemid = PageGetItemId(page, offnum);
		if (!ItemIdIsUsed(itemid) || ItemIdIsDead(itemid))
			continue;

		/* Process this item or chain of items */
		ndeleted += leopard_prune_chain(buffer, offnum, &prstate);
	}

	/* Clear the offset information once we have processed the given page. */
	if (off_loc)
		*off_loc = InvalidOffsetNumber;

	/* Any error while applying the changes is critical */
	START_CRIT_SECTION();

	/* Have we found any prunable items? */
	if (prstate.nredirected > 0 || prstate.ndead > 0 || prstate.nunused > 0)
	{
		/*
		 * Apply the planned item changes, then repair page fragmentation, and
		 * update the page's hint bit about whether it has free line pointers.
		 */
		leopard_page_prune_execute(buffer,
								prstate.redirected, prstate.nredirected,
								prstate.nowdead, prstate.ndead,
								prstate.nowunused, prstate.nunused, true /* do_partial_cleanup */);

		/*
		 * Update the page's pd_prune_xid field to either zero, or the lowest
		 * XID of any soon-prunable tuple.
		 */
		((PageHeader) page)->pd_prune_xid = prstate.new_prune_xid;

		/*
		 * Also clear the "page is full" flag, since there's no point in
		 * repeating the prune/defrag process until something else happens to
		 * the page.
		 */
		PageClearFull(page);

		MarkBufferDirty(buffer);

		/*
		 * Emit a WAL XLOG_LEOPARD2_PRUNE record showing what we did
		 */
		if (RelationNeedsWAL(relation))
		{
			xl_leopard_prune xlrec;
			XLogRecPtr	recptr;

			xlrec.latestRemovedXid = prstate.latestRemovedXid;
			xlrec.nredirected = prstate.nredirected;
			xlrec.ndead = prstate.ndead;

			XLogBeginInsert();
			XLogRegisterData((char *) &xlrec, SizeOfLeopardPrune);

			XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

			/*
			 * The OffsetNumber arrays are not actually in the buffer, but we
			 * pretend that they are.  When XLogInsert stores the whole
			 * buffer, the offset arrays need not be stored too.
			 */
			if (prstate.nredirected > 0)
				XLogRegisterBufData(0, (char *) prstate.redirected,
									prstate.nredirected *
									sizeof(OffsetNumber) * 2);

			if (prstate.ndead > 0)
				XLogRegisterBufData(0, (char *) prstate.nowdead,
									prstate.ndead * sizeof(OffsetNumber));

			if (prstate.nunused > 0)
				XLogRegisterBufData(0, (char *) prstate.nowunused,
									prstate.nunused * sizeof(OffsetNumber));

			recptr = XLogInsert(RM_LEOPARD2_ID, XLOG_LEOPARD2_PRUNE);

			PageSetLSN(BufferGetPage(buffer), recptr);
		}
	}
	else
	{
		/*
		 * If we didn't prune anything, but have found a new value for the
		 * pd_prune_xid field, update it and mark the buffer dirty. This is
		 * treated as a non-WAL-logged hint.
		 *
		 * Also clear the "page is full" flag if it is set, since there's no
		 * point in repeating the prune/defrag process until something else
		 * happens to the page.
		 */
		if (((PageHeader) page)->pd_prune_xid != prstate.new_prune_xid ||
			PageIsFull(page))
		{
			((PageHeader) page)->pd_prune_xid = prstate.new_prune_xid;
			PageClearFull(page);
			MarkBufferDirtyHint(buffer, true);
		}
	}

	END_CRIT_SECTION();

	/*
	 * If requested, report the number of tuples reclaimed to pgstats. This is
	 * ndeleted minus ndead, because we don't want to count a now-DEAD root
	 * item as a deletion for this purpose.
	 */
	if (report_stats && ndeleted > prstate.ndead)
		pgstat_update_leopard_dead_tuples(relation, ndeleted - prstate.ndead);

	/*
	 * XXX Should we update the FSM information of this page ?
	 *
	 * There are two schools of thought here. We may not want to update FSM
	 * information so that the page is not used for unrelated UPDATEs/INSERTs
	 * and any free space in this page will remain available for further
	 * UPDATEs in *this* page, thus improving chances for doing HOT updates.
	 *
	 * But for a large table and where a page does not receive further UPDATEs
	 * for a long time, we might waste this space by not updating the FSM
	 * information. The relation may get extended and fragmented further.
	 *
	 * One possibility is to leave "fillfactor" worth of space in this page
	 * and update FSM with the remaining space.
	 */

	return ndeleted;
}


/*
 * Perform visibility checks for leopard pruning.
 *
 * This is more complicated than just using GlobalVisTestIsRemovableXid()
 * because of old_snapshot_threshold. We only want to increase the threshold
 * that triggers errors for old snapshots when we actually decide to remove a
 * row based on the limited horizon.
 *
 * Due to its cost we also only want to call
 * TransactionIdLimitedForOldSnapshots() if necessary, i.e. we might not have
 * done so in leopard_hot_prune_opt() if pd_prune_xid was old enough. But we
 * still want to be able to remove rows that are too new to be removed
 * according to prstate->vistest, but that can be removed based on
 * old_snapshot_threshold. So we call TransactionIdLimitedForOldSnapshots() on
 * demand in here, if appropriate.
 */
static HTSV_Result
leopard_prune_satisfies_vacuum(PruneState *prstate, LeopardTuple tup, Buffer buffer)
{
	HTSV_Result res;
	TransactionId dead_after;

	res = LeopardTupleSatisfiesVacuumHorizon(tup, buffer, &dead_after);

	if (res != LEOPARDTUPLE_RECENTLY_DEAD)
		return res;

	/*
	 * If we are already relying on the limited xmin, there is no need to
	 * delay doing so anymore.
	 */
	if (prstate->old_snap_used)
	{
		Assert(TransactionIdIsValid(prstate->old_snap_xmin));

		if (TransactionIdPrecedes(dead_after, prstate->old_snap_xmin))
			res = LEOPARDTUPLE_DEAD;
		return res;
	}

	/*
	 * First check if GlobalVisTestIsRemovableXid() is sufficient to find the
	 * row dead. If not, and old_snapshot_threshold is enabled, try to use the
	 * lowered horizon.
	 */
	if (GlobalVisTestIsRemovableXid(prstate->vistest, dead_after))
		res = LEOPARDTUPLE_DEAD;
	else if (OldSnapshotThresholdActive())
	{
		/* haven't determined limited horizon yet, requests */
		if (!TransactionIdIsValid(prstate->old_snap_xmin))
		{
			TransactionId horizon =
			GlobalVisTestNonRemovableHorizon(prstate->vistest);

			TransactionIdLimitedForOldSnapshots(horizon, prstate->rel,
												&prstate->old_snap_xmin,
												&prstate->old_snap_ts);
		}

		if (TransactionIdIsValid(prstate->old_snap_xmin) &&
			TransactionIdPrecedes(dead_after, prstate->old_snap_xmin))
		{
			/*
			 * About to remove row based on snapshot_too_old. Need to raise
			 * the threshold so problematic accesses would error.
			 */
			Assert(!prstate->old_snap_used);
			SetOldSnapshotThresholdTimestamp(prstate->old_snap_ts,
											 prstate->old_snap_xmin);
			prstate->old_snap_used = true;
			res = LEOPARDTUPLE_DEAD;
		}
	}

	return res;
}


/*
 * Prune specified line pointer or a HOT chain originating at line pointer.
 *
 * If the item is an index-referenced tuple (i.e. not a leopard-only tuple),
 * the HOT chain is pruned by removing all DEAD tuples at the start of the HOT
 * chain.  We also prune any RECENTLY_DEAD tuples preceding a DEAD tuple.
 * This is OK because a RECENTLY_DEAD tuple preceding a DEAD tuple is really
 * DEAD, our visibility test is just too coarse to detect it.
 *
 * The root line pointer is redirected to the tuple immediately after the
 * latest DEAD tuple.  If all tuples in the chain are DEAD, the root line
 * pointer is marked LP_DEAD.  (This includes the case of a DEAD simple
 * tuple, which we treat as a chain of length 1.)
 *
 * We don't actually change the page here. We just add entries to the arrays in
 * prstate showing the changes to be made.  Items to be redirected are added
 * to the redirected[] array (two entries per redirection); items to be set to
 * LP_DEAD state are added to nowdead[]; and items to be set to LP_UNUSED
 * state are added to nowunused[].
 *
 * Returns the number of tuples (to be) deleted from the page.
 */
static int
leopard_prune_chain(Buffer buffer, OffsetNumber rootoffnum, PruneState *prstate)
{
	int			ndeleted = 0;
	Page		dp = (Page) BufferGetPage(buffer);
	TransactionId priorXmax = InvalidTransactionId;
	ItemId		rootlp;
	LeopardTupleHeader leopardtup;
	OffsetNumber latestdead = InvalidOffsetNumber,
				maxoff = PageGetMaxOffsetNumber(dp),
				offnum;
	OffsetNumber chainitems[MaxLeopardTuplesPerPage];
	int			nchain = 0,
				i;

	rootlp = PageGetItemId(dp, rootoffnum);

	/*
	 * If it's a leopard-only tuple, then it is not the start of a HOT chain.
	 */
	if (ItemIdIsNormal(rootlp))
	{
		Assert(prstate->htsv[rootoffnum] != -1);
		leopardtup = (LeopardTupleHeader) PageGetItem(dp, rootlp);

		if (LeopardTupleHeaderIsLeopardOnly(leopardtup))
		{
			/*
			 * If the tuple is DEAD and doesn't chain to anything else, mark
			 * it unused immediately.  (If it does chain, we can only remove
			 * it as part of pruning its chain.)
			 *
			 * We need this primarily to handle aborted HOT updates, that is,
			 * XMIN_INVALID leopard-only tuples.  Those might not be linked to by
			 * any chain, since the parent tuple might be re-updated before
			 * any pruning occurs.  So we have to be able to reap them
			 * separately from chain-pruning.  (Note that
			 * LeopardTupleHeaderIsHotUpdated will never return true for an
			 * XMIN_INVALID tuple, so this code will work even when there were
			 * sequential updates within the aborted transaction.)
			 *
			 * Note that we might first arrive at a dead leopard-only tuple
			 * either here or while following a chain below.  Whichever path
			 * gets there first will mark the tuple unused.
			 */
			if (prstate->htsv[rootoffnum] == LEOPARDTUPLE_DEAD &&
				!LeopardTupleHeaderIsHotUpdated(leopardtup))
			{
				leopard_prune_record_unused(prstate, rootoffnum);
				LeopardTupleHeaderAdvanceLatestRemovedXid(leopardtup,
													   &prstate->latestRemovedXid);
				ndeleted++;
			}

			/* Nothing more to do */
			return ndeleted;
		}
	}

	/* Start from the root tuple */
	offnum = rootoffnum;

	/* while not end of the chain */
	for (;;)
	{
		ItemId		lp;
		bool		tupdead,
					recent_dead;

		/* Some sanity checks */
		if (offnum < FirstOffsetNumber || offnum > maxoff)
			break;

		/* If item is already processed, stop --- it must not be same chain */
		if (prstate->marked[offnum])
			break;

		lp = PageGetItemId(dp, offnum);

		/* Unused item obviously isn't part of the chain */
		if (!ItemIdIsUsed(lp))
			break;

		/*
		 * If we are looking at the redirected root line pointer, jump to the
		 * first normal tuple in the chain.  If we find a redirect somewhere
		 * else, stop --- it must not be same chain.
		 */
		if (ItemIdIsRedirected(lp))
		{
			if (nchain > 0)
				break;			/* not at start of chain */
			chainitems[nchain++] = offnum;
			offnum = ItemIdGetRedirect(rootlp);
			continue;
		}

		/*
		 * Likewise, a dead line pointer can't be part of the chain. (We
		 * already eliminated the case of dead root tuple outside this
		 * function.)
		 */
		if (ItemIdIsDead(lp))
			break;

		Assert(ItemIdIsNormal(lp));
		Assert(prstate->htsv[offnum] != -1);
		leopardtup = (LeopardTupleHeader) PageGetItem(dp, lp);

		/*
		 * Check the tuple XMIN against prior XMAX, if any
		 */
		if (TransactionIdIsValid(priorXmax) &&
			!TransactionIdEquals(LeopardTupleHeaderGetXmin(leopardtup), priorXmax))
			break;

		/*
		 * OK, this tuple is indeed a member of the chain.
		 */
		chainitems[nchain++] = offnum;

		/*
		 * Check tuple's visibility status.
		 */
		tupdead = recent_dead = false;

		switch ((HTSV_Result) prstate->htsv[offnum])
		{
			case LEOPARDTUPLE_DEAD:
				tupdead = true;

				/* Leopard: keep track of dead tuple lengths */
				prstate->dead_size += ItemIdGetLength(lp);

				break;

			case LEOPARDTUPLE_RECENTLY_DEAD:
				/*
				 * Is the tuple archivable? Needs to be an UPDATEd tuple,
				 * so that we don't remove the last tuple in a chain, if it
				 * was deleted since we need it there to allow us to judge
				 * if the chain contains an archived tuple.
				 */
				if (prstate->leopard_archive &&
					LeopardTupleHeaderIsHotUpdated(leopardtup))
				{
					prstate->recently_dead_size += ItemIdGetLength(lp);
					prstate->archive[prstate->narchive] = offnum;
					prstate->archive_root[prstate->narchive] = rootoffnum;
					prstate->narchive++;
					tupdead = true;
					break;
				}

				recent_dead = true;

				/*
				 * This tuple may soon become DEAD.  Update the hint field so
				 * that the page is reconsidered for pruning in future.
				 */
				leopard_prune_record_prunable(prstate,
										   LeopardTupleHeaderGetUpdateXid(leopardtup));
				break;

			case LEOPARDTUPLE_DELETE_IN_PROGRESS:

				/*
				 * This tuple may soon become DEAD.  Update the hint field so
				 * that the page is reconsidered for pruning in future.
				 */
				leopard_prune_record_prunable(prstate,
										   LeopardTupleHeaderGetUpdateXid(leopardtup));
				break;

			case LEOPARDTUPLE_LIVE:
			case LEOPARDTUPLE_INSERT_IN_PROGRESS:

				/*
				 * If we wanted to optimize for aborts, we might consider
				 * marking the page prunable when we see INSERT_IN_PROGRESS.
				 * But we don't.  See related decisions about when to mark the
				 * page prunable in leopardam.c.
				 */
				break;

			default:
				elog(ERROR, "unexpected LeopardTupleSatisfiesVacuum result");
				break;
		}

		/*
		 * Remember the last DEAD tuple seen.  We will advance past
		 * RECENTLY_DEAD tuples just in case there's a DEAD one after them;
		 * but we can't advance past anything else.  (XXX is it really worth
		 * continuing to scan beyond RECENTLY_DEAD?  The case where we will
		 * find another DEAD tuple is a fairly unusual corner case.)
		 */
		if (tupdead)
		{
			latestdead = offnum;
			LeopardTupleHeaderAdvanceLatestRemovedXid(leopardtup,
												   &prstate->latestRemovedXid);
		}
		else if (!recent_dead)
			break;

		/*
		 * If the tuple is not HOT-updated, then we are at the end of this
		 * HOT-update chain.
		 */
		if (!LeopardTupleHeaderIsHotUpdated(leopardtup))
			break;

		/* HOT implies it can't have moved to different partition */
		Assert(!LeopardTupleHeaderIndicatesMovedPartitions(leopardtup));

		/*
		 * Advance to next chain member.
		 */
		Assert(ItemPointerGetBlockNumber(&leopardtup->t_ctid) ==
			   BufferGetBlockNumber(buffer));
		offnum = ItemPointerGetOffsetNumber(&leopardtup->t_ctid);
		priorXmax = LeopardTupleHeaderGetUpdateXid(leopardtup);
	}

	/*
	 * If we found a DEAD tuple in the chain, adjust the HOT chain so that all
	 * the DEAD tuples at the start of the chain are removed and the root line
	 * pointer is appropriately redirected.
	 */
	if (OffsetNumberIsValid(latestdead))
	{
		/*
		 * Mark as unused each intermediate item that we are able to remove
		 * from the chain.
		 *
		 * When the previous item is the last dead tuple seen, we are at the
		 * right candidate for redirection.
		 */
		for (i = 1; (i < nchain) && (chainitems[i - 1] != latestdead); i++)
		{
			leopard_prune_record_unused(prstate, chainitems[i]);
			ndeleted++;
		}

		/*
		 * If the root entry had been a normal tuple, we are deleting it, so
		 * count it in the result.  But changing a redirect (even to DEAD
		 * state) doesn't count.
		 */
		if (ItemIdIsNormal(rootlp))
			ndeleted++;

		/*
		 * If the DEAD tuple is at the end of the chain, the entire chain is
		 * dead and the root line pointer can be marked dead.  Otherwise just
		 * redirect the root to the correct chain member.
		 */
		if (i >= nchain)
			leopard_prune_record_dead(prstate, rootoffnum);
		else
			leopard_prune_record_redirect(prstate, rootoffnum, chainitems[i]);
	}
	else if (nchain < 2 && ItemIdIsRedirected(rootlp))
	{
		/*
		 * We found a redirect item that doesn't point to a valid follow-on
		 * item.  This can happen if the loop in leopard_page_prune caused us to
		 * visit the dead successor of a redirect item before visiting the
		 * redirect item.  We can clean up by setting the redirect item to
		 * DEAD state.
		 */
		leopard_prune_record_dead(prstate, rootoffnum);
	}

	if (LEOPARD_DEBUG && nchain > 1 && prstate->leopard_archive)
	{
		elog(NOTICE, "rootoffnum %u latestdead %u", rootoffnum, latestdead);
		for (i = 0; i < nchain; i++)
		{
			elog(NOTICE, " offset %u", chainitems[i]);
		}
	}

	return ndeleted;
}

/* Record lowest soon-prunable XID */
static void
leopard_prune_record_prunable(PruneState *prstate, TransactionId xid)
{
	/*
	 * This should exactly match the PageSetPrunable macro.  We can't store
	 * directly into the page header yet, so we update working state.
	 */
	Assert(TransactionIdIsNormal(xid));
	if (!TransactionIdIsValid(prstate->new_prune_xid) ||
		TransactionIdPrecedes(xid, prstate->new_prune_xid))
		prstate->new_prune_xid = xid;
}

/* Record line pointer to be redirected */
static void
leopard_prune_record_redirect(PruneState *prstate,
						   OffsetNumber offnum, OffsetNumber rdoffnum)
{
	Assert(prstate->nredirected < MaxLeopardTuplesPerPage);
	prstate->redirected[prstate->nredirected * 2] = offnum;
	prstate->redirected[prstate->nredirected * 2 + 1] = rdoffnum;
	prstate->nredirected++;
	Assert(!prstate->marked[offnum]);
	prstate->marked[offnum] = true;
	Assert(!prstate->marked[rdoffnum]);
	prstate->marked[rdoffnum] = true;
}

/* Record line pointer to be marked dead */
static void
leopard_prune_record_dead(PruneState *prstate, OffsetNumber offnum)
{
	Assert(prstate->ndead < MaxLeopardTuplesPerPage);
	prstate->nowdead[prstate->ndead] = offnum;
	prstate->ndead++;
	Assert(!prstate->marked[offnum]);
	prstate->marked[offnum] = true;
}

/* Record line pointer to be marked unused */
static void
leopard_prune_record_unused(PruneState *prstate, OffsetNumber offnum)
{
	Assert(prstate->nunused < MaxLeopardTuplesPerPage);
	prstate->nowunused[prstate->nunused] = offnum;
	prstate->nunused++;
	Assert(!prstate->marked[offnum]);
	prstate->marked[offnum] = true;
}


/*
 * Perform the actual page changes needed by leopard_page_prune.
 * It is expected that the caller has a super-exclusive lock on the
 * buffer.
 */
void
leopard_page_prune_execute(Buffer buffer,
						OffsetNumber *redirected, int nredirected,
						OffsetNumber *nowdead, int ndead,
						OffsetNumber *nowunused, int nunused, bool do_partial_cleanup)
{
	Page		page = (Page) BufferGetPage(buffer);
	OffsetNumber *offnum;
	int			i;

	/* Shouldn't be called unless there's something to do */
	Assert(nredirected > 0 || ndead > 0 || nunused > 0);

	/* Update all redirected line pointers */
	offnum = redirected;
	for (i = 0; i < nredirected; i++)
	{
		OffsetNumber fromoff = *offnum++;
		OffsetNumber tooff = *offnum++;
		ItemId		fromlp = PageGetItemId(page, fromoff);

		ItemIdSetRedirect(fromlp, tooff);
	}

	/* Update all now-dead line pointers */
	offnum = nowdead;
	for (i = 0; i < ndead; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);

		ItemIdSetDead(lp);
	}

	/* Update all now-unused line pointers */
	offnum = nowunused;
	for (i = 0; i < nunused; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);

		ItemIdSetUnused(lp);
	}

	/*
	 * Finally, repair any fragmentation, and update the page's hint bit about
	 * whether it has free pointers.
	 */
	PageRepairFragmentation(page);
}


/*
 * For all items in this page, find their respective root line pointers.
 * If item k is part of a HOT-chain with root at item j, then we set
 * root_offsets[k - 1] = j.
 *
 * The passed-in root_offsets array must have MaxLeopardTuplesPerPage entries.
 * Unused entries are filled with InvalidOffsetNumber (zero).
 *
 * The function must be called with at least share lock on the buffer, to
 * prevent concurrent prune operations.
 *
 * Note: The information collected here is valid only as long as the caller
 * holds a pin on the buffer. Once pin is released, a tuple might be pruned
 * and reused by a completely unrelated tuple.
 */
void
leopard_get_root_tuples(Page page, OffsetNumber *root_offsets)
{
	OffsetNumber offnum,
				maxoff;

	MemSet(root_offsets, InvalidOffsetNumber,
		   MaxLeopardTuplesPerPage * sizeof(OffsetNumber));

	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum))
	{
		ItemId		lp = PageGetItemId(page, offnum);
		LeopardTupleHeader leopardtup;
		OffsetNumber nextoffnum;
		TransactionId priorXmax;

		/* skip unused and dead items */
		if (!ItemIdIsUsed(lp) || ItemIdIsDead(lp))
			continue;

		if (ItemIdIsNormal(lp))
		{
			leopardtup = (LeopardTupleHeader) PageGetItem(page, lp);

			/*
			 * Check if this tuple is part of a HOT-chain rooted at some other
			 * tuple. If so, skip it for now; we'll process it when we find
			 * its root.
			 */
			if (LeopardTupleHeaderIsLeopardOnly(leopardtup))
				continue;

			/*
			 * This is either a plain tuple or the root of a HOT-chain.
			 * Remember it in the mapping.
			 */
			root_offsets[offnum - 1] = offnum;

			/* If it's not the start of a HOT-chain, we're done with it */
			if (!LeopardTupleHeaderIsHotUpdated(leopardtup))
				continue;

			/* Set up to scan the HOT-chain */
			nextoffnum = ItemPointerGetOffsetNumber(&leopardtup->t_ctid);
			priorXmax = LeopardTupleHeaderGetUpdateXid(leopardtup);
		}
		else
		{
			/* Must be a redirect item. We do not set its root_offsets entry */
			Assert(ItemIdIsRedirected(lp));
			/* Set up to scan the HOT-chain */
			nextoffnum = ItemIdGetRedirect(lp);
			priorXmax = InvalidTransactionId;
		}

		/*
		 * Now follow the HOT-chain and collect other tuples in the chain.
		 *
		 * Note: Even though this is a nested loop, the complexity of the
		 * function is O(N) because a tuple in the page should be visited not
		 * more than twice, once in the outer loop and once in HOT-chain
		 * chases.
		 */
		for (;;)
		{
			/* Sanity check */
			if (nextoffnum < FirstOffsetNumber || nextoffnum > maxoff)
				break;

			lp = PageGetItemId(page, nextoffnum);

			/* Check for broken chains */
			if (!ItemIdIsNormal(lp))
				break;

			leopardtup = (LeopardTupleHeader) PageGetItem(page, lp);

			if (TransactionIdIsValid(priorXmax) &&
				!TransactionIdEquals(priorXmax, LeopardTupleHeaderGetXmin(leopardtup)))
				break;

			/* Remember the root line pointer for this item */
			root_offsets[nextoffnum - 1] = offnum;

			/* Advance to next chain member, if any */
			if (!LeopardTupleHeaderIsHotUpdated(leopardtup))
				break;

			/* HOT implies it can't have moved to different partition */
			Assert(!LeopardTupleHeaderIndicatesMovedPartitions(leopardtup));

			nextoffnum = ItemPointerGetOffsetNumber(&leopardtup->t_ctid);
			priorXmax = LeopardTupleHeaderGetUpdateXid(leopardtup);
		}
	}
}

/*
 * leopard_make_space_for_update
 *
 * Just as we do with RelationGetBufferForTuple(), if we return true,
 * returns pinned and exclusive-locked buffer.
 *
 * Returns true if enough space in the buffer now exists for the supplied tuple,
 * or false if there is still insufficient space.
 *
 * First, we see if we can get a valid cleanup lock, if not return false.
 *
 * Next, scan the buffer and see if we can prune normally to make space. If so,
 * do that and then return true.
 *
 * Lastly, we try to archive recently dead rows. If that is possible, do it,
 * and if it works then return true.
 *
 * Or if none of that works, return false.
 */
bool
leopard_make_space_for_update(Relation relation, Buffer buffer, Size newtupsize)
{
	GlobalVisState *vistest;
	int			ndeleted = 0;
	Page		page = BufferGetPage(buffer);
	OffsetNumber offnum,
				maxoff;
	PruneState	prstate;
	LeopardTupleData tup;

	/*
	 * We are called with an Exclusive buffer lock
	 *
	 * Check that we have a valid cleanup lock.
	 *
	 * Don't use the RDA for rows from temporary tables
	 */
	if (BufferIsLocal(buffer))
		return false;
	else
	{
		BufferDesc *bufHdr = GetBufferDescriptor(buffer - 1);
		uint32		buf_state = LockBufHdr(bufHdr);
		bool		cleanup_ok = false;

		/*
		 * If the buffer is pinned only by us, partial cleanup is ok.
		 *
		 * This assumes that the executor scan for this UPDATE already
		 * pinned the buffer before we execute leopard_update().
		 * As a result, we don't bother to call IsBufferCleanupOK()
		 */
		Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
		if (BUF_STATE_GET_REFCOUNT(buf_state) == 1)
			cleanup_ok = true;

		UnlockBufHdr(bufHdr, buf_state);

		if (!cleanup_ok)
		{
			if (LEOPARD_DEBUG)
				elog(NOTICE, "cleanup not ok");
			return false;
		}
	}

	vistest = GlobalVisTestFor(relation);

	prstate.dead_size = prstate.recently_dead_size = 0;
	prstate.leopard_archive = true;
	prstate.narchive = 0;
	memset(prstate.archive, InvalidOffsetNumber, sizeof(prstate.archive));
	memset(prstate.archive_root, InvalidOffsetNumber, sizeof(prstate.archive_root));

	/* From here, much of this function is identical to heap_page_prune() */

	/*
	 * Our strategy is to scan the page and make lists of items to change,
	 * then apply the changes within a critical section.  This keeps as much
	 * logic as possible out of the critical section, and also ensures that
	 * WAL replay will work the same as the normal case.
	 *
	 * First, initialize the new pd_prune_xid value to zero (indicating no
	 * prunable tuples).  If we find any tuples which may soon become
	 * prunable, we will save the lowest relevant XID in new_prune_xid. Also
	 * initialize the rest of our working state.
	 */
	prstate.new_prune_xid = InvalidTransactionId;
	prstate.rel = relation;
	prstate.vistest = vistest;
	prstate.old_snap_xmin = InvalidTransactionId;
	prstate.old_snap_ts = 0;
	prstate.old_snap_used = false;
	prstate.latestRemovedXid = InvalidTransactionId;
	prstate.nredirected = prstate.ndead = prstate.nunused = 0;
	memset(prstate.marked, 0, sizeof(prstate.marked));

	maxoff = PageGetMaxOffsetNumber(page);
	tup.t_tableOid = RelationGetRelid(prstate.rel);

	/*
	 * Determine HTSV for all tuples.
	 *
	 * This is required for correctness to deal with cases where running HTSV
	 * twice could result in different results (e.g. RECENTLY_DEAD can turn to
	 * DEAD if another checked item causes GlobalVisTestIsRemovableFullXid()
	 * to update the horizon, INSERT_IN_PROGRESS can change to DEAD if the
	 * inserting transaction aborts, ...). That in turn could cause
	 * leopard_prune_chain() to behave incorrectly if a tuple is reached twice,
	 * once directly via a leopard_prune_chain() and once following a HOT chain.
	 *
	 * It's also good for performance. Most commonly tuples within a page are
	 * stored at decreasing offsets (while the items are stored at increasing
	 * offsets). When processing all tuples on a page this leads to reading
	 * memory at decreasing offsets within a page, with a variable stride.
	 * That's hard for CPU prefetchers to deal with. Processing the items in
	 * reverse order (and thus the tuples in increasing order) increases
	 * prefetching efficiency significantly / decreases the number of cache
	 * misses.
	 */
	for (offnum = maxoff;
		 offnum >= FirstOffsetNumber;
		 offnum = OffsetNumberPrev(offnum))
	{
		ItemId		itemid = PageGetItemId(page, offnum);
		LeopardTupleHeader leopardtup;

		/* Nothing to do if slot doesn't contain a tuple */
		if (!ItemIdIsNormal(itemid))
		{
			prstate.htsv[offnum] = -1;
			continue;
		}

		leopardtup = (LeopardTupleHeader) PageGetItem(page, itemid);
		tup.t_data = leopardtup;
		tup.t_len = ItemIdGetLength(itemid);
		ItemPointerSet(&(tup.t_self), BufferGetBlockNumber(buffer), offnum);

		prstate.htsv[offnum] = leopard_prune_satisfies_vacuum(&prstate, &tup,
														   buffer);
	}

	/* Scan the page */
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid;

		/* Ignore items already processed as part of an earlier chain */
		if (prstate.marked[offnum])
			continue;

		/* Nothing to do if slot is empty or already dead */
		itemid = PageGetItemId(page, offnum);
		if (!ItemIdIsUsed(itemid) || ItemIdIsDead(itemid))
			continue;

		/* Process this item or chain of items */
		ndeleted += leopard_prune_chain(buffer, offnum, &prstate);
	}

	if (LEOPARD_DEBUG && prstate.dead_size > 0 && prstate.recently_dead_size > 0)
	{
		elog(NOTICE, "dead space %zu  recently dead space %zu", prstate.dead_size, prstate.recently_dead_size);
		elog(NOTICE, "narchive %u nredirected %u ndead %u nunused %u", prstate.narchive, prstate.nredirected, prstate.ndead, prstate.nunused);
	}

	/*
	 * Decide whether to prune to make space.
	 */
	if (prstate.dead_size < newtupsize)
	{
		Oid	toid = relation->rd_id;
		int i = 0;

		/*
		 * Is there enough recently dead space that we can archive to make space?
		 */
		if (prstate.recently_dead_size < newtupsize)
			return false;

		/*
		 * XXX archiving everything may be counter productive, so consider whether
		 * we should archive just a couple of rows to make space. If we do less
		 * than all tuples it would make sense to pick the earliest rows, by
		 * doing some kind of limit sort on xmax.
		 * XXX potentially we could archive just from the chain being updated,
		 * rather than randomly selected tuples.
		 */
		while (OffsetNumberIsValid(prstate.archive[i]))
		{
			OffsetNumber	offnum = prstate.archive[i];
			OffsetNumber	rootoffnum = prstate.archive_root[i];
			ItemId			itemid = PageGetItemId(page, offnum);
			ItemPointerData	roottid;
			LeopardTupleHeader leopardtup;

			if (LEOPARD_DEBUG)
				elog(NOTICE, "archive offnum %u root %u", offnum, rootoffnum);

				/* Nothing to do if slot doesn't contain a tuple */
			if (!ItemIdIsNormal(itemid))
			{
				if (LEOPARD_DEBUG)
					elog(ERROR, "item is not normal");
				continue;
			}

			leopardtup = (LeopardTupleHeader) PageGetItem(page, itemid);
			tup.t_data = leopardtup;
			tup.t_len = ItemIdGetLength(itemid);
			ItemPointerSet(&(tup.t_self), BufferGetBlockNumber(buffer), offnum);
			ItemPointerSet(&roottid, BufferGetBlockNumber(buffer), rootoffnum);

			rda_insert(toid, (HeapTuple) &tup, roottid);

			i++;
		}
	}

	/* Any error while applying the changes is critical */
	START_CRIT_SECTION();

	/* Have we found any prunable items? */
	if (prstate.nredirected > 0 || prstate.ndead > 0 || prstate.nunused > 0)
	{
		/*
		 * Apply the planned item changes, then repair page fragmentation, and
		 * update the page's hint bit about whether it has free line pointers.
		 */
		leopard_page_prune_execute(buffer,
								prstate.redirected, prstate.nredirected,
								prstate.nowdead, prstate.ndead,
								prstate.nowunused, prstate.nunused, true /* do_partial_cleanup */);

		/*
		 * Update the page's pd_prune_xid field to either zero, or the lowest
		 * XID of any soon-prunable tuple.
		 */
		((PageHeader) page)->pd_prune_xid = prstate.new_prune_xid;

		/*
		 * Also clear the "page is full" flag, since there's no point in
		 * repeating the prune/defrag process until something else happens to
		 * the page.
		 */
		PageClearFull(page);

		MarkBufferDirty(buffer);

		/*
		 * Emit a WAL XLOG_LEOPARD2_PRUNE record showing what we did
		 */
		if (RelationNeedsWAL(relation))
		{
			xl_leopard_prune xlrec;
			XLogRecPtr	recptr;

			/*
			 * Don't set latestRemovedXid if we are archiving, because the rows
			 * will still be accessible on a standby, so there is no need to
			 * cause a recovery conflict. InvalidTransactionId is a valid value
			 * to send, see ResolveRecoveryConflictWithSnapshot().
			 */
			if (prstate.leopard_archive)
				xlrec.latestRemovedXid = InvalidTransactionId;
			else
				xlrec.latestRemovedXid = prstate.latestRemovedXid;
			xlrec.nredirected = prstate.nredirected;
			xlrec.ndead = prstate.ndead;

			XLogBeginInsert();
			XLogRegisterData((char *) &xlrec, SizeOfLeopardPrune);

			XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

			/*
			 * The OffsetNumber arrays are not actually in the buffer, but we
			 * pretend that they are.  When XLogInsert stores the whole
			 * buffer, the offset arrays need not be stored too.
			 */
			if (prstate.nredirected > 0)
				XLogRegisterBufData(0, (char *) prstate.redirected,
									prstate.nredirected *
									sizeof(OffsetNumber) * 2);

			if (prstate.ndead > 0)
				XLogRegisterBufData(0, (char *) prstate.nowdead,
									prstate.ndead * sizeof(OffsetNumber));

			if (prstate.nunused > 0)
				XLogRegisterBufData(0, (char *) prstate.nowunused,
									prstate.nunused * sizeof(OffsetNumber));

			recptr = XLogInsert(RM_LEOPARD2_ID, XLOG_LEOPARD2_PRUNE);

			PageSetLSN(BufferGetPage(buffer), recptr);
		}
	}
	else if (!prstate.leopard_archive)
	{
		/*
		 * If we didn't prune anything, but have found a new value for the
		 * pd_prune_xid field, update it and mark the buffer dirty. This is
		 * treated as a non-WAL-logged hint.
		 *
		 * Also clear the "page is full" flag if it is set, since there's no
		 * point in repeating the prune/defrag process until something else
		 * happens to the page.
		 */
		if (((PageHeader) page)->pd_prune_xid != prstate.new_prune_xid ||
			PageIsFull(page))
		{
			((PageHeader) page)->pd_prune_xid = prstate.new_prune_xid;
			PageClearFull(page);
			MarkBufferDirtyHint(buffer, true);
		}
	}

	END_CRIT_SECTION();

	/*
	 * If requested, report the number of tuples reclaimed to pgstats. This is
	 * ndeleted minus ndead, because we don't want to count a now-DEAD root
	 * item as a deletion for this purpose.
	 */
	if (!prstate.leopard_archive && ndeleted > prstate.ndead)
		pgstat_update_leopard_dead_tuples(relation, ndeleted - prstate.ndead);

	/* Don't release buffer lock or pin */
	return true;
}
