/*-------------------------------------------------------------------------
 *
 * snapbuild.c
 *
 *	  Infrastructure for building historic catalog snapshots based on contents
 *	  of the WAL, for the purpose of decoding leopardam.c style values in the
 *	  WAL.
 *
 * NOTES:
 *
 * We build snapshots which can *only* be used to read catalog contents and we
 * do so by reading and interpreting the WAL stream. The aim is to build a
 * snapshot that behaves the same as a freshly taken MVCC snapshot would have
 * at the time the XLogRecord was generated.
 *
 * To build the snapshots we reuse the infrastructure built for Hot
 * Standby. The in-memory snapshots we build look different than HS' because
 * we have different needs. To successfully decode data from the WAL we only
 * need to access catalog tables and (sys|rel|cat)cache, not the actual user
 * tables since the data we decode is wholly contained in the WAL
 * records. Also, our snapshots need to be different in comparison to normal
 * MVCC ones because in contrast to those we cannot fully rely on the clog and
 * pg_subtrans for information about committed transactions because they might
 * commit in the future from the POV of the WAL entry we're currently
 * decoding. This definition has the advantage that we only need to prevent
 * removal of catalog rows, while normal table's rows can still be
 * removed. This is achieved by using the replication slot mechanism.
 *
 * As the percentage of transactions modifying the catalog normally is fairly
 * small in comparisons to ones only manipulating user data, we keep track of
 * the committed catalog modifying ones inside [xmin, xmax) instead of keeping
 * track of all running transactions like it's done in a normal snapshot. Note
 * that we're generally only looking at transactions that have acquired an
 * xid. That is we keep a list of transactions between snapshot->(xmin, xmax)
 * that we consider committed, everything else is considered aborted/in
 * progress. That also allows us not to care about subtransactions before they
 * have committed which means this module, in contrast to HS, doesn't have to
 * care about suboverflowed subtransactions and similar.
 *
 * One complexity of doing this is that to e.g. handle mixed DDL/DML
 * transactions we need Snapshots that see intermediate versions of the
 * catalog in a transaction. During normal operation this is achieved by using
 * CommandIds/cmin/cmax. The problem with that however is that for space
 * efficiency reasons only one value of that is stored
 * (cf. combocid.c). Since combo CIDs are only available in memory we log
 * additional information which allows us to get the original (cmin, cmax)
 * pair during visibility checks. Check the reorderbuffer.c's comment above
 * ResolveCminCmaxDuringDecoding() for details.
 *
 * To facilitate all this we need our own visibility routine, as the normal
 * ones are optimized for different usecases.
 *
 * To replace the normal catalog snapshots with decoding ones use the
 * SetupHistoricSnapshot() and TeardownHistoricSnapshot() functions.
 *
 *
 *
 * The snapbuild machinery is starting up in several stages, as illustrated
 * by the following graph describing the SnapBuild->state transitions:
 *
 *		   +-------------------------+
 *	  +----|		 START			 |-------------+
 *	  |    +-------------------------+			   |
 *	  |					|						   |
 *	  |					|						   |
 *	  |		   running_xacts #1					   |
 *	  |					|						   |
 *	  |					|						   |
 *	  |					v						   |
 *	  |    +-------------------------+			   v
 *	  |    |   BUILDING_SNAPSHOT	 |------------>|
 *	  |    +-------------------------+			   |
 *	  |					|						   |
 *	  |					|						   |
 *	  | running_xacts #2, xacts from #1 finished   |
 *	  |					|						   |
 *	  |					|						   |
 *	  |					v						   |
 *	  |    +-------------------------+			   v
 *	  |    |	   FULL_SNAPSHOT	 |------------>|
 *	  |    +-------------------------+			   |
 *	  |					|						   |
 * running_xacts		|					   saved snapshot
 * with zero xacts		|				  at running_xacts's lsn
 *	  |					|						   |
 *	  | running_xacts with xacts from #2 finished  |
 *	  |					|						   |
 *	  |					v						   |
 *	  |    +-------------------------+			   |
 *	  +--->|SNAPBUILD_CONSISTENT	 |<------------+
 *		   +-------------------------+
 *
 * Initially the machinery is in the START stage. When an xl_running_xacts
 * record is read that is sufficiently new (above the safe xmin horizon),
 * there's a state transition. If there were no running xacts when the
 * running_xacts record was generated, we'll directly go into CONSISTENT
 * state, otherwise we'll switch to the BUILDING_SNAPSHOT state. Having a full
 * snapshot means that all transactions that start henceforth can be decoded
 * in their entirety, but transactions that started previously can't. In
 * FULL_SNAPSHOT we'll switch into CONSISTENT once all those previously
 * running transactions have committed or aborted.
 *
 * Only transactions that commit after CONSISTENT state has been reached will
 * be replayed, even though they might have started while still in
 * FULL_SNAPSHOT. That ensures that we'll reach a point where no previous
 * changes has been exported, but all the following ones will be. That point
 * is a convenient point to initialize replication from, which is why we
 * export a snapshot at that point, which *can* be used to read normal data.
 *
 * Portions Copyright (c) 2022, EnterpriseDB Corporation. All Rights Reserved.
 *
 *
 *
 *
 *
 * Copyright (c) 2012-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/snapbuild.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/leopardam_xlog.h"
#include "access/transam.h"
#include "access/xact.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"
#include "storage/block.h"		/* debugging output */
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/standby.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/snapshot.h"
#include "access/leopardsnapbuild.h"

/*
 * This struct contains the current state of the snapshot building
 * machinery. Besides a forward declaration in the header, it is not exposed
 * to the public, so we can easily change its contents.
 */
struct SnapBuild
{
	/* how far are we along building our first full snapshot */
	SnapBuildState state;

	/* private memory context used to allocate memory for this module. */
	MemoryContext context;

	/* all transactions < than this have committed/aborted */
	TransactionId xmin;

	/* all transactions >= than this are uncommitted */
	TransactionId xmax;

	/*
	 * Don't replay commits from an LSN < this LSN. This can be set externally
	 * but it will also be advanced (never retreat) from within snapbuild.c.
	 */
	XLogRecPtr	start_decoding_at;

	/*
	 * LSN at which we found a consistent point at the time of slot creation.
	 * This is also the point where we have exported a snapshot for the
	 * initial copy.
	 *
	 * The prepared transactions that are not covered by initial snapshot
	 * needs to be sent later along with commit prepared and they must be
	 * before this point.
	 */
	XLogRecPtr	initial_consistent_point;

	/*
	 * Don't start decoding WAL until the "xl_running_xacts" information
	 * indicates there are no running xids with an xid smaller than this.
	 */
	TransactionId initial_xmin_horizon;

	/* Indicates if we are building full snapshot or just catalog one. */
	bool		building_full_snapshot;

	/*
	 * Snapshot that's valid to see the catalog state seen at this moment.
	 */
	Snapshot	snapshot;

	/*
	 * LSN of the last location we are sure a snapshot has been serialized to.
	 */
	XLogRecPtr	last_serialized_snapshot;

	/*
	 * The reorderbuffer we need to update with usable snapshots et al.
	 */
	ReorderBuffer *reorder;

	/*
	 * TransactionId at which the next phase of initial snapshot building will
	 * happen. InvalidTransactionId if not known (i.e. SNAPBUILD_START), or
	 * when no next phase necessary (SNAPBUILD_CONSISTENT).
	 */
	TransactionId next_phase_at;

	/*
	 * Array of transactions which could have catalog changes that committed
	 * between xmin and xmax.
	 */
	struct
	{
		/* number of committed transactions */
		size_t		xcnt;

		/* available space for committed transactions */
		size_t		xcnt_space;

		/*
		 * Until we reach a CONSISTENT state, we record commits of all
		 * transactions, not just the catalog changing ones. Record when that
		 * changes so we know we cannot export a snapshot safely anymore.
		 */
		bool		includes_all_transactions;

		/*
		 * Array of committed transactions that have modified the catalog.
		 *
		 * As this array is frequently modified we do *not* keep it in
		 * xidComparator order. Instead we sort the array when building &
		 * distributing a snapshot.
		 *
		 * TODO: It's unclear whether that reasoning has much merit. Every
		 * time we add something here after becoming consistent will also
		 * require distributing a snapshot. Storing them sorted would
		 * potentially also make it easier to purge (but more complicated wrt
		 * wraparound?). Should be improved if sorting while building the
		 * snapshot shows up in profiles.
		 */
		TransactionId *xip;
	}			committed;
};

/*
 * Starting a transaction -- which we need to do while exporting a snapshot --
 * removes knowledge about the previously used resowner, so we save it here.
 */
static ResourceOwner SavedResourceOwnerDuringExport = NULL;
static bool ExportInProgress = false;

/*
 * Array of transactions and subtransactions that were running when
 * the xl_running_xacts record that we decoded was written. The array is
 * sorted in xidComparator order. We remove xids from this array when
 * they become old enough to matter, and then it eventually becomes empty.
 * This array is allocated in builder->context so its lifetime is the same
 * as the snapshot builder.
 *
 * We normally rely on some WAL record types such as HEAP2_NEW_CID to know
 * if the transaction has changed the catalog. But it could happen that the
 * logical decoding decodes only the commit record of the transaction after
 * restoring the previously serialized snapshot in which case we will miss
 * adding the xid to the snapshot and end up looking at the catalogs with the
 * wrong snapshot.
 *
 * Now to avoid the above problem, if the COMMIT record of the xid listed in
 * InitialRunningXacts has XACT_XINFO_HAS_INVALS flag, we mark both the top
 * transaction and its substransactions as containing catalog changes.
 *
 * We could end up adding the transaction that didn't change catalog
 * to the snapshot since we cannot distinguish whether the transaction
 * has catalog changes only by checking the COMMIT record. It doesn't
 * have the information on which (sub) transaction has catalog changes,
 * and XACT_XINFO_HAS_INVALS doesn't necessarily indicate that the
 * transaction has catalog change. But that won't be a problem since we
 * use snapshot built during decoding only for reading system catalogs.
 */
static TransactionId *InitialRunningXacts = NULL;
static int	NInitialRunningXacts = 0;

/* ->committed and InitailRunningXacts manipulation */
static void SnapBuildPurgeOlderTxn(SnapBuild *builder);

/* snapshot building/manipulation/distribution functions */
static Snapshot SnapBuildBuildSnapshot(SnapBuild *builder);



static void SnapBuildSnapIncRefcount(Snapshot snap);



/* xlog reading helper functions for SnapBuildProcessRunningXacts */

static void SnapBuildWaitSnapshot(xl_running_xacts *running, TransactionId cutoff);

/* serialization functions */

static bool SnapBuildRestore(SnapBuild *builder, XLogRecPtr lsn);

/*
 * Allocate a new snapshot builder.
 *
 * xmin_horizon is the xid >= which we can be sure no catalog rows have been
 * removed, start_lsn is the LSN >= we want to replay commits.
 */


/*
 * Free a snapshot builder.
 */


/*
 * Free an unreferenced snapshot that has previously been built by us.
 */


/*
 * In which state of snapshot building are we?
 */


/*
 * Return the LSN at which the snapshot was exported
 */
XLogRecPtr
SnapBuildInitialConsistentPoint(SnapBuild *builder)
{
	return builder->initial_consistent_point;
}

/*
 * Should the contents of transaction ending at 'ptr' be decoded?
 */


/*
 * Increase refcount of a snapshot.
 *
 * This is used when handing out a snapshot to some external resource or when
 * adding a Snapshot as builder->snapshot.
 */
static void
SnapBuildSnapIncRefcount(Snapshot snap)
{
	snap->active_count++;
}

/*
 * Decrease refcount of a snapshot and free if the refcount reaches zero.
 *
 * Externally visible, so that external resources that have been handed an
 * IncRef'ed Snapshot can adjust its refcount easily.
 */


/*
 * Build a new snapshot, based on currently committed catalog-modifying
 * transactions.
 *
 * In-progress transactions with catalog access are *not* allowed to modify
 * these snapshots; they have to copy them and fill in appropriate ->curcid
 * and ->subxip/subxcnt values.
 */
static Snapshot
SnapBuildBuildSnapshot(SnapBuild *builder)
{
	Snapshot	snapshot;
	Size		ssize;

	Assert(builder->state >= SNAPBUILD_FULL_SNAPSHOT);

	ssize = sizeof(SnapshotData)
		+ sizeof(TransactionId) * builder->committed.xcnt
		+ sizeof(TransactionId) * 1 /* toplevel xid */ ;

	snapshot = MemoryContextAllocZero(builder->context, ssize);

	snapshot->snapshot_type = SNAPSHOT_HISTORIC_MVCC;

	/*
	 * We misuse the original meaning of SnapshotData's xip and subxip fields
	 * to make the more fitting for our needs.
	 *
	 * In the 'xip' array we store transactions that have to be treated as
	 * committed. Since we will only ever look at tuples from transactions
	 * that have modified the catalog it's more efficient to store those few
	 * that exist between xmin and xmax (frequently there are none).
	 *
	 * Snapshots that are used in transactions that have modified the catalog
	 * also use the 'subxip' array to store their toplevel xid and all the
	 * subtransaction xids so we can recognize when we need to treat rows as
	 * visible that are not in xip but still need to be visible. Subxip only
	 * gets filled when the transaction is copied into the context of a
	 * catalog modifying transaction since we otherwise share a snapshot
	 * between transactions. As long as a txn hasn't modified the catalog it
	 * doesn't need to treat any uncommitted rows as visible, so there is no
	 * need for those xids.
	 *
	 * Both arrays are qsort'ed so that we can use bsearch() on them.
	 */
	Assert(TransactionIdIsNormal(builder->xmin));
	Assert(TransactionIdIsNormal(builder->xmax));

	snapshot->xmin = builder->xmin;
	snapshot->xmax = builder->xmax;

	/* store all transactions to be treated as committed by this snapshot */
	snapshot->xip =
		(TransactionId *) ((char *) snapshot + sizeof(SnapshotData));
	snapshot->xcnt = builder->committed.xcnt;
	memcpy(snapshot->xip,
		   builder->committed.xip,
		   builder->committed.xcnt * sizeof(TransactionId));

	/* sort so we can bsearch() */
	qsort(snapshot->xip, snapshot->xcnt, sizeof(TransactionId), xidComparator);

	/*
	 * Initially, subxip is empty, i.e. it's a snapshot to be used by
	 * transactions that don't modify the catalog. Will be filled by
	 * ReorderBufferCopySnap() if necessary.
	 */
	snapshot->subxcnt = 0;
	snapshot->subxip = NULL;

	snapshot->suboverflowed = false;
	snapshot->takenDuringRecovery = false;
	snapshot->copied = false;
	snapshot->curcid = FirstCommandId;
	snapshot->active_count = 0;
	snapshot->regd_count = 0;
	snapshot->snapXactCompletionCount = 0;

	return snapshot;
}

/*
 * Build the initial slot snapshot and convert it to a normal snapshot that
 * is understood by LeopardTupleSatisfiesMVCC.
 *
 * The snapshot will be usable directly in current transaction or exported
 * for loading in different transaction.
 */


/*
 * Export a snapshot so it can be set in another session with SET TRANSACTION
 * SNAPSHOT.
 *
 * For that we need to start a transaction in the current backend as the
 * importing side checks whether the source transaction is still open to make
 * sure the xmin horizon hasn't advanced since then.
 */


/*
 * Ensure there is a snapshot and if not build one for current transaction.
 */


/*
 * Reset a previously SnapBuildExportSnapshot()'ed snapshot if there is
 * any. Aborts the previously started transaction and resets the resource
 * owner back to its original value.
 */


/*
 * Clear snapshot export state during transaction abort.
 */


/*
 * Handle the effects of a single leopard change, appropriate to the current state
 * of the snapshot builder and returns whether changes made at (xid, lsn) can
 * be decoded.
 */


/*
 * Do CommandId/combo CID handling after reading an xl_leopard_new_cid record.
 * This implies that a transaction has done some form of write to system
 * catalogs.
 */
void
LeopardSnapBuildProcessNewCid(SnapBuild *builder, TransactionId xid,
					   XLogRecPtr lsn, xl_leopard_new_cid *xlrec)
{
	CommandId	cid;

	/*
	 * we only log new_cid's if a catalog tuple was modified, so mark the
	 * transaction as containing catalog modifications
	 */
	ReorderBufferXidSetCatalogChanges(builder->reorder, xid, lsn);

	ReorderBufferAddNewTupleCids(builder->reorder, xlrec->top_xid, lsn,
								 xlrec->target_node, xlrec->target_tid,
								 xlrec->cmin, xlrec->cmax,
								 xlrec->combocid);

	/* figure out new command id */
	if (xlrec->cmin != InvalidCommandId &&
		xlrec->cmax != InvalidCommandId)
		cid = Max(xlrec->cmin, xlrec->cmax);
	else if (xlrec->cmax != InvalidCommandId)
		cid = xlrec->cmax;
	else if (xlrec->cmin != InvalidCommandId)
		cid = xlrec->cmin;
	else
	{
		cid = InvalidCommandId; /* silence compiler */
		elog(ERROR, "xl_leopard_new_cid record without a valid CommandId");
	}

	ReorderBufferAddNewCommandId(builder->reorder, xid, lsn, cid + 1);
}

/*
 * Add a new Snapshot to all transactions we're decoding that currently are
 * in-progress so they can see new catalog contents made by the transaction
 * that just committed. This is necessary because those in-progress
 * transactions will use the new catalog's contents from here on (at the very
 * least everything they do needs to be compatible with newer catalog
 * contents).
 */


/*
 * Keep track of a new catalog changing transaction that has committed.
 */


/*
 * Remove knowledge about transactions we treat as committed and the initial
 * running transactions that are smaller than ->xmin. Those won't ever get
 * checked via the ->committed or InitialRunningXacts array, respectively.
 * The committed xids will get checked via the clog machinery.
 *
 * We can ideally remove the transaction from InitialRunningXacts array
 * once it is finished (committed/aborted) but that could be costly as we need
 * to maintain the xids order in the array.
 */
static void
SnapBuildPurgeOlderTxn(SnapBuild *builder)
{
	int			off;
	TransactionId *workspace;
	int			surviving_xids = 0;

	/* not ready yet */
	if (!TransactionIdIsNormal(builder->xmin))
		return;

	/* TODO: Neater algorithm than just copying and iterating? */
	workspace =
		MemoryContextAlloc(builder->context,
						   builder->committed.xcnt * sizeof(TransactionId));

	/* copy xids that still are interesting to workspace */
	for (off = 0; off < builder->committed.xcnt; off++)
	{
		if (NormalTransactionIdPrecedes(builder->committed.xip[off],
										builder->xmin))
			;					/* remove */
		else
			workspace[surviving_xids++] = builder->committed.xip[off];
	}

	/* copy workspace back to persistent state */
	memcpy(builder->committed.xip, workspace,
		   surviving_xids * sizeof(TransactionId));

	elog(DEBUG3, "purged committed transactions from %u to %u, xmin: %u, xmax: %u",
		 (uint32) builder->committed.xcnt, (uint32) surviving_xids,
		 builder->xmin, builder->xmax);
	builder->committed.xcnt = surviving_xids;

	pfree(workspace);

	/* Quick exit if there is no initial running transactions */
	if (NInitialRunningXacts == 0)
		return;

	/* bound check if there is at least one transaction to remove */
	if (!NormalTransactionIdPrecedes(InitialRunningXacts[0],
									 builder->xmin))
		return;

	/*
	 * purge xids in InitialRunningXacts as well. The purged array must also
	 * be sorted in xidComparator order.
	 */
	workspace =
		MemoryContextAlloc(builder->context,
						   NInitialRunningXacts * sizeof(TransactionId));
	surviving_xids = 0;
	for (off = 0; off < NInitialRunningXacts; off++)
	{
		if (NormalTransactionIdPrecedes(InitialRunningXacts[off],
										builder->xmin))
			;					/* remove */
		else
			workspace[surviving_xids++] = InitialRunningXacts[off];
	}

	if (surviving_xids > 0)
		memcpy(InitialRunningXacts, workspace,
			   sizeof(TransactionId) * surviving_xids);
	else
	{
		pfree(InitialRunningXacts);
		InitialRunningXacts = NULL;
	}

	elog(DEBUG3, "purged initial running transactions from %u to %u, oldest running xid %u",
		 (uint32) NInitialRunningXacts,
		 (uint32) surviving_xids,
		 builder->xmin);

	NInitialRunningXacts = surviving_xids;
	pfree(workspace);
}

/*
 * Handle everything that needs to be done when a transaction commits
 */



/* -----------------------------------
 * Snapshot building functions dealing with xlog records
 * -----------------------------------
 */

/*
 * Process a running xacts record, and use its information to first build a
 * historic snapshot and later to release resources that aren't needed
 * anymore.
 */



/*
 * Build the start of a snapshot that's capable of decoding the catalog.
 *
 * Helper function for SnapBuildProcessRunningXacts() while we're not yet
 * consistent.
 *
 * Returns true if there is a point in performing internal maintenance/cleanup
 * using the xl_running_xacts record.
 */
static bool
SnapBuildFindSnapshot(SnapBuild *builder, XLogRecPtr lsn, xl_running_xacts *running)
{
	/* ---
	 * Build catalog decoding snapshot incrementally using information about
	 * the currently running transactions. There are several ways to do that:
	 *
	 * a) There were no running transactions when the xl_running_xacts record
	 *	  was inserted, jump to CONSISTENT immediately. We might find such a
	 *	  state while waiting on c)'s sub-states.
	 *
	 * b) This (in a previous run) or another decoding slot serialized a
	 *	  snapshot to disk that we can use.  Can't use this method for the
	 *	  initial snapshot when slot is being created and needs full snapshot
	 *	  for export or direct use, as that snapshot will only contain catalog
	 *	  modifying transactions.
	 *
	 * c) First incrementally build a snapshot for catalog tuples
	 *	  (BUILDING_SNAPSHOT), that requires all, already in-progress,
	 *	  transactions to finish.  Every transaction starting after that
	 *	  (FULL_SNAPSHOT state), has enough information to be decoded.  But
	 *	  for older running transactions no viable snapshot exists yet, so
	 *	  CONSISTENT will only be reached once all of those have finished.
	 * ---
	 */

	/*
	 * xl_running_xact record is older than what we can use, we might not have
	 * all necessary catalog rows anymore.
	 */
	if (TransactionIdIsNormal(builder->initial_xmin_horizon) &&
		NormalTransactionIdPrecedes(running->oldestRunningXid,
									builder->initial_xmin_horizon))
	{
		ereport(DEBUG1,
				(errmsg_internal("skipping snapshot at %X/%X while building logical decoding snapshot, xmin horizon too low",
								 LSN_FORMAT_ARGS(lsn)),
				 errdetail_internal("initial xmin horizon of %u vs the snapshot's %u",
									builder->initial_xmin_horizon, running->oldestRunningXid)));


		SnapBuildWaitSnapshot(running, builder->initial_xmin_horizon);

		return true;
	}

	/*
	 * a) No transaction were running, we can jump to consistent.
	 *
	 * This is not affected by races around xl_running_xacts, because we can
	 * miss transaction commits, but currently not transactions starting.
	 *
	 * NB: We might have already started to incrementally assemble a snapshot,
	 * so we need to be careful to deal with that.
	 */
	if (running->oldestRunningXid == running->nextXid)
	{
		if (builder->start_decoding_at == InvalidXLogRecPtr ||
			builder->start_decoding_at <= lsn)
			/* can decode everything after this */
			builder->start_decoding_at = lsn + 1;

		/* As no transactions were running xmin/xmax can be trivially set. */
		builder->xmin = running->nextXid;	/* < are finished */
		builder->xmax = running->nextXid;	/* >= are running */

		/* so we can safely use the faster comparisons */
		Assert(TransactionIdIsNormal(builder->xmin));
		Assert(TransactionIdIsNormal(builder->xmax));

		builder->state = SNAPBUILD_CONSISTENT;
		builder->next_phase_at = InvalidTransactionId;

		ereport(LOG,
				(errmsg("logical decoding found consistent point at %X/%X",
						LSN_FORMAT_ARGS(lsn)),
				 errdetail("There are no running transactions.")));

		return false;
	}
	/* b) valid on disk state and not building full snapshot */
	else if (!builder->building_full_snapshot &&
			 SnapBuildRestore(builder, lsn))
	{
		int			nxacts = running->subxcnt + running->xcnt;
		Size		sz = sizeof(TransactionId) * nxacts;

		/*
		 * Remember the transactions and subtransactions that were running
		 * when xl_running_xacts record that we decoded was written. We use
		 * this later to identify the transactions have performed catalog
		 * changes. See SnapBuildXidSetCatalogChanges.
		 */
		NInitialRunningXacts = nxacts;
		InitialRunningXacts = MemoryContextAlloc(builder->context, sz);
		memcpy(InitialRunningXacts, running->xids, sz);
		qsort(InitialRunningXacts, nxacts, sizeof(TransactionId), xidComparator);

		/* there won't be any state to cleanup */
		return false;
	}

	/*
	 * c) transition from START to BUILDING_SNAPSHOT.
	 *
	 * In START state, and a xl_running_xacts record with running xacts is
	 * encountered.  In that case, switch to BUILDING_SNAPSHOT state, and
	 * record xl_running_xacts->nextXid.  Once all running xacts have finished
	 * (i.e. they're all >= nextXid), we have a complete catalog snapshot.  It
	 * might look that we could use xl_running_xact's ->xids information to
	 * get there quicker, but that is problematic because transactions marked
	 * as running, might already have inserted their commit record - it's
	 * infeasible to change that with locking.
	 */
	else if (builder->state == SNAPBUILD_START)
	{
		builder->state = SNAPBUILD_BUILDING_SNAPSHOT;
		builder->next_phase_at = running->nextXid;

		/*
		 * Start with an xmin/xmax that's correct for future, when all the
		 * currently running transactions have finished. We'll update both
		 * while waiting for the pending transactions to finish.
		 */
		builder->xmin = running->nextXid;	/* < are finished */
		builder->xmax = running->nextXid;	/* >= are running */

		/* so we can safely use the faster comparisons */
		Assert(TransactionIdIsNormal(builder->xmin));
		Assert(TransactionIdIsNormal(builder->xmax));

		ereport(LOG,
				(errmsg("logical decoding found initial starting point at %X/%X",
						LSN_FORMAT_ARGS(lsn)),
				 errdetail("Waiting for transactions (approximately %d) older than %u to end.",
						   running->xcnt, running->nextXid)));

		SnapBuildWaitSnapshot(running, running->nextXid);
	}

	/*
	 * c) transition from BUILDING_SNAPSHOT to FULL_SNAPSHOT.
	 *
	 * In BUILDING_SNAPSHOT state, and this xl_running_xacts' oldestRunningXid
	 * is >= than nextXid from when we switched to BUILDING_SNAPSHOT.  This
	 * means all transactions starting afterwards have enough information to
	 * be decoded.  Switch to FULL_SNAPSHOT.
	 */
	else if (builder->state == SNAPBUILD_BUILDING_SNAPSHOT &&
			 TransactionIdPrecedesOrEquals(builder->next_phase_at,
										   running->oldestRunningXid))
	{
		builder->state = SNAPBUILD_FULL_SNAPSHOT;
		builder->next_phase_at = running->nextXid;

		ereport(LOG,
				(errmsg("logical decoding found initial consistent point at %X/%X",
						LSN_FORMAT_ARGS(lsn)),
				 errdetail("Waiting for transactions (approximately %d) older than %u to end.",
						   running->xcnt, running->nextXid)));

		SnapBuildWaitSnapshot(running, running->nextXid);
	}

	/*
	 * c) transition from FULL_SNAPSHOT to CONSISTENT.
	 *
	 * In FULL_SNAPSHOT state (see d) ), and this xl_running_xacts'
	 * oldestRunningXid is >= than nextXid from when we switched to
	 * FULL_SNAPSHOT.  This means all transactions that are currently in
	 * progress have a catalog snapshot, and all their changes have been
	 * collected.  Switch to CONSISTENT.
	 */
	else if (builder->state == SNAPBUILD_FULL_SNAPSHOT &&
			 TransactionIdPrecedesOrEquals(builder->next_phase_at,
										   running->oldestRunningXid))
	{
		builder->state = SNAPBUILD_CONSISTENT;
		builder->next_phase_at = InvalidTransactionId;

		ereport(LOG,
				(errmsg("logical decoding found consistent point at %X/%X",
						LSN_FORMAT_ARGS(lsn)),
				 errdetail("There are no old transactions anymore.")));
	}

	/*
	 * We already started to track running xacts and need to wait for all
	 * in-progress ones to finish. We fall through to the normal processing of
	 * records so incremental cleanup can be performed.
	 */
	return true;

}

/* ---
 * Iterate through xids in record, wait for all older than the cutoff to
 * finish.  Then, if possible, log a new xl_running_xacts record.
 *
 * This isn't required for the correctness of decoding, but to:
 * a) allow isolationtester to notice that we're currently waiting for
 *	  something.
 * b) log a new xl_running_xacts record where it'd be helpful, without having
 *	  to wait for bgwriter or checkpointer.
 * ---
 */
static void
SnapBuildWaitSnapshot(xl_running_xacts *running, TransactionId cutoff)
{
	int			off;

	for (off = 0; off < running->xcnt; off++)
	{
		TransactionId xid = running->xids[off];

		/*
		 * Upper layers should prevent that we ever need to wait on ourselves.
		 * Check anyway, since failing to do so would either result in an
		 * endless wait or an Assert() failure.
		 */
		if (TransactionIdIsCurrentTransactionId(xid))
			elog(ERROR, "waiting for ourselves");

		if (TransactionIdFollows(xid, cutoff))
			continue;

		XactLockTableWait(xid, NULL, NULL, XLTW_None);
	}

	/*
	 * All transactions we needed to finish finished - try to ensure there is
	 * another xl_running_xacts record in a timely manner, without having to
	 * wait for bgwriter or checkpointer to log one.  During recovery we can't
	 * enforce that, so we'll have to wait.
	 */
	if (!RecoveryInProgress())
	{
		LogStandbySnapshot();
	}
}

/* -----------------------------------
 * Snapshot serialization support
 * -----------------------------------
 */

/*
 * We store current state of struct SnapBuild on disk in the following manner:
 *
 * struct SnapBuildOnDisk;
 * TransactionId * running.xcnt_space;
 * TransactionId * committed.xcnt; (*not xcnt_space*)
 *
 */
typedef struct SnapBuildOnDisk
{
	/* first part of this struct needs to be version independent */

	/* data not covered by checksum */
	uint32		magic;
	pg_crc32c	checksum;

	/* data covered by checksum */

	/* version, in case we want to support pg_upgrade */
	uint32		version;
	/* how large is the on disk data, excluding the constant sized part */
	uint32		length;

	/* version dependent part */
	SnapBuild	builder;

	/* variable amount of TransactionIds follows */
} SnapBuildOnDisk;

#define SnapBuildOnDiskConstantSize \
	offsetof(SnapBuildOnDisk, builder)
#define SnapBuildOnDiskNotChecksummedSize \
	offsetof(SnapBuildOnDisk, version)

#define SNAPBUILD_MAGIC 0x51A1E001
#define SNAPBUILD_VERSION 4

/*
 * Store/Load a snapshot from disk, depending on the snapshot builder's state.
 *
 * Supposed to be used by external (i.e. not snapbuild.c) code that just read
 * a record that's a potential location for a serialized snapshot.
 */


/*
 * Serialize the snapshot 'builder' at the location 'lsn' if it hasn't already
 * been done by another decoding process.
 */


/*
 * Restore a snapshot into 'builder' if previously one has been stored at the
 * location indicated by 'lsn'. Returns true if successful, false otherwise.
 */
static bool
SnapBuildRestore(SnapBuild *builder, XLogRecPtr lsn)
{
	SnapBuildOnDisk ondisk;
	int			fd;
	char		path[MAXPGPATH];
	Size		sz;
	int			readBytes;
	pg_crc32c	checksum;

	/* no point in loading a snapshot if we're already there */
	if (builder->state == SNAPBUILD_CONSISTENT)
		return false;

	sprintf(path, "pg_logical/snapshots/%X-%X.snap",
			LSN_FORMAT_ARGS(lsn));

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);

	if (fd < 0 && errno == ENOENT)
		return false;
	else if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	/* ----
	 * Make sure the snapshot had been stored safely to disk, that's normally
	 * cheap.
	 * Note that we do not need PANIC here, nobody will be able to use the
	 * slot without fsyncing, and saving it won't succeed without an fsync()
	 * either...
	 * ----
	 */
	fsync_fname(path, false);
	fsync_fname("pg_logical/snapshots", true);


	/* read statically sized portion of snapshot */
	pgstat_report_wait_start(WAIT_EVENT_SNAPBUILD_READ);
	readBytes = read(fd, &ondisk, SnapBuildOnDiskConstantSize);
	pgstat_report_wait_end();
	if (readBytes != SnapBuildOnDiskConstantSize)
	{
		int			save_errno = errno;

		CloseTransientFile(fd);

		if (readBytes < 0)
		{
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", path)));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read file \"%s\": read %d of %zu",
							path, readBytes,
							(Size) SnapBuildOnDiskConstantSize)));
	}

	if (ondisk.magic != SNAPBUILD_MAGIC)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("snapbuild state file \"%s\" has wrong magic number: %u instead of %u",
						path, ondisk.magic, SNAPBUILD_MAGIC)));

	if (ondisk.version != SNAPBUILD_VERSION)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("snapbuild state file \"%s\" has unsupported version: %u instead of %u",
						path, ondisk.version, SNAPBUILD_VERSION)));

	INIT_CRC32C(checksum);
	COMP_CRC32C(checksum,
				((char *) &ondisk) + SnapBuildOnDiskNotChecksummedSize,
				SnapBuildOnDiskConstantSize - SnapBuildOnDiskNotChecksummedSize);

	/* read SnapBuild */
	pgstat_report_wait_start(WAIT_EVENT_SNAPBUILD_READ);
	readBytes = read(fd, &ondisk.builder, sizeof(SnapBuild));
	pgstat_report_wait_end();
	if (readBytes != sizeof(SnapBuild))
	{
		int			save_errno = errno;

		CloseTransientFile(fd);

		if (readBytes < 0)
		{
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", path)));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read file \"%s\": read %d of %zu",
							path, readBytes, sizeof(SnapBuild))));
	}
	COMP_CRC32C(checksum, &ondisk.builder, sizeof(SnapBuild));

	/* restore committed xacts information */
	sz = sizeof(TransactionId) * ondisk.builder.committed.xcnt;
	ondisk.builder.committed.xip = MemoryContextAllocZero(builder->context, sz);
	pgstat_report_wait_start(WAIT_EVENT_SNAPBUILD_READ);
	readBytes = read(fd, ondisk.builder.committed.xip, sz);
	pgstat_report_wait_end();
	if (readBytes != sz)
	{
		int			save_errno = errno;

		CloseTransientFile(fd);

		if (readBytes < 0)
		{
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", path)));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read file \"%s\": read %d of %zu",
							path, readBytes, sz)));
	}
	COMP_CRC32C(checksum, ondisk.builder.committed.xip, sz);

	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", path)));

	FIN_CRC32C(checksum);

	/* verify checksum of what we've read */
	if (!EQ_CRC32C(checksum, ondisk.checksum))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("checksum mismatch for snapbuild state file \"%s\": is %u, should be %u",
						path, checksum, ondisk.checksum)));

	/*
	 * ok, we now have a sensible snapshot here, figure out if it has more
	 * information than we have.
	 */

	/*
	 * We are only interested in consistent snapshots for now, comparing
	 * whether one incomplete snapshot is more "advanced" seems to be
	 * unnecessarily complex.
	 */
	if (ondisk.builder.state < SNAPBUILD_CONSISTENT)
		goto snapshot_not_interesting;

	/*
	 * Don't use a snapshot that requires an xmin that we cannot guarantee to
	 * be available.
	 */
	if (TransactionIdPrecedes(ondisk.builder.xmin, builder->initial_xmin_horizon))
		goto snapshot_not_interesting;

	/* consistent snapshots have no next phase */
	Assert(ondisk.builder.next_phase_at == InvalidTransactionId);

	/* ok, we think the snapshot is sensible, copy over everything important */
	builder->xmin = ondisk.builder.xmin;
	builder->xmax = ondisk.builder.xmax;
	builder->state = ondisk.builder.state;

	builder->committed.xcnt = ondisk.builder.committed.xcnt;
	/* We only allocated/stored xcnt, not xcnt_space xids ! */
	/* don't overwrite preallocated xip, if we don't have anything here */
	if (builder->committed.xcnt > 0)
	{
		pfree(builder->committed.xip);
		builder->committed.xcnt_space = ondisk.builder.committed.xcnt;
		builder->committed.xip = ondisk.builder.committed.xip;
	}
	ondisk.builder.committed.xip = NULL;

	/* our snapshot is not interesting anymore, build a new one */
	if (builder->snapshot != NULL)
	{
		SnapBuildSnapDecRefcount(builder->snapshot);
	}
	builder->snapshot = SnapBuildBuildSnapshot(builder);
	SnapBuildSnapIncRefcount(builder->snapshot);

	ReorderBufferSetRestartPoint(builder->reorder, lsn);

	Assert(builder->state == SNAPBUILD_CONSISTENT);

	ereport(LOG,
			(errmsg("logical decoding found consistent point at %X/%X",
					LSN_FORMAT_ARGS(lsn)),
			 errdetail("Logical decoding will begin using saved snapshot.")));
	return true;

snapshot_not_interesting:
	if (ondisk.builder.committed.xip != NULL)
		pfree(ondisk.builder.committed.xip);
	return false;
}

/*
 * Remove all serialized snapshots that are not required anymore because no
 * slot can need them. This doesn't actually have to run during a checkpoint,
 * but it's a convenient point to schedule this.
 *
 * NB: We run this during checkpoints even if logical decoding is disabled so
 * we cleanup old slots at some point after it got disabled.
 */


/*
 * If the given xid is in the list of the initial running xacts, we mark the
 * transaction and its subtransactions as containing catalog changes. See
 * comments for NInitialRunningXacts and InitialRunningXacts for additional
 * info.
 */
void
SnapBuildXidSetCatalogChanges(SnapBuild *builder, TransactionId xid, int subxcnt,
							  TransactionId *subxacts, XLogRecPtr lsn)
{
	/*
	 * Skip if there is no initial running xacts information or the
	 * transaction is already marked as containing catalog changes.
	 */
	if (NInitialRunningXacts == 0 ||
		ReorderBufferXidHasCatalogChanges(builder->reorder, xid))
		return;

	if (bsearch(&xid, InitialRunningXacts, NInitialRunningXacts,
				sizeof(TransactionId), xidComparator) != NULL)
	{
		ReorderBufferXidSetCatalogChanges(builder->reorder, xid, lsn);

		for (int i = 0; i < subxcnt; i++)
		{
			ReorderBufferAssignChild(builder->reorder, xid, subxacts[i], lsn);
			ReorderBufferXidSetCatalogChanges(builder->reorder, subxacts[i], lsn);
		}
	}
}
