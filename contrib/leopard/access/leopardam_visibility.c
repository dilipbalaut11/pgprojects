/*-------------------------------------------------------------------------
 *
 * leopardam_visibility.c
 *	  Tuple visibility rules for tuples stored in leopard.
 *
 * NOTE: all the LeopardTupleSatisfies routines will update the tuple's
 * "hint" status bits if we see that the inserting or deleting transaction
 * has now committed or aborted (and it is safe to set the hint bits).
 * If the hint bits are changed, MarkBufferDirtyHint is called on
 * the passed-in buffer.  The caller must hold not only a pin, but at least
 * shared buffer content lock on the buffer containing the tuple.
 *
 * NOTE: When using a non-MVCC snapshot, we must check
 * TransactionIdIsInProgress (which looks in the PGPROC array)
 * before TransactionIdDidCommit/TransactionIdDidAbort (which look in
 * pg_xact).  Otherwise we have a race condition: we might decide that a
 * just-committed transaction crashed, because none of the tests succeed.
 * xact.c is careful to record commit/abort in pg_xact before it unsets
 * MyProc->xid in the PGPROC array.  That fixes that problem, but it
 * also means there is a window where TransactionIdIsInProgress and
 * TransactionIdDidCommit will both return true.  If we check only
 * TransactionIdDidCommit, we could consider a tuple committed when a
 * later GetSnapshotData call will still think the originating transaction
 * is in progress, which leads to application-level inconsistency.  The
 * upshot is that we gotta check TransactionIdIsInProgress first in all
 * code paths, except for a few cases where we are looking at
 * subtransactions of our own main transaction and so there can't be any
 * race condition.
 *
 * When using an MVCC snapshot, we rely on XidInMVCCSnapshot rather than
 * TransactionIdIsInProgress, but the logic is otherwise the same: do not
 * check pg_xact until after deciding that the xact is no longer in progress.
 *
 *
 * Summary of visibility functions:
 *
 *	 LeopardTupleSatisfiesMVCC()
 *		  visible to supplied snapshot, excludes current command
 *	 LeopardTupleSatisfiesUpdate()
 *		  visible to instant snapshot, with user-supplied command
 *		  counter and more complex result
 *	 LeopardTupleSatisfiesSelf()
 *		  visible to instant snapshot and current command
 *	 LeopardTupleSatisfiesDirty()
 *		  like LeopardTupleSatisfiesSelf(), but includes open transactions
 *	 LeopardTupleSatisfiesVacuum()
 *		  visible to any running transaction, used by VACUUM
 *	 LeopardTupleSatisfiesNonVacuumable()
 *		  Snapshot-style API for LeopardTupleSatisfiesVacuum
 *	 LeopardTupleSatisfiesToast()
 *		  visible unless part of interrupted vacuum, used for TOAST
 *	 LeopardTupleSatisfiesAny()
 *		  all tuples are visible
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
 * IDENTIFICATION
 *	  src/backend/access/leopard/leopardam_visibility.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/leopardam.h"
#include "access/leopardtup_details.h"
#include "access/multixact.h"
#include "access/subtrans.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/combocid.h"
#include "utils/snapmgr.h"


/*
 * SetHintBits()
 *
 * Set commit/abort hint bits on a tuple, if appropriate at this time.
 *
 * It is only safe to set a transaction-committed hint bit if we know the
 * transaction's commit record is guaranteed to be flushed to disk before the
 * buffer, or if the table is temporary or unlogged and will be obliterated by
 * a crash anyway.  We cannot change the LSN of the page here, because we may
 * hold only a share lock on the buffer, so we can only use the LSN to
 * interlock this if the buffer's LSN already is newer than the commit LSN;
 * otherwise we have to just refrain from setting the hint bit until some
 * future re-examination of the tuple.
 *
 * We can always set hint bits when marking a transaction aborted.  (Some
 * code in leopardam.c relies on that!)
 *
 * Normal commits may be asynchronous, so for those we need to get the LSN
 * of the transaction and then check whether this is flushed.
 *
 * The caller should pass xid as the XID of the transaction to check, or
 * InvalidTransactionId if no check is needed.
 */
static inline void
SetHintBits(LeopardTupleHeader tuple, Buffer buffer,
			uint16 infomask, TransactionId xid)
{
	if (TransactionIdIsValid(xid))
	{
		/* NB: xid must be known committed here! */
		XLogRecPtr	commitLSN = TransactionIdGetCommitLSN(xid);

		if (BufferIsPermanent(buffer) && XLogNeedsFlush(commitLSN) &&
			BufferGetLSNAtomic(buffer) < commitLSN)
		{
			/* not flushed and no LSN interlock, so don't set hint */
			return;
		}
	}

	tuple->t_infomask |= infomask;
	MarkBufferDirtyHint(buffer, true);
}

/*
 * LeopardTupleSetHintBits --- exported version of SetHintBits()
 *
 * This must be separate because of C99's brain-dead notions about how to
 * implement inline functions.
 */
void
LeopardTupleSetHintBits(LeopardTupleHeader tuple, Buffer buffer,
					 uint16 infomask, TransactionId xid)
{
	SetHintBits(tuple, buffer, infomask, xid);
}


/*
 * LeopardTupleSatisfiesSelf
 *		True iff leopard tuple is valid "for itself".
 *
 * See SNAPSHOT_MVCC's definition for the intended behaviour.
 *
 * Note:
 *		Assumes leopard tuple is valid.
 *
 * The satisfaction of "itself" requires the following:
 *
 * ((Xmin == my-transaction &&				the row was updated by the current transaction, and
 *		(Xmax is null						it was not deleted
 *		 [|| Xmax != my-transaction)])			[or it was deleted by another transaction]
 * ||
 *
 * (Xmin is committed &&					the row was modified by a committed transaction, and
 *		(Xmax is null ||					the row has not been deleted, or
 *			(Xmax != my-transaction &&			the row was deleted by another transaction
 *			 Xmax is not committed)))			that has not been committed
 */
static bool
LeopardTupleSatisfiesSelf(LeopardTuple leopardtup, Snapshot snapshot, Buffer buffer)
{
	LeopardTupleHeader tuple = leopardtup->t_data;

	Assert(ItemPointerIsValid(&leopardtup->t_self));
	Assert(leopardtup->t_tableOid != InvalidOid);

	if (!LeopardTupleHeaderXminCommitted(tuple))
	{
		if (LeopardTupleHeaderXminInvalid(tuple))
			return false;

		if (TransactionIdIsCurrentTransactionId(LeopardTupleHeaderGetRawXmin(tuple)))
		{
			if (tuple->t_infomask & LEOPARD_XMAX_INVALID)	/* xid invalid */
				return true;

			if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
				return true;

			if (tuple->t_infomask & LEOPARD_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = LeopardTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
					return true;
				else
					return false;
			}

			if (!TransactionIdIsCurrentTransactionId(LeopardTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, LEOPARD_XMAX_INVALID,
							InvalidTransactionId);
				return true;
			}

			return false;
		}
		else if (TransactionIdIsInProgress(LeopardTupleHeaderGetRawXmin(tuple)))
			return false;
		else if (TransactionIdDidCommit(LeopardTupleHeaderGetRawXmin(tuple)))
			SetHintBits(tuple, buffer, LEOPARD_XMIN_COMMITTED,
						LeopardTupleHeaderGetRawXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, LEOPARD_XMIN_INVALID,
						InvalidTransactionId);
			return false;
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & LEOPARD_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (tuple->t_infomask & LEOPARD_XMAX_COMMITTED)
	{
		if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;			/* updated by other */
	}

	if (tuple->t_infomask & LEOPARD_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;

		xmax = LeopardTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
			return false;
		if (TransactionIdIsInProgress(xmax))
			return true;
		if (TransactionIdDidCommit(xmax))
			return false;
		/* it must have aborted or crashed */
		return true;
	}

	if (TransactionIdIsCurrentTransactionId(LeopardTupleHeaderGetRawXmax(tuple)))
	{
		if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;
	}

	if (TransactionIdIsInProgress(LeopardTupleHeaderGetRawXmax(tuple)))
		return true;

	if (!TransactionIdDidCommit(LeopardTupleHeaderGetRawXmax(tuple)))
	{
		/* it must have aborted or crashed */
		SetHintBits(tuple, buffer, LEOPARD_XMAX_INVALID,
					InvalidTransactionId);
		return true;
	}

	/* xmax transaction committed */

	if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		SetHintBits(tuple, buffer, LEOPARD_XMAX_INVALID,
					InvalidTransactionId);
		return true;
	}

	SetHintBits(tuple, buffer, LEOPARD_XMAX_COMMITTED,
				LeopardTupleHeaderGetRawXmax(tuple));
	return false;
}

/*
 * LeopardTupleSatisfiesAny
 *		Dummy "satisfies" routine: any tuple satisfies SnapshotAny.
 */
static bool
LeopardTupleSatisfiesAny(LeopardTuple leopardtup, Snapshot snapshot, Buffer buffer)
{
	return true;
}

/*
 * LeopardTupleSatisfiesToast
 *		True iff leopard tuple is valid as a TOAST row.
 *
 * See SNAPSHOT_TOAST's definition for the intended behaviour.
 *
 * This is a simplified version that only checks for VACUUM moving conditions.
 * It's appropriate for TOAST usage because TOAST really doesn't want to do
 * its own time qual checks; if you can see the main table row that contains
 * a TOAST reference, you should be able to see the TOASTed value.  However,
 * vacuuming a TOAST table is independent of the main table, and in case such
 * a vacuum fails partway through, we'd better do this much checking.
 *
 * Among other things, this means you can't do UPDATEs of rows in a TOAST
 * table.
 */
static bool
LeopardTupleSatisfiesToast(LeopardTuple leopardtup, Snapshot snapshot,
						Buffer buffer)
{
	LeopardTupleHeader tuple = leopardtup->t_data;

	Assert(ItemPointerIsValid(&leopardtup->t_self));
	Assert(leopardtup->t_tableOid != InvalidOid);

	if (!LeopardTupleHeaderXminCommitted(tuple))
	{
		if (LeopardTupleHeaderXminInvalid(tuple))
			return false;


		/*
		 * An invalid Xmin can be left behind by a speculative insertion that
		 * is canceled by super-deleting the tuple.  This also applies to
		 * TOAST tuples created during speculative insertion.
		 */
		else if (!TransactionIdIsValid(LeopardTupleHeaderGetXmin(tuple)))
			return false;
	}

	/* otherwise assume the tuple is valid for TOAST. */
	return true;
}

/*
 * LeopardTupleSatisfiesUpdate
 *
 *	This function returns a more detailed result code than most of the
 *	functions in this file, since UPDATE needs to know more than "is it
 *	visible?".  It also allows for user-supplied CommandId rather than
 *	relying on CurrentCommandId.
 *
 *	The possible return codes are:
 *
 *	TM_Invisible: the tuple didn't exist at all when the scan started, e.g. it
 *	was created by a later CommandId.
 *
 *	TM_Ok: The tuple is valid and visible, so it may be updated.
 *
 *	TM_SelfModified: The tuple was updated by the current transaction, after
 *	the current scan started.
 *
 *	TM_Updated: The tuple was updated by a committed transaction (including
 *	the case where the tuple was moved into a different partition).
 *
 *	TM_Deleted: The tuple was deleted by a committed transaction.
 *
 *	TM_BeingModified: The tuple is being updated by an in-progress transaction
 *	other than the current transaction.  (Note: this includes the case where
 *	the tuple is share-locked by a MultiXact, even if the MultiXact includes
 *	the current transaction.  Callers that want to distinguish that case must
 *	test for it themselves.)
 */
TM_Result
LeopardTupleSatisfiesUpdate(LeopardTuple leopardtup, CommandId curcid,
						 Buffer buffer)
{
	LeopardTupleHeader tuple = leopardtup->t_data;

	Assert(ItemPointerIsValid(&leopardtup->t_self));
	Assert(leopardtup->t_tableOid != InvalidOid);

	if (!LeopardTupleHeaderXminCommitted(tuple))
	{
		if (LeopardTupleHeaderXminInvalid(tuple))
			return TM_Invisible;

		if (TransactionIdIsCurrentTransactionId(LeopardTupleHeaderGetRawXmin(tuple)))
		{
			if (LeopardTupleHeaderGetCmin(tuple) >= curcid)
				return TM_Invisible;	/* inserted after scan started */

			if (tuple->t_infomask & LEOPARD_XMAX_INVALID)	/* xid invalid */
				return TM_Ok;

			if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			{
				TransactionId xmax;

				xmax = LeopardTupleHeaderGetRawXmax(tuple);

				/*
				 * Careful here: even though this tuple was created by our own
				 * transaction, it might be locked by other transactions, if
				 * the original version was key-share locked when we updated
				 * it.
				 */

				if (tuple->t_infomask & LEOPARD_XMAX_IS_MULTI)
				{
					if (MultiXactIdIsRunning(xmax, true))
						return TM_BeingModified;
					else
						return TM_Ok;
				}

				/*
				 * If the locker is gone, then there is nothing of interest
				 * left in this Xmax; otherwise, report the tuple as
				 * locked/updated.
				 */
				if (!TransactionIdIsInProgress(xmax))
					return TM_Ok;
				return TM_BeingModified;
			}

			if (tuple->t_infomask & LEOPARD_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = LeopardTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* deleting subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
				{
					if (MultiXactIdIsRunning(LeopardTupleHeaderGetRawXmax(tuple),
											 false))
						return TM_BeingModified;
					return TM_Ok;
				}
				else
				{
					if (LeopardTupleHeaderGetCmax(tuple) >= curcid)
						return TM_SelfModified; /* updated after scan started */
					else
						return TM_Invisible;	/* updated before scan started */
				}
			}

			if (!TransactionIdIsCurrentTransactionId(LeopardTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, LEOPARD_XMAX_INVALID,
							InvalidTransactionId);
				return TM_Ok;
			}

			if (LeopardTupleHeaderGetCmax(tuple) >= curcid)
				return TM_SelfModified; /* updated after scan started */
			else
				return TM_Invisible;	/* updated before scan started */
		}
		else if (TransactionIdIsInProgress(LeopardTupleHeaderGetRawXmin(tuple)))
			return TM_Invisible;
		else if (TransactionIdDidCommit(LeopardTupleHeaderGetRawXmin(tuple)))
			SetHintBits(tuple, buffer, LEOPARD_XMIN_COMMITTED,
						LeopardTupleHeaderGetRawXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, LEOPARD_XMIN_INVALID,
						InvalidTransactionId);
			return TM_Invisible;
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & LEOPARD_XMAX_INVALID)	/* xid invalid or aborted */
		return TM_Ok;

	if (tuple->t_infomask & LEOPARD_XMAX_COMMITTED)
	{
		if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return TM_Ok;
		if (!ItemPointerEquals(&leopardtup->t_self, &tuple->t_ctid))
			return TM_Updated;	/* updated by other */
		else
			return TM_Deleted;	/* deleted by other */
	}

	if (tuple->t_infomask & LEOPARD_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		if (LEOPARD_LOCKED_UPGRADED(tuple->t_infomask))
			return TM_Ok;

		if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		{
			if (MultiXactIdIsRunning(LeopardTupleHeaderGetRawXmax(tuple), true))
				return TM_BeingModified;

			SetHintBits(tuple, buffer, LEOPARD_XMAX_INVALID, InvalidTransactionId);
			return TM_Ok;
		}

		xmax = LeopardTupleGetUpdateXid(tuple);
		if (!TransactionIdIsValid(xmax))
		{
			if (MultiXactIdIsRunning(LeopardTupleHeaderGetRawXmax(tuple), false))
				return TM_BeingModified;
		}

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			if (LeopardTupleHeaderGetCmax(tuple) >= curcid)
				return TM_SelfModified; /* updated after scan started */
			else
				return TM_Invisible;	/* updated before scan started */
		}

		if (MultiXactIdIsRunning(LeopardTupleHeaderGetRawXmax(tuple), false))
			return TM_BeingModified;

		if (TransactionIdDidCommit(xmax))
		{
			if (!ItemPointerEquals(&leopardtup->t_self, &tuple->t_ctid))
				return TM_Updated;
			else
				return TM_Deleted;
		}

		/*
		 * By here, the update in the Xmax is either aborted or crashed, but
		 * what about the other members?
		 */

		if (!MultiXactIdIsRunning(LeopardTupleHeaderGetRawXmax(tuple), false))
		{
			/*
			 * There's no member, even just a locker, alive anymore, so we can
			 * mark the Xmax as invalid.
			 */
			SetHintBits(tuple, buffer, LEOPARD_XMAX_INVALID,
						InvalidTransactionId);
			return TM_Ok;
		}
		else
		{
			/* There are lockers running */
			return TM_BeingModified;
		}
	}

	if (TransactionIdIsCurrentTransactionId(LeopardTupleHeaderGetRawXmax(tuple)))
	{
		if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return TM_BeingModified;
		if (LeopardTupleHeaderGetCmax(tuple) >= curcid)
			return TM_SelfModified; /* updated after scan started */
		else
			return TM_Invisible;	/* updated before scan started */
	}

	if (TransactionIdIsInProgress(LeopardTupleHeaderGetRawXmax(tuple)))
		return TM_BeingModified;

	if (!TransactionIdDidCommit(LeopardTupleHeaderGetRawXmax(tuple)))
	{
		/* it must have aborted or crashed */
		SetHintBits(tuple, buffer, LEOPARD_XMAX_INVALID,
					InvalidTransactionId);
		return TM_Ok;
	}

	/* xmax transaction committed */

	if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		SetHintBits(tuple, buffer, LEOPARD_XMAX_INVALID,
					InvalidTransactionId);
		return TM_Ok;
	}

	SetHintBits(tuple, buffer, LEOPARD_XMAX_COMMITTED,
				LeopardTupleHeaderGetRawXmax(tuple));
	if (!ItemPointerEquals(&leopardtup->t_self, &tuple->t_ctid))
		return TM_Updated;		/* updated by other */
	else
		return TM_Deleted;		/* deleted by other */
}

/*
 * LeopardTupleSatisfiesDirty
 *		True iff leopard tuple is valid including effects of open transactions.
 *
 * See SNAPSHOT_DIRTY's definition for the intended behaviour.
 *
 * This is essentially like LeopardTupleSatisfiesSelf as far as effects of
 * the current transaction and committed/aborted xacts are concerned.
 * However, we also include the effects of other xacts still in progress.
 *
 * A special hack is that the passed-in snapshot struct is used as an
 * output argument to return the xids of concurrent xacts that affected the
 * tuple.  snapshot->xmin is set to the tuple's xmin if that is another
 * transaction that's still in progress; or to InvalidTransactionId if the
 * tuple's xmin is committed good, committed dead, or my own xact.
 * Similarly for snapshot->xmax and the tuple's xmax.  If the tuple was
 * inserted speculatively, meaning that the inserter might still back down
 * on the insertion without aborting the whole transaction, the associated
 * token is also returned in snapshot->speculativeToken.
 */
static bool
LeopardTupleSatisfiesDirty(LeopardTuple leopardtup, Snapshot snapshot,
						Buffer buffer)
{
	LeopardTupleHeader tuple = leopardtup->t_data;

	Assert(ItemPointerIsValid(&leopardtup->t_self));
	Assert(leopardtup->t_tableOid != InvalidOid);

	snapshot->xmin = snapshot->xmax = InvalidTransactionId;
	snapshot->speculativeToken = 0;

	if (!LeopardTupleHeaderXminCommitted(tuple))
	{
		if (LeopardTupleHeaderXminInvalid(tuple))
			return false;

		if (TransactionIdIsCurrentTransactionId(LeopardTupleHeaderGetRawXmin(tuple)))
		{
			if (tuple->t_infomask & LEOPARD_XMAX_INVALID)	/* xid invalid */
				return true;

			if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
				return true;

			if (tuple->t_infomask & LEOPARD_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = LeopardTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
					return true;
				else
					return false;
			}

			if (!TransactionIdIsCurrentTransactionId(LeopardTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, LEOPARD_XMAX_INVALID,
							InvalidTransactionId);
				return true;
			}

			return false;
		}
		else if (TransactionIdIsInProgress(LeopardTupleHeaderGetRawXmin(tuple)))
		{
			/*
			 * Return the speculative token to caller.  Caller can worry about
			 * xmax, since it requires a conclusively locked row version, and
			 * a concurrent update to this tuple is a conflict of its
			 * purposes.
			 */
			if (LeopardTupleHeaderIsSpeculative(tuple))
			{
				snapshot->speculativeToken =
					LeopardTupleHeaderGetSpeculativeToken(tuple);

				Assert(snapshot->speculativeToken != 0);
			}

			snapshot->xmin = LeopardTupleHeaderGetRawXmin(tuple);
			/* XXX shouldn't we fall through to look at xmax? */
			return true;		/* in insertion by other */
		}
		else if (TransactionIdDidCommit(LeopardTupleHeaderGetRawXmin(tuple)))
			SetHintBits(tuple, buffer, LEOPARD_XMIN_COMMITTED,
						LeopardTupleHeaderGetRawXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, LEOPARD_XMIN_INVALID,
						InvalidTransactionId);
			return false;
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & LEOPARD_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (tuple->t_infomask & LEOPARD_XMAX_COMMITTED)
	{
		if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;			/* updated by other */
	}

	if (tuple->t_infomask & LEOPARD_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;

		xmax = LeopardTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
			return false;
		if (TransactionIdIsInProgress(xmax))
		{
			snapshot->xmax = xmax;
			return true;
		}
		if (TransactionIdDidCommit(xmax))
			return false;
		/* it must have aborted or crashed */
		return true;
	}

	if (TransactionIdIsCurrentTransactionId(LeopardTupleHeaderGetRawXmax(tuple)))
	{
		if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;
	}

	if (TransactionIdIsInProgress(LeopardTupleHeaderGetRawXmax(tuple)))
	{
		if (!LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			snapshot->xmax = LeopardTupleHeaderGetRawXmax(tuple);
		return true;
	}

	if (!TransactionIdDidCommit(LeopardTupleHeaderGetRawXmax(tuple)))
	{
		/* it must have aborted or crashed */
		SetHintBits(tuple, buffer, LEOPARD_XMAX_INVALID,
					InvalidTransactionId);
		return true;
	}

	/* xmax transaction committed */

	if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		SetHintBits(tuple, buffer, LEOPARD_XMAX_INVALID,
					InvalidTransactionId);
		return true;
	}

	SetHintBits(tuple, buffer, LEOPARD_XMAX_COMMITTED,
				LeopardTupleHeaderGetRawXmax(tuple));
	return false;				/* updated by other */
}

/*
 * LeopardTupleSatisfiesMVCC
 *		True iff leopard tuple is valid for the given MVCC snapshot.
 *
 * See SNAPSHOT_MVCC's definition for the intended behaviour.
 *
 * Notice that here, we will not update the tuple status hint bits if the
 * inserting/deleting transaction is still running according to our snapshot,
 * even if in reality it's committed or aborted by now.  This is intentional.
 * Checking the true transaction state would require access to high-traffic
 * shared data structures, creating contention we'd rather do without, and it
 * would not change the result of our visibility check anyway.  The hint bits
 * will be updated by the first visitor that has a snapshot new enough to see
 * the inserting/deleting transaction as done.  In the meantime, the cost of
 * leaving the hint bits unset is basically that each LeopardTupleSatisfiesMVCC
 * call will need to run TransactionIdIsCurrentTransactionId in addition to
 * XidInMVCCSnapshot (but it would have to do the latter anyway).  In the old
 * coding where we tried to set the hint bits as soon as possible, we instead
 * did TransactionIdIsInProgress in each call --- to no avail, as long as the
 * inserting/deleting transaction was still running --- which was more cycles
 * and more contention on ProcArrayLock.
 */
static bool
LeopardTupleSatisfiesMVCC(LeopardTuple leopardtup, Snapshot snapshot,
					   Buffer buffer)
{
	LeopardTupleHeader tuple = leopardtup->t_data;

	Assert(ItemPointerIsValid(&leopardtup->t_self));
	Assert(leopardtup->t_tableOid != InvalidOid);

	if (!LeopardTupleHeaderXminCommitted(tuple))
	{
		if (LeopardTupleHeaderXminInvalid(tuple))
			return false;

		if (TransactionIdIsCurrentTransactionId(LeopardTupleHeaderGetRawXmin(tuple)))
		{
			if (LeopardTupleHeaderGetCmin(tuple) >= snapshot->curcid)
				return false;	/* inserted after scan started */

			if (tuple->t_infomask & LEOPARD_XMAX_INVALID)	/* xid invalid */
				return true;

			if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
				return true;

			if (tuple->t_infomask & LEOPARD_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = LeopardTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
					return true;
				else if (LeopardTupleHeaderGetCmax(tuple) >= snapshot->curcid)
					return true;	/* updated after scan started */
				else
					return false;	/* updated before scan started */
			}

			if (!TransactionIdIsCurrentTransactionId(LeopardTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, LEOPARD_XMAX_INVALID,
							InvalidTransactionId);
				return true;
			}

			if (LeopardTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}
		else if (XidInMVCCSnapshot(LeopardTupleHeaderGetRawXmin(tuple), snapshot))
			return false;
		else if (TransactionIdDidCommit(LeopardTupleHeaderGetRawXmin(tuple)))
			SetHintBits(tuple, buffer, LEOPARD_XMIN_COMMITTED,
						LeopardTupleHeaderGetRawXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, LEOPARD_XMIN_INVALID,
						InvalidTransactionId);
			return false;
		}
	}
	else
	{
		/* xmin is committed, but maybe not according to our snapshot */
		if (!LeopardTupleHeaderXminFrozen(tuple) &&
			XidInMVCCSnapshot(LeopardTupleHeaderGetRawXmin(tuple), snapshot))
			return false;		/* treat as still in progress */
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & LEOPARD_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return true;

	if (tuple->t_infomask & LEOPARD_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		/* already checked above */
		Assert(!LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

		xmax = LeopardTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			if (LeopardTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}
		if (XidInMVCCSnapshot(xmax, snapshot))
			return true;
		if (TransactionIdDidCommit(xmax))
			return false;		/* updating transaction committed */
		/* it must have aborted or crashed */
		return true;
	}

	if (!(tuple->t_infomask & LEOPARD_XMAX_COMMITTED))
	{
		if (TransactionIdIsCurrentTransactionId(LeopardTupleHeaderGetRawXmax(tuple)))
		{
			if (LeopardTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}

		if (XidInMVCCSnapshot(LeopardTupleHeaderGetRawXmax(tuple), snapshot))
			return true;

		if (!TransactionIdDidCommit(LeopardTupleHeaderGetRawXmax(tuple)))
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, LEOPARD_XMAX_INVALID,
						InvalidTransactionId);
			return true;
		}

		/* xmax transaction committed */
		SetHintBits(tuple, buffer, LEOPARD_XMAX_COMMITTED,
					LeopardTupleHeaderGetRawXmax(tuple));
	}
	else
	{
		/* xmax is committed, but maybe not according to our snapshot */
		if (XidInMVCCSnapshot(LeopardTupleHeaderGetRawXmax(tuple), snapshot))
			return true;		/* treat as still in progress */
	}

	/* xmax transaction committed */

	return false;
}


/*
 * LeopardTupleSatisfiesVacuum
 *
 *	Determine the status of tuples for VACUUM purposes.  Here, what
 *	we mainly want to know is if a tuple is potentially visible to *any*
 *	running transaction.  If so, it can't be removed yet by VACUUM.
 *
 * OldestXmin is a cutoff XID (obtained from
 * GetOldestNonRemovableTransactionId()).  Tuples deleted by XIDs >=
 * OldestXmin are deemed "recently dead"; they might still be visible to some
 * open transaction, so we can't remove them, even if we see that the deleting
 * transaction has committed.
 */
HTSV_Result
LeopardTupleSatisfiesVacuum(LeopardTuple leopardtup, TransactionId OldestXmin,
						 Buffer buffer)
{
	TransactionId dead_after = InvalidTransactionId;
	HTSV_Result res;

	res = LeopardTupleSatisfiesVacuumHorizon(leopardtup, buffer, &dead_after);

	if (res == LEOPARDTUPLE_RECENTLY_DEAD)
	{
		Assert(TransactionIdIsValid(dead_after));

		if (TransactionIdPrecedes(dead_after, OldestXmin))
			res = LEOPARDTUPLE_DEAD;
	}
	else
		Assert(!TransactionIdIsValid(dead_after));

	return res;
}

/*
 * Work horse for LeopardTupleSatisfiesVacuum and similar routines.
 *
 * In contrast to LeopardTupleSatisfiesVacuum this routine, when encountering a
 * tuple that could still be visible to some backend, stores the xid that
 * needs to be compared with the horizon in *dead_after, and returns
 * LEOPARDTUPLE_RECENTLY_DEAD. The caller then can perform the comparison with
 * the horizon.  This is e.g. useful when comparing with different horizons.
 *
 * Note: LEOPARDTUPLE_DEAD can still be returned here, e.g. if the inserting
 * transaction aborted.
 */
HTSV_Result
LeopardTupleSatisfiesVacuumHorizon(LeopardTuple leopardtup, Buffer buffer, TransactionId *dead_after)
{
	LeopardTupleHeader tuple = leopardtup->t_data;

	Assert(ItemPointerIsValid(&leopardtup->t_self));
	Assert(leopardtup->t_tableOid != InvalidOid);
	Assert(dead_after != NULL);

	*dead_after = InvalidTransactionId;

	/*
	 * Has inserting transaction committed?
	 *
	 * If the inserting transaction aborted, then the tuple was never visible
	 * to any other transaction, so we can delete it immediately.
	 */
	if (!LeopardTupleHeaderXminCommitted(tuple))
	{
		if (LeopardTupleHeaderXminInvalid(tuple))
			return LEOPARDTUPLE_DEAD;
		else if (TransactionIdIsCurrentTransactionId(LeopardTupleHeaderGetRawXmin(tuple)))
		{
			if (tuple->t_infomask & LEOPARD_XMAX_INVALID)	/* xid invalid */
				return LEOPARDTUPLE_INSERT_IN_PROGRESS;
			/* only locked? run infomask-only check first, for performance */
			if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask) ||
				LeopardTupleHeaderIsOnlyLocked(tuple))
				return LEOPARDTUPLE_INSERT_IN_PROGRESS;
			/* inserted and then deleted by same xact */
			if (TransactionIdIsCurrentTransactionId(LeopardTupleHeaderGetUpdateXid(tuple)))
				return LEOPARDTUPLE_DELETE_IN_PROGRESS;
			/* deleting subtransaction must have aborted */
			return LEOPARDTUPLE_INSERT_IN_PROGRESS;
		}
		else if (TransactionIdIsInProgress(LeopardTupleHeaderGetRawXmin(tuple)))
		{
			/*
			 * It'd be possible to discern between INSERT/DELETE in progress
			 * here by looking at xmax - but that doesn't seem beneficial for
			 * the majority of callers and even detrimental for some. We'd
			 * rather have callers look at/wait for xmin than xmax. It's
			 * always correct to return INSERT_IN_PROGRESS because that's
			 * what's happening from the view of other backends.
			 */
			return LEOPARDTUPLE_INSERT_IN_PROGRESS;
		}
		else if (TransactionIdDidCommit(LeopardTupleHeaderGetRawXmin(tuple)))
			SetHintBits(tuple, buffer, LEOPARD_XMIN_COMMITTED,
						LeopardTupleHeaderGetRawXmin(tuple));
		else
		{
			/*
			 * Not in Progress, Not Committed, so either Aborted or crashed
			 */
			SetHintBits(tuple, buffer, LEOPARD_XMIN_INVALID,
						InvalidTransactionId);
			return LEOPARDTUPLE_DEAD;
		}

		/*
		 * At this point the xmin is known committed, but we might not have
		 * been able to set the hint bit yet; so we can no longer Assert that
		 * it's set.
		 */
	}

	/*
	 * Okay, the inserter committed, so it was good at some point.  Now what
	 * about the deleting transaction?
	 */
	if (tuple->t_infomask & LEOPARD_XMAX_INVALID)
		return LEOPARDTUPLE_LIVE;

	if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		/*
		 * "Deleting" xact really only locked it, so the tuple is live in any
		 * case.  However, we should make sure that either XMAX_COMMITTED or
		 * XMAX_INVALID gets set once the xact is gone, to reduce the costs of
		 * examining the tuple for future xacts.
		 */
		if (!(tuple->t_infomask & LEOPARD_XMAX_COMMITTED))
		{
			if (tuple->t_infomask & LEOPARD_XMAX_IS_MULTI)
			{
				/*
				 * If it's a pre-pg_upgrade tuple, the multixact cannot
				 * possibly be running; otherwise have to check.
				 */
				if (!LEOPARD_LOCKED_UPGRADED(tuple->t_infomask) &&
					MultiXactIdIsRunning(LeopardTupleHeaderGetRawXmax(tuple),
										 true))
					return LEOPARDTUPLE_LIVE;
				SetHintBits(tuple, buffer, LEOPARD_XMAX_INVALID, InvalidTransactionId);
			}
			else
			{
				if (TransactionIdIsInProgress(LeopardTupleHeaderGetRawXmax(tuple)))
					return LEOPARDTUPLE_LIVE;
				SetHintBits(tuple, buffer, LEOPARD_XMAX_INVALID,
							InvalidTransactionId);
			}
		}

		/*
		 * We don't really care whether xmax did commit, abort or crash. We
		 * know that xmax did lock the tuple, but it did not and will never
		 * actually update it.
		 */

		return LEOPARDTUPLE_LIVE;
	}

	if (tuple->t_infomask & LEOPARD_XMAX_IS_MULTI)
	{
		TransactionId xmax = LeopardTupleGetUpdateXid(tuple);

		/* already checked above */
		Assert(!LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsInProgress(xmax))
			return LEOPARDTUPLE_DELETE_IN_PROGRESS;
		else if (TransactionIdDidCommit(xmax))
		{
			/*
			 * The multixact might still be running due to lockers.  Need to
			 * allow for pruning if below the xid horizon regardless --
			 * otherwise we could end up with a tuple where the updater has to
			 * be removed due to the horizon, but is not pruned away.  It's
			 * not a problem to prune that tuple, because any remaining
			 * lockers will also be present in newer tuple versions.
			 */
			*dead_after = xmax;
			return LEOPARDTUPLE_RECENTLY_DEAD;
		}
		else if (!MultiXactIdIsRunning(LeopardTupleHeaderGetRawXmax(tuple), false))
		{
			/*
			 * Not in Progress, Not Committed, so either Aborted or crashed.
			 * Mark the Xmax as invalid.
			 */
			SetHintBits(tuple, buffer, LEOPARD_XMAX_INVALID, InvalidTransactionId);
		}

		return LEOPARDTUPLE_LIVE;
	}

	if (!(tuple->t_infomask & LEOPARD_XMAX_COMMITTED))
	{
		if (TransactionIdIsInProgress(LeopardTupleHeaderGetRawXmax(tuple)))
			return LEOPARDTUPLE_DELETE_IN_PROGRESS;
		else if (TransactionIdDidCommit(LeopardTupleHeaderGetRawXmax(tuple)))
			SetHintBits(tuple, buffer, LEOPARD_XMAX_COMMITTED,
						LeopardTupleHeaderGetRawXmax(tuple));
		else
		{
			/*
			 * Not in Progress, Not Committed, so either Aborted or crashed
			 */
			SetHintBits(tuple, buffer, LEOPARD_XMAX_INVALID,
						InvalidTransactionId);
			return LEOPARDTUPLE_LIVE;
		}

		/*
		 * At this point the xmax is known committed, but we might not have
		 * been able to set the hint bit yet; so we can no longer Assert that
		 * it's set.
		 */
	}

	/*
	 * Deleter committed, allow caller to check if it was recent enough that
	 * some open transactions could still see the tuple.
	 */
	*dead_after = LeopardTupleHeaderGetRawXmax(tuple);
	return LEOPARDTUPLE_RECENTLY_DEAD;
}


/*
 * LeopardTupleSatisfiesNonVacuumable
 *
 *	True if tuple might be visible to some transaction; false if it's
 *	surely dead to everyone, ie, vacuumable.
 *
 *	See SNAPSHOT_NON_VACUUMABLE's definition for the intended behaviour.
 *
 *	This is an interface to LeopardTupleSatisfiesVacuum that's callable via
 *	LeopardTupleSatisfiesSnapshot, so it can be used through a Snapshot.
 *	snapshot->vistest must have been set up with the horizon to use.
 */
static bool
LeopardTupleSatisfiesNonVacuumable(LeopardTuple leopardtup, Snapshot snapshot,
								Buffer buffer)
{
	TransactionId dead_after = InvalidTransactionId;
	HTSV_Result res;

	res = LeopardTupleSatisfiesVacuumHorizon(leopardtup, buffer, &dead_after);

	if (res == LEOPARDTUPLE_RECENTLY_DEAD)
	{
		Assert(TransactionIdIsValid(dead_after));

		if (GlobalVisTestIsRemovableXid(snapshot->vistest, dead_after))
			res = LEOPARDTUPLE_DEAD;
	}
	else
		Assert(!TransactionIdIsValid(dead_after));

	return res != LEOPARDTUPLE_DEAD;
}


/*
 * LeopardTupleIsSurelyDead
 *
 *	Cheaply determine whether a tuple is surely dead to all onlookers.
 *	We sometimes use this in lieu of LeopardTupleSatisfiesVacuum when the
 *	tuple has just been tested by another visibility routine (usually
 *	LeopardTupleSatisfiesMVCC) and, therefore, any hint bits that can be set
 *	should already be set.  We assume that if no hint bits are set, the xmin
 *	or xmax transaction is still running.  This is therefore faster than
 *	LeopardTupleSatisfiesVacuum, because we consult neither procarray nor CLOG.
 *	It's okay to return false when in doubt, but we must return true only
 *	if the tuple is removable.
 */
bool
LeopardTupleIsSurelyDead(LeopardTuple leopardtup, GlobalVisState *vistest)
{
	LeopardTupleHeader tuple = leopardtup->t_data;

	Assert(ItemPointerIsValid(&leopardtup->t_self));
	Assert(leopardtup->t_tableOid != InvalidOid);

	/*
	 * If the inserting transaction is marked invalid, then it aborted, and
	 * the tuple is definitely dead.  If it's marked neither committed nor
	 * invalid, then we assume it's still alive (since the presumption is that
	 * all relevant hint bits were just set moments ago).
	 */
	if (!LeopardTupleHeaderXminCommitted(tuple))
		return LeopardTupleHeaderXminInvalid(tuple) ? true : false;

	/*
	 * If the inserting transaction committed, but any deleting transaction
	 * aborted, the tuple is still alive.
	 */
	if (tuple->t_infomask & LEOPARD_XMAX_INVALID)
		return false;

	/*
	 * If the XMAX is just a lock, the tuple is still alive.
	 */
	if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return false;

	/*
	 * If the Xmax is a MultiXact, it might be dead or alive, but we cannot
	 * know without checking pg_multixact.
	 */
	if (tuple->t_infomask & LEOPARD_XMAX_IS_MULTI)
		return false;

	/* If deleter isn't known to have committed, assume it's still running. */
	if (!(tuple->t_infomask & LEOPARD_XMAX_COMMITTED))
		return false;

	/* Deleter committed, so tuple is dead if the XID is old enough. */
	return GlobalVisTestIsRemovableXid(vistest,
									   LeopardTupleHeaderGetRawXmax(tuple));
}

/*
 * Is the tuple really only locked?  That is, is it not updated?
 *
 * It's easy to check just infomask bits if the locker is not a multi; but
 * otherwise we need to verify that the updating transaction has not aborted.
 *
 * This function is here because it follows the same visibility rules laid out
 * at the top of this file.
 */
bool
LeopardTupleHeaderIsOnlyLocked(LeopardTupleHeader tuple)
{
	TransactionId xmax;

	/* if there's no valid Xmax, then there's obviously no update either */
	if (tuple->t_infomask & LEOPARD_XMAX_INVALID)
		return true;

	if (tuple->t_infomask & LEOPARD_XMAX_LOCK_ONLY)
		return true;

	/* invalid xmax means no update */
	if (!TransactionIdIsValid(LeopardTupleHeaderGetRawXmax(tuple)))
		return true;

	/*
	 * if LEOPARD_XMAX_LOCK_ONLY is not set and not a multi, then this must
	 * necessarily have been updated
	 */
	if (!(tuple->t_infomask & LEOPARD_XMAX_IS_MULTI))
		return false;

	/* ... but if it's a multi, then perhaps the updating Xid aborted. */
	xmax = LeopardTupleGetUpdateXid(tuple);

	/* not LOCKED_ONLY, so it has to have an xmax */
	Assert(TransactionIdIsValid(xmax));

	if (TransactionIdIsCurrentTransactionId(xmax))
		return false;
	if (TransactionIdIsInProgress(xmax))
		return false;
	if (TransactionIdDidCommit(xmax))
		return false;

	/*
	 * not current, not in progress, not committed -- must have aborted or
	 * crashed
	 */
	return true;
}

/*
 * check whether the transaction id 'xid' is in the pre-sorted array 'xip'.
 */
static bool
TransactionIdInArray(TransactionId xid, TransactionId *xip, Size num)
{
	return num > 0 &&
		bsearch(&xid, xip, num, sizeof(TransactionId), xidComparator) != NULL;
}

/*
 * See the comments for LeopardTupleSatisfiesMVCC for the semantics this function
 * obeys.
 *
 * Only usable on tuples from catalog tables!
 *
 * We don't need to support LEOPARD_MOVED_(IN|OFF) for now because we only support
 * reading catalog pages which couldn't have been created in an older version.
 *
 * We don't set any hint bits in here as it seems unlikely to be beneficial as
 * those should already be set by normal access and it seems to be too
 * dangerous to do so as the semantics of doing so during timetravel are more
 * complicated than when dealing "only" with the present.
 */
static bool
LeopardTupleSatisfiesHistoricMVCC(LeopardTuple leopardtup, Snapshot snapshot,
							   Buffer buffer)
{
	LeopardTupleHeader tuple = leopardtup->t_data;
	TransactionId xmin = LeopardTupleHeaderGetXmin(tuple);
	TransactionId xmax = LeopardTupleHeaderGetRawXmax(tuple);

	Assert(ItemPointerIsValid(&leopardtup->t_self));
	Assert(leopardtup->t_tableOid != InvalidOid);

	/* inserting transaction aborted */
	if (LeopardTupleHeaderXminInvalid(tuple))
	{
		Assert(!TransactionIdDidCommit(xmin));
		return false;
	}
	/* check if it's one of our txids, toplevel is also in there */
	else if (TransactionIdInArray(xmin, snapshot->subxip, snapshot->subxcnt))
	{
		bool		resolved;
		CommandId	cmin = LeopardTupleHeaderGetRawCommandId(tuple);
		CommandId	cmax = InvalidCommandId;

		/*
		 * another transaction might have (tried to) delete this tuple or
		 * cmin/cmax was stored in a combo CID. So we need to lookup the
		 * actual values externally.
		 */
		resolved = ResolveCminCmaxDuringDecoding(HistoricSnapshotGetTupleCids(), snapshot,
												 leopardtup, buffer,
												 &cmin, &cmax);

		/*
		 * If we haven't resolved the combo CID to cmin/cmax, that means we
		 * have not decoded the combo CID yet. That means the cmin is
		 * definitely in the future, and we're not supposed to see the tuple
		 * yet.
		 *
		 * XXX This only applies to decoding of in-progress transactions. In
		 * regular logical decoding we only execute this code at commit time,
		 * at which point we should have seen all relevant combo CIDs. So
		 * ideally, we should error out in this case but in practice, this
		 * won't happen. If we are too worried about this then we can add an
		 * elog inside ResolveCminCmaxDuringDecoding.
		 *
		 * XXX For the streaming case, we can track the largest combo CID
		 * assigned, and error out based on this (when unable to resolve combo
		 * CID below that observed maximum value).
		 */
		if (!resolved)
			return false;

		Assert(cmin != InvalidCommandId);

		if (cmin >= snapshot->curcid)
			return false;		/* inserted after scan started */
		/* fall through */
	}
	/* committed before our xmin horizon. Do a normal visibility check. */
	else if (TransactionIdPrecedes(xmin, snapshot->xmin))
	{
		Assert(!(LeopardTupleHeaderXminCommitted(tuple) &&
				 !TransactionIdDidCommit(xmin)));

		/* check for hint bit first, consult clog afterwards */
		if (!LeopardTupleHeaderXminCommitted(tuple) &&
			!TransactionIdDidCommit(xmin))
			return false;
		/* fall through */
	}
	/* beyond our xmax horizon, i.e. invisible */
	else if (TransactionIdFollowsOrEquals(xmin, snapshot->xmax))
	{
		return false;
	}
	/* check if it's a committed transaction in [xmin, xmax) */
	else if (TransactionIdInArray(xmin, snapshot->xip, snapshot->xcnt))
	{
		/* fall through */
	}

	/*
	 * none of the above, i.e. between [xmin, xmax) but hasn't committed. I.e.
	 * invisible.
	 */
	else
	{
		return false;
	}

	/* at this point we know xmin is visible, go on to check xmax */

	/* xid invalid or aborted */
	if (tuple->t_infomask & LEOPARD_XMAX_INVALID)
		return true;
	/* locked tuples are always visible */
	else if (LEOPARD_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return true;

	/*
	 * We can see multis here if we're looking at user tables or if somebody
	 * SELECT ... FOR SHARE/UPDATE a system table.
	 */
	else if (tuple->t_infomask & LEOPARD_XMAX_IS_MULTI)
	{
		xmax = LeopardTupleGetUpdateXid(tuple);
	}

	/* check if it's one of our txids, toplevel is also in there */
	if (TransactionIdInArray(xmax, snapshot->subxip, snapshot->subxcnt))
	{
		bool		resolved;
		CommandId	cmin;
		CommandId	cmax = LeopardTupleHeaderGetRawCommandId(tuple);

		/* Lookup actual cmin/cmax values */
		resolved = ResolveCminCmaxDuringDecoding(HistoricSnapshotGetTupleCids(), snapshot,
												 leopardtup, buffer,
												 &cmin, &cmax);

		/*
		 * If we haven't resolved the combo CID to cmin/cmax, that means we
		 * have not decoded the combo CID yet. That means the cmax is
		 * definitely in the future, and we're still supposed to see the
		 * tuple.
		 *
		 * XXX This only applies to decoding of in-progress transactions. In
		 * regular logical decoding we only execute this code at commit time,
		 * at which point we should have seen all relevant combo CIDs. So
		 * ideally, we should error out in this case but in practice, this
		 * won't happen. If we are too worried about this then we can add an
		 * elog inside ResolveCminCmaxDuringDecoding.
		 *
		 * XXX For the streaming case, we can track the largest combo CID
		 * assigned, and error out based on this (when unable to resolve combo
		 * CID below that observed maximum value).
		 */
		if (!resolved || cmax == InvalidCommandId)
			return true;

		if (cmax >= snapshot->curcid)
			return true;		/* deleted after scan started */
		else
			return false;		/* deleted before scan started */
	}
	/* below xmin horizon, normal transaction state is valid */
	else if (TransactionIdPrecedes(xmax, snapshot->xmin))
	{
		Assert(!(tuple->t_infomask & LEOPARD_XMAX_COMMITTED &&
				 !TransactionIdDidCommit(xmax)));

		/* check hint bit first */
		if (tuple->t_infomask & LEOPARD_XMAX_COMMITTED)
			return false;

		/* check clog */
		return !TransactionIdDidCommit(xmax);
	}
	/* above xmax horizon, we cannot possibly see the deleting transaction */
	else if (TransactionIdFollowsOrEquals(xmax, snapshot->xmax))
		return true;
	/* xmax is between [xmin, xmax), check known committed array */
	else if (TransactionIdInArray(xmax, snapshot->xip, snapshot->xcnt))
		return false;
	/* xmax is between [xmin, xmax), but known not to have committed yet */
	else
		return true;
}

/*
 * LeopardTupleSatisfiesVisibility
 *		True iff leopard tuple satisfies a time qual.
 *
 * Notes:
 *	Assumes leopard tuple is valid, and buffer at least share locked.
 *
 *	Hint bits in the LeopardTuple's t_infomask may be updated as a side effect;
 *	if so, the indicated buffer is marked dirty.
 */
bool
LeopardTupleSatisfiesVisibility(LeopardTuple tup, Snapshot snapshot, Buffer buffer)
{
	switch (snapshot->snapshot_type)
	{
		case SNAPSHOT_MVCC:
			return LeopardTupleSatisfiesMVCC(tup, snapshot, buffer);
			break;
		case SNAPSHOT_SELF:
			return LeopardTupleSatisfiesSelf(tup, snapshot, buffer);
			break;
		case SNAPSHOT_ANY:
			return LeopardTupleSatisfiesAny(tup, snapshot, buffer);
			break;
		case SNAPSHOT_TOAST:
			return LeopardTupleSatisfiesToast(tup, snapshot, buffer);
			break;
		case SNAPSHOT_DIRTY:
			return LeopardTupleSatisfiesDirty(tup, snapshot, buffer);
			break;
		case SNAPSHOT_HISTORIC_MVCC:
			return LeopardTupleSatisfiesHistoricMVCC(tup, snapshot, buffer);
			break;
		case SNAPSHOT_NON_VACUUMABLE:
			return LeopardTupleSatisfiesNonVacuumable(tup, snapshot, buffer);
			break;
	}

	return false;				/* keep compiler quiet */
}

bool
LeopardTupleSatisfiesRDA(LeopardTuple leopardtup, Snapshot snapshot)
{
	LeopardTupleHeader tuple = leopardtup->t_data;

	if (snapshot->snapshot_type != SNAPSHOT_MVCC)
		return false;

	Assert(ItemPointerIsValid(&leopardtup->t_self));

	if (!LeopardTupleHeaderXminCommitted(tuple))
		return false;

	if (!XidInMVCCSnapshot(LeopardTupleHeaderGetRawXmin(tuple), snapshot))
		return false;

	/*
	 * There might be some other conditions we can use to filter
	 * away wasted lookups of the RDA. If so, put them here.
	 */

	return true;
}
