/*-------------------------------------------------------------------------
 *
 * walsummarizer.c
 *
 * XXX SUMMARIZE WHAT THE WAL SUMMARIZER DOES HERE
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/walsummarizer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/timeline.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogrecovery.h"
#include "access/xlogutils.h"
#include "backup/blkreftable.h"
#include "catalog/storage_xlog.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "postmaster/walsummarizer.h"
#include "replication/walreceiver.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/latch.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

/*
 * Data in shared memory related to WAL summarization.
 *
 * This is protected by WALSummarizerLock.
 */
typedef struct
{
	bool		initialized;
	TimeLineID	summarizer_tli;
	XLogRecPtr	summarizer_lsn;
	bool		lsn_is_exact;
} WalSummarizerData;

/*
 * Private data for our xlogreader's page read callback.
 */
typedef struct
{
	TimeLineID	tli;
	bool		historic;
	XLogRecPtr	read_upto;
	bool		end_of_wal;
} SummarizerReadLocalXLogPrivate;

/*
 * Between activity cycles, we sleep for a time that is a multiple of
 * SLEEP_QUANTUM_DURATION, which is measured in milliseconds. The multiplier
 * can be anywhere between MIN_SLEEP_QUANTA and MAX_SLEEP_QUANTA depending on
 * how busy the system is.
 */
#define SLEEP_QUANTUM_DURATION	5000
#define MIN_SLEEP_QUANTA		1
#define MAX_SLEEP_QUANTA		12

/* Pointer to shared memory state. */
static WalSummarizerData *WalSummarizerCtl;

/* Only used in walsummarizer process. */
static long sleep_quanta = MIN_SLEEP_QUANTA;

/*
 * GUC parameters
 */
int			wal_summarize_mb = 256;
int			wal_summarize_keep_time = 7 * 24 * 60;

static void ConsiderSummarizingWAL(TimeLineID *current_tli,
								   XLogRecPtr *current_lsn);
static XLogRecPtr GetLatestLSN(TimeLineID *tli);
static void HandleWalSummarizerInterrupts(void);
static XLogRecPtr GetCheckPointRedoLSN(XLogRecPtr previous_lsn,
									   XLogRecPtr start_lsn,
									   XLogRecPtr end_lsn);
static XLogRecPtr SummarizeWAL(TimeLineID tli, bool historic,
							   XLogRecPtr start_lsn, bool exact,
							   XLogRecPtr cutoff_lsn, XLogRecPtr maximum_lsn);
static void SummarizeSmgrRecord(XLogReaderState *xlogreader,
								BlockRefTable *brtab);
static void SummarizeXactRecord(XLogReaderState *xlogreader,
								BlockRefTable *brtab);
static int summarizer_read_local_xlog_page(XLogReaderState *state,
										   XLogRecPtr targetPagePtr,
										   int reqLen,
										   XLogRecPtr targetRecPtr,
										   char *cur_page);

/*
 * Amount of shared memory required for this module.
 */
Size
WalSummarizerShmemSize(void)
{
	return sizeof(WalSummarizerData);
}

/*
 * Create or attach to shared memory segment for this module.
 */
void
WalSummarizerShmemInit(void)
{
	bool		found;

	WalSummarizerCtl = (WalSummarizerData *)
		ShmemInitStruct("Wal Summarizer Ctl", WalSummarizerShmemSize(),
						&found);

	if (!found)
	{
		/*
		 * First time through, so initialize.
		 *
		 * We're just filling in dummy values here -- the real initialization
		 * will happen when GetOldestUnsummarizedLSN() is called for the first
		 * time.
		 */
		WalSummarizerCtl->initialized = false;
		WalSummarizerCtl->summarizer_tli = 0;
		WalSummarizerCtl->summarizer_lsn = InvalidXLogRecPtr;
		WalSummarizerCtl->lsn_is_exact = false;
	}
}

/*
 * Entry point for walsummarizer process.
 */
void
WalSummarizerMain(void)
{
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext context;
	XLogRecPtr	current_lsn = InvalidXLogRecPtr;
	TimeLineID	current_tli;
	ereport(DEBUG1,
			(errmsg_internal("WAL summarizer started")));

	/*
	 * Properly accept or ignore signals the postmaster might send us
	 *
	 * We have no particular use for SIGINT at the moment, but seems
	 * reasonable to treat like SIGTERM.
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	/* SIGQUIT handler was already set up by InitPostmasterChild */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN); /* not used */

	/* Create and switch to a memory context that we can reset on error. */
	context = AllocSetContextCreate(TopMemoryContext,
									"Wal Summarizer",
									ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(context);

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);

	/*
	 * If an exception is encountered, processing resumes here.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/* Release resources we might have acquired. */
		LWLockReleaseAll();
		ConditionVariableCancelSleep();
		pgstat_report_wait_end();
		ReleaseAuxProcessResources(false);
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(context);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/*
		 * Sleep at least 1 second after any error.  A write error is likely
		 * to be repeated, and we don't want to be filling the error logs as
		 * fast as we can.
		 */
		pg_usleep(1000000L);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	/*
	 * Loop forever
	 */
	for (;;)
	{
		/* Process any signals received recently. */
		HandleWalSummarizerInterrupts();

		/* If it's time to generate WAL summaries, then do that. */
		ConsiderSummarizingWAL(&current_tli, &current_lsn);

		/* XXX also need to think about removing old WAL summaries */

		/* Wait for something to happen. */
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 sleep_quanta * SLEEP_QUANTUM_DURATION,
						 WAIT_EVENT_WAL_SUMMARIZER_MAIN);
		ResetLatch(MyLatch);
	}
}

/*
 * Get the oldest LSN in this server's timeline history that has not yet been
 * summarized.
 *
 * If *tli != NULL, it will be set to the TLI for the LSN that is returned.
 *
 * If *lsn_is_exact != NULL, it will be set to true if the returned LSN is
 * necessarily the start of a WAL record and false if it's just the beginning
 * of a WAL segment.
 */
XLogRecPtr
GetOldestUnsummarizedLSN(TimeLineID *tli, bool *lsn_is_exact)
{
	XLogRecPtr	latest_lsn;
	TimeLineID	latest_tli;
	LWLockMode	mode = LW_SHARED;
	int			n;
	XLogSegNo	oldest_segno;
	List	   *tles;
	XLogRecPtr	unsummarized_lsn;
	TimeLineID	unsummarized_tli = 0;

	/* If not summarizing WAL, do nothing. */
	if (wal_summarize_mb == 0)
		return InvalidXLogRecPtr;

	/*
	 * Initially, we acquire the lock in shared mode and try to fetch the
	 * required information. If the data structure hasn't been initialized,
	 * we reacquire the lock in shared mode so that we can initialize it.
	 * However, if someone else does that first before we get the lock, then
	 * we can just return the requested information after all.
	 */
	while (true)
	{
		LWLockAcquire(WALSummarizerLock, mode);

		if (WalSummarizerCtl->initialized)
		{
			unsummarized_lsn = WalSummarizerCtl->summarizer_lsn;
			if (tli != NULL)
				*tli = WalSummarizerCtl->summarizer_tli;
			if (lsn_is_exact != NULL)
				*lsn_is_exact = WalSummarizerCtl->lsn_is_exact;
			LWLockRelease(WALSummarizerLock);
			return unsummarized_lsn;
		}

		if (mode == LW_EXCLUSIVE)
			break;

		LWLockRelease(WALSummarizerLock);
		mode = LW_EXCLUSIVE;
	}

	/*
	 * The data structure needs to be initialized, and we are the first to
	 * obtain the lock in exclusive mode, so it's our job to do that
	 * initialization.
	 *
	 * So, find the oldest timeline on which WAL still exists, and the earliest
	 * segment for which it exists.
	 *
	 * XXX. We should skip timelines or segments that are already summarized,
	 * unless we instead choose to handle that elsewhere.
	 */
	latest_lsn = GetLatestLSN(&latest_tli);
	tles = readTimeLineHistory(latest_tli);
	for (n = list_length(tles) - 1; n >= 0; --n)
	{
		TimeLineHistoryEntry *tle = list_nth(tles, n);

		oldest_segno = XLogGetOldestSegno(tle->tli);
		if (oldest_segno != 0)
		{
			unsummarized_tli = tle->tli;
			XLogSegNoOffsetToRecPtr(oldest_segno, 0, wal_segment_size,
									unsummarized_lsn);
			break;
		}
	}

	/*
	 * It really should not be possible for us to find no WAL, but since WAL
	 * summarization is a non-critical function, tolerate that situation if
	 * somehow it occurs.
	 */
	if (unsummarized_tli == 0)
	{
		ereport(LOG,
				errmsg("no WAL found on timeline %d", latest_tli));
		LWLockRelease(WALSummarizerLock);
		return InvalidXLogRecPtr;
	}

	/* Update shared memory with the discovered values. */
	WalSummarizerCtl->initialized = true;
	WalSummarizerCtl->summarizer_lsn = unsummarized_lsn;
	WalSummarizerCtl->summarizer_tli = unsummarized_tli;
	WalSummarizerCtl->lsn_is_exact = false;

	/* Also return the to the caller as required. */
	if (tli != NULL)
		*tli = WalSummarizerCtl->summarizer_tli;
	if (lsn_is_exact != NULL)
		*lsn_is_exact = WalSummarizerCtl->lsn_is_exact;
	LWLockRelease(WALSummarizerLock);

	return unsummarized_lsn;
}

/*
 * Determine whether it's time to generate any WAL summaries. If it is,
 * generate as many as possible.
 *
 * As a side effect, adjusts sleep_quanta, which controls the time for which
 * the main loop will sleep before calling this function again.
 */
static void
ConsiderSummarizingWAL(TimeLineID *current_tli, XLogRecPtr *current_lsn)
{
	uint64		bytes_per_summary = wal_summarize_mb * 1024 * 1024;
	XLogRecPtr	cutoff_lsn;
	bool		exact;
	XLogRecPtr	latest_lsn;
	TimeLineID	latest_tli;
	XLogRecPtr	previous_lsn;
	TimeLineID	previous_tli;
	XLogRecPtr	switch_lsn = InvalidXLogRecPtr;
	TimeLineID	switch_tli = 0;
	XLogRecPtr	end_of_summary_lsn;
	int			summaries_produced = 0;

	/* Fetch information about previous progress from shared memory. */
	previous_lsn = GetOldestUnsummarizedLSN(&previous_tli, &exact);

	/*
	 * If this happens, it probably means that WAL summarization has been
	 * disabled and therefore this process should exit. It could also mean that
	 * we couldn't figure out to begin summaring because pg_wal is all messed
	 * up. In that case, a message will have already been logged.
	 */
	if (XLogRecPtrIsInvalid(previous_lsn))
	{
		sleep_quanta = MAX_SLEEP_QUANTA;
		return;
	}

	/* Find the LSN and TLI up to which we can safely summarize. */
	latest_lsn = GetLatestLSN(&latest_tli);

	if (XLogRecPtrIsInvalid(*current_lsn))
	{
		*current_tli = previous_tli;
		*current_lsn = previous_lsn;
	}

	{
		int i=1;
		//while(i);
	}

	/*
	 * It might be time to generate a summary, or we might be far enough
	 * behind that we need to generate multiple summaries in quick succesion.
	 * Loop until we've produced as many as required.
	 */
	while (true)
	{
		/* Process any signals received recently. */
		HandleWalSummarizerInterrupts();

		/*
		 * If we're summarizing a historic timeline and we haven't yet
		 * computed the point at which to switch to the next timeline, do
		 * that now.
		 *
		 * Note that if this is a standby, what was previously the current
		 * timeline could become historic at any time.
		 *
		 * latest_tli can't change during a single execution of this while
		 * loop, but previous_tli can change multiple times, and switch_tli
		 * needs to be recomputed each time.
		 *
		 * We could try to make this more efficient by caching the results
		 * of readTimeLineHistory when latest_tli has not changed, but since
		 * we only have to do this once per timeline switch, we probably
		 * wouldn't save any significant amount of work in practice.
		 */
		if (previous_tli != latest_tli && XLogRecPtrIsInvalid(switch_lsn))
		{
			List   *tles = readTimeLineHistory(latest_tli);

			switch_lsn = tliSwitchPoint(previous_tli, tles, &switch_tli);
		}


		cutoff_lsn = GetCheckPointRedoLSN(previous_lsn, *current_lsn, 
										  *current_tli == latest_tli ?
										  latest_lsn : switch_lsn);

		if (!XLogRecPtrIsInvalid(switch_lsn) &&
			(XLogRecPtrIsInvalid(cutoff_lsn) ||  cutoff_lsn > switch_lsn))
			cutoff_lsn = switch_lsn;

		elog(DEBUG2,
			 "WAL summarization cutoff is TLI %d @ %X/%X, flush position is %X/%X",
			 latest_tli, LSN_FORMAT_ARGS(cutoff_lsn), LSN_FORMAT_ARGS(latest_lsn));

		if (XLogRecPtrIsInvalid(cutoff_lsn))
		{
			*current_tli = latest_tli;
			*current_lsn = latest_lsn;
			break;
		}

		/* Summarize WAL. */
		end_of_summary_lsn = SummarizeWAL(previous_tli,
										  previous_tli != latest_tli,
										  previous_lsn, exact,
										  cutoff_lsn, latest_lsn);
		Assert(!XLogRecPtrIsInvalid(end_of_summary_lsn));
		Assert(end_of_summary_lsn == cutoff_lsn);

		/* We have sucessfully produced a summary. */
		ereport(LOG,
				errmsg("summarized WAL on TLI %d from %X/%X to %X/%X",
					   previous_tli,
					   LSN_FORMAT_ARGS(previous_lsn),
					   LSN_FORMAT_ARGS(end_of_summary_lsn)));
		++summaries_produced;
		*current_lsn = end_of_summary_lsn;

		/*
		 * Update state for next loop iteration.
		 *
		 * Next summary file should start from exactly where this one ended.
		 * Timeline remains unchanged unless a switch LSN was computed and
		 * we have reached it.
		 */
		previous_lsn = end_of_summary_lsn;
		exact = true;
		if (!XLogRecPtrIsInvalid(switch_lsn) && cutoff_lsn >= switch_lsn)
		{
			previous_tli = switch_tli;
			switch_lsn = InvalidXLogRecPtr;
			switch_tli = 0;
		}

		/* Update state in shared memory. */
		LWLockAcquire(WALSummarizerLock, LW_EXCLUSIVE);
		WalSummarizerCtl->summarizer_lsn = end_of_summary_lsn;
		WalSummarizerCtl->summarizer_tli = previous_tli;
		WalSummarizerCtl->lsn_is_exact = true;
		LWLockRelease(WALSummarizerLock);
	}

	/*
	 * Increase the sleep time if we didn't produce any summaries, are not
	 * close to reaching the cutoff LSN, and aren't already sleeping for the
	 * maximum time.
	 */
	if (summaries_produced == 0 && sleep_quanta < MAX_SLEEP_QUANTA &&
		latest_lsn < cutoff_lsn - bytes_per_summary / 2)
		sleep_quanta++;

	/*
	 * Reduce the sleep time if we seem not to be keeping up, unless it's
	 * already minimal. Reduce it more sharply if we produced multiple
	 * summaries, since that indicates we're falling well behind.
	 */
	if (sleep_quanta > MIN_SLEEP_QUANTA)
	{
		if (summaries_produced > 1)
			sleep_quanta = Max(sleep_quanta / 2, MIN_SLEEP_QUANTA);
		else if (latest_lsn > cutoff_lsn + bytes_per_summary / 2)
			sleep_quanta--;
	}
}

/*
 * Get the latest LSN that is eligible to be summarized, and set *tli to the
 * corresponding timeline.
 */
static XLogRecPtr
GetLatestLSN(TimeLineID *tli)
{
	if (!RecoveryInProgress())
	{
		/* Don't summarize WAL before it's flushed. */
		return GetFlushRecPtr(tli);
	}
	else
	{
		XLogRecPtr	flush_lsn;
		TimeLineID	flush_tli;
		XLogRecPtr	replay_lsn;
		TimeLineID	replay_tli;

		/*
		 * What we really want to know is how much WAL has been flushed to
		 * disk, but the only flush position available is the one provided
		 * by the walreceiver, which may not be running, because this could
		 * be crash recovery or recovery via restore_command. So use either
		 * the WAL receiver's flush position or the replay position, whichever
		 * is further ahead, on the theory that if the WAL has been replayed
		 * then it must also have been flushed to disk.
		 */
		flush_lsn = GetWalRcvFlushRecPtr(NULL, &flush_tli);
		replay_lsn = GetXLogReplayRecPtr(&replay_tli);
		if (flush_lsn > replay_lsn)
		{
			*tli = flush_tli;
			return flush_lsn;
		}
		else
		{
			*tli = replay_tli;
			return replay_lsn;
		}
	}
}

/*
 * Interrupt handler for main loop of WAL summarizer process.
 */
static void
HandleWalSummarizerInterrupts(void)
{
	if (ProcSignalBarrierPending)
		ProcessProcSignalBarrier();

	if (ConfigReloadPending)
	{
		ConfigReloadPending = false;
		ProcessConfigFile(PGC_SIGHUP);
	}

	if (ShutdownRequestPending || wal_summarize_mb == 0)
	{
		ereport(DEBUG1,
				errmsg_internal("WAL summarizer shutting down"));
		proc_exit(0);
	}

	/* Perform logging of memory contexts of this process */
	if (LogMemoryContextPending)
		ProcessLogMemoryContextInterrupt();
}

static XLogRecPtr
GetCheckPointRedoLSN(XLogRecPtr previous_lsn,
					 XLogRecPtr start_lsn, XLogRecPtr end_lsn)
{
	XLogReaderState *xlogreader;
	ReadLocalXLogPageNoWaitPrivate *private_data;	
	XLogRecPtr 		 lsn = InvalidXLogRecPtr;

	private_data = (ReadLocalXLogPageNoWaitPrivate *)
		palloc0(sizeof(ReadLocalXLogPageNoWaitPrivate));

	/* Create xlogreader. */
	xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
									XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
											   .segment_open = &wal_segment_open,
											   .segment_close = &wal_segment_close),
									private_data);
	if (xlogreader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));


	start_lsn = XLogFindNextRecord(xlogreader, start_lsn);
	if (XLogRecPtrIsInvalid(start_lsn))
		return InvalidXLogRecPtr;

	XLogBeginRead(xlogreader, start_lsn);

	/*
	 * Main loop: read xlog records one by one.
	 */
	while (xlogreader->EndRecPtr < end_lsn)
	{
		char   *errormsg;
		XLogRecord *record = XLogReadRecord(xlogreader, &errormsg);

		if (record == NULL)
		{
			if (errormsg)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read WAL at %X/%X: %s",
						 LSN_FORMAT_ARGS(xlogreader->EndRecPtr), errormsg)));
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read WAL at %X/%X",
						 LSN_FORMAT_ARGS(xlogreader->EndRecPtr))));
		}

		if (xlogreader->ReadRecPtr != previous_lsn &&
			record->xl_info == XLOG_CHECKPOINT_REDOPOINT)
		{
			lsn = xlogreader->ReadRecPtr;
			break;
		}
	}

	XLogReaderFree(xlogreader);

	return lsn;
}

/*
 * Summarize a range of WAL records on a single timeline.
 *
 * 'tli' is the timeline to be summarized. 'historic' should be false if the
 * timeline in question is the latest one and true otherwise.
 *
 * 'start_lsn' is the point at which we should start summarizing. If this
 * value comes from the end LSN of the previous record as returned by the
 * xlograder machinery, 'exact' should be true; otherwise, 'exact' should
 * be false, and this function will search forward for the start of a valid
 * WAL record.
 *
 * 'cutoff_lsn' is the point at which we should stop summarizing. The first
 * record that ends at or after cutoff_lsn will be the last one included
 * in the summary.
 *
 * 'maximum_lsn' identifies the point beyond which we can't count on being
 * able to read any more WAL. It should be the switch point when reading a
 * historic timeline, or the most-recently-measured end of WAL when reading
 * the current timeline.
 *
 * The return value is the LSN at which the WAL summary actually ends. This
 * might be exactly cutoff_lsn, but will often be slightly greater, because
 * there may not be a record ending at exactly that LSN.
 */
static XLogRecPtr
SummarizeWAL(TimeLineID tli, bool historic,
			 XLogRecPtr start_lsn, bool exact,
			 XLogRecPtr cutoff_lsn, XLogRecPtr maximum_lsn)
{
	SummarizerReadLocalXLogPrivate *private_data;
	XLogReaderState *xlogreader;
	XLogRecPtr	summary_start_lsn;
	XLogRecPtr	summary_end_lsn = cutoff_lsn;
	char		temp_path[MAXPGPATH];
	char		final_path[MAXPGPATH];
	File		file;
	BlockRefTable *brtab = CreateEmptyBlockRefTable();

	/* Initialize private data for xlogreader. */
	private_data = (SummarizerReadLocalXLogPrivate *)
		palloc0(sizeof(SummarizerReadLocalXLogPrivate));
	private_data->tli = tli;
	private_data->read_upto = maximum_lsn;

	/* Create xlogreader. */
	xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
									XL_ROUTINE(.page_read = &summarizer_read_local_xlog_page,
											   .segment_open = &wal_segment_open,
											   .segment_close = &wal_segment_close),
									private_data);
	if (xlogreader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	/*
	 * When exact = false, we're starting from an arbitrary point in the WAL
	 * and must search forward for the start of the next record.
	 *
	 * When exact = true, start_lsn should be either the LSN where a record
	 * begins, or the LSN of a page where the page header is immediately
	 * followed by the start of a new record. XLogBeginRead should tolerate
	 * either case.
	 *
	 * We need to allow for both cases because the behavior of xlogreader
	 * varies. When a record spans two or more xlog pages, the ending LSN
	 * reported by xlogreader will be the starting LSN of the followign record,
	 * but when an xlog page boundary falls between two records, the end LSN
	 * for the first will be reported as the first byte of the following page.
	 * We can't know until we read that page how large the header will be, but
	 * we'll have to skip over it to find the next record.
	 */
	if (exact)
	{
		/*
		 * Even if start_lsn is the beginning of a page rather than the
		 * beginning of the first record on that page, we should still use it
		 * as the start LSN for the summary file. That's because we detect
		 * missing summary files by looking for cases where the end LSN of
		 * one file is less than the start LSN of the next file. When only
		 * a page header is skipped, nothing has been missed.
		 */
		XLogBeginRead(xlogreader, start_lsn);
		summary_start_lsn = start_lsn;
	}
	else
	{
		summary_start_lsn = XLogFindNextRecord(xlogreader, start_lsn);
		if (XLogRecPtrIsInvalid(summary_start_lsn))
		{
			/*
			 * If we hit end-of-WAL while trying to find the next valid record,
			 * we must be on a historic timeline that has no valid records
			 * that begin after start_lsn and before cutoff_lsn. This should
			 * only happen as a result of a record that is continued across
			 * multiple pages and doesn't end before the TLI switch.
			 */
			if (private_data->end_of_wal)
			{
				ereport(DEBUG1,
						errmsg_internal("could not read WAL from timeline %d at %X/%X: end of WAL at %X/%X",
										tli,
										LSN_FORMAT_ARGS(start_lsn),
										LSN_FORMAT_ARGS(private_data->read_upto)));

				/* Arrange for an empty summary file to be generated. */
				summary_start_lsn = start_lsn;
				xlogreader->EndRecPtr = cutoff_lsn;
			}
			else
				ereport(ERROR,
						(errmsg("could not find a valid record after %X/%X",
								LSN_FORMAT_ARGS(start_lsn))));
		}
	}

	/*
	 * Main loop: read xlog records one by one.
	 */
	while (xlogreader->EndRecPtr < cutoff_lsn)
	{
		int		block_id;
		char *errormsg;
		XLogRecord *record = XLogReadRecord(xlogreader, &errormsg);

		if (record == NULL)
		{
			SummarizerReadLocalXLogPrivate *private_data;

			private_data = (SummarizerReadLocalXLogPrivate *)
				xlogreader->private_data;
			if (private_data->end_of_wal)
			{
				/*
				 * This timeline must be historic and must end before we were
				 * able to read a complete record. This should only happen
				 * as a result of a record that is continued across multiple
				 * pages and doesn't end before the TLI switch.
				 */
				ereport(DEBUG1,
						errmsg_internal("could not read WAL from timeline %d at %X/%X: end of WAL at %X/%X",
										tli,
										LSN_FORMAT_ARGS(xlogreader->EndRecPtr),
										LSN_FORMAT_ARGS(private_data->read_upto)));
				/* Summary ends exactly at cutoff. */
				summary_end_lsn = cutoff_lsn;
				break;
			}
			if (errormsg)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read WAL at %X/%X: %s",
						 LSN_FORMAT_ARGS(xlogreader->EndRecPtr), errormsg)));
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read WAL at %X/%X",
						 LSN_FORMAT_ARGS(xlogreader->EndRecPtr))));
		}

		if (xlogreader->ReadRecPtr >= cutoff_lsn)
		{
			/*
			 * Woops! We've read a record that *starts* after the cutoff LSN,
			 * contrary to our goal of reading only until we hit the first
			 * record that ends at or after the cutoff LSN. Pretend we didn't
			 * read it after all by bailing out of this loop right here, before
			 * we do anything with this record.
			 *
			 * This can happen because the last record before the cutoff LSN
			 * might be continued across multiple pages, and then we might
			 * come to a page with XLP_FIRST_IS_OVERWRITE_CONTRECORD set.
			 * In that case, the record that was continued across multiple
			 * pages is incomplete and will be disregarded, and the read
			 * will restart from the beginning of the page that is flagged
			 * XLP_FIRST_IS_OVERWRITE_CONTRECORD.
			 *
			 * If this case occurs, we can fairly say that the current summary
			 * file ends at the cutoff LSN exactly. The first record on the
			 * page marked XLP_FIRST_IS_OVERWRITE_CONTRECORD will be discovered
			 * when generating the next summary file.
			 */
			summary_end_lsn = cutoff_lsn;
			break;
		}

		/* Special handling for particular types of WAL records. */
		switch (XLogRecGetRmid(xlogreader))
		{
			case RM_SMGR_ID:
				SummarizeSmgrRecord(xlogreader, brtab);
				break;
			case RM_XACT_ID:
				SummarizeXactRecord(xlogreader, brtab);
				break;
			default:
				break;
		}

		/* Feed block references from xlog record to block reference table. */
		for (block_id = 0; block_id <= XLogRecMaxBlockId(xlogreader);
			 block_id++)
		{
			RelFileLocator	rlocator;
			ForkNumber		forknum;
			BlockNumber		blocknum;

			if (!XLogRecGetBlockTagExtended(xlogreader, block_id, &rlocator,
											&forknum, &blocknum, NULL))
				continue;
			/*
			 * The FSM isn't fully WAL-logged, so just ignore any references
			 * to it.
			 */
			if (forknum != FSM_FORKNUM)
				BlockRefTableMarkBlockModified(brtab, &rlocator, forknum,
											   blocknum);
		}

		/* Update our notion of where this summary file ends. */
		summary_end_lsn = xlogreader->EndRecPtr;
	}

	/* Destroy xlogreader. */
	pfree(xlogreader->private_data);
	XLogReaderFree(xlogreader);

	/* Generate temporary and final path name. */
	snprintf(temp_path, MAXPGPATH,
			 XLOGDIR "/summaries/temp.summary");
	snprintf(final_path, MAXPGPATH,
			 XLOGDIR "/summaries/%08X%08X%08X%08X%08X.summary",
			 tli,
			 LSN_FORMAT_ARGS(summary_start_lsn),
			 LSN_FORMAT_ARGS(summary_end_lsn));

	/* Open the temporary file for writing. */
	file = PathNameOpenFile(temp_path, O_WRONLY | O_CREAT | O_TRUNC);
	if (file < 0)
		ereport(ERROR,
				 (errcode_for_file_access(),
				  errmsg("could not create file \"%s\": %m", temp_path)));

	/* Write the data. */
	WriteBlockRefTable(brtab, file);
	BlockRefTableReset(brtab);

	/* Close temporary file and shut down xlogreader. */
	FileClose(file);

	/* Durably rename the new summary into place. */
	durable_rename(temp_path, final_path, ERROR);

	return summary_end_lsn;
}

/*
 * Special handling for WAL records with RM_SMGR_ID.
 */
static void
SummarizeSmgrRecord(XLogReaderState *xlogreader, BlockRefTable *brtab)
{
	uint8	info = XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK;

	if (info == XLOG_SMGR_CREATE)
	{
		xl_smgr_create *xlrec;

		/*
		 * If a new relation fork is created on disk, there is no point
		 * tracking anything about which blocks have been modified, because the
		 * whole thing will be new. Hence, set the limit block for this fork
		 * to 0.
		 *
		 * Ignore the FSM fork, which is not fully WAL-logged.
		 */
		xlrec = (xl_smgr_create *) XLogRecGetData(xlogreader);

		if (xlrec->forkNum != FSM_FORKNUM)
			BlockRefTableSetLimitBlock(brtab, &xlrec->rlocator,
									   xlrec->forkNum, 0);
	}
	else if (info == XLOG_SMGR_TRUNCATE)
	{
		xl_smgr_truncate *xlrec;

		xlrec = (xl_smgr_truncate *) XLogRecGetData(xlogreader);

		/*
		 * If a relation fork is truncated on disk, there is in point in
		 * tracking anything about block modifications beyond the truncation
		 * point.
		 *
		 * We ignore SMGR_TRUNCATE_FSM here because the FSM isn't fully
		 * WAL-logged and thus we can't track modified blocks for it anyway.
		 */
		if ((xlrec->flags & SMGR_TRUNCATE_HEAP) != 0)
			BlockRefTableSetLimitBlock(brtab, &xlrec->rlocator,
									   MAIN_FORKNUM, xlrec->blkno);
		if ((xlrec->flags & SMGR_TRUNCATE_VM) != 0)
			BlockRefTableSetLimitBlock(brtab, &xlrec->rlocator,
									   VISIBILITYMAP_FORKNUM, xlrec->blkno);
	}
}

/*
 * Special handling for WAL recods with RM_XACT_ID.
 */
static void
SummarizeXactRecord(XLogReaderState *xlogreader, BlockRefTable *brtab)
{
	uint8	info = XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK;
	uint8	xact_info = info & XLOG_XACT_OPMASK;

	if (xact_info == XLOG_XACT_COMMIT ||
		xact_info == XLOG_XACT_COMMIT_PREPARED)
	{
		xl_xact_commit *xlrec = (xl_xact_commit *) XLogRecGetData(xlogreader);
		xl_xact_parsed_commit parsed;
		int		i;

		ParseCommitRecord(XLogRecGetInfo(xlogreader), xlrec, &parsed);
		for (i = 0; i < parsed.nrels; ++i)
		{
			ForkNumber	forknum;

			for (forknum = 0; forknum <= MAX_FORKNUM; ++forknum)
				if (forknum != FSM_FORKNUM)
					BlockRefTableSetLimitBlock(brtab, &parsed.xlocators[i],
											   forknum, 0);
		}
	}
	else if (xact_info == XLOG_XACT_ABORT ||
			 xact_info == XLOG_XACT_ABORT_PREPARED)
	{
		xl_xact_abort *xlrec = (xl_xact_abort *) XLogRecGetData(xlogreader);
		xl_xact_parsed_abort parsed;
		int		i;

		ParseAbortRecord(XLogRecGetInfo(xlogreader), xlrec, &parsed);
		for (i = 0; i < parsed.nrels; ++i)
		{
			ForkNumber	forknum;

			for (forknum = 0; forknum <= MAX_FORKNUM; ++forknum)
				if (forknum != FSM_FORKNUM)
					BlockRefTableSetLimitBlock(brtab, &parsed.xlocators[i],
											   forknum, 0);
		}
	}
}

/*
 * Similar to read_local_xlog_page, but limited to read from one particular
 * timeline. If the end of WAL is reached, it will wait for more if reading
 * from the current timeline, or give up if reading from a historic timeline.
 * In the latter case, it will also set private_data->end_of_wal = true.
 *
 * Caller must set private_data->tli to the TLI of interest,
 * private_data->read_upto to the lowest LSN that is not known to be safe
 * to read on that timeline, and private_data->historic to true if and only
 * if the timeline is not the current timeline. This function will update
 * private_data->read_upto and private_data->historic if more WAL appears
 * on the current timeline or if the current timeline becomes historic.
 */
static int
summarizer_read_local_xlog_page(XLogReaderState *state,
								XLogRecPtr targetPagePtr, int reqLen,
								XLogRecPtr targetRecPtr, char *cur_page)
{
	XLogRecPtr	loc;
	int			count;
	WALReadError errinfo;
	SummarizerReadLocalXLogPrivate *private_data;

	private_data = (SummarizerReadLocalXLogPrivate *)
			state->private_data;

	loc = targetPagePtr + reqLen;

	while (true)
	{
		if (targetPagePtr + XLOG_BLCKSZ <= private_data->read_upto)
		{
			/*
			 * more than one block available; read only that block, have caller
			 * come back if they need more.
			 */
			count = XLOG_BLCKSZ;
			break;
		}
		else if (targetPagePtr + reqLen > private_data->read_upto)
		{
			/* We don't seem to have enough data. */
			if (private_data->historic)
			{
				/*
				 * This is a historic timeline, so there will never be any
				 * more data than we have currently.
				 */
				private_data->end_of_wal = true;
				return -1;
			}
			else
			{
				XLogRecPtr	latest_lsn;
				TimeLineID	latest_tli;

				/*
				 * This is - or at least was up until very recently - the
				 * current timeline, so more data might show up.  Pause briefly
				 * here so we don't tight-loop while waiting for that to
				 * happen.
				 */
				HandleWalSummarizerInterrupts();
				(void) WaitLatch(MyLatch,
								 WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
								 1,
								 WAIT_EVENT_WAL_SUMMARIZER_WAL);

				latest_lsn = GetLatestLSN(&latest_tli);
				if (private_data->tli == latest_tli)
				{
					/* Still the current timeline, update max LSN. */
					Assert(latest_lsn >= private_data->read_upto);
					private_data->read_upto = latest_lsn;
				}
				else
				{
					List   *tles = readTimeLineHistory(latest_tli);
					XLogRecPtr	switchpoint;

					/*
					 * The timeline we're scanning is no longer the latest
					 * one. Figure out when it ended and allow reads up to
					 * exactly that point.
					 */
					private_data->historic = true;
					switchpoint = tliSwitchPoint(private_data->tli, tles,
												 NULL);
					Assert(switchpoint >= private_data->read_upto);
					private_data->read_upto = switchpoint;
				}

				/* Go around and try again. */
			}
		}
		else
		{
			/* enough bytes available to satisfy the request */
			count = private_data->read_upto - targetPagePtr;
			break;
		}
	}

	/*
	 * Even though we just determined how much of the page can be validly read
	 * as 'count', read the whole page anyway. It's guaranteed to be
	 * zero-padded up to the page boundary if it's incomplete.
	 */
	if (!WALRead(state, cur_page, targetPagePtr, XLOG_BLCKSZ,
				 private_data->tli, &errinfo))
		WALReadRaiseError(&errinfo);

	/* number of valid bytes in the buffer */
	return count;
}
