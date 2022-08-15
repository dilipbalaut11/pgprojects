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
#include "access/xlogutils.h"
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

static void ConsiderSummarizingWAL(void);
static void HandleWalSummarizerInterrupts(void);
static XLogRecPtr SummarizeWAL(TimeLineID tli, XLogRecPtr start_lsn,
							   XLogRecPtr cutoff_lsn, XLogRecPtr current_lsn,
							   bool exact);
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

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	ereport(DEBUG1,
			(errmsg_internal("WAL summarizer started")));

	/* XXX error handing */

	/*
	 * Loop forever
	 */
	for (;;)
	{
		/* Process any signals received recently. */
		HandleWalSummarizerInterrupts();

		/* Perhaps do some real work. */
		ConsiderSummarizingWAL();

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
	 * The first step is to figure out the current LSN and timeline. Since WAL
	 * should only be summarized once it's been flushed to disk, the latest LSN
	 * for our purposes means whatever was most recently flushed to disk.
	 */
	if (RecoveryInProgress())
		latest_lsn = GetWalRcvFlushRecPtr(NULL, &latest_tli);
	else
		latest_lsn = GetFlushRecPtr(&latest_tli);

	/*
	 * Find the oldest timeline on which WAL still exists, and the earliest
	 * segment for which it exists.
	 *
	 * XXX. We should skip timelines or segments that are already summarized,
	 * unless we instead choose to handle that elsewhere.
	 */
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
ConsiderSummarizingWAL(void)
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

	/*
	 * WAL should only only be summarized once it's been flushed to disk, so
	 * for our purposes here, the latest LSN for our purposes means whatever
	 * was most recently flushed to disk.
	 */
	if (RecoveryInProgress())
		latest_lsn = GetWalRcvFlushRecPtr(NULL, &latest_tli);
	else
		latest_lsn = GetFlushRecPtr(&latest_tli);

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

		/*
		 * Figure out where we want to stop the next WAL summary. We'll
		 * summarize until we read a record that ends after the cutoff_lsn
		 * computed below, so the actual range of LSNs covered by the summary
		 * will typically be a little bit greater than the cutoff (and could
		 * be far greater if the last WAL record is really big).
		 *
		 * If we just established the cutoff LSN by adding bytes_per_summary
		 * to previous_lsn, each file would cover on average a little more than
		 * the configured number of bytes. That wouldn't be a disaster, but we
		 * prefer to make the average number of bytes per summary equal to the
		 * configured value. To accomplish that, we make the cutoff LSNs
		 * multiples of bytes_per_summary. That way, the target number of bytes
		 * for each cycle
		 * is reduced by the overrun from the previous cycle.
		 *
		 * A further advantage of this is that the number of bytes per summary
		 * will often be a multiple of the WAL segment size, so we'll tend to
		 * align summaries with the ends of segments.
		 *
		 * To avoid emitting really small summary files, if this algorithm
		 * would produce a summary file covering less than one-fifth of the
		 * target amount of WAL, bump the cutoff LSN to the next multiple of
		 * bytes_per_summary. This should normally only happen when first
		 * starting WAL summarization, but could also occur if the last summary
		 * ended with a very large record.
		 */
		cutoff_lsn =
			((previous_lsn / bytes_per_summary) + 1) * bytes_per_summary;
		if (cutoff_lsn - previous_lsn < bytes_per_summary / 5)
			cutoff_lsn += bytes_per_summary;
		if (!XLogRecPtrIsInvalid(switch_lsn) && cutoff_lsn > switch_lsn)
			cutoff_lsn = switch_lsn;
		elog(DEBUG2,
			 "WAL summarization cutoff is TLI %d @ %X/%X, flush position is %X/%X",
			 latest_tli, LSN_FORMAT_ARGS(cutoff_lsn), LSN_FORMAT_ARGS(latest_lsn));

		/*
		 * If we've past the cutoff LSN, then we have all of the WAL that we
		 * want to include in the next summary, except possibly for the last
		 * record, which might still be incomplete. If we fail to read the
		 * entire last record, we'll have to retry the whole summarization
		 * process. While that shouldn't break anything, it's a waste of
		 * resources -- so wait until the latest LSN values is at least 6 XLOG
		 * blocks past the cutoff before starting summarization. Most WAL
		 * records are smaller than that.
		 *
		 * If we're summarizing a historic timeline, no more WAL is going to
		 * be generated on this timeline ever, so we can go ahead and summarize
		 * it right now.
		 */
		if (latest_lsn < cutoff_lsn + 6 * XLOG_BLCKSZ &&
			previous_tli == latest_tli)
			break;

		/* Summarize WAL. */
		end_of_summary_lsn = SummarizeWAL(previous_tli, previous_lsn,
										  cutoff_lsn, latest_lsn, exact);
		if (XLogRecPtrIsInvalid(end_of_summary_lsn))
		{
			/*
			 * If we discover that the last record that was supposed to be
			 * part of this summary extends past the flush position or just
			 * doesn't exist on disk at all, we'll have to retry creating
			 * this summary later.
			 */
			ereport(LOG,	/* XXX reduce log level */
					errmsg("can't yet summarize WAL on TLI %d starting at %X/%X with cutoff %X/%X",
						   previous_tli,
						   LSN_FORMAT_ARGS(previous_lsn),
						   LSN_FORMAT_ARGS(cutoff_lsn)));
			break;
		}

		/* We have sucessfully produced a summary. */
		ereport(LOG,
				errmsg("summarized WAL on TLI %d from %X/%X to %X/%X",
					   previous_tli,
					   LSN_FORMAT_ARGS(previous_lsn),
					   LSN_FORMAT_ARGS(end_of_summary_lsn)));
		++summaries_produced;

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
 * Interrupt handler for main loop of WAL writer process.
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

/*
 * Summarize WAL on the timeline given by tli, starting at start_lsn and
 * stopping when the LSN of the next record would be greater than or equal to
 * cutoff_lsn.
 *
 * If exact = true, start_lsn is an exact value; if exact = false, search
 * forward for the first WAL record beginning after start_lsn.
 *
 * It's possible that the last record extends far beyond cutoff_lsn; if so,
 * it might not have been fully written and flushed yet. If we're unable to
 * read enough WAL or the last record extends beyond maximum_lsn, we'll need
 * retry later after more WAL is safely on disk. In this case, returns
 * InvalidXLogRecPtr; else, returns the ending LSN of the last record in the
 * new summary.
 */
static XLogRecPtr
SummarizeWAL(TimeLineID tli, XLogRecPtr start_lsn, XLogRecPtr cutoff_lsn,
			 XLogRecPtr maximum_lsn, bool exact)
{
	SummarizerReadLocalXLogPrivate *private_data;
	XLogReaderState *xlogreader;
	XLogRecPtr	first_record_lsn;
	XLogRecPtr	result_lsn = InvalidXLogRecPtr;

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
	 * Normally, we know the exact LSN from which we wish to start reading
	 * WAL, but sometimes all we know is that we want to start summarizing from
	 * the first complete record in a certain WAL file. In that case, we have
	 * to search forward from the beginning of the file for where the
	 * record actually starts.
	 */
	if (exact)
	{
		first_record_lsn = start_lsn;
		XLogBeginRead(xlogreader, first_record_lsn);
	}
	else
	{
		first_record_lsn = XLogFindNextRecord(xlogreader, start_lsn);
		if (XLogRecPtrIsInvalid(first_record_lsn))
			ereport(ERROR,
					(errmsg("could not find a valid record after %X/%X",
							LSN_FORMAT_ARGS(start_lsn))));
	}

	/*
	 * Main loop: read xlog records one by one.
	 */
	while (true)
	{
		char *errormsg;
		XLogRecord *record = XLogReadRecord(xlogreader, &errormsg);

		if (record == NULL)
		{
			SummarizerReadLocalXLogPrivate *private_data;

			private_data = (SummarizerReadLocalXLogPrivate *)
				xlogreader->private_data;
			if (private_data->end_of_wal)
				break;
			if (errormsg)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read WAL at %X/%X: %s",
						 LSN_FORMAT_ARGS(first_record_lsn), errormsg)));
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read WAL at %X/%X",
						 LSN_FORMAT_ARGS(first_record_lsn))));
		}

		if (xlogreader->EndRecPtr >= cutoff_lsn)
		{
			result_lsn = xlogreader->EndRecPtr;
			break;
		}
	}

	/* Destroy xlogreader. */
	pfree(xlogreader->private_data);
	XLogReaderFree(xlogreader);

	/*
	 * If we were able to read all the necessary WAL, write out a summary file.
	 */
	if (!XLogRecPtrIsInvalid(result_lsn))
	{
		char		temp_path[MAXPGPATH];
		char		final_path[MAXPGPATH];
		File		file;

		/* Generate temporary and final path name. */
		snprintf(temp_path, MAXPGPATH,
				 XLOGDIR "/summaries/temp.summary");
		snprintf(final_path, MAXPGPATH,
				 XLOGDIR "/summaries/%08X%08X%08X%08X%08X.summary",
				 tli,
				 LSN_FORMAT_ARGS(first_record_lsn),
				 LSN_FORMAT_ARGS(result_lsn));

		/* Open the temporary file for writing. */
		file = PathNameOpenFile(temp_path, O_WRONLY | O_CREAT | O_TRUNC);
		if (file <= 0)
			ereport(ERROR,
					 (errcode_for_file_access(),
					  errmsg("could not create file \"%s\": %m", temp_path)));

		/* XXX write actual data to temporary file */

		/* Close temporary file. */
		FileClose(file);

		/* Durably rename it into place. */
		durable_rename(temp_path, final_path, ERROR);
	}

	return result_lsn;
}

/*
 * Similar to read_local_xlog_page, but much simpler because the caller is
 * required to specify the TLI and the highest LSN that is safe to read.
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

	if (targetPagePtr + XLOG_BLCKSZ <= private_data->read_upto)
	{
		/*
		 * more than one block available; read only that block, have caller
		 * come back if they need more.
		 */
		count = XLOG_BLCKSZ;
	}
	else if (targetPagePtr + reqLen > private_data->read_upto)
	{
		/* not enough data there */
		private_data->end_of_wal = true;
		return -1;
	}
	else
	{
		/* enough bytes available to satisfy the request */
		count = private_data->read_upto - targetPagePtr;
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
