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

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "postmaster/walsummarizer.h"
#include "replication/walreceiver.h"
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
 * Between activity cycles, we sleep for a time that is a multiple of
 * SLEEP_QUANTUM_DURATION, which is measured in milliseconds. The multiplier
 * can be anywhere between MIN_SLEEP_QUANTA and MAX_SLEEP_QUANTA depending on
 * how busy the system is.
 */
#define SLEEP_QUANTUM_DURATION	5000
#define MIN_SLEEP_QUANTA		1
#define MAX_SLEEP_QUANTA		12

/*
 * Private data for this module.
 */
static WalSummarizerData * WalSummarizerCtl;
static long sleep_quanta = MIN_SLEEP_QUANTA;

/*
 * GUC parameters
 */
int			wal_summarize_mb = 256;
int			wal_summarize_keep_time = 7 * 24 * 60;

static void ConsiderSummarizingWAL(void);
static void HandleWalSummarizerInterrupts(void);
static XLogRecPtr SummarizeWAL(XLogRecPtr start_lsn, XLogRecPtr cutoff_lsn,
							   bool exact);

Size
WalSummarizerShmemSize(void)
{
	return sizeof(WalSummarizerData);
}

void
WalSummarizerShmemInit(void)
{
	bool		found;

	WalSummarizerCtl = (WalSummarizerData *)
		ShmemInitStruct("Wal Summarizer Ctl", WalSummarizerShmemSize(),
						&found);

	if (!found)
	{
		/* First time through, so initialize */
		WalSummarizerCtl->initialized = false;
		WalSummarizerCtl->summarizer_tli = 0;
		WalSummarizerCtl->summarizer_lsn = InvalidXLogRecPtr;
		WalSummarizerCtl->lsn_is_exact = false;
	}
}

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

XLogRecPtr
GetOldestUnsummarizedLSN(TimeLineID *tli, bool *lsn_is_exact)
{
	XLogRecPtr	latest_lsn;
	TimeLineID	latest_tli;
	LWLockMode	mode = LW_SHARED;
	XLogSegNo	oldest_segno;
	XLogRecPtr	unsummarized_lsn;
	TimeLineID	unsummarized_tli;

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
	 * XXX. We should really start with the oldest TLI that isn't yet fully
	 * summarized, and the oldest LSN on that timeline that is not yet fully
	 * summarized. But the code for that is not written yet, so punt.
	 */
	oldest_segno = XLogGetOldestSegno(latest_tli);
	XLogSegNoOffsetToRecPtr(oldest_segno, 0, wal_segment_size,
							unsummarized_lsn);
	unsummarized_tli = latest_tli;

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
	XLogRecPtr	end_of_summary_lsn;
	int			summaries_produced = 0;

	/* Fetch information about previous progress from shared memory. */
	previous_lsn = GetOldestUnsummarizedLSN(&previous_tli, &exact);

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
		elog(LOG,	/* XXX reduce log level */
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
		 */
		if (latest_lsn < cutoff_lsn + 6 * XLOG_BLCKSZ)
			break;

		/* XXX clean up some things here */
		elog(LOG,
			 "summarizing from %X/%X with cutoff %X/%X",
			 LSN_FORMAT_ARGS(previous_lsn), LSN_FORMAT_ARGS(cutoff_lsn));
		end_of_summary_lsn = SummarizeWAL(previous_lsn, cutoff_lsn, exact);
		elog(LOG, "summary ended at %X/%X",
			 LSN_FORMAT_ARGS(end_of_summary_lsn));
		++summaries_produced;

		/* Update state in shared memory. */
		LWLockAcquire(WALSummarizerLock, LW_EXCLUSIVE);
		WalSummarizerCtl->summarizer_lsn = end_of_summary_lsn;
		WalSummarizerCtl->summarizer_tli = latest_tli;	/* XXX */
		WalSummarizerCtl->lsn_is_exact = exact = true;
		LWLockRelease(WALSummarizerLock);

		/* Update state for next loop iteration. */
		previous_lsn = end_of_summary_lsn;
		previous_tli = latest_tli;						/* XXX */
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
 * Interrupt handler for main loops of WAL writer process.
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
SummarizeWAL(XLogRecPtr start_lsn, XLogRecPtr cutoff_lsn, bool exact)
{
	ReadLocalXLogPageNoWaitPrivate *private_data;
	XLogReaderState *xlogreader;
	XLogRecPtr	first_record_lsn;
	XLogRecPtr	result_lsn;

	private_data = (ReadLocalXLogPageNoWaitPrivate *)
		palloc0(sizeof(ReadLocalXLogPageNoWaitPrivate));

	/* XXX the TLI has to matter here */
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
	result_lsn = first_record_lsn;

	while (true)
	{
		char *errormsg;
		XLogRecord *record = XLogReadRecord(xlogreader, &errormsg);

		if (record == NULL)
		{
			ReadLocalXLogPageNoWaitPrivate *private_data;

			private_data = (ReadLocalXLogPageNoWaitPrivate *)
				xlogreader->private_data;
			if (private_data->end_of_wal)
			{
				elog(LOG, "too soon"); /* XXX */
				break;
			}
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

		if (xlogreader->EndRecPtr > cutoff_lsn)
		{
			result_lsn = xlogreader->EndRecPtr;
			break;
		}
	}

	pfree(xlogreader->private_data);
	XLogReaderFree(xlogreader);

	return result_lsn;
}