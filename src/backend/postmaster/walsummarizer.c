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

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "postmaster/walsummarizer.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/procsignal.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

/*
 * GUC parameters
 */
int			wal_summarize_mb = 256;
int			wal_summarize_keep_time = 7 * 24 * 60;

static void HandleWalSummarizerInterrupts(void);

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
		/* Process any signals received recently */
		HandleWalSummarizerInterrupts();

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 60 * 1000,
						 WAIT_EVENT_WAL_SUMMARIZER_MAIN);
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
