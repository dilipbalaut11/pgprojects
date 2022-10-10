/*-------------------------------------------------------------------------
 *
 * walsummary.h
 *	  WAL summary management
 *
 * Portions Copyright (c) 2010-2022, PostgreSQL Global Development Group
 *
 * src/include/backup/walsummary.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef WALSUMMARY_H
#define WALSUMMARY_H

#include <time.h>

#include "access/xlogdefs.h"
#include "nodes/pg_list.h"
#include "storage/fd.h"

typedef struct WalSummaryFile
{
	XLogRecPtr	start_lsn;
	XLogRecPtr	end_lsn;
	TimeLineID	tli;
} WalSummaryFile;

extern List *GetWalSummaries(TimeLineID tli, XLogRecPtr start_lsn,
							 XLogRecPtr end_lsn);
extern bool WalSummariesAreComplete(List *wslist, TimeLineID tli,
									XLogRecPtr start_lsn, XLogRecPtr end_lsn);
extern File OpenWalSummaryFile(WalSummaryFile *ws, bool missing_ok);
extern void RemoveWalSummaryIfOlderThan(WalSummaryFile *ws,
										time_t cutoff_time);

#endif							/* WALSUMMARY_H */
