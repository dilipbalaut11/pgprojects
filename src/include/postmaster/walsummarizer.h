#ifndef WALSUMMARIZER_H
#define WALSUMMARIZER_H

#include "access/xlogdefs.h"

extern int wal_summarize_mb;
extern int wal_summarize_keep_time;

extern Size WalSummarizerShmemSize(void);
extern void WalSummarizerShmemInit(void);
extern void WalSummarizerMain(void) pg_attribute_noreturn();

extern XLogRecPtr GetOldestUnsummarizedLSN(TimeLineID *tli, bool *lsn_is_exact);

#endif
