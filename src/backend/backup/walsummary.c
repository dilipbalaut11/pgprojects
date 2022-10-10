/*-------------------------------------------------------------------------
 *
 * walsummary.c
 *	  Functions for accessing and managing WAL summary data.
 *
 * Portions Copyright (c) 2010-2022, PostgreSQL Global Development Group
 *
 * src/backend/backup/walsummary.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlog_internal.h"
#include "backup/walsummary.h"

static bool IsWalSummaryFilename(char *filename);

/*
 * Get a list of WAL summaries.
 *
 * If tli != 0, only WAL summaries with the indicated TLI will be included.
 *
 * If start_lsn != InvalidXLogRecPtr, only summaries that end before the
 * indicated LSN will be included.
 *
 * If end_lsn != InvalidXLogRecPtr, only summaries that start before the
 * indicated LSN will be included.
 *
 * The intent is that you can call GetWalSummaries(tli, start_lsn, end_lsn)
 * to get all WAL summaries on the indicated timeline that overlap the
 * specified LSN range.
 */
List *
GetWalSummaries(TimeLineID tli, XLogRecPtr start_lsn, XLogRecPtr end_lsn)
{
	DIR *sdir;
	struct dirent *dent;
	List   *result = NIL;

	sdir = AllocateDir(XLOGDIR "/summaries");
	while ((dent = ReadDir(sdir, XLOGDIR "/summaries")) != NULL)
	{
		WalSummaryFile *ws;
		uint32	tmp[5];
		TimeLineID	file_tli;
		XLogRecPtr	file_start_lsn;
		XLogRecPtr	file_end_lsn;

		/* Decode filename, or skip if it's not in the expected format. */
		if (!IsWalSummaryFilename(dent->d_name))
			continue;
		sscanf(dent->d_name, "%08X%08X%08X%08X%08X",
			   &tmp[0], &tmp[1], &tmp[2], &tmp[3], &tmp[4]);
		file_tli = tmp[0];
		file_start_lsn = ((uint64) tmp[1]) << 32 | tmp[2];
		file_end_lsn = ((uint64) tmp[3]) << 32 | tmp[4];

		/* Skip if it doesn't match the filter criteria. */
		if (tli != 0 && tli != file_tli)
			continue;
		if (!XLogRecPtrIsInvalid(start_lsn) && start_lsn > file_end_lsn)
			continue;
		if (!XLogRecPtrIsInvalid(end_lsn) && end_lsn < file_start_lsn)
			continue;

		/* Add it to the list. */
		ws = palloc(sizeof(WalSummaryFile));
		ws->tli = file_tli;
		ws->start_lsn = file_start_lsn;
		ws->end_lsn = file_end_lsn;
		result = lappend(result, ws);
	}
	FreeDir(sdir);

	return result;
}

/*
 * XXX
 */
bool
WalSummariesAreComplete(List *wslist, TimeLineID tli, XLogRecPtr start_lsn,
						XLogRecPtr end_lsn)
{
	return false;
}

/*
 * Open a WAL summary file.
 *
 * This will throw an error in case of trouble. As an exception, if
 * missing_ok = true and the trouble is specifically that the file does
 * not exist, it will not throw an error and will return a value less than 0.
 */
File
OpenWalSummaryFile(WalSummaryFile *ws, bool missing_ok)
{
	char	path[MAXPGPATH];
	File	file;

	snprintf(path, MAXPGPATH,
			 XLOGDIR "/summaries/%08X%08X%08X%08X%08X.summary",
			 ws->tli,
			 LSN_FORMAT_ARGS(ws->start_lsn),
			 LSN_FORMAT_ARGS(ws->end_lsn));

	file = PathNameOpenFile(path, O_RDONLY);
	if (file < 0 && (errno != EEXIST || !missing_ok))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	return file;
}

/*
 * Test whether a filename looks like a WAL summary file.
 */
static bool
IsWalSummaryFilename(char *filename)
{
	return strspn(filename, "0123456789ABCDEF") == 40 &&
		strcmp(filename + 40, ".summary") == 0;
}
