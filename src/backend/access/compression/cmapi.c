/*-------------------------------------------------------------------------
 *
 * compression/cmapi.c
 *	  Functions for compression access methods
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/compression/cmapi.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "access/cmapi.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
#include "utils/syscache.h"

static struct CompressionRoutine BuiltInCMRoutine[] =
	{
		{"pglz",
		 pglz_cmcompress,
		 pglz_cmdecompress,
		 pglz_cmdecompress_slice},
		{"zlib",
		 zlib_cmcompress,
		 zlib_cmdecompress,
		 NULL}
	};

/*
 * GetCompressionMethod - Search the array of built-in compression method
 * and return the compression id.
 */
int32
GetCompressionMethod(char *cmname)
{
	int i;

	for (i = 0; i < MAX_BUILTIN_COMPRESSION_METHOD; i++)
	{
		if (strcmp(BuiltInCMRoutine[i].cmname, cmname) == 0)
			return i;
	}

	return 0;
}

char *
GetCompressionName(int32 cmid)
{
	if (cmid >= MAX_BUILTIN_COMPRESSION_METHOD)
		elog(ERROR, "Invalid compression method %d", cmid);
	return BuiltInCMRoutine[cmid].cmname;
}

/*
 * GetCompressionRoutine - Get the compression method handler routines.
 */
CompressionRoutine *
GetCompressionRoutine(int32 cmid)
{
	if (cmid >= MAX_BUILTIN_COMPRESSION_METHOD)
		elog(ERROR, "Invalid compression method %d", cmid);

	return &BuiltInCMRoutine[cmid];
}
