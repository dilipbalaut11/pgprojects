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

/* Built-in compression method routines */
static struct CompressionRoutine BuiltInCMRoutine[] =
{
	{"pglz", pglz_cmcompress, pglz_cmdecompress, pglz_cmdecompress_slice},
	{"zlib", zlib_cmcompress, zlib_cmdecompress, NULL}
};

/*
 * GetCompressionMethod - Get compression method from compression name
 *
 * Search the array of built-in compression method
 * and return the compression method.
 */
char
GetCompressionMethod(Form_pg_attribute att, char *compression)
{
	int i;

	/* No compression for PLAIN storage. */
	if (att->attstorage == TYPSTORAGE_PLAIN)
		return InvalidCompressionMethod;

	/* Fallback to default compression if it's not specified */
	if (compression == NULL)
		return DefaultCompressionMethod;

	/* Search in the built-in compression method array */
	for (i = 0; i < MAX_BUILTIN_COMPRESSION_METHOD; i++)
	{
		if (strcmp(BuiltInCMRoutine[i].cmname, compression) == 0)
			return compression[0];
	}

	return InvalidCompressionMethod;
}

/*
 * Get the compression method id.
 */
char
GetCompressionMethodFromCMID(PGCompressionID cmid)
{
	switch (cmid)
	{
		case PGLZ_COMPRESSION_ID:
			return PGLZ_COMPRESSION;
		case ZLIB_COMPRESSION_ID:
			return ZLIB_COMPRESSION;
		default:
			elog(ERROR, "Invalid compression method id %d", cmid);
	}
}

/*
 * Get the compression method id.
 */
PGCompressionID
GetCompressionMethodID(char cmethod)
{
	switch(cmethod)
	{
		case PGLZ_COMPRESSION:
			return PGLZ_COMPRESSION_ID;
		case ZLIB_COMPRESSION:
			return ZLIB_COMPRESSION_ID;
		default:
			elog(ERROR, "Invalid compression method %c", cmethod);
	}
}

/*
 * GetCompressionName - Get the compression method name.
 */
char *
GetCompressionName(char cmethod)
{
	if (!IsValidCompression(cmethod))
		return NULL;

	return BuiltInCMRoutine[GetCompressionMethodID(cmethod)].cmname;
}

/*
 * GetCompressionRoutine - Get the compression method handler routines.
 */
CompressionRoutine *
GetCompressionRoutine(PGCompressionID cmid)
{
	if (cmid >= MAX_BUILTIN_COMPRESSION_METHOD)
		elog(ERROR, "Invalid compression method %d", cmid);

	return &BuiltInCMRoutine[cmid];
}
