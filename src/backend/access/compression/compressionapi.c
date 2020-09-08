/*-------------------------------------------------------------------------
 *
 * compression/compressionapi.c
 *	  Functions for compression methods
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/compression/compressionapi.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/compressionapi.h"

/* Compression routines for the built-in compression methods. */
static struct CompressionRoutine BuiltInCompressionRoutines[] =
{
	{"pglz", pglz_cmcompress, pglz_cmdecompress, pglz_cmdecompress_slice},
	{"zlib", zlib_cmcompress, zlib_cmdecompress, NULL}
};

/*
 * GetCompressionMethod - Get compression method from compression name
 *
 * Search the array of built-in compression methods array and return the
 * compression method.  If the compression name is not found in the buil-in
 * compression array then return invalid compression method.
 */
char
GetCompressionMethod(char *compression)
{
	int			i;

	/* Search in the built-in compression method array */
	for (i = 0; i < MAX_BUILTIN_COMPRESSION_METHOD; i++)
	{
		if (strcmp(BuiltInCompressionRoutines[i].cmname, compression) == 0)
			return GetCompressionMethodFromCompressionId(i);
	}

	return InvalidCompressionMethod;
}

/*
 * GetCompressionMethodFromCompressionId - Get the compression method from id.
 *
 * Translate the compression id into the compression method.
 */
char
GetCompressionMethodFromCompressionId(CompressionId cmid)
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
 * GetCompressionMethodId - Translate the compression method id to compression
 * method.
 */
CompressionId
GetCompressionMethodId(char cm)
{
	switch (cm)
	{
		case PGLZ_COMPRESSION:
			return PGLZ_COMPRESSION_ID;
		case ZLIB_COMPRESSION:
			return ZLIB_COMPRESSION_ID;
		default:
			elog(ERROR, "Invalid compression method %c", cm);
	}
}

/*
 * GetCompressionName - Get the name compression method.
 */
char *
GetCompressionName(char cm)
{
	if (!IsValidCompression(cm))
		return NULL;

	return BuiltInCompressionRoutines[GetCompressionMethodId(cm)].cmname;
}

/*
 * GetCompressionRoutine - Get the compression method handler routines.
 */
CompressionRoutine *
GetCompressionRoutine(CompressionId cmid)
{
	if (cmid >= MAX_BUILTIN_COMPRESSION_METHOD)
		elog(ERROR, "Invalid compression method %d", cmid);

	return &BuiltInCompressionRoutines[cmid];
}
