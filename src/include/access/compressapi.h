/*-------------------------------------------------------------------------
 *
 * compressapi.h
 *	  API for Postgres compression methods.
 *
 * Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * src/include/access/compressapi.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef COMPRESSAPI_H
#define COMPRESSAPI_H

#include "postgres.h"

/*
 * Built-in compression methods.  pg_attribute will store this in the
 * attcompression column.
 */
#define PGLZ_COMPRESSION			'p'
#define LZ4_COMPRESSION				'l'

#define InvalidCompressionMethod	'\0'
#define CompressionMethodIsValid(cm)  ((bool) ((cm) != InvalidCompressionMethod))

#define NO_LZ4_SUPPORT() \
	ereport(ERROR, \
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
			 errmsg("unsupported LZ4 compression method"), \
			 errdetail("This functionality requires the server to be built with lz4 support."), \
			 errhint("You need to rebuild PostgreSQL using --with-lz4.")))

/*
 * Built-in compression method-id.  The toast compression header will store
 * this in the first 2 bits of the raw length.  These built-in compression
 * method-id are directly mapped to the built-in compression methods.
 */
typedef enum CompressionId
{
	PGLZ_COMPRESSION_ID = 0,
	LZ4_COMPRESSION_ID = 1
} CompressionId;

/* use default compression method if it is not specified. */
#define DefaultCompressionMethod PGLZ_COMPRESSION
#define IsValidCompression(cm)  ((cm) != InvalidCompressionMethod)

#define IsStorageCompressible(storage) ((storage) != TYPSTORAGE_PLAIN && \
										(storage) != TYPSTORAGE_EXTERNAL)
/* compression handler routines */
typedef struct varlena *(*cmcompress_function) (const struct varlena *value);
typedef struct varlena *(*cmdecompress_function) (const struct varlena *value);
typedef struct varlena *(*cmdecompress_slice_function)
			(const struct varlena *value, int32 slicelength);

/*
 * API struct for a compression routines.
 *
 * 'cmname'	- name of the compression method
 * 'datum_compress' - varlena compression function.
 * 'datum_decompress' - varlena decompression function.
 * 'datum_decompress_slice' - varlena slice decompression functions.
 */
typedef struct CompressionRoutine
{
	char		cmname[64];
	cmcompress_function datum_compress;
	cmdecompress_function datum_decompress;
	cmdecompress_slice_function datum_decompress_slice;
} CompressionRoutine;

extern const CompressionRoutine pglz_compress_methods;
extern const CompressionRoutine lz4_compress_methods;

/*
 * CompressionMethodToId - Convert compression method to compression id.
 *
 * For more details refer comment atop CompressionId in compressapi.h
 */
static inline CompressionId
CompressionMethodToId(char method)
{
	switch (method)
	{
		case PGLZ_COMPRESSION:
			return PGLZ_COMPRESSION_ID;
		case LZ4_COMPRESSION:
			return LZ4_COMPRESSION_ID;
		default:
			elog(ERROR, "invalid compression method %c", method);
	}
}

/*
 * CompressionIdToMethod - Convert compression id to compression method
 *
 * For more details refer comment atop CompressionId in compressapi.h
 */
static inline Oid
CompressionIdToMethod(CompressionId cmid)
{
	switch (cmid)
	{
		case PGLZ_COMPRESSION_ID:
			return PGLZ_COMPRESSION;
		case LZ4_COMPRESSION_ID:
			return LZ4_COMPRESSION;
		default:
			elog(ERROR, "invalid compression method id %d", cmid);
	}
}

/*
 * CompressionNameToMethod - Get compression method from compression name
 *
 * Search in the available built-in methods.  If the compression not found
 * in the built-in methods then return InvalidCompressionMethod.
 */
static inline char
CompressionNameToMethod(char *compression)
{
	if (strcmp(pglz_compress_methods.cmname, compression) == 0)
		return PGLZ_COMPRESSION;
	else if (strcmp(lz4_compress_methods.cmname, compression) == 0)
	{
#ifndef HAVE_LIBLZ4
		NO_LZ4_SUPPORT();
#endif
		return LZ4_COMPRESSION;
	}

	return InvalidCompressionMethod;
}

/*
 * GetCompressionRoutines - Get compression handler routines
 */
static inline const CompressionRoutine*
GetCompressionRoutines(char method)
{
	switch (method)
	{
		case PGLZ_COMPRESSION:
			return &pglz_compress_methods;
		case LZ4_COMPRESSION:
			return &lz4_compress_methods;
		default:
			elog(ERROR, "invalid compression method %u", method);
	}
}

/*
 * GetCompressionMethodName - Get compression method name
 */
static inline const char*
GetCompressionMethodName(char method)
{
	return GetCompressionRoutines(method)->cmname;
}

#endif							/* COMPRESSAPI_H */
