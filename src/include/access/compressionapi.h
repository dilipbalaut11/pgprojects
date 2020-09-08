/*-------------------------------------------------------------------------
 *
 * compressionapi.h
 *	  API for Postgres compression methods.
 *
 * Copyright (c) 2015-2017, PostgreSQL Global Development Group
 *
 * src/include/access/compressionapi.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef COMPRESSIONAPI_H
#define COMPRESSIONAPI_H

#include "postgres.h"

/*
 * Built-in compression methods.  Pg_attribute will store this in the
 * attcompression column.
 */
#define PGLZ_COMPRESSION			'p'
#define ZLIB_COMPRESSION			'z'
#define InvalidCompressionMethod	'\0'

/*
 * Built-in compression method-id.  The toast header will store this
 * in the first 2 bits of the length.  Using this we can directly
 * access the compression method handler routine and those will be
 * used for the decompression.
 */
typedef enum CompressionId
{
	PGLZ_COMPRESSION_ID,
	ZLIB_COMPRESSION_ID
} CompressionId;

#define MAX_BUILTIN_COMPRESSION_METHOD ZLIB_COMPRESSION_ID + 1

#define DefaultCompressionMethod PGLZ_COMPRESSION
#define IsValidCompression(cm)  ((cm) != InvalidCompressionMethod)

typedef struct CompressionRoutine CompressionRoutine;

typedef struct varlena *(*cmcompress_function) (const struct varlena *value);
typedef struct varlena *(*cmdecompress_slice_function)
			(const struct varlena *value, int32 slicelength);

/*
 * API struct for a compression routine.
 *
 * 'cmcompress' and 'cmdecompress' - varlena compression functions.
 */
struct CompressionRoutine
{
	char		cmname[64];		/* Name of the compression method */
	cmcompress_function cmcompress;
	cmcompress_function cmdecompress;
	cmdecompress_slice_function cmdecompress_slice;
};

extern char GetCompressionMethod(char *compression);
extern char GetCompressionMethodFromCompressionId(CompressionId cmid);
extern CompressionId GetCompressionMethodId(char cm_method);
extern char *GetCompressionName(char cm_method);
extern CompressionRoutine *GetCompressionRoutine(CompressionId cmid);

/*
 * buit-in compression method handler function declaration
 * XXX we can add header files for pglz and zlib and move these
 * declarations to specific header files.
 */
extern struct varlena *pglz_cmcompress(const struct varlena *value);
extern struct varlena *pglz_cmdecompress(const struct varlena *value);
extern struct varlena *pglz_cmdecompress_slice(const struct varlena *value,
											   int32 slicelength);
extern struct varlena *zlib_cmcompress(const struct varlena *value);
extern struct varlena *zlib_cmdecompress(const struct varlena *value);
#endif							/* CMAPI_H */
