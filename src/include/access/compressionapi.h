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

#include "catalog/pg_compression_d.h"
#include "nodes/nodes.h"

/*
 * Built-in compression method-id.  The toast compression header will store
 * this in the first 2 bits of the raw length.  These built-in compression
 * method-id are directly mapped to the built-in compression method oid.  And,
 * using that oid we can get the compression handler routine by fetching the
 * pg_compression catalog row.
 */
typedef enum CompressionId
{
	PGLZ_COMPRESSION_ID,
	LZ4_COMPRESSION_ID
} CompressionId;

/* Use default compression method if it is not specified. */
#define DefaultCompressionOid	PGLZ_COMPRESSION_OID

typedef struct CompressionRoutine CompressionRoutine;

/* compresion handler routines */
typedef struct varlena *(*cmcompress_function) (const struct varlena *value);
typedef struct varlena *(*cmdecompress_slice_function)
			(const struct varlena *value, int32 slicelength);

/*
 * API struct for a compression.
 *
 * 'cmcompress' and 'cmdecompress' - varlena compression functions.
 */
struct CompressionRoutine
{
	NodeTag type;

	/* name of the compression method */
	char		cmname[64];

	/* compression routine for the compression method */
	cmcompress_function cmcompress;

	/* decompression routine for the compression method */
	cmcompress_function cmdecompress;

	/* slice decompression routine for the compression method */
	cmdecompress_slice_function cmdecompress_slice;
};

/* access/compression/compresssionapi.c */
extern Oid GetCompressionOid(const char *compression);
extern char *GetCompressionNameFromOid(Oid cmoid);
extern CompressionRoutine *GetCompressionRoutine(Oid cmoid);
extern Oid GetCompressionOidFromCompressionId(CompressionId cmid);
extern CompressionId GetCompressionId(Oid cmoid);

#endif							/* COMPRESSIONAPI_H */
