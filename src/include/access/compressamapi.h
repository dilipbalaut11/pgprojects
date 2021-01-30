/*-------------------------------------------------------------------------
 *
 * compressamapi.h
 *	  API for Postgres compression methods.
 *
 * Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * src/include/access/compressamapi.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef COMPRESSAMAPI_H
#define COMPRESSAMAPI_H

#include "postgres.h"

#include "catalog/pg_am_d.h"
#include "nodes/nodes.h"
#include "utils/guc.h"

/*
 * Built-in compression method-id.  The toast compression header will store
 * this in the first 2 bits of the raw length.  These built-in compression
 * method-id are directly mapped to the built-in compression method oid.
 */
typedef enum CompressionId
{
	PGLZ_COMPRESSION_ID = 0,
	LZ4_COMPRESSION_ID = 1
} CompressionId;

/* Default compression method if not specified. */
#define DEFAULT_TOAST_COMPRESSION "pglz"
#define DefaultCompressionOid	PGLZ_COMPRESSION_AM_OID

/* GUC */
extern char	*default_toast_compression;
extern void assign_default_toast_compression(const char *newval, void *extra);
extern bool check_default_toast_compression(char **newval, void **extra, GucSource source);

extern Oid GetDefaultToastCompression(void);

#define IsStorageCompressible(storage) ((storage) != TYPSTORAGE_PLAIN && \
										(storage) != TYPSTORAGE_EXTERNAL)
/* compression handler routines */
typedef struct varlena *(*cmcompress_function) (const struct varlena *value);
typedef struct varlena *(*cmdecompress_function) (const struct varlena *value);
typedef struct varlena *(*cmdecompress_slice_function)
			(const struct varlena *value, int32 slicelength);

/*
 * API struct for a compression AM.
 *
 * 'datum_compress' - varlena compression function.
 * 'datum_decompress' - varlena decompression function.
 * 'datum_decompress_slice' - varlena slice decompression functions.
 */
typedef struct CompressionAmRoutine
{
	NodeTag		type;

	cmcompress_function datum_compress;
	cmdecompress_function datum_decompress;
	cmdecompress_slice_function datum_decompress_slice;
} CompressionAmRoutine;

extern const CompressionAmRoutine pglz_compress_methods;
extern const CompressionAmRoutine lz4_compress_methods;

/*
 * CompressionOidToId - Convert compression Oid to built-in compression id.
 *
 * For more details refer comment atop CompressionId in compressamapi.h
 */
static inline CompressionId
CompressionOidToId(Oid cmoid)
{
	switch (cmoid)
	{
		case PGLZ_COMPRESSION_AM_OID:
			return PGLZ_COMPRESSION_ID;
		case LZ4_COMPRESSION_AM_OID:
			return LZ4_COMPRESSION_ID;
		default:
			elog(ERROR, "invalid compression method oid %u", cmoid);
	}
}

/*
 * CompressionIdToOid - Convert built-in compression id to Oid
 *
 * For more details refer comment atop CompressionId in compressamapi.h
 */
static inline Oid
CompressionIdToOid(CompressionId cmid)
{
	switch (cmid)
	{
		case PGLZ_COMPRESSION_ID:
			return PGLZ_COMPRESSION_AM_OID;
		case LZ4_COMPRESSION_ID:
			return LZ4_COMPRESSION_AM_OID;
		default:
			elog(ERROR, "invalid compression method id %d", cmid);
	}
}

#endif							/* COMPRESSAMAPI_H */
