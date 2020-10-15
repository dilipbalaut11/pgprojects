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
 * Built-in compression method-id.  The toast header will store this
 * in the first 2 bits of the length.  Using this we can directly
 * access the compression method handler routine and those will be
 * used for the decompression.
 */
typedef enum CompressionId
{
	PGLZ_COMPRESSION_ID = 0,
	ZLIB_COMPRESSION_ID = 1,
	CUSTOME_COMPRESSION_ID = 3
} CompressionId;

#define DefaultCompressionOid PGLZ_COMPRESSION_OID
#define IsCustomeCompressionID(cmid) ((cmid) == CUSTOME_COMPRESSION_ID)

typedef struct CompressionRoutine CompressionRoutine;

typedef struct varlena *(*cmcompress_function) (const struct varlena *value,
												int32 toast_header_size);
typedef struct varlena *(*cmdecompress_slice_function)
						(const struct varlena *value,
						int32 toast_header_size,
						int32 slicelength);

/*
 * API struct for a compression routine.
 *
 * 'cmcompress' and 'cmdecompress' - varlena compression functions.
 */
struct CompressionRoutine
{
	NodeTag type;

	char		cmname[64];		/* Name of the compression method */
	cmcompress_function cmcompress;
	cmcompress_function cmdecompress;
	cmdecompress_slice_function cmdecompress_slice;
};

extern Oid GetCompressionOidFromCompressionId(CompressionId cmid);
extern CompressionId GetCompressionId(Oid cmoid);
extern char *GetCompressionNameFromOid(Oid cmoid);
extern CompressionRoutine *GetCompressionRoutine(Oid cmoid);
extern Oid GetCompressionOid(const char *compression);

#endif							/* CMAPI_H */
