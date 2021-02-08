/*-------------------------------------------------------------------------
 *
 * compressamapi.h
 *	  API for Postgres compression methods.
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * src/include/access/compressamapi.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef COMPRESSAMAPI_H
#define COMPRESSAMAPI_H

#include "postgres.h"

#include "fmgr.h"
#include "access/amapi.h"
#include "catalog/pg_am_d.h"
#include "nodes/nodes.h"


/*
 * Built-in compression method-id.  The toast compression header will store
 * this in the first 2 bits of the raw length.  These built-in compression
 * method-id are directly mapped to the built-in compression method oid.
 */
typedef enum CompressionId
{
	PGLZ_COMPRESSION_ID = 0,
	LZ4_COMPRESSION_ID = 1,
	/* one free slot for the future built-in method */
	CUSTOM_COMPRESSION_ID = 3
} CompressionId;

/* Use default compression method if it is not specified. */
#define DefaultCompressionOid	PGLZ_COMPRESSION_AM_OID
#define IsCustomCompression(cmid)     ((cmid) == CUSTOM_COMPRESSION_ID)
#define IsStorageCompressible(storage) ((storage) != TYPSTORAGE_PLAIN && \
										(storage) != TYPSTORAGE_EXTERNAL)
/* compression handler routines */
typedef struct varlena *(*cmcompress_function) (const struct varlena *value,
												int32 toast_header_size);
typedef struct varlena *(*cmdecompress_function) (const struct varlena *value,
												  int32 toast_header_size);
typedef struct varlena *(*cmdecompress_slice_function)
												(const struct varlena *value,
												 int32 toast_header_size,
												 int32 slicelength);

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

/*
 * GetCompressionAmRoutineByAmId - look up the handler of the compression access
 * method with the given OID, and get its CompressionAmRoutine struct.
 */
static inline CompressionAmRoutine *
GetCompressionAmRoutineByAmId(Oid amoid)
{
	regproc		amhandler;
	Datum		datum;
	CompressionAmRoutine *routine;

	/* Get handler function OID for the access method */
	amhandler = GetAmHandlerByAmId(amoid, AMTYPE_COMPRESSION, false);
	Assert(OidIsValid(amhandler));

	/* And finally, call the handler function to get the API struct */
	datum = OidFunctionCall0(amhandler);
	routine = (CompressionAmRoutine *) DatumGetPointer(datum);

	if (routine == NULL || !IsA(routine, CompressionAmRoutine))
		elog(ERROR, "compression access method handler function %u did not return an CompressionAmRoutine struct",
			 amhandler);

	return routine;
}

#endif							/* COMPRESSAMAPI_H */
