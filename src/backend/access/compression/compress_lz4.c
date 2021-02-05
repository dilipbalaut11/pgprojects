/*-------------------------------------------------------------------------
 *
 * compress_lz4.c
 *		lz4 compression method.
 *
 * Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/compression/compress_lz4.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef HAVE_LIBLZ4
#include <lz4.h>
#endif

#include "access/compressamapi.h"
#include "fmgr.h"
#include "utils/builtins.h"

/*
 * lz4_cmcompress - compression routine for lz4 compression method
 *
 * Compresses source into dest using the LZ4 defaults. Returns the
 * compressed varlena, or NULL if compression fails.
 */
static struct varlena *
lz4_cmcompress(const struct varlena *value)
{
#ifndef HAVE_LIBLZ4
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("not built with lz4 support")));
#else
	int32		valsize;
	int32		len;
	int32		max_size;
	struct varlena *tmp = NULL;

	valsize = VARSIZE_ANY_EXHDR(value);

	/*
	 * Figure out the maximum possible size of the LZ4 output, add the bytes
	 * that will be needed for varlena overhead, and allocate that amount.
	 */
	max_size = LZ4_compressBound(valsize);
	tmp = (struct varlena *) palloc(max_size + VARHDRSZ_COMPRESS);

	len = LZ4_compress_default(VARDATA_ANY(value),
							   (char *) tmp + VARHDRSZ_COMPRESS,
							   valsize, max_size);
	if (len <= 0)
		elog(ERROR, "could not compress data with lz4");

	/* data is incompressible so just free the memory and return NULL */
	if (len > valsize)
	{
		pfree(tmp);
		return NULL;
	}

	SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_COMPRESS);

	return tmp;
#endif
}

/*
 * lz4_cmdecompress - decompression routine for lz4 compression method
 *
 * Returns the decompressed varlena.
 */
static struct varlena *
lz4_cmdecompress(const struct varlena *value)
{
#ifndef HAVE_LIBLZ4
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("not built with lz4 support")));
#else
	int32		rawsize;
	struct varlena *result;

	/* allocate memory for the uncompressed data */
	result = (struct varlena *) palloc(VARRAWSIZE_4B_C(value) + VARHDRSZ);

	/* decompress the data */
	rawsize = LZ4_decompress_safe((char *) value + VARHDRSZ_COMPRESS,
								  VARDATA(result),
								  VARSIZE(value) - VARHDRSZ_COMPRESS,
								  VARRAWSIZE_4B_C(value));
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed lz4 data is corrupt")));


	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
#endif
}

/*
 * lz4_cmdecompress_slice - slice decompression routine for lz4 compression
 *
 * Decompresses part of the data. Returns the decompressed varlena.
 */
static struct varlena *
lz4_cmdecompress_slice(const struct varlena *value, int32 slicelength)
{
#ifndef HAVE_LIBLZ4
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("not built with lz4 support")));
#elif LZ4_VERSION_NUMBER < 10803
	return lz4_cmdecompress(value);
#else
	int32		rawsize;
	struct varlena *result;

	/* allocate memory for the uncompressed data */
	result = (struct varlena *) palloc(slicelength + VARHDRSZ);

	/* decompress the data */
	rawsize = LZ4_decompress_safe_partial((char *) value + VARHDRSZ_COMPRESS,
										  VARDATA(result),
										  VARSIZE(value) - VARHDRSZ_COMPRESS,
										  slicelength,
										  slicelength);
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed lz4 data is corrupt")));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
#endif
}

/* ------------------------------------------------------------------------
 * Definition of the lz4 compression access method.
 * ------------------------------------------------------------------------
 */
const CompressionAmRoutine lz4_compress_methods = {
	.type = T_CompressionAmRoutine,
	.datum_compress = lz4_cmcompress,
	.datum_decompress = lz4_cmdecompress,
	.datum_decompress_slice = lz4_cmdecompress_slice
};

/* lz4 compression handler function */
Datum
lz4handler(PG_FUNCTION_ARGS)
{
#ifndef HAVE_LIBLZ4
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("not built with lz4 support")));
#else
	PG_RETURN_POINTER(&lz4_compress_methods);
#endif
}
