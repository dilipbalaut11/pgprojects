/*-------------------------------------------------------------------------
 *
 * compress_pglz.c
 *	  pglz compression method
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/compression/compress_pglz.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/compressamapi.h"
#include "common/pg_lzcompress.h"

#include "fmgr.h"
#include "utils/builtins.h"

/*
 * pglz_cmcompress - compression routine for pglz compression method
 *
 * Compresses source into dest using the default strategy. Returns the
 * compressed varlena, or NULL if compression fails.
 */
static struct varlena *
pglz_cmcompress(const struct varlena *value, int32 header_size)
{
	int32		valsize,
				len;
	struct varlena *tmp = NULL;

	valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));

	/*
	 * No point in wasting a palloc cycle if value size is outside the allowed
	 * range for compression.
	 */
	if (valsize < PGLZ_strategy_default->min_input_size ||
		valsize > PGLZ_strategy_default->max_input_size)
		return NULL;

	/*
	 * Get maximum size of the compressed data that pglz compression may output
	 * and allocate the memory for holding the compressed data and the header.
	 */
	tmp = (struct varlena *) palloc(PGLZ_MAX_OUTPUT(valsize) +
									header_size);

	len = pglz_compress(VARDATA_ANY(value),
						valsize,
						(char *) tmp + header_size,
						NULL);
	if (len < 0)
	{
		pfree(tmp);
		return NULL;
	}

	SET_VARSIZE_COMPRESSED(tmp, len + header_size);

	return tmp;
}

/*
 * pglz_cmdecompress - decompression routine for pglz compression method
 *
 * Returns the decompressed varlena.
 */
static struct varlena *
pglz_cmdecompress(const struct varlena *value, int32 header_size)
{
	struct varlena *result;
	int32		rawsize;

	result = (struct varlena *) palloc(VARRAWSIZE_4B_C(value) + VARHDRSZ);

	rawsize = pglz_decompress((char *) value + header_size,
							  VARSIZE(value) - header_size,
							  VARDATA(result),
							  VARRAWSIZE_4B_C(value), true);
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed pglz data is corrupt")));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
}

/*
 * pglz_decompress - slice decompression routine for pglz compression method
 *
 * Decompresses part of the data. Returns the decompressed varlena.
 */
static struct varlena *
pglz_cmdecompress_slice(const struct varlena *value, int32 header_size,
						int32 slicelength)
{
	struct varlena *result;
	int32		rawsize;

	result = (struct varlena *) palloc(slicelength + VARHDRSZ);

	rawsize = pglz_decompress((char *) value + header_size,
							  VARSIZE(value) - header_size,
							  VARDATA(result),
							  slicelength, false);
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed pglz data is corrupt")));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
}

/* ------------------------------------------------------------------------
 * Definition of the pglz compression access method.
 * ------------------------------------------------------------------------
 */
const CompressionAmRoutine pglz_compress_methods = {
	.type = T_CompressionAmRoutine,
	.datum_compress = pglz_cmcompress,
	.datum_decompress = pglz_cmdecompress,
	.datum_decompress_slice = pglz_cmdecompress_slice
};

/* pglz compression handler function */
Datum
pglzhandler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&pglz_compress_methods);
}
