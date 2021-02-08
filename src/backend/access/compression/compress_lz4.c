/*-------------------------------------------------------------------------
 *
 * compress_lz4.c
 *		lz4 compression method.
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
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
#include "commands/defrem.h"
#include "fmgr.h"
#include "utils/builtins.h"

/*
 * Check options if specified. All validation is located here so
 * we don't need to do it again in cminitstate function.
 */
static void
lz4_cmcheck(List *options)
{
#ifndef HAVE_LIBLZ4
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("not built with lz4 support")));
#else
	ListCell	*lc;

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "acceleration") == 0)
		{
			int32 acceleration =
				pg_atoi(defGetString(def), sizeof(acceleration), 0);

			if (acceleration < INT32_MIN || acceleration > INT32_MAX)
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unexpected value for lz4 compression acceleration: \"%s\"",
								defGetString(def)),
					 errhint("expected value between INT32_MIN and INT32_MAX")
					));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_PARAMETER),
					 errmsg("unknown compression option for lz4: \"%s\"", def->defname)));
	}
#endif
}

static void *
lz4_cminitstate(List *options)
{
#ifndef HAVE_LIBLZ4
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("not built with lz4 support")));
#else
	int32	*acceleration = palloc(sizeof(int32));

	/* initialize with the default acceleration */
	*acceleration = 1;

	if (list_length(options) > 0)
	{
		ListCell	*lc;

		foreach(lc, options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "acceleration") == 0)
				*acceleration = pg_atoi(defGetString(def), sizeof(int32), 0);
		}
	}

	return acceleration;
#endif
}

/*
 * lz4_cmcompress - compression routine for lz4 compression method
 *
 * Compresses source into dest using the default strategy. Returns the
 * compressed varlena, or NULL if compression fails.
 */
static struct varlena *
lz4_cmcompress(const struct varlena *value, int32 header_size, void *options)
{
#ifndef HAVE_LIBLZ4
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("not built with lz4 support")));
#else
	int32		valsize;
	int32		len;
	int32		max_size;
	int32      *acceleration = (int32 *) options;
	struct varlena *tmp = NULL;

	valsize = VARSIZE_ANY_EXHDR(value);

	/*
	 * Get maximum size of the compressed data that lz4 compression may output
	 * and allocate the memory for holding the compressed data and the header.
	 */
	max_size = LZ4_compressBound(valsize);
	tmp = (struct varlena *) palloc(max_size + header_size);

	len = LZ4_compress_fast(VARDATA_ANY(value),
							(char *) tmp + header_size,
							valsize, max_size, *acceleration);
	if (len <= 0)
		elog(ERROR, "could not compress data with lz4");

	/* data is incompressible so just free the memory and return NULL */
	if (len > valsize)
	{
		pfree(tmp);
		return NULL;
	}

	SET_VARSIZE_COMPRESSED(tmp, len + header_size);

	return tmp;
#endif
}

/*
 * lz4_cmdecompress - decompression routine for lz4 compression method
 *
 * Returns the decompressed varlena.
 */
static struct varlena *
lz4_cmdecompress(const struct varlena *value, int32 header_size)
{
#ifndef HAVE_LIBLZ4
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("not built with lz4 support")));
#else
	int32		rawsize;
	struct varlena *result;

	/* allocate memory for holding the uncompressed data */
	result = (struct varlena *) palloc(VARRAWSIZE_4B_C(value) + VARHDRSZ);

	/* decompress data using lz4 routine */
	rawsize = LZ4_decompress_safe((char *) value + header_size,
								  VARDATA(result),
								  VARSIZE(value) - header_size,
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
lz4_cmdecompress_slice(const struct varlena *value, int32 header_size,
					  int32 slicelength)
{
#ifndef HAVE_LIBLZ4
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("not built with lz4 support")));
#else
	int32		rawsize;
	struct varlena *result;

	/* allocate memory for holding the uncompressed data */
	result = (struct varlena *) palloc(VARRAWSIZE_4B_C(value) + VARHDRSZ);

	/* decompress partial data using lz4 routine */
	rawsize = LZ4_decompress_safe_partial((char *) value + header_size,
										  VARDATA(result),
										  VARSIZE(value) - header_size,
										  slicelength,
										  VARRAWSIZE_4B_C(value));
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
	.datum_check = lz4_cmcheck,
	.datum_initstate = lz4_cminitstate,
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
