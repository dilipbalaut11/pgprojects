/*-------------------------------------------------------------------------
 *
 * cm_lz4.c
 *		lz4 compression method.
 *
 * Portions Copyright (c) 2016-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1990-1993, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/cm_lz4/cm_lz4.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "postgres.h"
#include "access/compressionapi.h"
#include "access/toast_internals.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "utils/builtins.h"

#ifdef HAVE_LIBLZ4
#include "lz4.h"

/*
 * Check options if specified. All validation is located here so
 * we don't need do it again in cminitstate function.
 */
static void
lz4_cmcheck(List *options)
{
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
					 errmsg("unexpected parameter for lz4: \"%s\"", def->defname)));
	}
}

static void *
lz4_cminitstate(List *options)
{
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
	int32		valsize;
	int32		len;
	int32		max_size;
	int32	   *acceleration = (int32 *) options;
	struct varlena *tmp = NULL;

	valsize = VARSIZE_ANY_EXHDR(value);

	max_size = LZ4_compressBound(VARSIZE_ANY_EXHDR(value));
	tmp = (struct varlena *) palloc(max_size + header_size);

	len = LZ4_compress_fast(VARDATA_ANY(value),
							(char *) tmp + header_size,
							valsize, max_size, *acceleration);
	if (len <= 0)
	{
		pfree(tmp);
		elog(ERROR, "lz4: could not compress data");
	}

	SET_VARSIZE_COMPRESSED(tmp, len + header_size);
	return tmp;
}

/*
 * lz4_cmdecompress - decompression routine for lz4 compression method
 *
 * Returns the decompressed varlena.
 */
static struct varlena *
lz4_cmdecompress(const struct varlena *value, int32 header_size)
{
	int32		rawsize;
	struct varlena *result;

	result = (struct varlena *) palloc(TOAST_COMPRESS_RAWSIZE(value) + VARHDRSZ);
	SET_VARSIZE(result, TOAST_COMPRESS_RAWSIZE(value) + VARHDRSZ);

	rawsize = LZ4_decompress_safe((char *) value + header_size,
								  VARDATA(result),
								  VARSIZE(value) - header_size,
								  TOAST_COMPRESS_RAWSIZE(value));
	if (rawsize < 0)
		elog(ERROR, "lz4: compressed data is corrupted");

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
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
	int32		rawsize;
	struct varlena *result;

	result = (struct varlena *) palloc(slicelength + VARHDRSZ);
	SET_VARSIZE(result, TOAST_COMPRESS_RAWSIZE(value) + VARHDRSZ);

	rawsize = LZ4_decompress_safe_partial((char *) value + header_size,
										  VARDATA(result),
										  VARSIZE(value) - header_size,
										  slicelength,
										  slicelength);
	if (rawsize < 0)
		elog(ERROR, "lz4: compressed data is corrupted");

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
}
#endif

/* lz4 is the default compression method */
Datum
lz4handler(PG_FUNCTION_ARGS)
{
#ifndef HAVE_LIBLZ4
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("not built with lz4 support")));
#else
	CompressionRoutine *routine = makeNode(CompressionRoutine);

	routine->cmcheck = lz4_cmcheck;
	routine->cminitstate = lz4_cminitstate;
	routine->cmcompress = lz4_cmcompress;
	routine->cmdecompress = lz4_cmdecompress;
	routine->cmdecompress_slice = lz4_cmdecompress_slice;
	PG_RETURN_POINTER(routine);
#endif
}
