/*-------------------------------------------------------------------------
 *
 * cm_pglz.c
 *	  pglz compression method
 *
 * Copyright (c) 2015-2018, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/compression/cm_pglz.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/cmapi.h"
#include "commands/defrem.h"
#include "common/pg_lzcompress.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"

/*
 * Configure PGLZ_Strategy struct for compression function
 */
static void *
pglz_cminitstate(Oid acoid)
{
	PGLZ_Strategy *strategy = palloc(sizeof(PGLZ_Strategy));

	/* initialize with default strategy values */
	memcpy(strategy, PGLZ_strategy_default, sizeof(PGLZ_Strategy));

	return (void *) strategy;
}

static struct varlena *
pglz_cmcompress(CompressionAmOptions *cmoptions, const struct varlena *value)
{
	int32		valsize,
				len;
	struct varlena *tmp = NULL;
	PGLZ_Strategy *strategy;

	valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));
	strategy = (PGLZ_Strategy *) cmoptions->acstate;

	Assert(strategy != NULL);
	if (valsize < strategy->min_input_size ||
		valsize > strategy->max_input_size)
		return NULL;

	tmp = (struct varlena *) palloc(PGLZ_MAX_OUTPUT(valsize) +
									VARHDRSZ_CUSTOM_COMPRESSED);
	len = pglz_compress(VARDATA_ANY(value),
						valsize,
						(char *) tmp + VARHDRSZ_CUSTOM_COMPRESSED,
						strategy);

	if (len >= 0)
	{
		SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_CUSTOM_COMPRESSED);
		return tmp;
	}

	pfree(tmp);
	return NULL;
}

static struct varlena *
pglz_cmdecompress(CompressionAmOptions *cmoptions, const struct varlena *value)
{
	struct varlena *result;
	int32		rawsize;

	Assert(VARATT_IS_CUSTOM_COMPRESSED(value));
	result = (struct varlena *) palloc(VARRAWSIZE_4B_C(value) + VARHDRSZ);

	rawsize = pglz_decompress((char *) value + VARHDRSZ_CUSTOM_COMPRESSED,
						VARSIZE(value) - VARHDRSZ_CUSTOM_COMPRESSED,
						VARDATA(result),
						VARRAWSIZE_4B_C(value), true);

	if (rawsize < 0)
		elog(ERROR, "pglz: compressed data is corrupted");

	SET_VARSIZE(result, rawsize + VARHDRSZ);
	return result;
}

static struct varlena *
pglz_cmdecompress_slice(CompressionAmOptions *cmoptions, const struct varlena *value,
							int32 slicelength)
{
	struct varlena *result;
	int32		rawsize;

	Assert(VARATT_IS_CUSTOM_COMPRESSED(value));
	result = (struct varlena *) palloc(VARRAWSIZE_4B_C(value) + VARHDRSZ);

	rawsize = pglz_decompress((char *) value + VARHDRSZ_CUSTOM_COMPRESSED,
						VARSIZE(value) - VARHDRSZ_CUSTOM_COMPRESSED,
						VARDATA(result),
                        slicelength, false);

    if (rawsize < 0)
		elog(ERROR, "pglz: compressed data is corrupted");

	SET_VARSIZE(result, rawsize + VARHDRSZ);
	return result;
}

/* pglz is the default compression method */
Datum
pglzhandler(PG_FUNCTION_ARGS)
{
	CompressionAmRoutine *routine = makeNode(CompressionAmRoutine);

	routine->cminitstate = pglz_cminitstate;
	routine->cmcompress = pglz_cmcompress;
	routine->cmdecompress = pglz_cmdecompress;
	routine->cmdecompress_slice = pglz_cmdecompress_slice;

	PG_RETURN_POINTER(routine);
}
