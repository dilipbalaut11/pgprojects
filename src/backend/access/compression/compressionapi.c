/*-------------------------------------------------------------------------
 *
 * compression/compressionapi.c
 *	  Functions for compression methods
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/compression/compressionapi.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/compressionapi.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "catalog/pg_compression.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"

/*
 * GetCompressionOid - given an compression method name, look up its OID.
 */
Oid
GetCompressionOid(const char *cmname)
{
	HeapTuple tup;
	Oid oid = InvalidOid;

	tup = SearchSysCache1(CMNAME, CStringGetDatum(cmname));
	if (HeapTupleIsValid(tup))
	{
		Form_pg_compression cmform = (Form_pg_compression) GETSTRUCT(tup);

		oid = cmform->oid;
		ReleaseSysCache(tup);
	}

	return oid;
}

/*
 * GetCompressionNameFromOid - given an compression method oid, look up its
 * 							   name.
 */
char *
GetCompressionNameFromOid(Oid cmoid)
{
	HeapTuple tup;
	char *cmname = NULL;

	tup = SearchSysCache1(CMOID, ObjectIdGetDatum(cmoid));
	if (HeapTupleIsValid(tup))
	{
		Form_pg_compression cmform = (Form_pg_compression) GETSTRUCT(tup);

		cmname = pstrdup(NameStr(cmform->cmname));
		ReleaseSysCache(tup);
	}

	return cmname;
}

/*
 * GetCompressionRoutine - given an compression method oid, look up
 *						   its handler routines.
 */
CompressionRoutine *
GetCompressionRoutine(Oid cmoid)
{
	Datum datum;
	HeapTuple tup;
	CompressionRoutine *routine = NULL;

	if (!OidIsValid(cmoid))
		return NULL;

	tup = SearchSysCache1(CMOID, ObjectIdGetDatum(cmoid));
	if (HeapTupleIsValid(tup))
	{
		Form_pg_compression cmform = (Form_pg_compression) GETSTRUCT(tup);

		datum = OidFunctionCall0(cmform->cmhandler);
		routine = (CompressionRoutine *) DatumGetPointer(datum);

		if (routine == NULL || !IsA(routine, CompressionRoutine))
			elog(ERROR, "compression method handler function %u "
						"did not return a CompressionRoutine struct",
				 cmform->cmhandler);
		ReleaseSysCache(tup);
	}

	return routine;
}

/*
 * GetCompressionMethodFromCompressionId - Get the compression method oid from
 * 										   the compression id.
 */
Oid
GetCompressionOidFromCompressionId(CompressionId cmid)
{
	switch (cmid)
	{
		case PGLZ_COMPRESSION_ID:
			return PGLZ_COMPRESSION_OID;
		case LZ4_COMPRESSION_ID:
			return LZ4_COMPRESSION_OID;
		default:
			elog(ERROR, "Invalid compression method id %d", cmid);
	}
}

/*
 * GetCompressionId - Get the compression id from compression oid
 */
CompressionId
GetCompressionId(Oid cmoid)
{
	switch (cmoid)
	{
		case PGLZ_COMPRESSION_OID:
			return PGLZ_COMPRESSION_ID;
		case LZ4_COMPRESSION_OID:
			return LZ4_COMPRESSION_ID;
		default:
			elog(ERROR, "Invalid compression method %d", cmoid);
	}
}
