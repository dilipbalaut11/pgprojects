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
	Datum		datum;
	HeapTuple	tup;
	CompressionRoutine *routine = NULL;

	Assert(OidIsValid(cmoid));

	/* Call the compression handler function to get the routines */
	switch(cmoid)
	{
		case PGLZ_COMPRESSION_OID:
			routine = (CompressionRoutine *)
						DatumGetPointer(OidFunctionCall0(F_PGLZHANDLER));

			break;
		case LZ4_COMPRESSION_OID:
			routine = (CompressionRoutine *)
						DatumGetPointer(OidFunctionCall0(F_LZ4HANDLER));
			break;
		default:
			elog(ERROR, "invalid compression Oid %u", cmoid);
	}

	return routine;
}

/*
 * CompressionOidtoId - Convert compression Oid to built-in compression id.
 *
 * For more details refer comment atop CompressionId in compressionapi.h
 */
Oid
CompressionOidToId(CompressionId cmid)
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
 * CompressionIdtoOid - Convert built-in compression id to Oid
 *
 * For more details refer comment atop CompressionId in compressionapi.h
 */
CompressionId
CompressionIdToOid(Oid cmoid)
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
