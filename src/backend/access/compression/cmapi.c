/*-------------------------------------------------------------------------
 *
 * compression/cmapi.c
 *	  Functions for compression access methods
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/compression/cmapi.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "access/cmapi.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
#include "utils/syscache.h"

/*
 * InvokeCompressionAmHandler - call the specified access method handler routine to get
 * its CompressionAmRoutine struct, which will be palloc'd in the caller's context.
 */
CompressionAmRoutine *
InvokeCompressionAmHandler(Oid amhandler)
{
	Datum		datum;
	CompressionAmRoutine *routine;

	datum = OidFunctionCall0(amhandler);
	routine = (CompressionAmRoutine *) DatumGetPointer(datum);

	if (routine == NULL || !IsA(routine, CompressionAmRoutine))
		elog(ERROR, "compression method handler function %u "
			 "did not return an CompressionAmRoutine struct",
			 amhandler);

	return routine;
}
