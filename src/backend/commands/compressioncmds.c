/*-------------------------------------------------------------------------
 *
 * compressioncmds.c
 *	  Routines for SQL commands for compression access methods
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/compressioncmds.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"

#include "access/cmapi.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_am.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_proc_d.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"

/*
 * Link compression with an attribute.
 *
 * When compression is not specified returns default attribute compression.
 * It is possible case for CREATE TABLE and ADD COLUMN commands
 * where COMPRESSION syntax is optional.
 */
Oid
GetAttributeCompression(Form_pg_attribute att, char *compression)
{
	/* No compression for PLAIN storage. */
	if (att->attstorage == TYPSTORAGE_PLAIN)
		return InvalidOid;

	/* Fallback to default compression if it's not specified */
	if (compression == NULL)
		return DefaultCompressionOid;

	return get_compression_am_oid(compression, false);
}

/*
 * Get compression name by attribute compression Oid.
 */
char *
GetCompressionName(Oid amoid)
{
	HeapTuple	tuple;
	Form_pg_am amform;
	char	  *amname;

	if (!OidIsValid(amoid))
		return NULL;

	/* Get handler function OID for the access method */
	tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(amoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for attribute compression %u", amoid);

	amform = (Form_pg_am) GETSTRUCT(tuple);
	amname = pstrdup(NameStr(amform->amname));

	ReleaseSysCache(tuple);

	return amname;
}
