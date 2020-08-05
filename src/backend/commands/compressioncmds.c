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
CreateAttributeCompression(Form_pg_attribute att,
						   ColumnCompression *compression)
{
	/* No compression for PLAIN storage. */
	if (att->attstorage == TYPSTORAGE_PLAIN)
		return InvalidOid;

	/* Fallback to default compression if it's not specified */
	if (compression == NULL)
		return DefaultCompressionOid;

	return get_compression_am_oid(compression->amname, false);
}

/*
 * Construct ColumnCompression node by attribute compression Oid.
 */
ColumnCompression *
MakeColumnCompression(Oid amoid)
{
	HeapTuple	tuple;
	Form_pg_am amform;
	ColumnCompression *node;

	if (!OidIsValid(amoid))
		return NULL;

	/* Get handler function OID for the access method */
	tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(amoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for attribute compression %u", amoid);

	amform = (Form_pg_am) GETSTRUCT(tuple);
	node = makeNode(ColumnCompression);
	node->amname = pstrdup(NameStr(amform->amname));

	ReleaseSysCache(tuple);

	return node;
}

/*
 * Compare compression options for two columns.
 */
void
CheckCompressionMismatch(ColumnCompression *c1, ColumnCompression *c2,
						 const char *attributeName)
{
	if (strcmp(c1->amname, c2->amname))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("column \"%s\" has a compression method conflict",
						attributeName),
				 errdetail("%s versus %s", c1->amname, c2->amname)));
}

