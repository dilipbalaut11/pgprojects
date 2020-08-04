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
#include "catalog/pg_attr_compression_d.h"
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

#if 0
/*
 * Return list of compression methods used in specified column.
 */
Datum
pg_column_compression(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);
	char	   *attname = TextDatumGetCString(PG_GETARG_TEXT_P(1));
	Relation	rel;
	HeapTuple	tuple;
	AttrNumber	attnum;
	List	   *amoids = NIL;
	Oid			amoid;
	ListCell   *lc;

	ScanKeyData key[2];
	SysScanDesc scan;
	StringInfoData result;

	attnum = get_attnum(relOid, attname);
	if (attnum == InvalidAttrNumber)
		PG_RETURN_NULL();

	/* Collect related builtin compression access methods */
	lookup_builtin_dependencies(relOid, attnum, &amoids);

	/* Collect other related access methods */
	rel = table_open(AttrCompressionRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_attr_compression_acrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relOid));
	ScanKeyInit(&key[1],
				Anum_pg_attr_compression_acattnum,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(attnum));

	scan = systable_beginscan(rel, AttrCompressionRelidAttnumIndexId,
							  true, NULL, 2, key);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_attr_compression acform;

		acform = (Form_pg_attr_compression) GETSTRUCT(tuple);
		amoid = get_am_oid(NameStr(acform->acname), false);
		amoids = list_append_unique_oid(amoids, amoid);
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	if (!list_length(amoids))
		PG_RETURN_NULL();

	/* Construct the list separated by comma */
	amoid = InvalidOid;
	initStringInfo(&result);
	foreach(lc, amoids)
	{
		if (OidIsValid(amoid))
			appendStringInfoString(&result, ", ");

		amoid = lfirst_oid(lc);
		appendStringInfoString(&result, get_am_name(amoid));
	}

	PG_RETURN_TEXT_P(CStringGetTextDatum(result.data));

}
#endif
