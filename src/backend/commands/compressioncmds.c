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

/* Set by pg_upgrade_support functions */
Oid			binary_upgrade_next_attr_compression_oid = InvalidOid;

/*
 * When conditions of compression satisfies one if builtin attribute
 * compresssion tuples the compressed attribute will be linked to
 * builtin compression without new record in pg_attr_compression.
 * So the fact that the column has a builtin compression we only can find out
 * by its dependency.
 */
static void
lookup_builtin_dependencies(Oid attrelid, AttrNumber attnum,
							List **amoids)
{
	LOCKMODE	lock = AccessShareLock;
	Oid			amoid = InvalidOid;
	HeapTuple	tup;
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[3];

	rel = table_open(DependRelationId, lock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(attrelid));
	ScanKeyInit(&key[2],
				Anum_pg_depend_objsubid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum((int32) attnum));

	scan = systable_beginscan(rel, DependDependerIndexId, true,
							  NULL, 3, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend depform = (Form_pg_depend) GETSTRUCT(tup);

		if (depform->refclassid == AttrCompressionRelationId)
		{
			Assert(IsBuiltinCompression(depform->refobjid));
			amoid = GetAttrCompressionAmOid(depform->refobjid);
			*amoids = list_append_unique_oid(*amoids, amoid);
		}
	}

	systable_endscan(scan);
	table_close(rel, lock);
}
#if 0
/*
 * Find identical attribute compression for reuse and fill the list with
 * used compression access methods.
 */
static Oid
lookup_attribute_compression(Oid attrelid, AttrNumber attnum, Oid amoid)
{
	Relation	rel;
	HeapTuple	tuple;
	SysScanDesc scan;
	Oid			result = InvalidOid;
	ScanKeyData key[2];


	Assert((attrelid > 0 && attnum > 0) || (attrelid == 0 && attnum == 0));

	rel = table_open(AttrCompressionRelationId, AccessShareLock);
	ScanKeyInit(&key[0],
				Anum_pg_attr_compression_acrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(attrelid));

	ScanKeyInit(&key[1],
				Anum_pg_attr_compression_acattnum,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(attnum));

	scan = systable_beginscan(rel, AttrCompressionRelidAttnumIndexId,
							  true, NULL, 2, key);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Oid			acoid,
					tup_amoid;
		Datum		values[Natts_pg_attr_compression];
		bool		nulls[Natts_pg_attr_compression];
		char	   *amname;

		heap_deform_tuple(tuple, RelationGetDescr(rel), values, nulls);
		acoid = DatumGetObjectId(values[Anum_pg_attr_compression_acoid - 1]);
		amname = NameStr(*DatumGetName(values[Anum_pg_attr_compression_acname - 1]));
		tup_amoid = get_am_oid(amname, false);

		if (tup_amoid == amoid)
			break;
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return result;
}
#endif
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
 * Remove the attribute compression record from pg_attr_compression.
 */
void
RemoveAttributeCompression(Oid acoid)
{
	Relation	relation;
	HeapTuple	tup;

	tup = SearchSysCache1(ATTCOMPRESSIONOID, ObjectIdGetDatum(acoid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for attribute compression %u", acoid);

	/* check we're not trying to remove builtin attribute compression */
	Assert(((Form_pg_attr_compression) GETSTRUCT(tup))->acrelid != 0);

	/* delete the record from catalogs */
	relation = table_open(AttrCompressionRelationId, RowExclusiveLock);
	CatalogTupleDelete(relation, &tup->t_self);
	table_close(relation, RowExclusiveLock);
	ReleaseSysCache(tup);
}

/*
 * CleanupAttributeCompression
 *
 * Remove entries in pg_attr_compression of the column except current
 * attribute compression and related with specified list of access methods.
 */
void
CleanupAttributeCompression(Oid relid, AttrNumber attnum, List *keepAmOids)
{
	Oid			acoid,
				amoid;
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[3];
	HeapTuple	tuple,
				attrtuple;
	Form_pg_attribute attform;
	List	   *removed = NIL;
	ListCell   *lc;

	attrtuple = SearchSysCache2(ATTNUM,
								ObjectIdGetDatum(relid),
								Int16GetDatum(attnum));

	if (!HeapTupleIsValid(attrtuple))
		elog(ERROR, "cache lookup failed for attribute %d of relation %u",
			 attnum, relid);
	attform = (Form_pg_attribute) GETSTRUCT(attrtuple);
	acoid = attform->attcompression;
	ReleaseSysCache(attrtuple);

	Assert(relid > 0 && attnum > 0);
	Assert(!IsBinaryUpgrade);

	rel = table_open(AttrCompressionRelationId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_pg_attr_compression_acrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	ScanKeyInit(&key[1],
				Anum_pg_attr_compression_acattnum,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(attnum));

	scan = systable_beginscan(rel, AttrCompressionRelidAttnumIndexId,
							  true, NULL, 2, key);

	/*
	 * Remove attribute compression tuples and collect removed Oids
	 * to list.
	 */
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_attr_compression acform;

		acform = (Form_pg_attr_compression) GETSTRUCT(tuple);
		amoid = get_am_oid(NameStr(acform->acname), false);

		/* skip current compression */
		if (acform->acoid == acoid)
			continue;

		if (!list_member_oid(keepAmOids, amoid))
		{
			removed = lappend_oid(removed, acform->acoid);
			CatalogTupleDelete(rel, &tuple->t_self);
		}
	}

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	/*
	 * Now remove dependencies between attribute compression (dependent)
	 * and column.
	 */
	rel = table_open(DependRelationId, RowExclusiveLock);
	foreach(lc, removed)
	{
		Oid			tup_acoid = lfirst_oid(lc);

		ScanKeyInit(&key[0],
					Anum_pg_depend_classid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(AttrCompressionRelationId));
		ScanKeyInit(&key[1],
					Anum_pg_depend_objid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(tup_acoid));

		scan = systable_beginscan(rel, DependDependerIndexId, true,
								  NULL, 2, key);

		while (HeapTupleIsValid(tuple = systable_getnext(scan)))
			CatalogTupleDelete(rel, &tuple->t_self);

		systable_endscan(scan);
	}
	table_close(rel, RowExclusiveLock);

	/* Now remove dependencies with builtin compressions */
	rel = table_open(DependRelationId, RowExclusiveLock);
	ScanKeyInit(&key[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	ScanKeyInit(&key[2],
				Anum_pg_depend_objsubid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum((int32) attnum));

	scan = systable_beginscan(rel, DependDependerIndexId, true,
							  NULL, 3, key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_depend depform = (Form_pg_depend) GETSTRUCT(tuple);

		if (depform->refclassid != AttrCompressionRelationId)
			continue;

		/* skip current compression */
		if (depform->refobjid == acoid)
			continue;

		amoid = GetAttrCompressionAmOid(depform->refobjid);
		if (!list_member_oid(keepAmOids, amoid))
			CatalogTupleDelete(rel, &tuple->t_self);
	}

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
}

/*
 * Construct ColumnCompression node by attribute compression Oid.
 */
ColumnCompression *
MakeColumnCompression(Oid acoid)
{
	HeapTuple	tuple;
	Form_pg_attr_compression acform;
	ColumnCompression *node;

	if (!OidIsValid(acoid))
		return NULL;

	tuple = SearchSysCache1(ATTCOMPRESSIONOID, ObjectIdGetDatum(acoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for attribute compression %u", acoid);

	acform = (Form_pg_attr_compression) GETSTRUCT(tuple);
	node = makeNode(ColumnCompression);
	node->amname = pstrdup(NameStr(acform->acname));
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
