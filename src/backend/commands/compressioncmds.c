/*-------------------------------------------------------------------------
 *
 * compressioncmds.c
 *	  Routines for SQL commands for attribute compression methods
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

#include "access/compressionapi.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_proc.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "parser/parse_func.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/syscache.h"

/*
 * CreateCompressionMethod
 *		Registers a new compression method.
 */
ObjectAddress
CreateCompressionMethod(CreateCmStmt *stmt)
{
	Relation	rel;
	ObjectAddress myself;
	ObjectAddress referenced;
	Oid			cmoid;
	Oid			cmhandler;
	bool		nulls[Natts_pg_compression];
	Datum		values[Natts_pg_compression];
	HeapTuple	tup;
	Oid			funcargtypes[1] = {INTERNALOID};

	rel = table_open(CompressionRelationId, RowExclusiveLock);

	/* Must be super user */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to create compression method \"%s\"",
						stmt->cmname),
				 errhint("Must be superuser to create an access method.")));

	/* Check if name is used */
	cmoid = GetSysCacheOid1(CMNAME, Anum_pg_compression_oid,
							CStringGetDatum(stmt->cmname));
	if (OidIsValid(cmoid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("compression method \"%s\" already exists",
						stmt->cmname)));
	}

	if (stmt->handler_name == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("handler function is not specified")));

	/* Get the handler function oid */
	cmhandler = LookupFuncName(stmt->handler_name, 1, funcargtypes, false);

	/*
	 * Insert tuple into pg_compression.
	 */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	cmoid = GetNewOidWithIndex(rel, AmOidIndexId, Anum_pg_compression_oid);
	values[Anum_pg_compression_oid - 1] = ObjectIdGetDatum(cmoid);
	values[Anum_pg_compression_cmname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->cmname));
	values[Anum_pg_compression_cmhandler - 1] = ObjectIdGetDatum(cmhandler);

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);

	myself.classId = CompressionRelationId;
	myself.objectId = cmoid;
	myself.objectSubId = 0;

	/* Record dependency on handler function */
	referenced.classId = ProcedureRelationId;
	referenced.objectId = cmhandler;
	referenced.objectSubId = 0;

	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	recordDependencyOnCurrentExtension(&myself, false);

	InvokeObjectPostCreateHook(CompressionRelationId, cmoid, 0);

	table_close(rel, RowExclusiveLock);

	return myself;
}

Oid
get_cm_oid(const char *cmname, bool missing_ok)
{
	Oid			cmoid;

	cmoid = GetCompressionOid(cmname);
	if (!OidIsValid(cmoid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("compression type \"%s\" not recognized", cmname)));

	return cmoid;
}

/*
 * get list of all supported compression methods for the given attribute.
 *
 * If oldcmoids list is passed then it will delete the attribute dependency
 * on the compression methods passed in the oldcmoids, otherwise it will
 * return the list of all the compression method on which the attribute has
 * dependency.
 */
static List *
lookup_attribute_compression(Oid attrelid, AttrNumber attnum, List *oldcmoids)
{
	LOCKMODE	lock = AccessShareLock;
	HeapTuple	tup;
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[3];
	List	   *cmoids = NIL;

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

		if (depform->refclassid == CompressionRelationId)
		{
			if (oldcmoids && list_member_oid(oldcmoids, depform->refobjid))
				CatalogTupleDelete(rel, &tup->t_self);
			else if (oldcmoids == NULL)
				cmoids = list_append_unique_oid(cmoids, depform->refobjid);
		}
	}

	systable_endscan(scan);
	table_close(rel, lock);

	return cmoids;
}

/*
 * Remove the attribute dependency on the old compression methods given in the
 * cmoids list.
 */
static void
remove_old_dependencies(Oid attrelid, AttrNumber attnum, List *cmoids)
{
	lookup_attribute_compression(attrelid, attnum, cmoids);
}

/*
 * Check whether the given compression method oid is supported by
 * the target attribue.
 */
bool
IsCompressionSupported(Form_pg_attribute att, Oid cmoid)
{
	List	   *cmoids = NIL;

	/* Check whether it is same as the current compression oid */
	if (cmoid == att->attcompression)
		return true;

	/* Check the oid in all preserved compresion methods */
	cmoids = lookup_attribute_compression(att->attrelid, att->attnum, NULL);
	if (list_member_oid(cmoids, cmoid))
		return true;
	else
		return false;
}

/*
 * Get the compression method oid based on the compression method name.  When
 * compression is not specified returns default attribute compression.  It is
 * possible case for CREATE TABLE and ADD COLUMN commands where COMPRESSION
 * syntax is optional.
 *
 * For ALTER command, check all the supported compression methods for the
 * attribute and if the preserve list is not passed or some of the old
 * compression methods are not given in the preserved list then delete
 * dependency from the old compression methods and force the table rewrite.
 */
Oid
GetAttributeCompression(Form_pg_attribute att, ColumnCompression *compression,
						bool *need_rewrite)
{
	Oid			cmoid;
	ListCell   *cell;

	/* no compression for the plain storage */
	if (att->attstorage == TYPSTORAGE_PLAIN)
		return InvalidOid;

	/* fallback to default compression if it's not specified */
	if (compression == NULL)
		return DefaultCompressionOid;

	cmoid = GetCompressionOid(compression->cmname);
	if (!OidIsValid(cmoid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("compression type \"%s\" not recognized", compression->cmname)));

	/*
	 * Determine if the column needs rewrite or not. Rewrite conditions: SET
	 * COMPRESSION without PRESERVE - SET COMPRESSION with PRESERVE but not
	 * with full list of previous access methods.
	 */
	if (need_rewrite != NULL)
	{
		List	   *previous_cmoids = NIL;

		previous_cmoids =
			lookup_attribute_compression(att->attrelid, att->attnum, NULL);

		if (compression->preserve != NIL)
		{
			foreach(cell, compression->preserve)
			{
				char	   *cmname_p = strVal(lfirst(cell));
				Oid			cmoid_p = GetCompressionOid(cmname_p);

				if (!list_member_oid(previous_cmoids, cmoid_p))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("\"%s\" compression access method cannot be preserved", cmname_p),
							 errhint("use \"pg_column_compression\" function for list of compression methods")));

				/*
				 * Remove from previous list, also protect from multiple
				 * mentions of one access method in PRESERVE list
				 */
				previous_cmoids = list_delete_oid(previous_cmoids, cmoid_p);
			}
		}

		/*
		 * If the list of previous Oids is not empty after deletions then
		 * we need to rewrite tuples in the table.
		 */
		if (list_length(previous_cmoids) != 0)
		{
			remove_old_dependencies(att->attrelid, att->attnum,
									previous_cmoids);
			*need_rewrite = true;
		}

		/* Cleanup */
		list_free(previous_cmoids);
	}

	return cmoid;
}

/*
 * Construct ColumnCompression node from the compression method oid.
 */
ColumnCompression *
MakeColumnCompression(Oid attcompression)
{
	ColumnCompression *node;

	if (!OidIsValid(attcompression))
		return NULL;

	node = makeNode(ColumnCompression);
	node->cmname = GetCompressionNameFromOid(attcompression);

	return node;
}
