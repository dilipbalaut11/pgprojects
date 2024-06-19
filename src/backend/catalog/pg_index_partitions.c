/*-------------------------------------------------------------------------
 *
 * pg_index_partitions.c
 *	  routines to support manipulation of the pg_index_partitions relation
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_index_partitions.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/catalog.h"
#include "catalog/pg_index_partitions.h"
#include "partitioning/partdesc.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"

/*
 * IndexGetNextPartitionID - Get the next partition ID of the global index
 *
 * Obtain the next partition ID to be allocated for the specified global index
 * relation. Also update this value in the cache for the next allocation.
 */
PartitionId
IndexGetNextPartitionID(Relation irel)
{
	PartitionId partid;

	/* If the cache is not already build then do it first. */
	if (irel->rd_indexpartinfo == NULL)
		BuildIndexPartitionInfo(irel, CurrentMemoryContext);

	/* Use the max_partid + 1 value as the next parition id. */
	partid = irel->rd_indexpartinfo->max_partid + 1;

	/*
	 * Increase the max_partid in cache, in case the cache is invalidated we
	 * will get the max value again from the system catalog, so there should
	 * not be any issue.
	 */
	irel->rd_indexpartinfo->max_partid = partid;

	/* TODO: check availability of this partid and find the unused value*/

	return partid;
}

/*
 * InsertIndexPartitionEntry - Insert a reloid to parition id mapping
 */
void
InsertIndexPartitionEntry(Relation irel, Oid reloid, PartitionId partid)
{
	Datum		values[Natts_pg_index_partitions];
	bool		nulls[Natts_pg_index_partitions];
	HeapTuple	tuple;
	Relation	rel;
	Oid			indexoid = RelationGetRelid(irel);

	rel = table_open(IndexPartitionsRelationId, RowExclusiveLock);

	/*
	 * Make the pg_index_partitions entry
	 */
	values[Anum_pg_index_partitions_indexoid - 1] = ObjectIdGetDatum(indexoid);
	values[Anum_pg_index_partitions_reloid - 1] = ObjectIdGetDatum(reloid);
	values[Anum_pg_index_partitions_partid - 1] = PartitionIdGetDatum(partid);

	memset(nulls, 0, sizeof(nulls));

	tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	CatalogTupleInsert(rel, tuple);

	heap_freetuple(tuple);

	table_close(rel, RowExclusiveLock);
}

/*
 * BuildIndexPartitionInfo - Cache for parittion id to reloid mapping
 *
 * Build a cache for faster access to the mappping from partition id to the
 * relation oid.  For more detail on this mapping refer to the comments in
 * pg_index_partition.h and also atop PartitionId declaration in c.h.
 */
void
BuildIndexPartitionInfo(Relation relation, MemoryContext context)
{
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	tuple;
	Relation	rel;
	PartitionId	maxpartid = InvalidIndexPartitionId;
	IndexPartitionInfo	map;
	MemoryContext oldcontext;
	HASHCTL		ctl;

	rel = table_open(IndexPartitionsRelationId, AccessShareLock);

	oldcontext = MemoryContextSwitchTo(context);
	map = (IndexPartitionInfoData *) palloc0(sizeof(IndexPartitionInfoData));
	map->context = context;

	ctl.keysize = sizeof(int32);
	ctl.entrysize = sizeof(IndexPartitionInfoEntry);
	ctl.hcxt = context;

	map->pdir_hash = hash_create("index partition directory", 256, &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	MemoryContextSwitchTo(oldcontext);

	ScanKeyInit(&key,
				Anum_pg_index_partitions_indexoid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relation->rd_rel->oid));

	scan = systable_beginscan(rel, IndexIdPartitionsIndexId, true,
							  NULL, 1, &key);

	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Form_pg_index_partitions form = (Form_pg_index_partitions) GETSTRUCT(tuple);
		IndexPartitionInfoEntry *entry;
		bool		found;

		if (!OidIsValid(form->reloid))
			continue;

		entry = hash_search(map->pdir_hash, &form->partid, HASH_ENTER, &found);
		Assert(!found);
		entry->reloid = form->reloid;

		/* Let caller know of partition being detached */
		if (form->partid > maxpartid)
			maxpartid = form->partid;
	}

	map->max_partid = maxpartid;
	relation->rd_indexpartinfo = map;
	systable_endscan(scan);


	table_close(rel, AccessShareLock);
}

/*
 * IndexGetRelationPartitionId - Get partition id for the reloid
 *
 * Get the partition ID for the given partition relation OID
 * for the specified global index relation.
 */
PartitionId
IndexGetRelationPartitionId(Relation irel, Oid reloid)
{
	IndexPartitionInfo	map;
	HASH_SEQ_STATUS		hash_seq;
	PartitionId			partid = InvalidIndexPartitionId;
	IndexPartitionInfoEntry *entry;

	if (irel->rd_indexpartinfo == NULL)
		BuildIndexPartitionInfo(irel, CurrentMemoryContext);

	map = irel->rd_indexpartinfo;

	hash_seq_init(&hash_seq, map->pdir_hash);

	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (entry->reloid == reloid)
		{
			partid = entry->partid;
			hash_seq_term(&hash_seq);
			break;
		}
	}

	return partid;
}

/*
 * IndexGetPartitionReloid - Get relation oid for the paritionid
 *
 * Get the relation OID for the given partition ID for the specified global
 * index relation.
 */
Oid
IndexGetPartitionReloid(Relation irel, PartitionId partid)
{
	IndexPartitionInfo	map = irel->rd_indexpartinfo;
	IndexPartitionInfoEntry *entry;
	bool		found;

	entry = hash_search(map->pdir_hash, &partid, HASH_FIND, &found);
	if (!found)
		return InvalidOid;

	return entry->reloid;
}

/*
 * Detach a input 'reloid' from then global index with Oid 'indexoid'.
 * Basically this just remove mapping from pg_index_partitions catalog.
 *
 * FIXME: instead of detaching reloid one by one can we pass the array of
 * reloids to be detached?
 */
static void
UpdateIndexPartitionEntry(Oid indexoid, Oid reloid)
{
	Relation	catalogRelation;
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	tuple;
	/*
	 * Find pg_inherits entries by inhparent.  (We need to scan them all in
	 * order to verify that no other partition is pending detach.)
	 */
	catalogRelation = table_open(IndexPartitionsRelationId, RowExclusiveLock);
	ScanKeyInit(&key,
				Anum_pg_index_partitions_indexoid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(indexoid));

	scan = systable_beginscan(catalogRelation, IndexIdPartitionsIndexId, true,
							  NULL, 1, &key);

	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Form_pg_index_partitions form = (Form_pg_index_partitions) GETSTRUCT(tuple);
		HeapTuple	newtup;

		if (form->reloid != reloid)
			continue;

			newtup = heap_copytuple(tuple);
			((Form_pg_index_partitions) GETSTRUCT(newtup))->reloid = InvalidOid;

			CatalogTupleUpdate(catalogRelation,
							   &tuple->t_self,
							   newtup);
			heap_freetuple(newtup);
	}

	/* Done */
	systable_endscan(scan);
	table_close(catalogRelation, RowExclusiveLock);
}

/*
 * IndexPartitionDetachRecurse - Detach all partitions from global indexes
 *
 * Recursively detach all the leaf partitions underneath the given relation
 * from all the global indexes provided in the global_indexes list. If this
 * is a leaf relation itself, then directly detach it by marking the reloid
 * invalid in the mapping in pg_index_partitions mapping.
 */
void
IndexPartitionDetachRecurse(Relation rel, List *global_indexes)
{
	PartitionDesc pd;

	/* FIXME: handle other relkind?*/
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		pd = RelationGetPartitionDesc(rel, true);

		for (int i = 0; i < pd->nparts; i++)
		{
			Relation	partRel;

			partRel = table_open(pd->oids[i], ShareRowExclusiveLock);

			IndexPartitionDetachRecurse(partRel, global_indexes);
			table_close(partRel, ShareRowExclusiveLock);
		}
	}
	else if (rel->rd_rel->relkind == RELKIND_RELATION)
	{
		ListCell *l;

		foreach(l, global_indexes)
		{
			Oid		indexoid = lfirst_oid(l);

			UpdateIndexPartitionEntry(indexoid, RelationGetRelid(rel));
		}
	}
}
