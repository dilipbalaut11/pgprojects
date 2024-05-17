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

#define		InvalidIndexPartitionId		0
#define		FirstValidIndexPartitionId	1

/*
 * Get the next partition id to be allocated for the input index relation.
 * also update this value in the cache for the next allocation.
 */
static int32
IndexGetNextPartitionID(Relation irel)
{
	int32 partid;

	if (irel->rd_indexpartinfo == NULL)
		BuildIndexPartitionInfo(irel, CurrentMemoryContext);

	partid = irel->rd_indexpartinfo->max_partid + 1;

	/*
	 * TODO: Recheck this logic:
	 * Increase the max_partid in cache, in case the cache is invalidated we
	 * will get the max value again from the system catalog, so there should
	 * not be any issue.
	 */
	irel->rd_indexpartinfo->max_partid = partid;

	/* TODO: check availability of this partid and find the unused value*/

	return partid;
}

/*
 * Create a single pg_index_partitions row with the given data
 */
static void
InsertIndexPartitionEntry(Relation irel, Oid reloid, int32 partid)
{
	Datum		values[Natts_pg_index_partitions];
	bool		nulls[Natts_pg_index_partitions];
	HeapTuple	tuple;
	Relation	rel;
	Oid			indexoid = irel->rd_rel->oid;

	rel = table_open(IndexPartitionsRelationId, RowExclusiveLock);

	/*
	 * Make the pg_index_partitions entry
	 */
	values[Anum_pg_index_partitions_indexoid - 1] = ObjectIdGetDatum(indexoid);
	values[Anum_pg_index_partitions_reloid - 1] = ObjectIdGetDatum(reloid);
	values[Anum_pg_index_partitions_partid - 1] = Int32GetDatum(partid);

	memset(nulls, 0, sizeof(nulls));

	tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	CatalogTupleInsert(rel, tuple);

	heap_freetuple(tuple);

	table_close(rel, RowExclusiveLock);
}

/*
 * Recursively process the table and allocate the partition id for each leaf
 * partition for this global index and insert the entry into the
 * pg_index_partitions table.
 */
void
CreateIndexPartitionIdRecurse(Relation rel, Relation irel)
{
	PartitionDesc pd = RelationGetPartitionDesc(rel, true);

	for (int i = 0; i < pd->nparts; i++)
	{
		Relation	partRel;

		partRel = table_open(pd->oids[i], ShareRowExclusiveLock);

		if (partRel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
			CreateIndexPartitionIdRecurse(partRel, irel);
		else if (partRel->rd_rel->relkind == RELKIND_RELATION)
		{
			int32	partid = IndexGetNextPartitionID(irel);

			InsertIndexPartitionEntry(irel, partRel->rd_rel->oid, partid);
		}

		table_close(partRel, ShareRowExclusiveLock);
	}
}

/*
 * Initialize index-access-method support data for an index relation
 */
void
BuildIndexPartitionInfo(Relation relation, MemoryContext context)
{
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	tuple;
	Relation	rel;
	int32		maxpartid = InvalidIndexPartitionId;
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
	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
	{
		Form_pg_index_partitions form = (Form_pg_index_partitions) GETSTRUCT(tuple);
		IndexPartitionInfoEntry *entry;
		bool		found;

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
 * Get the the parition id for the geven partition relation oid for the input
 * global index relation.
 */
int32
IndexGetRelationPartID(Relation irel, Oid reloid)
{
	IndexPartitionInfo	map = irel->rd_indexpartinfo;
	HASH_SEQ_STATUS		hash_seq;
	int32				partid = InvalidIndexPartitionId;
	IndexPartitionInfoEntry *entry;

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
