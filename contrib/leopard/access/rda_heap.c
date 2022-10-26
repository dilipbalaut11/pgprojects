/*-------------------------------------------------------------------------
 *
 * rda_heap.c
 *        Recently Dead Archive (RDA) implemented as a HEAP
 *
 * Portions Copyright (c) 2022, EnterpriseDB Corporation. All Rights Reserved.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 *
 * API
 *     rda_insert() - adds a new tuple to the RDA
 *     rda_get() - retrieves a visible tuple from RDA
 *     rda_trim() - removes DEAD rows from RDA
 *
 *-------------------------------------------------------------------------
 * Various implementations are possible; this refers to the RDA_heap.
 * Alternate implementations would use same API, different file name.
 *
 * RDA Heap
 *
 * RDA is a HEAP table, logged normally, with RDA_NUM_PARTITIONS partitions.
 * RDA is partitioned by range on the xmax value, so while data normally
 * goes into one main table, this might also go into 2 or more tables in
 * various scenarios. Note that RDA data is never normally updated.
 *
 * No SQL is executed, so partition routing is performed here based
 * on mod(xmax) from the data, so it is predictable. We use that to match
 * against the regularised partition names to identity the partition.
 *
 * Only the final change to a row is stored for each transaction. If a
 * transaction updates a row multiple times, the earlier, never-visible
 * tuples will have entries here, but row_data will be NULL. This allows
 * the chain of entries to be maintained within the RDA, though this is
 * not re-checked at run-time.
 *
 * Note that xmin, xmax values are stored using OID datatype, since this
 * supports 32-bit unsigned integer values AND also supports btree ops,
 * whereas XID datatype does not.
 *
 * Partition pruning for rda_get() is also performed within this module.
 *
 * Rows are inserted by rda_insert() as TABLE_INSERT_FROZEN, making them
 * non-transactional, as is required for the RDA. This avoids MVCC
 * problems if rda_insert() is called from a xact that subsequently aborts.
 * Tuples have already been toasted, so the data stored won't benefit
 * from further toasting, so we set a high toast target to avoid that.
 * RDA is insert-only, never update or delete, so we pack both heap and
 * index with fillfactor=100 on the assumption that rollbacks are rare.
 * Also, we never use the FSM, since we expect append-only usage.
 * autovacuums are disabled, since all rows are already frozen and there
 * is nothing to clean up.
 *
 * To avoid race conditions between rda_insert() and rda_get() we hold
 * the main heap buffer lock until we have inserted both the rda tuple
 * and the rda index entry.
 *
 * rda_trim() will truncate partitions that are wholly invisible to all
 * users, so no deletion takes place. This is designed to be executed from
 * a background worker, by admin user or by foreground processes.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/pg_collation_d.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "nodes/makefuncs.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "leopardam.h"
#include "rda_heap.h"

/*--------------------------------------------------------------------------*/
/*
 * All definitions below must match the tables created by leopard--X.Y.sql
 */
#define RDA_PARTITION_BITS	5

/* should be 2 ^ RDA_PARTITION_BITS */
#define RDA_NUM_PARTITIONS	32

/* should be 32bits - RDA_PARTITION_BITS */
#define RDA_PARTITION_SHIFT	(32 - RDA_PARTITION_BITS)

#define RDA_xmax_get_partition_num(x) \
	(((uint32) x) >> RDA_PARTITION_SHIFT)

/* Partition names are "rda_pNNNN" */
#define RDA_NAME_LENGTH		10

#define RDA_NUM_ATTRS		6

/* AttrNums */
#define RDA_COLUMN_TOID		1
#define RDA_COLUMN_ROOT_TID	2
#define RDA_COLUMN_ROW_XMIN	3
#define RDA_COLUMN_ROW_XMAX	4
#define RDA_COLUMN_NEXT_TID	5
#define RDA_COLUMN_ROW_DATA	6

#define RDA_NUM_SCANKEYS	4

#define RDA_SCHEMA_NAME	"rda"

/*--------------------------------------------------------------------------*/

static Oid		cached_partition_oid = InvalidOid;
static uint32	cached_partition_num = 0;

/*--------------------------------------------------------------------------*/
/* Unit test support */

static	Oid				toid;
static ItemPointerData	root;

/*--------------------------------------------------------------------------*/
/* Private routines */

static int64
ItemPointerGetInt64(ItemPointer ip)
{
	BlockNumber bn = ItemPointerGetBlockNumber(ip);
	OffsetNumber on = ItemPointerGetOffsetNumber(ip);
	int64 bn64 = ((int64) bn) << 16;
	int64 on64 = (int64) on;

	/*
	 * Merge the blkid and offset into a single BIGINT value,
	 * so that all offsets on same block form a tight range.
	 */
	return bn64 + on64;
}

/*--------------------------------------------------------------------------*/
/* Unit tests */

PG_FUNCTION_INFO_V1(rda_unit_test_rda_insert);

Datum
rda_unit_test_rda_insert(PG_FUNCTION_ARGS)
{
	RangeVar   		*rv = makeRangeVar("public", "foo", -1);
	Relation 		test_heap;
	BufferHeapTupleTableSlot *hslot;
	TupleTableSlot 	*slot;
	TableScanDesc 	tableScan;
	HeapTuple		tuple;
	Buffer			buf;
	Page			page;
	OffsetNumber	root_offsets[MaxHeapTuplesPerPage];

	test_heap = table_openrv(rv, AccessShareLock);
	if (test_heap == NULL)
		elog(ERROR, "table not found");
	tableScan = table_beginscan(test_heap, SnapshotAny, 0, (ScanKey) NULL);
	slot = table_slot_create(test_heap, NULL);
	hslot = (BufferHeapTupleTableSlot *) slot;

	for (;;)
	{
		if (!table_scan_getnextslot(tableScan, ForwardScanDirection, slot))
			break;

		tuple = ExecFetchSlotHeapTuple(slot, false, NULL);
		buf = hslot->buffer;

		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		if (TransactionIdIsValid(HeapTupleHeaderGetRawXmax(tuple->t_data)))
		{
			ItemPointerCopy(&tuple->t_self, &root);
			if (HeapTupleIsHotUpdated(tuple) ||
				HeapTupleIsHeapOnly(tuple))
			{
				page = BufferGetPage(buf);
				leopard_get_root_tuples(page, root_offsets);
				ItemPointerSetOffsetNumber(&root, root_offsets[ItemPointerGetOffsetNumber(&tuple->t_self) - 1]);
			}
			toid = test_heap->rd_id;
			rda_insert(toid, tuple, root);
			/* Always emit this, because it is for unit test */
			elog(NOTICE, "inserted 1 row into RDA");
		}

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	}

	table_endscan(tableScan);
	ExecDropSingleTupleTableSlot(slot);
	table_close(test_heap, AccessShareLock);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(rda_unit_test_rda_get);

Datum
rda_unit_test_rda_get(PG_FUNCTION_ARGS)
{
	Snapshot snapshot = GetLatestSnapshot();
	LeopardTuple	tup = NULL;
	bool	found = false;

	/*
	 * We overwrite the snapshot values, so we're
	 * just using it as a scratchpad.
	 *
	 * This assumes that the toid and root variables
	 * are already set by prior execution of the
	 * rda_insert() unit test. Not pretty, but it works.
	 */

	snapshot->xmin = 500;
	snapshot->xmax = 700;
	found = rda_get(toid, root, snapshot, true, tup);

	snapshot->xmin = 500;
	snapshot->xmax = 741;
	found = rda_get(toid, root, snapshot, true, tup);

	snapshot->xmin = 741;
	snapshot->xmax = 741;
	found = rda_get(toid, root, snapshot, true, tup);

	snapshot->xmin = 741;
	snapshot->xmax = 742;
	found = rda_get(toid, root, snapshot, true, tup);

	snapshot->xmin = 741;
	snapshot->xmax = 800;
	found = rda_get(toid, root, snapshot, true, tup);

	snapshot->xmin = 500;
	snapshot->xmax = 800;
	found = rda_get(toid, root, snapshot, true, tup);

	snapshot->xmin = 800;
	snapshot->xmax = 800;
	found = rda_get(toid, root, snapshot, true, tup);

	snapshot->xmin = 800;
	snapshot->xmax = 268455456;
	found = rda_get(toid, root, snapshot, true, tup);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(rda_unit_test_rda_trim);

Datum
rda_unit_test_rda_trim(PG_FUNCTION_ARGS)
{
	rda_trim();

	PG_RETURN_VOID();
}

/*--------------------------------------------------------------------------*/
/* Public routines */

/*
 * rda_insert
 *
 * Inserts data into the RDA and also inserts into the rda_index, intended
 * to allow a tuple to be moved from another heap into the RDA. It is assumed
 * that any rows inserted are both HEAPTUPLE_RECENTLY_DEAD and HEAP_HOT_UPDATED,
 * so we don't expect to see any locked or deleted rows here, though this is
 * tested in the caller.
 *
 * Insert is performed using table_tuple_insert() with option TABLE_INSERT_FROZEN,
 * so that the changes to the table are effectively non-transactional, allowing
 * them to be visible by other transactions as soon as the rda buffer lock is
 * released. Partition routing is conducted based upon the tuple's xmax.
 *
 * It is assumed the caller holds an exclusive buffer lock on the heap block,
 * ensuring that the inserts into RDA and rda_index appear atomic for the intended
 * users. Note that admin users inspecting the RDA would not see these changes
 * as atomic, so be careful with tests/diagnostics that might give different results.
 *
 * This is the only supported mechanism for inserting rows into the RDA. Other
 * possible mechanisms such as INSERT SELECT and COPY should be used only as a matter
 * of last resort and could easily corrupt data, as would UPDATE or DELETE.
 * Old data is removed from the RDA by rda_trim(), see later.
 */
void
rda_insert(Oid toid, HeapTuple tuple, ItemPointerData root_tid)
{
	RangeVar   	*rv;
	Relation	rda_rel = NULL;
	Relation	rda_idx = NULL;
	IndexInfo	*rda_idx_info = NULL;
	HeapTuple   rda_tuple = NULL;
	TupleTableSlot *slot = NULL;
	TransactionId	row_xmax;
	List       *indexoidlist;
	ListCell   *l;
	char       *tuptxt = NULL;
	char        rda_partition_name[RDA_NAME_LENGTH];
	Datum		values[RDA_NUM_ATTRS];
	bool		isnull[RDA_NUM_ATTRS];
	uint32		partition_num;

	if (!ItemPointerIsValid(&root_tid) ||
		!OidIsValid(toid))
		elog(ERROR, "invalid input");

	/*
	 * Work out the partition to route this insert to, open it directly.
	 */
	row_xmax = HeapTupleHeaderGetRawXmax(tuple->t_data);
	partition_num = RDA_xmax_get_partition_num(row_xmax);

	if (OidIsValid(cached_partition_oid) &&
		partition_num == cached_partition_num)
	{
		rda_rel = table_open(cached_partition_oid, RowExclusiveLock);
	}
	else
	{
		snprintf(rda_partition_name, RDA_NAME_LENGTH, "rda_p%04d", partition_num);
		rv = makeRangeVar(RDA_SCHEMA_NAME, rda_partition_name, -1);

		rda_rel = table_openrv(rv, RowExclusiveLock);
	}

	if (rda_rel == NULL)
		elog(ERROR, "RDA table not opened");

	cached_partition_oid = rda_rel->rd_id;
	cached_partition_num = partition_num;

	/*
	 * Collect column values one by one, in the correct sequence.
	 *
	 * xids aren't indexable, so we use OID instead, rather than xid8.
	 * TIDs aren't indexable, so we pack them into a BIGINT for ease of access.
	 */

	/* tableoid OID */
	values[RDA_COLUMN_TOID - 1] = ObjectIdGetDatum(toid);
	isnull[RDA_COLUMN_TOID - 1] = false;

	/* root_tid BIGINT */
	values[RDA_COLUMN_ROOT_TID - 1] = Int64GetDatum(ItemPointerGetInt64(&root_tid));
	isnull[RDA_COLUMN_ROOT_TID - 1] = false;

	/* row_xmin OID */
	values[RDA_COLUMN_ROW_XMIN - 1] = ObjectIdGetDatum((Oid) HeapTupleHeaderGetXmin(tuple->t_data));
	isnull[RDA_COLUMN_ROW_XMIN - 1] = false;

	/* row_xmax OID */
	values[RDA_COLUMN_ROW_XMAX - 1] = ObjectIdGetDatum((Oid) row_xmax);
	isnull[RDA_COLUMN_ROW_XMAX - 1] = false;

	/* next_tid BIGINT - just for sanity checks */
	values[RDA_COLUMN_NEXT_TID - 1] = Int64GetDatum(ItemPointerGetInt64(&((tuple->t_data)->t_ctid)));
	isnull[RDA_COLUMN_NEXT_TID - 1] = false;

	/* Put the source tuple header into a Datum for insertion into an RDA col */
	tuptxt = palloc(VARHDRSZ + tuple->t_len + 1);
	SET_VARSIZE(tuptxt, VARHDRSZ + tuple->t_len + 1);
	memcpy(VARDATA(tuptxt), tuple->t_data, tuple->t_len);
	values[RDA_COLUMN_ROW_DATA - 1] = PointerGetDatum(tuptxt);
	isnull[RDA_COLUMN_ROW_DATA - 1] = false;

	rda_tuple = heap_form_tuple(rda_rel->rd_att, values, isnull);
	slot = MakeSingleTupleTableSlot(RelationGetDescr(rda_rel),
									&TTSOpsHeapTuple);
	ExecStoreHeapTuple(rda_tuple, slot, false);

	/*
	 * Perform the insert, one row at a time. BulkInsertState is NULL.
	 * Notice that we don't re-validate partition bounds, for performance.
	 *
	 * We use the current CommandId because we have to use something. This
	 * shouldn't make any difference, since we aren't using portals against
	 * RDA, nor doing updates or deletes.
	 */
	table_tuple_insert(rda_rel, slot, GetCurrentCommandId(true),
						TABLE_INSERT_SKIP_FSM | TABLE_INSERT_FROZEN, NULL);
	pfree(tuptxt);

	/* Find the index id for this partition */
	indexoidlist = RelationGetIndexList(rda_rel);

	/*
	 * RDA should have one valid index.
	 *
	 * Since we only do inserts and we regularly drop partitions, the
	 * index will never need a REINDEX, but we can't actually prevent
	 * someone from reindexing it. So take the last index entry.
	 */
	foreach(l, indexoidlist)
	{
		Oid			rda_partition_idx_oid;
		Relation	rel;

		rda_partition_idx_oid = lfirst_oid(l);
		rel = index_open(rda_partition_idx_oid, RowExclusiveLock);

		/*
		 * Use index only if it is fully ready.
		 */
		if (!rel->rd_index->indisready)
		{
			index_close(rel, RowExclusiveLock);
			continue;
		}
		if (rda_idx)
			index_close(rda_idx, RowExclusiveLock);
		rda_idx = rel;

		if (rda_idx_info)
			pfree(rda_idx_info);
		rda_idx_info = palloc(sizeof(IndexInfo *));
		rda_idx_info = BuildIndexInfo(rda_idx);

		/*
		 * Check the index for validity
		 */
		if (rda_idx_info->ii_NumIndexKeyAttrs != RDA_NUM_SCANKEYS ||
			rda_idx_info->ii_Expressions != NIL)
			continue;

		/* Note that we don't need speculative index info */

		/* Keep rda_idx_info for later use */
	}
	if (rda_idx == NULL)
		elog(ERROR, "could not open valid RDA index");
	list_free(indexoidlist);

	Assert(ItemPointerIsValid(&slot->tts_tid));

	/*
	 * Form the index tuple using the same array of values, since the leading
	 * columns of the table match the index columns.
	 */
	FormIndexDatum(rda_idx_info,
					slot,
					NULL, /* estate can be NULL since no expressions on index */
					values,
					isnull);

	/*
	 * Kluge, since RDA index is not a normal user index, so we don't want to
	 * throw serializable errors if people read data touched by others.
	 */
	rda_idx->rd_indam->ampredlocks = false;

	/*
	 * Insert into the rda_idx. Note that any other indexes added will not be
	 * automatically maintained by this low level code.
	 */
	index_insert(rda_idx,
				 values,
				 isnull,
				 &slot->tts_tid,
				 rda_rel,
				 UNIQUE_CHECK_NO,	/* No uniqueness check */
				 false,
				 rda_idx_info);

	ExecDropSingleTupleTableSlot(slot);

	if (rda_tuple)
		heap_freetuple(rda_tuple);
	if (rda_idx_info)
		pfree(rda_idx_info);

	/*
	 * Close and we're done
	 */
	index_close(rda_idx, RowExclusiveLock);
	table_close(rda_rel, RowExclusiveLock);
}

/*
 * rda_get
 *
 * Retrieves a visible tuple from the RDA for a specific Snapshot, using rda_index,
 * for a specific Relation and root TID. This is optimised for OLTP, which tends to
 * follow single tuples. A similar routine might be optimized for page-based access.
 *
 * Performs an index scan using rda_index, which will retrieve 0, 1 or many rows.
 * The RDA contains a portion of the update chain for tuple with the given root TID,
 * though the chain is not stored in any specific ordering in the heap.
 * The index scan is constrained by relation Oid, root TID, the Snapshot xmin, xmax values.
 * Rows are retrieved in index order, then further qualified against the Snapshot.
 * The scan completes when we identify a single visible row, or we run out of candidates.
 *
 * This is more complex because of partitioning. We start the search with the most recent
 * partition and move backwards in time over all partitions that cover the range
 * from partition_of(Snapshot->xmin) to partition_of(Snapshot->xmax). Since we partition
 * by xmax, you might think the lower bound seems wrong, but by definition any xmax
 * earlier than Snapshot->xmin would be visible, meaning any deleted row would be
 * invisible to the current snapshot.
 */
bool
rda_get(Oid toid, ItemPointerData root_tid, Snapshot snapshot, bool debug, LeopardTuple leopardTuple)
{
	ScanKeyData skey[RDA_NUM_SCANKEYS];
	HeapTuple	rda_tuple = NULL;
	int64		rtid = ItemPointerGetInt64(&root_tid);
	uint32		partition_min;
	uint32		partition_num;
	char        rda_partition_name[RDA_NAME_LENGTH];

	if (!IsMVCCSnapshot(snapshot) ||
		!TransactionIdIsValid(snapshot->xmin) ||
		!TransactionIdIsValid(snapshot->xmax))
		elog(ERROR, "rda_get() uses MVCC snapshots");

	if (!ItemPointerIsValid(&root_tid) ||
		!OidIsValid(toid))
		elog(ERROR, "invalid input");

	/*
	 * We search on RDA_NUM_SCANKEYS, all of which are non-NULL.
	 * Scan keys are marked as if _bt_preprocess_keys() had been run on them.
	 * C_COLLATION_OID is irrelevant, but is required.
	 */
	ScanKeyEntryInitialize(&skey[0],
				SK_BT_REQFWD | SK_BT_REQBKWD,
				RDA_COLUMN_TOID,
				BTEqualStrategyNumber,
				InvalidOid,
				C_COLLATION_OID,
				F_OIDEQ,
				ObjectIdGetDatum(toid));
	ScanKeyEntryInitialize(&skey[1],
				SK_BT_REQFWD | SK_BT_REQBKWD,
				RDA_COLUMN_ROOT_TID,
				BTEqualStrategyNumber,
				InvalidOid,
				C_COLLATION_OID,
				F_INT8EQ,
				Int64GetDatum(rtid));
	/*
	 * Notice that these next two scankeys do not use equality, which
	 * explains why RDA uses a btree, not a hash index. These extra
	 * scan keys don't do much until the xmin to xmax gap is large,
	 * when we might get 1000s of tuples. In that case, this is intended
	 * to avoid scanning the rda heap for all of those rows.
	 */
	ScanKeyEntryInitialize(&skey[2],
				SK_BT_REQFWD,
				RDA_COLUMN_ROW_XMIN,
				BTLessStrategyNumber,			/* < */
				InvalidOid,
				C_COLLATION_OID,
				F_OIDLT,
				ObjectIdGetDatum(snapshot->xmax));
	ScanKeyEntryInitialize(&skey[3],
				SK_BT_REQBKWD,
				RDA_COLUMN_ROW_XMAX,
				BTGreaterEqualStrategyNumber,	/* >= */
				InvalidOid,
				C_COLLATION_OID,
				F_OIDGE,
				ObjectIdGetDatum(snapshot->xmin));

#ifdef MAKE_VISIBILITY_INDEXABLE

	/*
	 * We could make the tests for XidInMVCCSnapshot() indexable, which
	 * would require us to pass the snapshot as a parameter for the
	 * indexscan. If there are none or few rows in the RDA index for the
	 * snapshot then this will be a loss, but if there are many rows in
	 * the RDA index for the snapshot it could be a good win. The critical
	 * factor is the number of transactions between snap->xmin and
	 * snap->xmax, and how many of those generate RDA entries.
	 *
	 * XXX Not yet sure whether to optimize this one way or the other, or
	 * invent some flexible scheme. For now, just accept visibility is
	 * not indexable because it is more work, which means the cost of
	 * retrieving data for older snapshots will increase with the age
	 * of the snapshot and the frequency of update of the table.
	 *
	 * Set up scan keys for XidInMVCCSnapshot tests, as yet unfinished.
	 *
	 * These would require a lot of pushups to get them to be accepted
	 * as indexable conditions by the optimizer, so just hardwire them.
	 */
	if (!OidIsValid(f4oid))
	{
		ftup = SearchSysCache3(PROCNAMEARGSNSP,
								PointerGetDatum(procedureName),
								PointerGetDatum(parameterTypes),
								ObjectIdGetDatum(procNamespace));
		if (HeapTupleIsValid(ftup))
		{
			Form_pg_proc fproc = (Form_pg_proc) GETSTRUCT(ftup);
			f4oid = fproc->oid;
		}
		else
			elog(ERROR, "function unavailable");
	}
	ScanKeyEntryInitialize(&skey[4],
				0,								/* doesnt start/stop scan */
				RDA_COLUMN_ROW_XMIN,
				BTEqalStrategyNumber,
				InvalidOid,
				C_COLLATION_OID,
				f4oid,
				ObjectIdGetDatum(snapshot));

	if (!OidIsValid(f5oid))
	{
		ftup = SearchSysCache3(PROCNAMEARGSNSP,
								PointerGetDatum(procedureName),
								PointerGetDatum(parameterTypes),
								ObjectIdGetDatum(procNamespace));
		if (HeapTupleIsValid(ftup))
		{
			fForm_pg_proc fproc = (Form_pg_proc) GETSTRUCT(ftup);
			f5oid = fproc->oid;
		}
		else
			elog(ERROR, "function unavailable");
	}
	ScanKeyEntryInitialize(&skey[5],
				0,								/* doesnt start/stop scan */
				RDA_COLUMN_ROW_XMAX,
				BTEqualStrategyNumber,
				InvalidOid,
				C_COLLATION_OID,
				f5oid,
				ObjectIdGetDatum(snapshot));
#endif

	/*
	 * Start searching for a visible row in the most recent partition,
	 * then work backwards. Better ideas welcome, but partitioning is
	 * critical to being able to release space from RDA regularly
	 * and using the simple mechanism of truncation.
	 */
	partition_num = RDA_xmax_get_partition_num(snapshot->xmax);
	partition_min = RDA_xmax_get_partition_num(snapshot->xmin);

	if (unlikely(debug))
		elog(NOTICE, "rda_get() snap->xmin %u snap->xmax %u",
						snapshot->xmin, snapshot->xmax);

	for (;;)
	{
		Relation	rda_rel = NULL;
		Relation	rda_idx = NULL;
		IndexInfo	*rda_idx_info = NULL;
		IndexScanDesc rda_scan;
		TupleTableSlot *slot = NULL;
		TupleDesc	rda_desc = NULL;
		RangeVar   	*rv;
		bool		shouldFree = false;
		bool		isnull = false;
		MemoryContext tmpcontext;
		MemoryContext oldcontext;
		uint32		ncandidates = 0;
		List       *indexoidlist;
		ListCell   *l;
		bool		found = false;
		bool		tuple_has_data = false;

		tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
											"rda_get workspace",
											ALLOCSET_DEFAULT_SIZES);
		oldcontext = MemoryContextSwitchTo(tmpcontext);

		/* open partition */
		snprintf(rda_partition_name, RDA_NAME_LENGTH, "rda_p%04d", partition_num);
		rv = makeRangeVar(RDA_SCHEMA_NAME, rda_partition_name, -1);

		rda_rel = table_openrv(rv, AccessShareLock);

		/* Find the index id for this partition */
		indexoidlist = RelationGetIndexList(rda_rel);

		/*
		 * RDA should have one valid index.
		 *
		 * Since we only do inserts and we regularly drop partitions, the
		 * index will never need a REINDEX, but we can't actually prevent
		 * someone from reindexing it. So take the last index entry.
		 */
		foreach(l, indexoidlist)
		{
			Oid			rda_partition_idx_oid;
			Relation	rel;

			rda_partition_idx_oid = lfirst_oid(l);
			rel = index_open(rda_partition_idx_oid, AccessShareLock);

			/*
			 * Use index only if it is fully ready.
			 */
			if (!rel->rd_index->indisready)
			{
				index_close(rel, AccessShareLock);
				continue;
			}
			if (rda_idx)
				index_close(rda_idx, AccessShareLock);
			rda_idx = rel;

			if (rda_idx_info)
				pfree(rda_idx_info);
			rda_idx_info = palloc(sizeof(IndexInfo *));
			rda_idx_info = BuildIndexInfo(rda_idx);

			/*
			 * Check the index for validity
			 */
			if (rda_idx_info->ii_NumIndexKeyAttrs != RDA_NUM_SCANKEYS ||
				rda_idx_info->ii_Expressions != NIL)
				continue;

			/* Note that we don't need speculative index info */

			pfree(rda_idx_info);
		}
		if (rda_idx == NULL)
			elog(ERROR, "could not open valid RDA index");
		list_free(indexoidlist);

		/*
		 * Kluge, since RDA index is not a normal user index, so we don't want to
		 * throw serializable errors if people read data touched by others.
		 */
		rda_idx->rd_indam->ampredlocks = false;

		slot = table_slot_create(rda_rel, NULL);

		rda_scan = index_beginscan(rda_rel, rda_idx, snapshot, RDA_NUM_SCANKEYS, 0);
		rda_scan->xs_want_itup = true;
		index_rescan(rda_scan, skey, RDA_NUM_SCANKEYS, NULL, 0);

		rda_desc = RelationGetDescr(rda_rel);

		/* Scan until we find a row that matches scankeys */
		while (index_getnext_slot(rda_scan, ForwardScanDirection, slot))
		{
			TransactionId	row_xmin;
			TransactionId	row_xmax;
			Datum			d;

			rda_tuple = ExecFetchSlotHeapTuple(slot, false, NULL);

			/*
			 * Don't deform the whole RDA tuple because it might be large
			 * and we don't yet know if this candidate tuple is visible.
			 */
			d = heap_getattr(rda_tuple,
								RDA_COLUMN_ROW_XMIN,
								rda_desc,
								&isnull);
			Assert(!isnull);
			row_xmin = (TransactionId) DatumGetObjectId(d);

			d = heap_getattr(rda_tuple,
								RDA_COLUMN_ROW_XMAX,
								rda_desc,
								&isnull);
			Assert(!isnull);
			row_xmax = (TransactionId) DatumGetObjectId(d);

			ncandidates++;

			if (unlikely(debug))
				elog(NOTICE, " candidate row with row_xmin %u and row_xmax %u", row_xmin, row_xmax);

			/*
			 * Each row in the RDA represents a tuple in a source table.
			 * The RDA rows retrieved here are visible to this scan,
			 * but the source table's row_xmin/row_xmax, stored as RDA columns,
			 * need to be tested for visibility. Confused??
			 */
			if (XidInMVCCSnapshot(row_xmax, snapshot) &&
				!XidInMVCCSnapshot(row_xmin, snapshot))
			{
				rda_tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);
				found = true;
				break;
			}
			else
				rda_tuple = NULL;	/* Not sure if this leaks? */

			/*
			 * When a snapshot is in use for hours, xmin to xmax could
			 * be very wide, so potentially the number of rows archived
			 * could be large. Make sure we don't do a tight loop.
			 */
			CHECK_FOR_INTERRUPTS();
		}

		/*
		 * If we found a tuple, let's unpack it and return victorious.
		 */
		if (rda_tuple)
		{
			bool		isnull;
			Datum		d;

			if (unlikely(debug))
				elog(NOTICE, "  rda_get() found visible row");

			/*
			 * Copy the data from the RDA row_data column into a tuple that can
			 * be returned directly to the caller.
			 */
			d = heap_getattr(rda_tuple,
								RDA_COLUMN_ROW_DATA,
								rda_desc,
								&isnull);

			/*
			 * This shouldn't happen, but we don't want to ERROR here
			 */
			if (!isnull)
			{
				Size len, tlen;

				len = MAXALIGN(sizeof(LeopardTupleHeader));
				tlen = VARSIZE_ANY_EXHDR(DatumGetPointer(d));
				len += MAXALIGN(tlen);

				/*
				 * Assemble the result tuple, copying the original row back from RDA
				 */
				leopardTuple = MemoryContextAllocZero(oldcontext, (uint32) len);
				leopardTuple->t_data = (LeopardTupleHeader) (leopardTuple + MAXALIGN(sizeof(LeopardTupleHeader)));
				memcpy(leopardTuple->t_data, VARDATA(d), tlen);
				leopardTuple->t_len = MAXALIGN(tlen);

				/*
				 * Set the tuple fields
				 */
				leopardTuple->t_tableOid = toid;
				ItemPointerSetInvalid(&leopardTuple->t_self);

				tuple_has_data = true;
			}
		}
		else if (ncandidates > 0 && unlikely(debug))
			elog(NOTICE, "  scan of %s found %u candidates, none visible",
							rda_partition_name,
							ncandidates);
		else if (unlikely(debug))
			elog(NOTICE, "  scan of %s found 0 candidates",
							rda_partition_name);

		/* end scan */
		index_endscan(rda_scan);
		ExecDropSingleTupleTableSlot(slot);

		/* close and release locks */
		index_close(rda_idx, AccessShareLock);
		table_close(rda_rel, AccessShareLock);

		MemoryContextSwitchTo(oldcontext);
		MemoryContextDelete(tmpcontext);

		if (found)
			return tuple_has_data;

		/*
		 * If we've searched the lowest partition and still not found a row
		 * then we're done. Hopefully, most searches end after just one partition.
		 */
		if (partition_num == partition_min)
			break;

		/*
		 * Move backwards in time until we hit the earliest partition, since we
		 * don't have any more intelligent way of searching.
		 *
		 * xids are circular so wrap correctly at 0 back to highest partition num
		 */
		if (partition_num == 0)
			partition_num  = RDA_NUM_PARTITIONS - 1;
		else
			partition_num--;

		CHECK_FOR_INTERRUPTS();
	}

	return false;
}

/*
 * rda_trim
 *
 * Examines all partitions of the RDA and truncates any partitions where
 * all rows are now DEAD (as opposed to RECENTLY_DEAD, as they are at insert).
 *
 * Intended for use by regular or occasional administrative users to allow
 * disk space to be recovered, or as a last resort by foreground processes
 * that need to clear space for further inserts into the RDA.
 */
void
rda_trim(void)
{
	Relation	rda_rel;
	TransactionId xmin;
	RangeVar   	*rv;
	char        rda_partition_name[RDA_NAME_LENGTH];
	uint32		i;

	/*
	 * Find the Oldest Xmin to use for removing rows from RDA, by
	 * looking at the first partition.
	 */
	rv = makeRangeVar(RDA_SCHEMA_NAME, "rda_p0000", -1);
	rda_rel = table_openrv(rv, AccessShareLock);
	xmin = GetOldestNonRemovableTransactionId(rda_rel);
	table_close(rda_rel, AccessShareLock);

	for (i=0; i < RDA_NUM_PARTITIONS; i++)
	{
		TransactionId lobound = (i << RDA_PARTITION_SHIFT);
		TransactionId hibound = (i << RDA_PARTITION_SHIFT) + ((1 << RDA_PARTITION_SHIFT) - 1);

		if (TransactionIdPrecedes(lobound, xmin) &&
			TransactionIdPrecedes(hibound, xmin))
		{

			snprintf(rda_partition_name, RDA_NAME_LENGTH, "rda_p%04d", i);
			rv = makeRangeVar(RDA_SCHEMA_NAME, rda_partition_name, -1);
			rda_rel = table_openrv(rv, AccessShareLock);

			/* check if table has any blocks, if so, truncate it */
			if (RelationGetNumberOfBlocks(rda_rel) > 0)
			{
				elog(LOG, "truncating %s (%u, %u) because xmin=%u", rda_partition_name, lobound, hibound, xmin);
				table_relation_nontransactional_truncate(rda_rel);
			}

			table_close(rda_rel, AccessShareLock);
		}
	}
}
