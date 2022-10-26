/*-------------------------------------------------------------------------
 *
 * leopardam.h
 *	  POSTGRES leopard access method definitions.
 *
 *
 * Portions Copyright (c) 2022, EnterpriseDB Corporation. All Rights Reserved.
 *
 *
 *
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/leopardam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LEOPARDAM_H
#define LEOPARDAM_H

#include "access/relation.h"	/* for backward compatibility */
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/skey.h"
#include "access/table.h"		/* for backward compatibility */
#include "access/tableam.h"
#include "nodes/lockoptions.h"
#include "nodes/primnodes.h"
#include "storage/bufpage.h"
#include "storage/dsm.h"
#include "storage/lockdefs.h"
#include "storage/shm_toc.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"
#include "access/genam.h"
#include "access/leopardtup.h"
#include "access/leopardtup_details.h"
#include "access/leopard_to_heap_map.h"
#include "utils/inval.h"
#include "utils/tuplesort.h"

extern bool leopard_debug;
#define LEOPARD_DEBUG   unlikely(leopard_debug)

/* "options" flag bits for leopard_insert */
#define LEOPARD_INSERT_SKIP_FSM	TABLE_INSERT_SKIP_FSM
#define LEOPARD_INSERT_FROZEN		TABLE_INSERT_FROZEN
#define LEOPARD_INSERT_NO_LOGICAL	TABLE_INSERT_NO_LOGICAL
#define LEOPARD_INSERT_SPECULATIVE 0x0010

typedef struct BulkInsertStateData *BulkInsertState;
struct TupleTableSlot;

#define MaxLockTupleMode	LockTupleExclusive

/*
 * Descriptor for leopard table scans.
 */
typedef struct LeopardScanDescData
{
	TableScanDescData rs_base;	/* AM independent part of the descriptor */

	/* state set up at leopard_initscan time */
	BlockNumber rs_nblocks;		/* total number of blocks in rel */
	BlockNumber rs_startblock;	/* block # to start at */
	BlockNumber rs_numblocks;	/* max number of blocks to scan */
	/* rs_numblocks is usually InvalidBlockNumber, meaning "scan whole rel" */

	/* scan current state */
	bool		rs_inited;		/* false = scan not init'd yet */
	BlockNumber rs_cblock;		/* current block # in scan, if any */
	Buffer		rs_cbuf;		/* current buffer in scan, if any */
	/* NB: if rs_cbuf is not InvalidBuffer, we hold a pin on that buffer */

	/* rs_numblocks is usually InvalidBlockNumber, meaning "scan whole rel" */
	BufferAccessStrategy rs_strategy;	/* access strategy for reads */

	LeopardTupleData rs_ctup;		/* current tuple in scan, if any */

	/*
	 * For parallel scans to store page allocation data.  NULL when not
	 * performing a parallel scan.
	 */
	ParallelBlockTableScanWorkerData *rs_parallelworkerdata;

	/* these fields only used in page-at-a-time mode and for bitmap scans */
	int			rs_cindex;		/* current tuple's index in vistuples */
	int			rs_ntuples;		/* number of visible tuples on page */
	OffsetNumber rs_vistuples[MaxLeopardTuplesPerPage];	/* their offsets */
}			LeopardScanDescData;
typedef struct LeopardScanDescData *LeopardScanDesc;

/*
 * Descriptor for fetches from leopard via an index.
 */
typedef struct IndexFetchLeopardData
{
	IndexFetchTableData xs_base;	/* AM independent part of the descriptor */

	Buffer		xs_cbuf;		/* current leopard buffer in scan, if any */
	/* NB: if xs_cbuf is not InvalidBuffer, we hold a pin on that buffer */
} IndexFetchLeopardData;

/* Result codes for LeopardTupleSatisfiesVacuum */
typedef enum
{
	LEOPARDTUPLE_DEAD,				/* tuple is dead and deletable */
	LEOPARDTUPLE_LIVE,				/* tuple is live (committed, no deleter) */
	LEOPARDTUPLE_RECENTLY_DEAD,	/* tuple is dead, but not deletable yet */
	LEOPARDTUPLE_INSERT_IN_PROGRESS,	/* inserting xact is still in progress */
	LEOPARDTUPLE_DELETE_IN_PROGRESS	/* deleting xact is still in progress */
} HTSV_Result;

/* ----------------
 *		function prototypes for leopard access method
 *
 * leopard_create, leopard_create_with_catalog, and leopard_drop_with_catalog
 * are declared in catalog/heap.h
 * ----------------
 */

typedef enum LeopardTupleResponseType
{
	LEOPARD_TUPLE_NOT_FOUND = 0,
	LEOPARD_TUPLE_FROM_BUFFER,
	LEOPARD_TUPLE_FROM_RDA
} LeopardTupleResponseType;


/*
 * LeopardScanIsValid
 *		True iff the leopard scan is valid.
 */
#define LeopardScanIsValid(scan) PointerIsValid(scan)

extern TableScanDesc leopard_beginscan(Relation relation, Snapshot snapshot,
									int nkeys, ScanKey key,
									ParallelTableScanDesc parallel_scan,
									uint32 flags);
extern void leopard_setscanlimits(TableScanDesc scan, BlockNumber startBlk,
							   BlockNumber numBlks);
extern void leopardgetpage(TableScanDesc scan, BlockNumber page);
extern void leopard_rescan(TableScanDesc scan, ScanKey key, bool set_params,
						bool allow_strat, bool allow_sync, bool allow_pagemode);
extern void leopard_endscan(TableScanDesc scan);
extern LeopardTuple leopard_getnext(TableScanDesc scan, ScanDirection direction);
extern bool leopard_getnextslot(TableScanDesc sscan,
							 ScanDirection direction, struct TupleTableSlot *slot);
extern void leopard_set_tidrange(TableScanDesc sscan, ItemPointer mintid,
							  ItemPointer maxtid);
extern bool leopard_getnextslot_tidrange(TableScanDesc sscan,
									  ScanDirection direction,
									  TupleTableSlot *slot);
extern bool leopard_fetch(Relation relation, Snapshot snapshot,
					   LeopardTuple tuple, Buffer *userbuf);
extern bool leopard_fetch_extended(Relation relation, Snapshot snapshot,
								LeopardTuple tuple, Buffer *userbuf,
								bool keep_buf);
extern LeopardTupleResponseType leopard_hot_search_buffer(ItemPointer tid, Relation relation,
								   Buffer buffer, Snapshot snapshot, LeopardTuple leopardTuple,
								   bool *all_dead, bool first_call);

extern void leopard_get_latest_tid(TableScanDesc scan, ItemPointer tid);

extern BulkInsertState LeopardGetBulkInsertState(void);
extern void LeopardFreeBulkInsertState(BulkInsertState);
extern void LeopardReleaseBulkInsertStatePin(BulkInsertState bistate);

extern void leopard_insert(Relation relation, LeopardTuple tup, CommandId cid,
						int options, BulkInsertState bistate);
extern void leopard_multi_insert(Relation relation, struct TupleTableSlot **slots,
							  int ntuples, CommandId cid, int options,
							  BulkInsertState bistate);
extern TM_Result leopard_delete(Relation relation, ItemPointer tid,
							 CommandId cid, Snapshot crosscheck, bool wait,
							 struct TM_FailureData *tmfd, bool changingPart);
extern void leopard_finish_speculative(Relation relation, ItemPointer tid);
extern void leopard_abort_speculative(Relation relation, ItemPointer tid);
extern TM_Result leopard_update(Relation relation, ItemPointer otid,
							 LeopardTuple newtup,
							 CommandId cid, Snapshot crosscheck, bool wait,
							 struct TM_FailureData *tmfd, LockTupleMode *lockmode);
extern TM_Result leopard_lock_tuple(Relation relation, LeopardTuple tuple,
								 CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy,
								 bool follow_update,
								 Buffer *buffer, struct TM_FailureData *tmfd);

extern void leopard_inplace_update(Relation relation, LeopardTuple tuple);
extern bool leopard_freeze_tuple(LeopardTupleHeader tuple,
							  TransactionId relfrozenxid, TransactionId relminmxid,
							  TransactionId cutoff_xid, TransactionId cutoff_multi);
extern bool leopard_tuple_needs_freeze(LeopardTupleHeader tuple, TransactionId cutoff_xid,
									MultiXactId cutoff_multi, Buffer buf);
extern bool leopard_tuple_needs_eventual_freeze(LeopardTupleHeader tuple);

extern void simple_leopard_insert(Relation relation, LeopardTuple tup);
extern void simple_leopard_delete(Relation relation, ItemPointer tid);
extern void simple_leopard_update(Relation relation, ItemPointer otid,
							   LeopardTuple tup);

extern TransactionId leopard_index_delete_tuples(Relation rel,
											  TM_IndexDeleteOp *delstate);

/* in leopard/pruneleopard.c */
struct GlobalVisState;
extern void leopard_page_prune_opt(Relation relation, Buffer buffer);
extern int	leopard_page_prune(Relation relation, Buffer buffer,
							struct GlobalVisState *vistest,
							TransactionId old_snap_xmin,
							TimestampTz old_snap_ts_ts,
							bool report_stats,
							OffsetNumber *off_loc);
extern void leopard_page_prune_execute(Buffer buffer,
									OffsetNumber *redirected, int nredirected,
									OffsetNumber *nowdead, int ndead,
									OffsetNumber *nowunused, int nunused, bool do_partial_cleanup);
extern void leopard_get_root_tuples(Page page, OffsetNumber *root_offsets);

extern bool leopard_make_space_for_update(Relation relation, Buffer buffer, Size newtupsize);

/* in leopard/vacuumlazy.c */
struct VacuumParams;
extern void leopard_vacuum_rel(Relation rel,
							struct VacuumParams *params, BufferAccessStrategy bstrategy);
extern void parallel_vacuum_main(dsm_segment *seg, shm_toc *toc);

/* in leopard/leopardam_visibility.c */
extern bool LeopardTupleSatisfiesVisibility(LeopardTuple stup, Snapshot snapshot,
										 Buffer buffer);
extern TM_Result LeopardTupleSatisfiesUpdate(LeopardTuple stup, CommandId curcid,
										  Buffer buffer);
extern HTSV_Result LeopardTupleSatisfiesVacuum(LeopardTuple stup, TransactionId OldestXmin,
											Buffer buffer);
extern HTSV_Result LeopardTupleSatisfiesVacuumHorizon(LeopardTuple stup, Buffer buffer,
												   TransactionId *dead_after);
extern void LeopardTupleSetHintBits(LeopardTupleHeader tuple, Buffer buffer,
								 uint16 infomask, TransactionId xid);
extern bool LeopardTupleHeaderIsOnlyLocked(LeopardTupleHeader tuple);
extern bool XidInMVCCSnapshot(TransactionId xid, Snapshot snapshot);
extern bool LeopardTupleIsSurelyDead(LeopardTuple leopardtup,
								  struct GlobalVisState *vistest);

extern bool LeopardTupleSatisfiesRDA(LeopardTuple leopardtup, Snapshot snapshot);

/*
 * To avoid leaking too much knowledge about reorderbuffer implementation
 * details this is implemented in reorderbuffer.c not leopardam_visibility.c
 */
struct HTAB;
extern bool ResolveCminCmaxDuringDecoding(struct HTAB *tuplecid_data,
										  Snapshot snapshot,
										  LeopardTuple leopardtup,
										  Buffer buffer,
										  CommandId *cmin, CommandId *cmax);
extern void LeopardCheckForSerializableConflictOut(bool valid, Relation relation, LeopardTuple tuple,
												Buffer buffer, Snapshot snapshot);



typedef struct LeopardTupleTableSlot
{
	TupleTableSlot base;

#define FIELDNO_HEAPTUPLETABLESLOT_TUPLE 1
	LeopardTuple	tuple;			/* physical tuple */
#define FIELDNO_HEAPTUPLETABLESLOT_OFF 2
	uint32		off;			/* saved state for slot_deform_heap_tuple */
	LeopardTupleData tupdata;		/* optional workspace for storing tuple */
} LeopardTupleTableSlot;



typedef struct BufferLeopardTupleTableSlot
{
	LeopardTupleTableSlot base;

	/*
	 * If buffer is not InvalidBuffer, then the slot is holding a pin on the
	 * indicated buffer page; drop the pin when we release the slot's
	 * reference to that buffer.  (TTS_FLAG_SHOULDFREE should not be set in
	 * such a case, since presumably tts_tuple is pointing into the buffer.)
	 */
	Buffer		buffer;			/* tuple's buffer, or InvalidBuffer */
} BufferLeopardTupleTableSlot;


static inline void
CacheInvalidateLeopardTuple(Relation relation, LeopardTuple tuple, LeopardTuple newtuple)
{
    CacheInvalidateHeapTuple(relation, (HeapTuple) tuple, (HeapTuple) newtuple);
}

static inline LeopardTuple
ExecFetchSlotLeopardTuple(TupleTableSlot *slot, bool materialize, bool *shouldFree)
{
    return (LeopardTuple)  ExecFetchSlotHeapTuple(slot, materialize, shouldFree);
}

static inline TupleTableSlot *
ExecStoreBufferLeopardTuple(LeopardTuple tuple, TupleTableSlot *slot, Buffer buffer)
{
    return ExecStoreBufferHeapTuple((HeapTuple) tuple, slot, buffer);
}

static inline TupleTableSlot *
ExecStoreLeopardTuple(LeopardTuple tuple, TupleTableSlot *slot, bool shouldFree)
{
    return ExecStoreHeapTuple((HeapTuple) tuple, slot, shouldFree);
}

static inline TupleTableSlot *
ExecStorePinnedBufferLeopardTuple(LeopardTuple tuple, TupleTableSlot *slot, Buffer buffer)
{
    return ExecStorePinnedBufferHeapTuple((HeapTuple) tuple, slot, buffer);
}

static inline void
tuplesort_putleopardtuple(Tuplesortstate *state, LeopardTuple tup)
{
    tuplesort_putheaptuple(state, (HeapTuple) tup);
}

static inline LeopardTuple
tuplesort_getleopardtuple(Tuplesortstate *state, bool forward)
{
    return (LeopardTuple) tuplesort_getheaptuple(state, forward);
}

static inline LeopardTuple
systable_getnext_ordered_leopardtup(SysScanDesc sysscan, ScanDirection direction)
{
    return (LeopardTuple) systable_getnext_ordered(sysscan, direction);
}

static inline CommandId
LeopardTupleHeaderGetCmin(LeopardTupleHeader tup)
{
	return HeapTupleHeaderGetCmin((HeapTupleHeader) tup);
}

static inline CommandId
LeopardTupleHeaderGetCmax(LeopardTupleHeader tup)
{
	return HeapTupleHeaderGetCmax((HeapTupleHeader) tup);
}

static inline void
LeopardTupleHeaderAdjustCmax(LeopardTupleHeader tup, CommandId *cmax, bool *iscombo)
{
	HeapTupleHeaderAdjustCmax((HeapTupleHeader) tup, cmax, iscombo);
}

#endif							/* LEOPARDAM_H */
