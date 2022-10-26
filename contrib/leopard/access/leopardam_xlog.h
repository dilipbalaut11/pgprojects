/*-------------------------------------------------------------------------
 *
 * leopardam_xlog.h
 *	  POSTGRES leopard access XLOG definitions.
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
 * src/include/access/leopardam_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LEOPARDAM_XLOG_H
#define LEOPARDAM_XLOG_H

#include "access/leopardtup.h"
#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/buf.h"
#include "storage/bufpage.h"
#include "storage/relfilenode.h"
#include "utils/relcache.h"
#include "access/leopard_to_heap_map.h"


/*
 * WAL record definitions for leopardam.c's WAL operations
 *
 * XLOG allows to store some information in high 4 bits of log
 * record xl_info field.  We use 3 for opcode and one for init bit.
 */
#define XLOG_LEOPARD_INSERT		0x00
#define XLOG_LEOPARD_DELETE		0x10
#define XLOG_LEOPARD_UPDATE		0x20
#define XLOG_LEOPARD_TRUNCATE		0x30
#define XLOG_LEOPARD_HOT_UPDATE	0x40
#define XLOG_LEOPARD_CONFIRM		0x50
#define XLOG_LEOPARD_LOCK			0x60
#define XLOG_LEOPARD_INPLACE		0x70

#define XLOG_LEOPARD_OPMASK		0x70
/*
 * When we insert 1st item on new page in INSERT, UPDATE, HOT_UPDATE,
 * or MULTI_INSERT, we can (and we do) restore entire page in redo
 */
#define XLOG_LEOPARD_INIT_PAGE		0x80
/*
 * We ran out of opcodes, so leopardam.c now has a second RmgrId.  These opcodes
 * are associated with RM_LEOPARD2_ID, but are not logically different from
 * the ones above associated with RM_LEOPARD_ID.  XLOG_LEOPARD_OPMASK applies to
 * these, too.
 */
#define XLOG_LEOPARD2_REWRITE		0x00
#define XLOG_LEOPARD2_PRUNE		0x10
#define XLOG_LEOPARD2_VACUUM		0x20
#define XLOG_LEOPARD2_FREEZE_PAGE	0x30
#define XLOG_LEOPARD2_VISIBLE		0x40
#define XLOG_LEOPARD2_MULTI_INSERT 0x50
#define XLOG_LEOPARD2_LOCK_UPDATED 0x60
#define XLOG_LEOPARD2_NEW_CID		0x70

/*
 * xl_leopard_insert/xl_leopard_multi_insert flag values, 8 bits are available.
 */
/* PD_ALL_VISIBLE was cleared */
#define XLLEOPARD_INSERT_ALL_VISIBLE_CLEARED			(1<<0)
#define XLLEOPARD_INSERT_LAST_IN_MULTI				(1<<1)
#define XLLEOPARD_INSERT_IS_SPECULATIVE				(1<<2)
#define XLLEOPARD_INSERT_CONTAINS_NEW_TUPLE			(1<<3)
#define XLLEOPARD_INSERT_ON_TOAST_RELATION			(1<<4)

/* all_frozen_set always implies all_visible_set */
#define XLLEOPARD_INSERT_ALL_FROZEN_SET				(1<<5)

/*
 * xl_leopard_update flag values, 8 bits are available.
 */
/* PD_ALL_VISIBLE was cleared */
#define XLLEOPARD_UPDATE_OLD_ALL_VISIBLE_CLEARED		(1<<0)
/* PD_ALL_VISIBLE was cleared in the 2nd page */
#define XLLEOPARD_UPDATE_NEW_ALL_VISIBLE_CLEARED		(1<<1)
#define XLLEOPARD_UPDATE_CONTAINS_OLD_TUPLE			(1<<2)
#define XLLEOPARD_UPDATE_CONTAINS_OLD_KEY				(1<<3)
#define XLLEOPARD_UPDATE_CONTAINS_NEW_TUPLE			(1<<4)
#define XLLEOPARD_UPDATE_PREFIX_FROM_OLD				(1<<5)
#define XLLEOPARD_UPDATE_SUFFIX_FROM_OLD				(1<<6)

/* convenience macro for checking whether any form of old tuple was logged */
#define XLH_UPDATE_CONTAINS_OLD						\
	(XLLEOPARD_UPDATE_CONTAINS_OLD_TUPLE | XLLEOPARD_UPDATE_CONTAINS_OLD_KEY)

/*
 * xl_leopard_delete flag values, 8 bits are available.
 */
/* PD_ALL_VISIBLE was cleared */
#define XLLEOPARD_DELETE_ALL_VISIBLE_CLEARED			(1<<0)
#define XLLEOPARD_DELETE_CONTAINS_OLD_TUPLE			(1<<1)
#define XLLEOPARD_DELETE_CONTAINS_OLD_KEY				(1<<2)
#define XLLEOPARD_DELETE_IS_SUPER						(1<<3)
#define XLLEOPARD_DELETE_IS_PARTITION_MOVE			(1<<4)

/* convenience macro for checking whether any form of old tuple was logged */
#define XLH_DELETE_CONTAINS_OLD						\
	(XLLEOPARD_DELETE_CONTAINS_OLD_TUPLE | XLLEOPARD_DELETE_CONTAINS_OLD_KEY)

/* This is what we need to know about delete */
typedef struct xl_leopard_delete
{
	TransactionId xmax;			/* xmax of the deleted tuple */
	OffsetNumber offnum;		/* deleted tuple's offset */
	uint8		infobits_set;	/* infomask bits */
	uint8		flags;
} xl_leopard_delete;

#define SizeOfLeopardDelete	(offsetof(xl_leopard_delete, flags) + sizeof(uint8))

/*
 * xl_leopard_truncate flag values, 8 bits are available.
 */
#define XLLEOPARD_TRUNCATE_CASCADE					(1<<0)
#define XLLEOPARD_TRUNCATE_RESTART_SEQS				(1<<1)

/*
 * For truncate we list all truncated relids in an array, followed by all
 * sequence relids that need to be restarted, if any.
 * All rels are always within the same database, so we just list dbid once.
 */
typedef struct xl_leopard_truncate
{
	Oid			dbId;
	uint32		nrelids;
	uint8		flags;
	Oid			relids[FLEXIBLE_ARRAY_MEMBER];
} xl_leopard_truncate;

#define SizeOfLeopardTruncate	(offsetof(xl_leopard_truncate, relids))

/*
 * We don't store the whole fixed part (LeopardTupleHeaderData) of an inserted
 * or updated tuple in WAL; we can save a few bytes by reconstructing the
 * fields that are available elsewhere in the WAL record, or perhaps just
 * plain needn't be reconstructed.  These are the fields we must store.
 */
typedef struct xl_leopard_header
{
	uint16		t_infomask2;
	uint16		t_infomask;
	uint8		t_hoff;
} xl_leopard_header;

#define SizeOfLeopardHeader	(offsetof(xl_leopard_header, t_hoff) + sizeof(uint8))

/* This is what we need to know about insert */
typedef struct xl_leopard_insert
{
	OffsetNumber offnum;		/* inserted tuple's offset */
	uint8		flags;

	/* xl_leopard_header & TUPLE DATA in backup block 0 */
} xl_leopard_insert;

#define SizeOfLeopardInsert	(offsetof(xl_leopard_insert, flags) + sizeof(uint8))

/*
 * This is what we need to know about a multi-insert.
 *
 * The main data of the record consists of this xl_leopard_multi_insert header.
 * 'offsets' array is omitted if the whole page is reinitialized
 * (XLOG_LEOPARD_INIT_PAGE).
 *
 * In block 0's data portion, there is an xl_multi_insert_tuple struct,
 * followed by the tuple data for each tuple. There is padding to align
 * each xl_multi_insert_tuple struct.
 */
typedef struct xl_leopard_multi_insert
{
	uint8		flags;
	uint16		ntuples;
	OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER];
} xl_leopard_multi_insert;

#define SizeOfLeopardMultiInsert	offsetof(xl_leopard_multi_insert, offsets)

typedef struct xl_multi_insert_tuple
{
	uint16		datalen;		/* size of tuple data that follows */
	uint16		t_infomask2;
	uint16		t_infomask;
	uint8		t_hoff;
	/* TUPLE DATA FOLLOWS AT END OF STRUCT */
} xl_multi_insert_tuple;

#define SizeOfMultiInsertTuple	(offsetof(xl_multi_insert_tuple, t_hoff) + sizeof(uint8))

/*
 * This is what we need to know about update|hot_update
 *
 * Backup blk 0: new page
 *
 * If XLLEOPARD_UPDATE_PREFIX_FROM_OLD or XLLEOPARD_UPDATE_SUFFIX_FROM_OLD flags are set,
 * the prefix and/or suffix come first, as one or two uint16s.
 *
 * After that, xl_leopard_header and new tuple data follow.  The new tuple
 * data doesn't include the prefix and suffix, which are copied from the
 * old tuple on replay.
 *
 * If XLLEOPARD_UPDATE_CONTAINS_NEW_TUPLE flag is given, the tuple data is
 * included even if a full-page image was taken.
 *
 * Backup blk 1: old page, if different. (no data, just a reference to the blk)
 */
typedef struct xl_leopard_update
{
	TransactionId old_xmax;		/* xmax of the old tuple */
	OffsetNumber old_offnum;	/* old tuple's offset */
	uint8		old_infobits_set;	/* infomask bits to set on old tuple */
	uint8		flags;
	TransactionId new_xmax;		/* xmax of the new tuple */
	OffsetNumber new_offnum;	/* new tuple's offset */

	/*
	 * If XLLEOPARD_UPDATE_CONTAINS_OLD_TUPLE or XLLEOPARD_UPDATE_CONTAINS_OLD_KEY flags
	 * are set, xl_leopard_header and tuple data for the old tuple follow.
	 */
} xl_leopard_update;

#define SizeOfLeopardUpdate	(offsetof(xl_leopard_update, new_offnum) + sizeof(OffsetNumber))

/*
 * This is what we need to know about page pruning (both during VACUUM and
 * during opportunistic pruning)
 *
 * The array of OffsetNumbers following the fixed part of the record contains:
 *	* for each redirected item: the item offset, then the offset redirected to
 *	* for each now-dead item: the item offset
 *	* for each now-unused item: the item offset
 * The total number of OffsetNumbers is therefore 2*nredirected+ndead+nunused.
 * Note that nunused is not explicitly stored, but may be found by reference
 * to the total record length.
 *
 * Requires a super-exclusive lock.
 */
typedef struct xl_leopard_prune
{
	TransactionId latestRemovedXid;
	uint16		nredirected;
	uint16		ndead;
	/* OFFSET NUMBERS are in the block reference 0 */
} xl_leopard_prune;

#define SizeOfLeopardPrune (offsetof(xl_leopard_prune, ndead) + sizeof(uint16))

/*
 * The vacuum page record is similar to the prune record, but can only mark
 * already dead items as unused
 *
 * Used by leopard vacuuming only.  Does not require a super-exclusive lock.
 */
typedef struct xl_leopard_vacuum
{
	uint16		nunused;
	/* OFFSET NUMBERS are in the block reference 0 */
} xl_leopard_vacuum;

#define SizeOfLeopardVacuum (offsetof(xl_leopard_vacuum, nunused) + sizeof(uint16))

/* flags for infobits_set */
#define XLHL_XMAX_IS_MULTI		0x01
#define XLHL_XMAX_LOCK_ONLY		0x02
#define XLHL_XMAX_EXCL_LOCK		0x04
#define XLHL_XMAX_KEYSHR_LOCK	0x08
#define XLHL_KEYS_UPDATED		0x10

/* flag bits for xl_leopard_lock / xl_leopard_lock_updated's flag field */
#define XLLEOPARD_LOCK_ALL_FROZEN_CLEARED		0x01

/* This is what we need to know about lock */
typedef struct xl_leopard_lock
{
	TransactionId locking_xid;	/* might be a MultiXactId not xid */
	OffsetNumber offnum;		/* locked tuple's offset on page */
	int8		infobits_set;	/* infomask and infomask2 bits to set */
	uint8		flags;			/* XLH_LOCK_* flag bits */
} xl_leopard_lock;

#define SizeOfLeopardLock	(offsetof(xl_leopard_lock, flags) + sizeof(int8))

/* This is what we need to know about locking an updated version of a row */
typedef struct xl_leopard_lock_updated
{
	TransactionId xmax;
	OffsetNumber offnum;
	uint8		infobits_set;
	uint8		flags;
} xl_leopard_lock_updated;

#define SizeOfLeopardLockUpdated	(offsetof(xl_leopard_lock_updated, flags) + sizeof(uint8))

/* This is what we need to know about confirmation of speculative insertion */
typedef struct xl_leopard_confirm
{
	OffsetNumber offnum;		/* confirmed tuple's offset on page */
} xl_leopard_confirm;

#define SizeOfLeopardConfirm	(offsetof(xl_leopard_confirm, offnum) + sizeof(OffsetNumber))

/* This is what we need to know about in-place update */
typedef struct xl_leopard_inplace
{
	OffsetNumber offnum;		/* updated tuple's offset on page */
	/* TUPLE DATA FOLLOWS AT END OF STRUCT */
} xl_leopard_inplace;

#define SizeOfLeopardInplace	(offsetof(xl_leopard_inplace, offnum) + sizeof(OffsetNumber))

/*
 * This struct represents a 'freeze plan', which is what we need to know about
 * a single tuple being frozen during vacuum.
 */
/* 0x01 was XLH_FREEZE_XMIN */
/* 0x02 was XLLEOPARD_FREEZE_XVAC */
/* 0x04 was XLLEOPARD_INVALID_XVAC */

typedef struct xl_leopard_freeze_tuple
{
	TransactionId xmax;
	OffsetNumber offset;
	uint16		t_infomask2;
	uint16		t_infomask;
	uint8		frzflags;
} xl_leopard_freeze_tuple;

/*
 * This is what we need to know about a block being frozen during vacuum
 *
 * Backup block 0's data contains an array of xl_leopard_freeze_tuple structs,
 * one for each tuple.
 */
typedef struct xl_leopard_freeze_page
{
	TransactionId cutoff_xid;
	uint16		ntuples;
} xl_leopard_freeze_page;

#define SizeOfLeopardFreezePage (offsetof(xl_leopard_freeze_page, ntuples) + sizeof(uint16))

/*
 * This is what we need to know about setting a visibility map bit
 *
 * Backup blk 0: visibility map buffer
 * Backup blk 1: leopard buffer
 */
typedef struct xl_leopard_visible
{
	TransactionId cutoff_xid;
	uint8		flags;
} xl_leopard_visible;

#define SizeOfLeopardVisible (offsetof(xl_leopard_visible, flags) + sizeof(uint8))

typedef struct xl_leopard_new_cid
{
	/*
	 * store toplevel xid so we don't have to merge cids from different
	 * transactions
	 */
	TransactionId top_xid;
	CommandId	cmin;
	CommandId	cmax;
	CommandId	combocid;		/* just for debugging */

	/*
	 * Store the relfilenode/ctid pair to facilitate lookups.
	 */
	RelFileNode target_node;
	ItemPointerData target_tid;
} xl_leopard_new_cid;

#define SizeOfLeopardNewCid (offsetof(xl_leopard_new_cid, target_tid) + sizeof(ItemPointerData))

/* logical rewrite xlog record header */
typedef struct xl_leopard_rewrite_mapping
{
	TransactionId mapped_xid;	/* xid that might need to see the row */
	Oid			mapped_db;		/* DbOid or InvalidOid for shared rels */
	Oid			mapped_rel;		/* Oid of the mapped relation */
	off_t		offset;			/* How far have we written so far */
	uint32		num_mappings;	/* Number of in-memory mappings */
	XLogRecPtr	start_lsn;		/* Insert LSN at begin of rewrite */
} xl_leopard_rewrite_mapping;

extern void LeopardTupleHeaderAdvanceLatestRemovedXid(LeopardTupleHeader tuple,
												   TransactionId *latestRemovedXid);

extern void leopard_redo(XLogReaderState *record);
extern void leopard_desc(StringInfo buf, XLogReaderState *record);
extern const char *leopard_identify(uint8 info);
extern void leopard_mask(char *pagedata, BlockNumber blkno);
extern void leopard2_redo(XLogReaderState *record);
extern void leopard2_desc(StringInfo buf, XLogReaderState *record);
extern const char *leopard2_identify(uint8 info);
extern void leopard_xlog_logical_rewrite(XLogReaderState *r);

extern XLogRecPtr log_leopard_freeze(Relation reln, Buffer buffer,
								  TransactionId cutoff_xid, xl_leopard_freeze_tuple *tuples,
								  int ntuples);
extern bool leopard_prepare_freeze_tuple(LeopardTupleHeader tuple,
									  TransactionId relfrozenxid,
									  TransactionId relminmxid,
									  TransactionId cutoff_xid,
									  TransactionId cutoff_multi,
									  xl_leopard_freeze_tuple *frz,
									  bool *totally_frozen);
extern void leopard_execute_freeze_tuple(LeopardTupleHeader tuple,
									  xl_leopard_freeze_tuple *xlrec_tp);
extern XLogRecPtr log_leopard_visible(RelFileNode rnode, Buffer leopard_buffer,
								   Buffer vm_buffer, TransactionId cutoff_xid, uint8 flags);

#define RM_LEOPARD2_ID RM_HEAP2_ID


#define RM_LEOPARD_ID RM_HEAP_ID


#endif							/* LEOPARDAM_XLOG_H */
