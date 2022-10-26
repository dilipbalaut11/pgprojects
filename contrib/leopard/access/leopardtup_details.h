/*-------------------------------------------------------------------------
 *
 * leopardtup_details.h
 *	  POSTGRES leopard tuple header definitions.
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
 * src/include/access/leopardtup_details.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LEOPARDTUP_DETAILS_H
#define LEOPARDTUP_DETAILS_H

#include "access/leopardtup.h"
#include "access/transam.h"
#include "access/tupdesc.h"
#include "access/tupmacs.h"
#include "storage/bufpage.h"
#include "access/htup_details.h"

/*
 * MaxTupleAttributeNumber limits the number of (user) columns in a tuple.
 * The key limit on this value is that the size of the fixed overhead for
 * a tuple, plus the size of the null-values bitmap (at 1 bit per column),
 * plus MAXALIGN alignment, must fit into t_hoff which is uint8.  On most
 * machines the upper limit without making t_hoff wider would be a little
 * over 1700.  We use round numbers here and for MaxLeopardAttributeNumber
 * so that alterations in LeopardTupleHeaderData layout won't change the
 * supported max number of columns.
 */
#define MaxTupleAttributeNumber 1664	/* 8 * 208 */

/*
 * MaxLeopardAttributeNumber limits the number of (user) columns in a table.
 * This should be somewhat less than MaxTupleAttributeNumber.  It must be
 * at least one less, else we will fail to do UPDATEs on a maximal-width
 * table (because UPDATE has to form working tuples that include CTID).
 * In practice we want some additional daylight so that we can gracefully
 * support operations that add hidden "resjunk" columns, for example
 * SELECT * FROM wide_table ORDER BY foo, bar, baz.
 * In any case, depending on column data types you will likely be running
 * into the disk-block-based limit on overall tuple size if you have more
 * than a thousand or so columns.  TOAST won't help.
 */
#define MaxLeopardAttributeNumber	1600	/* 8 * 200 */

/*
 * Leopard tuple header.  To avoid wasting space, the fields should be
 * laid out in such a way as to avoid structure padding.
 *
 * Datums of composite types (row types) share the same general structure
 * as on-disk tuples, so that the same routines can be used to build and
 * examine them.  However the requirements are slightly different: a Datum
 * does not need any transaction visibility information, and it does need
 * a length word and some embedded type information.  We can achieve this
 * by overlaying the xmin/cmin/xmax/cmax fields of a leopard tuple
 * with the fields needed in the Datum case.  Typically, all tuples built
 * in-memory will be initialized with the Datum fields; but when a tuple is
 * about to be inserted in a table, the transaction fields will be filled,
 * overwriting the datum fields.
 *
 * The overall structure of a leopard tuple looks like:
 *			fixed fields (LeopardTupleHeaderData struct)
 *			nulls bitmap (if LEOPARD_HASNULL is set in t_infomask)
 *			alignment padding (as needed to make user data MAXALIGN'd)
 *			object ID (if LEOPARD_HASOID_OLD is set in t_infomask, not created
 *          anymore)
 *			user data fields
 *
 * We store five "virtual" fields Xmin, Cmin, Xmax, Cmax, and Xvac in three
 * physical fields.  Xmin and Xmax are always really stored, but Cmin, Cmax
 * and Xvac share a field.  This works because we know that Cmin and Cmax
 * are only interesting for the lifetime of the inserting and deleting
 * transaction respectively.  If a tuple is inserted and deleted in the same
 * transaction, we store a "combo" command id that can be mapped to the real
 * cmin and cmax, but only by use of local state within the originating
 * backend.  See combocid.c for more details.  Meanwhile, Xvac is only set by
 * old-style VACUUM FULL, which does not have any command sub-structure and so
 * does not need either Cmin or Cmax.  (This requires that old-style VACUUM
 * FULL never try to move a tuple whose Cmin or Cmax is still interesting,
 * ie, an insert-in-progress or delete-in-progress tuple.)
 *
 * A word about t_ctid: whenever a new tuple is stored on disk, its t_ctid
 * is initialized with its own TID (location).  If the tuple is ever updated,
 * its t_ctid is changed to point to the replacement version of the tuple.  Or
 * if the tuple is moved from one partition to another, due to an update of
 * the partition key, t_ctid is set to a special value to indicate that
 * (see ItemPointerSetMovedPartitions).  Thus, a tuple is the latest version
 * of its row iff XMAX is invalid or
 * t_ctid points to itself (in which case, if XMAX is valid, the tuple is
 * either locked or deleted).  One can follow the chain of t_ctid links
 * to find the newest version of the row, unless it was moved to a different
 * partition.  Beware however that VACUUM might
 * erase the pointed-to (newer) tuple before erasing the pointing (older)
 * tuple.  Hence, when following a t_ctid link, it is necessary to check
 * to see if the referenced slot is empty or contains an unrelated tuple.
 * Check that the referenced tuple has XMIN equal to the referencing tuple's
 * XMAX to verify that it is actually the descendant version and not an
 * unrelated tuple stored into a slot recently freed by VACUUM.  If either
 * check fails, one may assume that there is no live descendant version.
 *
 * t_ctid is sometimes used to store a speculative insertion token, instead
 * of a real TID.  A speculative token is set on a tuple that's being
 * inserted, until the inserter is sure that it wants to go ahead with the
 * insertion.  Hence a token should only be seen on a tuple with an XMAX
 * that's still in-progress, or invalid/aborted.  The token is replaced with
 * the tuple's real TID when the insertion is confirmed.  One should never
 * see a speculative insertion token while following a chain of t_ctid links,
 * because they are not used on updates, only insertions.
 *
 * Following the fixed header fields, the nulls bitmap is stored (beginning
 * at t_bits).  The bitmap is *not* stored if t_infomask shows that there
 * are no nulls in the tuple.  If an OID field is present (as indicated by
 * t_infomask), then it is stored just before the user data, which begins at
 * the offset shown by t_hoff.  Note that t_hoff must be a multiple of
 * MAXALIGN.
 */

typedef struct LeopardTupleFields
{
	TransactionId t_xmin;		/* inserting xact ID */
	TransactionId t_xmax;		/* deleting or locking xact ID */

	CommandId	t_cid;		/* inserting or deleting command ID, or both */
} LeopardTupleFields;



struct LeopardTupleHeaderData
{
	union
	{
		LeopardTupleFields t_leopard;
		DatumTupleFields t_datum;
	}			t_choice;

	ItemPointerData t_ctid;		/* current TID of this or newer tuple (or a
								 * speculative insertion token) */

	/* Fields below here must match MinimalLeopardTupleData! */

#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK2 2
	uint16		t_infomask2;	/* number of attributes + various flags */

#define FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK 3
	uint16		t_infomask;		/* various flag bits, see below */

#define FIELDNO_HEAPTUPLEHEADERDATA_HOFF 4
	uint8		t_hoff;			/* sizeof header incl. bitmap, padding */

	/* ^ - 23 bytes - ^ */

#define FIELDNO_HEAPTUPLEHEADERDATA_BITS 5
	bits8		t_bits[FLEXIBLE_ARRAY_MEMBER];	/* bitmap of NULLs */

	/* MORE DATA FOLLOWS AT END OF STRUCT */
};

/* typedef appears in leopardtup.h */

#define SizeofLeopardTupleHeader offsetof(LeopardTupleHeaderData, t_bits)

/*
 * information stored in t_infomask:
 */
#define LEOPARD_HASNULL			0x0001	/* has null attribute(s) */
#define LEOPARD_HASVARWIDTH		0x0002	/* has variable-width attribute(s) */
#define LEOPARD_HASEXTERNAL		0x0004	/* has external stored attribute(s) */
#define LEOPARD_HASOID_OLD			0x0008	/* has an object-id field */
#define LEOPARD_XMAX_KEYSHR_LOCK	0x0010	/* xmax is a key-shared locker */
#define LEOPARD_COMBOCID			0x0020	/* t_cid is a combo CID */
#define LEOPARD_XMAX_EXCL_LOCK		0x0040	/* xmax is exclusive locker */
#define LEOPARD_XMAX_LOCK_ONLY		0x0080	/* xmax, if valid, is only a locker */

 /* xmax is a shared locker */
#define LEOPARD_XMAX_SHR_LOCK	(LEOPARD_XMAX_EXCL_LOCK | LEOPARD_XMAX_KEYSHR_LOCK)

#define LEOPARD_LOCK_MASK	(LEOPARD_XMAX_SHR_LOCK | LEOPARD_XMAX_EXCL_LOCK | \
						 LEOPARD_XMAX_KEYSHR_LOCK)
#define LEOPARD_XMIN_COMMITTED		0x0100	/* t_xmin committed */
#define LEOPARD_XMIN_INVALID		0x0200	/* t_xmin invalid/aborted */
#define LEOPARD_XMIN_FROZEN		(LEOPARD_XMIN_COMMITTED|LEOPARD_XMIN_INVALID)
#define LEOPARD_XMAX_COMMITTED		0x0400	/* t_xmax committed */
#define LEOPARD_XMAX_INVALID		0x0800	/* t_xmax invalid/aborted */
#define LEOPARD_XMAX_IS_MULTI		0x1000	/* t_xmax is a MultiXactId */
#define LEOPARD_UPDATED			0x2000	/* this is UPDATEd version of row */
/* 0x4000 was LEOPARD_MOVED_OFF */
/* 0x8000 was LEOPARD_MOVED_IN */
#define LEOPARD_XACT_MASK			0x3FF0	/* visibility-related bits */

/*
 * A tuple is only locked (i.e. not updated by its Xmax) if the
 * LEOPARD_XMAX_LOCK_ONLY bit is set; or, for pg_upgrade's sake, if the Xmax is
 * not a multi and the EXCL_LOCK bit is set.
 *
 * See also LeopardTupleHeaderIsOnlyLocked, which also checks for a possible
 * aborted updater transaction.
 *
 * Beware of multiple evaluations of the argument.
 */
#define LEOPARD_XMAX_IS_LOCKED_ONLY(infomask) \
	(((infomask) & LEOPARD_XMAX_LOCK_ONLY) || \
	 (((infomask) & (LEOPARD_XMAX_IS_MULTI | LEOPARD_LOCK_MASK)) == LEOPARD_XMAX_EXCL_LOCK))

/*
 * A tuple that has LEOPARD_XMAX_IS_MULTI and LEOPARD_XMAX_LOCK_ONLY but neither of
 * LEOPARD_XMAX_EXCL_LOCK and LEOPARD_XMAX_KEYSHR_LOCK must come from a tuple that was
 * share-locked in 9.2 or earlier and then pg_upgrade'd.
 *
 * In 9.2 and prior, LEOPARD_XMAX_IS_MULTI was only set when there were multiple
 * FOR SHARE lockers of that tuple.  That set LEOPARD_XMAX_LOCK_ONLY (with a
 * different name back then) but neither of LEOPARD_XMAX_EXCL_LOCK and
 * LEOPARD_XMAX_KEYSHR_LOCK.  That combination is no longer possible in 9.3 and
 * up, so if we see that combination we know for certain that the tuple was
 * locked in an earlier release; since all such lockers are gone (they cannot
 * survive through pg_upgrade), such tuples can safely be considered not
 * locked.
 *
 * We must not resolve such multixacts locally, because the result would be
 * bogus, regardless of where they stand with respect to the current valid
 * multixact range.
 */
#define LEOPARD_LOCKED_UPGRADED(infomask) \
( \
	 ((infomask) & LEOPARD_XMAX_IS_MULTI) != 0 && \
	 ((infomask) & LEOPARD_XMAX_LOCK_ONLY) != 0 && \
	 (((infomask) & (LEOPARD_XMAX_EXCL_LOCK | LEOPARD_XMAX_KEYSHR_LOCK)) == 0) \
)

/*
 * Use these to test whether a particular lock is applied to a tuple
 */
#define LEOPARD_XMAX_IS_SHR_LOCKED(infomask) \
	(((infomask) & LEOPARD_LOCK_MASK) == LEOPARD_XMAX_SHR_LOCK)
#define LEOPARD_XMAX_IS_EXCL_LOCKED(infomask) \
	(((infomask) & LEOPARD_LOCK_MASK) == LEOPARD_XMAX_EXCL_LOCK)
#define LEOPARD_XMAX_IS_KEYSHR_LOCKED(infomask) \
	(((infomask) & LEOPARD_LOCK_MASK) == LEOPARD_XMAX_KEYSHR_LOCK)

/* turn these all off when Xmax is to change */
#define LEOPARD_XMAX_BITS (LEOPARD_XMAX_COMMITTED | LEOPARD_XMAX_INVALID | \
						LEOPARD_XMAX_IS_MULTI | LEOPARD_LOCK_MASK | LEOPARD_XMAX_LOCK_ONLY)

/*
 * information stored in t_infomask2:
 */
#define LEOPARD_NATTS_MASK			0x07FF	/* 11 bits for number of attributes */
/* bits 0x1800 are available */
#define LEOPARD_KEYS_UPDATED		0x2000	/* tuple was updated and key cols
										 * modified, or tuple deleted */
#define LEOPARD_HOT_UPDATED		0x4000	/* tuple was HOT-updated */
#define LEOPARD_ONLY_TUPLE			0x8000	/* this is leopard-only tuple */

#define LEOPARD2_XACT_MASK			0xE000	/* visibility-related bits */

/*
 * LEOPARD_TUPLE_HAS_MATCH is a temporary flag used during hash joins.  It is
 * only used in tuples that are in the hash table, and those don't need
 * any visibility information, so we can overlay it on a visibility flag
 * instead of using up a dedicated bit.
 */
#define LEOPARD_TUPLE_HAS_MATCH	LEOPARD_ONLY_TUPLE /* tuple has a join match */

/*
 * LeopardTupleHeader accessor macros
 *
 * Note: beware of multiple evaluations of "tup" argument.  But the Set
 * macros evaluate their other argument only once.
 */

/*
 * LeopardTupleHeaderGetRawXmin returns the "raw" xmin field, which is the xid
 * originally used to insert the tuple.  However, the tuple might actually
 * be frozen (via LeopardTupleHeaderSetXminFrozen) in which case the tuple's xmin
 * is visible to every snapshot.  Prior to PostgreSQL 9.4, we actually changed
 * the xmin to FrozenTransactionId, and that value may still be encountered
 * on disk.
 */
#define LeopardTupleHeaderGetRawXmin(tup) \
( \
	(tup)->t_choice.t_leopard.t_xmin \
)

#define LeopardTupleHeaderGetXmin(tup) \
( \
	LeopardTupleHeaderXminFrozen(tup) ? \
		FrozenTransactionId : LeopardTupleHeaderGetRawXmin(tup) \
)

#define LeopardTupleHeaderSetXmin(tup, xid) \
( \
	(tup)->t_choice.t_leopard.t_xmin = (xid) \
)

#define LeopardTupleHeaderXminCommitted(tup) \
( \
	((tup)->t_infomask & LEOPARD_XMIN_COMMITTED) != 0 \
)

#define LeopardTupleHeaderXminInvalid(tup) \
( \
	((tup)->t_infomask & (LEOPARD_XMIN_COMMITTED|LEOPARD_XMIN_INVALID)) == \
		LEOPARD_XMIN_INVALID \
)

#define LeopardTupleHeaderXminFrozen(tup) \
( \
	((tup)->t_infomask & (LEOPARD_XMIN_FROZEN)) == LEOPARD_XMIN_FROZEN \
)

#define LeopardTupleHeaderSetXminCommitted(tup) \
( \
	AssertMacro(!LeopardTupleHeaderXminInvalid(tup)), \
	((tup)->t_infomask |= LEOPARD_XMIN_COMMITTED) \
)

#define LeopardTupleHeaderSetXminInvalid(tup) \
( \
	AssertMacro(!LeopardTupleHeaderXminCommitted(tup)), \
	((tup)->t_infomask |= LEOPARD_XMIN_INVALID) \
)

#define LeopardTupleHeaderSetXminFrozen(tup) \
( \
	AssertMacro(!LeopardTupleHeaderXminInvalid(tup)), \
	((tup)->t_infomask |= LEOPARD_XMIN_FROZEN) \
)

/*
 * LeopardTupleHeaderGetRawXmax gets you the raw Xmax field.  To find out the Xid
 * that updated a tuple, you might need to resolve the MultiXactId if certain
 * bits are set.  LeopardTupleHeaderGetUpdateXid checks those bits and takes care
 * to resolve the MultiXactId if necessary.  This might involve multixact I/O,
 * so it should only be used if absolutely necessary.
 */
#define LeopardTupleHeaderGetUpdateXid(tup) \
( \
	(!((tup)->t_infomask & LEOPARD_XMAX_INVALID) && \
	 ((tup)->t_infomask & LEOPARD_XMAX_IS_MULTI) && \
	 !((tup)->t_infomask & LEOPARD_XMAX_LOCK_ONLY)) ? \
		LeopardTupleGetUpdateXid(tup) \
	: \
		LeopardTupleHeaderGetRawXmax(tup) \
)

#define LeopardTupleHeaderGetRawXmax(tup) \
( \
	(tup)->t_choice.t_leopard.t_xmax \
)

#define LeopardTupleHeaderSetXmax(tup, xid) \
( \
	(tup)->t_choice.t_leopard.t_xmax = (xid) \
)

/*
 * LeopardTupleHeaderGetRawCommandId will give you what's in the header whether
 * it is useful or not.  Most code should use LeopardTupleHeaderGetCmin or
 * LeopardTupleHeaderGetCmax instead, but note that those Assert that you can
 * get a legitimate result, ie you are in the originating transaction!
 */
#define LeopardTupleHeaderGetRawCommandId(tup) \
( \
	(tup)->t_choice.t_leopard.t_cid \
)

/* SetCmin is reasonably simple since we never need a combo CID */
#define LeopardTupleHeaderSetCmin(tup, cid) \
do { \
		(tup)->t_choice.t_leopard.t_cid = (cid); \
	(tup)->t_infomask &= ~LEOPARD_COMBOCID; \
} while (0)

/* SetCmax must be used after LeopardTupleHeaderAdjustCmax; see combocid.c */
#define LeopardTupleHeaderSetCmax(tup, cid, iscombo) \
do { \
		(tup)->t_choice.t_leopard.t_cid = (cid); \
	if (iscombo) \
		(tup)->t_infomask |= LEOPARD_COMBOCID; \
	else \
		(tup)->t_infomask &= ~LEOPARD_COMBOCID; \
} while (0)



#define LeopardTupleHeaderIsSpeculative(tup) \
( \
	(ItemPointerGetOffsetNumberNoCheck(&(tup)->t_ctid) == SpecTokenOffsetNumber) \
)

#define LeopardTupleHeaderGetSpeculativeToken(tup) \
( \
	AssertMacro(LeopardTupleHeaderIsSpeculative(tup)), \
	ItemPointerGetBlockNumber(&(tup)->t_ctid) \
)

#define LeopardTupleHeaderSetSpeculativeToken(tup, token)	\
( \
	ItemPointerSet(&(tup)->t_ctid, token, SpecTokenOffsetNumber) \
)

#define LeopardTupleHeaderIndicatesMovedPartitions(tup) \
	ItemPointerIndicatesMovedPartitions(&(tup)->t_ctid)

#define LeopardTupleHeaderSetMovedPartitions(tup) \
	ItemPointerSetMovedPartitions(&(tup)->t_ctid)

#define HeapTupleHeaderGetDatumLength(tup) \
	VARSIZE(tup)

#define LeopardTupleHeaderSetDatumLength(tup, len) \
	SET_VARSIZE(tup, len)

#define HeapTupleHeaderGetTypeId(tup) \
( \
	(tup)->t_choice.t_datum.datum_typeid \
)

#define LeopardTupleHeaderSetTypeId(tup, typeid) \
( \
	(tup)->t_choice.t_datum.datum_typeid = (typeid) \
)

#define HeapTupleHeaderGetTypMod(tup) \
( \
	(tup)->t_choice.t_datum.datum_typmod \
)

#define LeopardTupleHeaderSetTypMod(tup, typmod) \
( \
	(tup)->t_choice.t_datum.datum_typmod = (typmod) \
)

/*
 * Note that we stop considering a tuple HOT-updated as soon as it is known
 * aborted or the would-be updating transaction is known aborted.  For best
 * efficiency, check tuple visibility before using this macro, so that the
 * INVALID bits will be as up to date as possible.
 */
#define LeopardTupleHeaderIsHotUpdated(tup) \
( \
	((tup)->t_infomask2 & LEOPARD_HOT_UPDATED) != 0 && \
	((tup)->t_infomask & LEOPARD_XMAX_INVALID) == 0 && \
	!LeopardTupleHeaderXminInvalid(tup) \
)

#define LeopardTupleHeaderSetHotUpdated(tup) \
( \
	(tup)->t_infomask2 |= LEOPARD_HOT_UPDATED \
)

#define LeopardTupleHeaderClearHotUpdated(tup) \
( \
	(tup)->t_infomask2 &= ~LEOPARD_HOT_UPDATED \
)

#define LeopardTupleHeaderIsLeopardOnly(tup) \
( \
  ((tup)->t_infomask2 & LEOPARD_ONLY_TUPLE) != 0 \
)

#define LeopardTupleHeaderSetHeapOnly(tup) \
( \
  (tup)->t_infomask2 |= LEOPARD_ONLY_TUPLE \
)

#define LeopardTupleHeaderClearHeapOnly(tup) \
( \
  (tup)->t_infomask2 &= ~LEOPARD_ONLY_TUPLE \
)

#define LeopardTupleHeaderHasMatch(tup) \
( \
  ((tup)->t_infomask2 & LEOPARD_TUPLE_HAS_MATCH) != 0 \
)

#define LeopardTupleHeaderSetMatch(tup) \
( \
  (tup)->t_infomask2 |= LEOPARD_TUPLE_HAS_MATCH \
)

#define LeopardTupleHeaderClearMatch(tup) \
( \
  (tup)->t_infomask2 &= ~LEOPARD_TUPLE_HAS_MATCH \
)

#define LeopardTupleHeaderGetNatts(tup) \
	((tup)->t_infomask2 & LEOPARD_NATTS_MASK)

#define LeopardTupleHeaderSetNatts(tup, natts) \
( \
	(tup)->t_infomask2 = ((tup)->t_infomask2 & ~LEOPARD_NATTS_MASK) | (natts) \
)

#define LeopardTupleHeaderHasExternal(tup) \
		(((tup)->t_infomask & LEOPARD_HASEXTERNAL) != 0)


/*
 * BITMAPLEN(NATTS) -
 *		Computes size of null bitmap given number of data columns.
 */
#define BITMAPLEN(NATTS)	(((int)(NATTS) + 7) / 8)

/*
 * MaxLeopardTupleSize is the maximum allowed size of a leopard tuple, including
 * header and MAXALIGN alignment padding.  Basically it's BLCKSZ minus the
 * other stuff that has to be on a disk page.  Since leopard pages use no
 * "special space", there's no deduction for that.
 *
 * NOTE: we allow for the ItemId that must point to the tuple, ensuring that
 * an otherwise-empty page can indeed hold a tuple of this size.  Because
 * ItemIds and tuples have different alignment requirements, don't assume that
 * you can, say, fit 2 tuples of size MaxLeopardTupleSize/2 on the same page.
 */
#define MaxLeopardTupleSize  (BLCKSZ - MAXALIGN(SizeOfPageHeaderData + sizeof(ItemIdData)))
#define MinLeopardTupleSize  MAXALIGN(SizeofLeopardTupleHeader)

/*
 * MaxLeopardTuplesPerPage is an upper bound on the number of tuples that can
 * fit on one leopard page.  (Note that indexes could have more, because they
 * use a smaller tuple header.)  We arrive at the divisor because each tuple
 * must be maxaligned, and it must have an associated line pointer.
 *
 * Note: with HOT, there could theoretically be more line pointers (not actual
 * tuples) than this on a leopard page.  However we constrain the number of line
 * pointers to this anyway, to avoid excessive line-pointer bloat and not
 * require increases in the size of work arrays.
 */
#define MaxLeopardTuplesPerPage	\
	((int) ((BLCKSZ - SizeOfPageHeaderData) / \
			(MAXALIGN(SizeofLeopardTupleHeader) + sizeof(ItemIdData))))

/*
 * MaxAttrSize is a somewhat arbitrary upper limit on the declared size of
 * data fields of char(n) and similar types.  It need not have anything
 * directly to do with the *actual* upper limit of varlena values, which
 * is currently 1Gb (see TOAST structures in postgres.h).  I've set it
 * at 10Mb which seems like a reasonable number --- tgl 8/6/00.
 */
#define MaxAttrSize		(10 * 1024 * 1024)


/*
 * MinimalLeopardTuple is an alternative representation that is used for transient
 * tuples inside the executor, in places where transaction status information
 * is not required, the tuple rowtype is known, and shaving off a few bytes
 * is worthwhile because we need to store many tuples.  The representation
 * is chosen so that tuple access routines can work with either full or
 * minimal tuples via a LeopardTupleData pointer structure.  The access routines
 * see no difference, except that they must not access the transaction status
 * or t_ctid fields because those aren't there.
 *
 * For the most part, MinimalTuples should be accessed via TupleTableSlot
 * routines.  These routines will prevent access to the "system columns"
 * and thereby prevent accidental use of the nonexistent fields.
 *
 * MinimalLeopardTupleData contains a length word, some padding, and fields matching
 * LeopardTupleHeaderData beginning with t_infomask2. The padding is chosen so
 * that offsetof(t_infomask2) is the same modulo MAXIMUM_ALIGNOF in both
 * structs.   This makes data alignment rules equivalent in both cases.
 *
 * When a minimal tuple is accessed via a LeopardTupleData pointer, t_data is
 * set to point MINIMAL_LEOPARD_TUPLE_OFFSET bytes before the actual start of the
 * minimal tuple --- that is, where a full tuple matching the minimal tuple's
 * data would start.  This trick is what makes the structs seem equivalent.
 *
 * Note that t_hoff is computed the same as in a full tuple, hence it includes
 * the MINIMAL_LEOPARD_TUPLE_OFFSET distance.  t_len does not include that, however.
 *
 * MINIMAL_LEOPARD_TUPLE_DATA_OFFSET is the offset to the first useful (non-pad) data
 * other than the length word.  tuplesort.c and tuplestore.c use this to avoid
 * writing the padding to disk.
 */
#define MINIMAL_LEOPARD_TUPLE_OFFSET \
	((offsetof(LeopardTupleHeaderData, t_infomask2) - sizeof(uint32)) / MAXIMUM_ALIGNOF * MAXIMUM_ALIGNOF)
#define MINIMAL_LEOPARD_TUPLE_PADDING \
	((offsetof(LeopardTupleHeaderData, t_infomask2) - sizeof(uint32)) % MAXIMUM_ALIGNOF)
#define MINIMAL_LEOPARD_TUPLE_DATA_OFFSET \
	offsetof(MinimalLeopardTupleData, t_infomask2)

struct MinimalLeopardTupleData
{
	uint32		t_len;			/* actual length of minimal tuple */

	char		mt_padding[MINIMAL_LEOPARD_TUPLE_PADDING];

	/* Fields below here must match LeopardTupleHeaderData! */

	uint16		t_infomask2;	/* number of attributes + various flags */

	uint16		t_infomask;		/* various flag bits, see below */

	uint8		t_hoff;			/* sizeof header incl. bitmap, padding */

	/* ^ - 23 bytes - ^ */

	bits8		t_bits[FLEXIBLE_ARRAY_MEMBER];	/* bitmap of NULLs */

	/* MORE DATA FOLLOWS AT END OF STRUCT */
};

/* typedef appears in leopardtup.h */

#define SizeofMinimalLeopardTupleHeader offsetof(MinimalLeopardTupleData, t_bits)


/*
 * GETSTRUCT - given a LeopardTuple pointer, return address of the user data
 */
#define GETSTRUCT(TUP) ((char *) ((TUP)->t_data) + (TUP)->t_data->t_hoff)

/*
 * Accessor macros to be used with LeopardTuple pointers.
 */

#define LeopardTupleHasNulls(tuple) \
		(((tuple)->t_data->t_infomask & LEOPARD_HASNULL) != 0)

#define LeopardTupleNoNulls(tuple) \
		(!((tuple)->t_data->t_infomask & LEOPARD_HASNULL))

#define LeopardTupleHasVarWidth(tuple) \
		(((tuple)->t_data->t_infomask & LEOPARD_HASVARWIDTH) != 0)

#define LeopardTupleAllFixed(tuple) \
		(!((tuple)->t_data->t_infomask & LEOPARD_HASVARWIDTH))

#define LeopardTupleHasExternal(tuple) \
		(((tuple)->t_data->t_infomask & LEOPARD_HASEXTERNAL) != 0)

#define LeopardTupleIsHotUpdated(tuple) \
		LeopardTupleHeaderIsHotUpdated((tuple)->t_data)

#define LeopardTupleSetHotUpdated(tuple) \
		LeopardTupleHeaderSetHotUpdated((tuple)->t_data)

#define LeopardTupleClearHotUpdated(tuple) \
		LeopardTupleHeaderClearHotUpdated((tuple)->t_data)

#define LeopardTupleIsLeopardOnly(tuple) \
		LeopardTupleHeaderIsLeopardOnly((tuple)->t_data)

#define LeopardTupleSetLeopardOnly(tuple) \
		LeopardTupleHeaderSetHeapOnly((tuple)->t_data)

#define LeopardTupleClearLeopardOnly(tuple) \
		LeopardTupleHeaderClearHeapOnly((tuple)->t_data)


/* ----------------
 *		leopardfastgetattr
 *
 *		Fetch a user attribute's value as a Datum (might be either a
 *		value, or a pointer into the data area of the tuple).
 *
 *		This must not be used when a system attribute might be requested.
 *		Furthermore, the passed attnum MUST be valid.  Use leopard_getattr()
 *		instead, if in doubt.
 *
 *		This gets called many times, so we macro the cacheable and NULL
 *		lookups, and call leopardnocachegetattr() for the rest.
 * ----------------
 */

#if !defined(DISABLE_COMPLEX_MACRO)

#define leopardfastgetattr(tup, attnum, tupleDesc, isnull)					\
(																	\
	AssertMacro((attnum) > 0),										\
	(*(isnull) = false),											\
	LeopardTupleNoNulls(tup) ?											\
	(																\
		TupleDescAttr((tupleDesc), (attnum)-1)->attcacheoff >= 0 ?	\
		(															\
			fetchatt(TupleDescAttr((tupleDesc), (attnum)-1),		\
				(char *) (tup)->t_data + (tup)->t_data->t_hoff +	\
				TupleDescAttr((tupleDesc), (attnum)-1)->attcacheoff)\
		)															\
		:															\
			leopardnocachegetattr((tup), (attnum), (tupleDesc))			\
	)																\
	:																\
	(																\
		att_isnull((attnum)-1, (tup)->t_data->t_bits) ?				\
		(															\
			(*(isnull) = true),										\
			(Datum)NULL												\
		)															\
		:															\
		(															\
			leopardnocachegetattr((tup), (attnum), (tupleDesc))			\
		)															\
	)																\
)
#else							/* defined(DISABLE_COMPLEX_MACRO) */

extern Datum leopardfastgetattr(LeopardTuple tup, int attnum, TupleDesc tupleDesc,
						 bool *isnull);
#endif							/* defined(DISABLE_COMPLEX_MACRO) */


/* ----------------
 *		leopard_getattr
 *
 *		Extract an attribute of a leopard tuple and return it as a Datum.
 *		This works for either system or user attributes.  The given attnum
 *		is properly range-checked.
 *
 *		If the field in question has a NULL value, we return a zero Datum
 *		and set *isnull == true.  Otherwise, we set *isnull == false.
 *
 *		<tup> is the pointer to the leopard tuple.  <attnum> is the attribute
 *		number of the column (field) caller wants.  <tupleDesc> is a
 *		pointer to the structure describing the row and all its fields.
 * ----------------
 */
#define leopard_getattr(tup, attnum, tupleDesc, isnull) \
	( \
		((attnum) > 0) ? \
		( \
			((attnum) > (int) LeopardTupleHeaderGetNatts((tup)->t_data)) ? \
				getmissingattr((tupleDesc), (attnum), (isnull)) \
			: \
				leopardfastgetattr((tup), (attnum), (tupleDesc), (isnull)) \
		) \
		: \
			leopard_getsysattr((tup), (attnum), (tupleDesc), (isnull)) \
	)


/* prototypes for functions in common/leopardtuple.c */
extern Size leopard_compute_data_size(TupleDesc tupleDesc,
								   Datum *values, bool *isnull);
extern void leopard_fill_tuple(TupleDesc tupleDesc,
							Datum *values, bool *isnull,
							char *data, Size data_size,
							uint16 *infomask, bits8 *bit);
extern bool leopard_attisnull(LeopardTuple tup, int attnum, TupleDesc tupleDesc);
extern Datum leopardnocachegetattr(LeopardTuple tup, int attnum,
							TupleDesc att);
extern Datum leopard_getsysattr(LeopardTuple tup, int attnum, TupleDesc tupleDesc,
							 bool *isnull);
extern Datum getmissingattr(TupleDesc tupleDesc,
							int attnum, bool *isnull);
extern LeopardTuple leopard_copytuple(LeopardTuple tuple);
extern void leopard_copytuple_with_tuple(LeopardTuple src, LeopardTuple dest);
extern Datum leopard_copy_tuple_as_datum(LeopardTuple tuple, TupleDesc tupleDesc);
extern LeopardTuple leopard_form_tuple(TupleDesc tupleDescriptor,
								 Datum *values, bool *isnull);
extern LeopardTuple leopard_modify_tuple(LeopardTuple tuple,
								   TupleDesc tupleDesc,
								   Datum *replValues,
								   bool *replIsnull,
								   bool *doReplace);
extern LeopardTuple leopard_modify_tuple_by_cols(LeopardTuple tuple,
										   TupleDesc tupleDesc,
										   int nCols,
										   int *replCols,
										   Datum *replValues,
										   bool *replIsnull);
extern void leopard_deform_tuple(LeopardTuple tuple, TupleDesc tupleDesc,
							  Datum *values, bool *isnull);
extern void leopard_freetuple(LeopardTuple leopardtup);
extern MinimalLeopardTuple leopard_form_minimal_tuple(TupleDesc tupleDescriptor,
											Datum *values, bool *isnull);
extern void leopard_free_minimal_tuple(MinimalLeopardTuple mtup);
extern MinimalLeopardTuple leopard_copy_minimal_tuple(MinimalLeopardTuple mtup);
extern LeopardTuple leopard_tuple_from_minimal_tuple(MinimalLeopardTuple mtup);
extern MinimalLeopardTuple minimal_tuple_from_leopard_tuple(LeopardTuple leopardtup);
extern size_t varsize_any(void *p);
extern LeopardTuple leopard_expand_tuple(LeopardTuple sourceTuple, TupleDesc tupleDesc);
extern MinimalLeopardTuple minimal_expand_leopard_tuple(LeopardTuple sourceTuple, TupleDesc tupleDesc);

#endif							/* LEOPARDTUP_DETAILS_H */
