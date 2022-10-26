/*-------------------------------------------------------------------------
 *
 * leopardtuple.c
 *	  This file contains leopard tuple accessor and mutator routines, as well
 *	  as various tuple utilities.
 *
 * Some notes about varlenas and this code:
 *
 * Before Postgres 8.3 varlenas always had a 4-byte length header, and
 * therefore always needed 4-byte alignment (at least).  This wasted space
 * for short varlenas, for example CHAR(1) took 5 bytes and could need up to
 * 3 additional padding bytes for alignment.
 *
 * Now, a short varlena (up to 126 data bytes) is reduced to a 1-byte header
 * and we don't align it.  To hide this from datatype-specific functions that
 * don't want to deal with it, such a datum is considered "toasted" and will
 * be expanded back to the normal 4-byte-header format by pg_detoast_datum.
 * (In performance-critical code paths we can use pg_detoast_datum_packed
 * and the appropriate access macros to avoid that overhead.)  Note that this
 * conversion is performed directly in leopard_form_tuple, without invoking
 * leopardtoast.c.
 *
 * This change will break any code that assumes it needn't detoast values
 * that have been put into a tuple but never sent to disk.  Hopefully there
 * are few such places.
 *
 * Varlenas still have alignment INT (or DOUBLE) in pg_type/pg_attribute, since
 * that's the normal requirement for the untoasted format.  But we ignore that
 * for the 1-byte-header format.  This means that the actual start position
 * of a varlena datum may vary depending on which format it has.  To determine
 * what is stored, we have to require that alignment padding bytes be zero.
 * (Postgres actually has always zeroed them, but now it's required!)  Since
 * the first byte of a 1-byte-header varlena can never be zero, we can examine
 * the first byte after the previous datum to tell if it's a pad byte or the
 * start of a 1-byte-header varlena.
 *
 * Note that while formerly we could rely on the first varlena column of a
 * system catalog to be at the offset suggested by the C struct for the
 * catalog, this is now risky: it's only safe if the preceding field is
 * word-aligned, so that there will never be any padding.
 *
 * We don't pack varlenas whose attstorage is PLAIN, since the data type
 * isn't expecting to have to detoast values.  This is used in particular
 * by oidvector and int2vector, which are used in the system catalogs
 * and we'd like to still refer to them via C struct offsets.
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
 *
 * IDENTIFICATION
 *	  src/backend/access/common/leopardtuple.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/leopardtoast.h"
#include "access/sysattr.h"
#include "access/tupdesc_details.h"
#include "executor/tuptable.h"
#include "utils/expandeddatum.h"
#include "access/leopard_to_heap_map.h"


/* Does att's datatype allow packing into the 1-byte-header varlena format? */
#define ATT_IS_PACKABLE(att) \
	((att)->attlen == -1 && (att)->attstorage != TYPSTORAGE_PLAIN)
/* Use this if it's already known varlena */
#define VARLENA_ATT_IS_PACKABLE(att) \
	((att)->attstorage != TYPSTORAGE_PLAIN)


/* ----------------------------------------------------------------
 *						misc support routines
 * ----------------------------------------------------------------
 */

/*
 * Return the missing value of an attribute, or NULL if there isn't one.
 */
Datum
getmissingattr(TupleDesc tupleDesc,
			   int attnum, bool *isnull)
{
	Form_pg_attribute att;

	Assert(attnum <= tupleDesc->natts);
	Assert(attnum > 0);

	att = TupleDescAttr(tupleDesc, attnum - 1);

	if (att->atthasmissing)
	{
		AttrMissing *attrmiss;

		Assert(tupleDesc->constr);
		Assert(tupleDesc->constr->missing);

		attrmiss = tupleDesc->constr->missing + (attnum - 1);

		if (attrmiss->am_present)
		{
			*isnull = false;
			return attrmiss->am_value;
		}
	}

	*isnull = true;
	return PointerGetDatum(NULL);
}

/*
 * leopard_compute_data_size
 *		Determine size of the data area of a tuple to be constructed
 */
Size
leopard_compute_data_size(TupleDesc tupleDesc,
					   Datum *values,
					   bool *isnull)
{
	Size		data_length = 0;
	int			i;
	int			numberOfAttributes = tupleDesc->natts;

	for (i = 0; i < numberOfAttributes; i++)
	{
		Datum		val;
		Form_pg_attribute atti;

		if (isnull[i])
			continue;

		val = values[i];
		atti = TupleDescAttr(tupleDesc, i);

		if (ATT_IS_PACKABLE(atti) &&
			VARATT_CAN_MAKE_SHORT(DatumGetPointer(val)))
		{
			/*
			 * we're anticipating converting to a short varlena header, so
			 * adjust length and don't count any alignment
			 */
			data_length += VARATT_CONVERTED_SHORT_SIZE(DatumGetPointer(val));
		}
		else if (atti->attlen == -1 &&
				 VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			/*
			 * we want to flatten the expanded value so that the constructed
			 * tuple doesn't depend on it
			 */
			data_length = att_align_nominal(data_length, atti->attalign);
			data_length += EOH_get_flat_size(DatumGetEOHP(val));
		}
		else
		{
			data_length = att_align_datum(data_length, atti->attalign,
										  atti->attlen, val);
			data_length = att_addlength_datum(data_length, atti->attlen,
											  val);
		}
	}

	return data_length;
}

/*
 * Per-attribute helper for leopard_fill_tuple and other routines building tuples.
 *
 * Fill in either a data value or a bit in the null bitmask
 */
static inline void
fill_val(Form_pg_attribute att,
		 bits8 **bit,
		 int *bitmask,
		 char **dataP,
		 uint16 *infomask,
		 Datum datum,
		 bool isnull)
{
	Size		data_length;
	char	   *data = *dataP;

	/*
	 * If we're building a null bitmap, set the appropriate bit for the
	 * current column value here.
	 */
	if (bit != NULL)
	{
		if (*bitmask != HIGHBIT)
			*bitmask <<= 1;
		else
		{
			*bit += 1;
			**bit = 0x0;
			*bitmask = 1;
		}

		if (isnull)
		{
			*infomask |= LEOPARD_HASNULL;
			return;
		}

		**bit |= *bitmask;
	}

	/*
	 * XXX we use the att_align macros on the pointer value itself, not on an
	 * offset.  This is a bit of a hack.
	 */
	if (att->attbyval)
	{
		/* pass-by-value */
		data = (char *) att_align_nominal(data, att->attalign);
		store_att_byval(data, datum, att->attlen);
		data_length = att->attlen;
	}
	else if (att->attlen == -1)
	{
		/* varlena */
		Pointer		val = DatumGetPointer(datum);

		*infomask |= LEOPARD_HASVARWIDTH;
		if (VARATT_IS_EXTERNAL(val))
		{
			if (VARATT_IS_EXTERNAL_EXPANDED(val))
			{
				/*
				 * we want to flatten the expanded value so that the
				 * constructed tuple doesn't depend on it
				 */
				ExpandedObjectHeader *eoh = DatumGetEOHP(datum);

				data = (char *) att_align_nominal(data,
												  att->attalign);
				data_length = EOH_get_flat_size(eoh);
				EOH_flatten_into(eoh, data, data_length);
			}
			else
			{
				*infomask |= LEOPARD_HASEXTERNAL;
				/* no alignment, since it's short by definition */
				data_length = VARSIZE_EXTERNAL(val);
				memcpy(data, val, data_length);
			}
		}
		else if (VARATT_IS_SHORT(val))
		{
			/* no alignment for short varlenas */
			data_length = VARSIZE_SHORT(val);
			memcpy(data, val, data_length);
		}
		else if (VARLENA_ATT_IS_PACKABLE(att) &&
				 VARATT_CAN_MAKE_SHORT(val))
		{
			/* convert to short varlena -- no alignment */
			data_length = VARATT_CONVERTED_SHORT_SIZE(val);
			SET_VARSIZE_SHORT(data, data_length);
			memcpy(data + 1, VARDATA(val), data_length - 1);
		}
		else
		{
			/* full 4-byte header varlena */
			data = (char *) att_align_nominal(data,
											  att->attalign);
			data_length = VARSIZE(val);
			memcpy(data, val, data_length);
		}
	}
	else if (att->attlen == -2)
	{
		/* cstring ... never needs alignment */
		*infomask |= LEOPARD_HASVARWIDTH;
		Assert(att->attalign == TYPALIGN_CHAR);
		data_length = strlen(DatumGetCString(datum)) + 1;
		memcpy(data, DatumGetPointer(datum), data_length);
	}
	else
	{
		/* fixed-length pass-by-reference */
		data = (char *) att_align_nominal(data, att->attalign);
		Assert(att->attlen > 0);
		data_length = att->attlen;
		memcpy(data, DatumGetPointer(datum), data_length);
	}

	data += data_length;
	*dataP = data;
}

/*
 * leopard_fill_tuple
 *		Load data portion of a tuple from values/isnull arrays
 *
 * We also fill the null bitmap (if any) and set the infomask bits
 * that reflect the tuple's data contents.
 *
 * NOTE: it is now REQUIRED that the caller have pre-zeroed the data area.
 */
void
leopard_fill_tuple(TupleDesc tupleDesc,
				Datum *values, bool *isnull,
				char *data, Size data_size,
				uint16 *infomask, bits8 *bit)
{
	bits8	   *bitP;
	int			bitmask;
	int			i;
	int			numberOfAttributes = tupleDesc->natts;

#ifdef USE_ASSERT_CHECKING
	char	   *start = data;
#endif

	if (bit != NULL)
	{
		bitP = &bit[-1];
		bitmask = HIGHBIT;
	}
	else
	{
		/* just to keep compiler quiet */
		bitP = NULL;
		bitmask = 0;
	}

	*infomask &= ~(LEOPARD_HASNULL | LEOPARD_HASVARWIDTH | LEOPARD_HASEXTERNAL);

	for (i = 0; i < numberOfAttributes; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		fill_val(attr,
				 bitP ? &bitP : NULL,
				 &bitmask,
				 &data,
				 infomask,
				 values ? values[i] : PointerGetDatum(NULL),
				 isnull ? isnull[i] : true);
	}

	Assert((data - start) == data_size);
}


/* ----------------------------------------------------------------
 *						leopard tuple interface
 * ----------------------------------------------------------------
 */

/* ----------------
 *		leopard_attisnull	- returns true iff tuple attribute is not present
 * ----------------
 */
bool
leopard_attisnull(LeopardTuple tup, int attnum, TupleDesc tupleDesc)
{
	/*
	 * We allow a NULL tupledesc for relations not expected to have missing
	 * values, such as catalog relations and indexes.
	 */
	Assert(!tupleDesc || attnum <= tupleDesc->natts);
	if (attnum > (int) LeopardTupleHeaderGetNatts(tup->t_data))
	{
		if (tupleDesc && TupleDescAttr(tupleDesc, attnum - 1)->atthasmissing)
			return false;
		else
			return true;
	}

	if (attnum > 0)
	{
		if (LeopardTupleNoNulls(tup))
			return false;
		return att_isnull(attnum - 1, tup->t_data->t_bits);
	}

	switch (attnum)
	{
		case TableOidAttributeNumber:
		case SelfItemPointerAttributeNumber:
		case MinTransactionIdAttributeNumber:
		case MinCommandIdAttributeNumber:
		case MaxTransactionIdAttributeNumber:
		case MaxCommandIdAttributeNumber:
			/* these are never null */
			break;

		default:
			elog(ERROR, "invalid attnum: %d", attnum);
	}

	return false;
}

/* ----------------
 *		leopardnocachegetattr
 *
 *		This only gets called from leopardfastgetattr() macro, in cases where
 *		we can't use a cacheoffset and the value is not null.
 *
 *		This caches attribute offsets in the attribute descriptor.
 *
 *		An alternative way to speed things up would be to cache offsets
 *		with the tuple, but that seems more difficult unless you take
 *		the storage hit of actually putting those offsets into the
 *		tuple you send to disk.  Yuck.
 *
 *		This scheme will be slightly slower than that, but should
 *		perform well for queries which hit large #'s of tuples.  After
 *		you cache the offsets once, examining all the other tuples using
 *		the same attribute descriptor will go much quicker. -cim 5/4/91
 *
 *		NOTE: if you need to change this code, see also leopard_deform_tuple.
 *		Also see nocache_index_getattr, which is the same code for index
 *		tuples.
 * ----------------
 */
Datum
leopardnocachegetattr(LeopardTuple tuple,
			   int attnum,
			   TupleDesc tupleDesc)
{
	LeopardTupleHeader tup = tuple->t_data;
	char	   *tp;				/* ptr to data part of tuple */
	bits8	   *bp = tup->t_bits;	/* ptr to null bitmap in tuple */
	bool		slow = false;	/* do we have to walk attrs? */
	int			off;			/* current offset within data */

	/* ----------------
	 *	 Three cases:
	 *
	 *	 1: No nulls and no variable-width attributes.
	 *	 2: Has a null or a var-width AFTER att.
	 *	 3: Has nulls or var-widths BEFORE att.
	 * ----------------
	 */

	attnum--;

	if (!LeopardTupleNoNulls(tuple))
	{
		/*
		 * there's a null somewhere in the tuple
		 *
		 * check to see if any preceding bits are null...
		 */
		int			byte = attnum >> 3;
		int			finalbit = attnum & 0x07;

		/* check for nulls "before" final bit of last byte */
		if ((~bp[byte]) & ((1 << finalbit) - 1))
			slow = true;
		else
		{
			/* check for nulls in any "earlier" bytes */
			int			i;

			for (i = 0; i < byte; i++)
			{
				if (bp[i] != 0xFF)
				{
					slow = true;
					break;
				}
			}
		}
	}

	tp = (char *) tup + tup->t_hoff;

	if (!slow)
	{
		Form_pg_attribute att;

		/*
		 * If we get here, there are no nulls up to and including the target
		 * attribute.  If we have a cached offset, we can use it.
		 */
		att = TupleDescAttr(tupleDesc, attnum);
		if (att->attcacheoff >= 0)
			return fetchatt(att, tp + att->attcacheoff);

		/*
		 * Otherwise, check for non-fixed-length attrs up to and including
		 * target.  If there aren't any, it's safe to cheaply initialize the
		 * cached offsets for these attrs.
		 */
		if (LeopardTupleHasVarWidth(tuple))
		{
			int			j;

			for (j = 0; j <= attnum; j++)
			{
				if (TupleDescAttr(tupleDesc, j)->attlen <= 0)
				{
					slow = true;
					break;
				}
			}
		}
	}

	if (!slow)
	{
		int			natts = tupleDesc->natts;
		int			j = 1;

		/*
		 * If we get here, we have a tuple with no nulls or var-widths up to
		 * and including the target attribute, so we can use the cached offset
		 * ... only we don't have it yet, or we'd not have got here.  Since
		 * it's cheap to compute offsets for fixed-width columns, we take the
		 * opportunity to initialize the cached offsets for *all* the leading
		 * fixed-width columns, in hope of avoiding future visits to this
		 * routine.
		 */
		TupleDescAttr(tupleDesc, 0)->attcacheoff = 0;

		/* we might have set some offsets in the slow path previously */
		while (j < natts && TupleDescAttr(tupleDesc, j)->attcacheoff > 0)
			j++;

		off = TupleDescAttr(tupleDesc, j - 1)->attcacheoff +
			TupleDescAttr(tupleDesc, j - 1)->attlen;

		for (; j < natts; j++)
		{
			Form_pg_attribute att = TupleDescAttr(tupleDesc, j);

			if (att->attlen <= 0)
				break;

			off = att_align_nominal(off, att->attalign);

			att->attcacheoff = off;

			off += att->attlen;
		}

		Assert(j > attnum);

		off = TupleDescAttr(tupleDesc, attnum)->attcacheoff;
	}
	else
	{
		bool		usecache = true;
		int			i;

		/*
		 * Now we know that we have to walk the tuple CAREFULLY.  But we still
		 * might be able to cache some offsets for next time.
		 *
		 * Note - This loop is a little tricky.  For each non-null attribute,
		 * we have to first account for alignment padding before the attr,
		 * then advance over the attr based on its length.  Nulls have no
		 * storage and no alignment padding either.  We can use/set
		 * attcacheoff until we reach either a null or a var-width attribute.
		 */
		off = 0;
		for (i = 0;; i++)		/* loop exit is at "break" */
		{
			Form_pg_attribute att = TupleDescAttr(tupleDesc, i);

			if (LeopardTupleHasNulls(tuple) && att_isnull(i, bp))
			{
				usecache = false;
				continue;		/* this cannot be the target att */
			}

			/* If we know the next offset, we can skip the rest */
			if (usecache && att->attcacheoff >= 0)
				off = att->attcacheoff;
			else if (att->attlen == -1)
			{
				/*
				 * We can only cache the offset for a varlena attribute if the
				 * offset is already suitably aligned, so that there would be
				 * no pad bytes in any case: then the offset will be valid for
				 * either an aligned or unaligned value.
				 */
				if (usecache &&
					off == att_align_nominal(off, att->attalign))
					att->attcacheoff = off;
				else
				{
					off = att_align_pointer(off, att->attalign, -1,
											tp + off);
					usecache = false;
				}
			}
			else
			{
				/* not varlena, so safe to use att_align_nominal */
				off = att_align_nominal(off, att->attalign);

				if (usecache)
					att->attcacheoff = off;
			}

			if (i == attnum)
				break;

			off = att_addlength_pointer(off, att->attlen, tp + off);

			if (usecache && att->attlen <= 0)
				usecache = false;
		}
	}

	return fetchatt(TupleDescAttr(tupleDesc, attnum), tp + off);
}

/* ----------------
 *		leopard_getsysattr
 *
 *		Fetch the value of a system attribute for a tuple.
 *
 * This is a support routine for the leopard_getattr macro.  The macro
 * has already determined that the attnum refers to a system attribute.
 * ----------------
 */
Datum
leopard_getsysattr(LeopardTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull)
{
	Datum		result;

	Assert(tup);

	/* Currently, no sys attribute ever reads as NULL. */
	*isnull = false;

	switch (attnum)
	{
		case SelfItemPointerAttributeNumber:
			/* pass-by-reference datatype */
			result = PointerGetDatum(&(tup->t_self));
			break;
		case MinTransactionIdAttributeNumber:
			result = TransactionIdGetDatum(LeopardTupleHeaderGetRawXmin(tup->t_data));
			break;
		case MaxTransactionIdAttributeNumber:
			result = TransactionIdGetDatum(LeopardTupleHeaderGetRawXmax(tup->t_data));
			break;
		case MinCommandIdAttributeNumber:
		case MaxCommandIdAttributeNumber:

			/*
			 * cmin and cmax are now both aliases for the same field, which
			 * can in fact also be a combo command id.  XXX perhaps we should
			 * return the "real" cmin or cmax if possible, that is if we are
			 * inside the originating transaction?
			 */
			result = CommandIdGetDatum(LeopardTupleHeaderGetRawCommandId(tup->t_data));
			break;
		case TableOidAttributeNumber:
			result = ObjectIdGetDatum(tup->t_tableOid);
			break;
		default:
			elog(ERROR, "invalid attnum: %d", attnum);
			result = 0;			/* keep compiler quiet */
			break;
	}
	return result;
}

/* ----------------
 *		leopard_copytuple
 *
 *		returns a copy of an entire tuple
 *
 * The LeopardTuple struct, tuple header, and tuple data are all allocated
 * as a single palloc() block.
 * ----------------
 */
LeopardTuple
leopard_copytuple(LeopardTuple tuple)
{
	LeopardTuple	newTuple;

	if (!HeapTupleIsValid(tuple) || tuple->t_data == NULL)
		return NULL;

	newTuple = (LeopardTuple) palloc(LEOPARDTUPLESIZE + tuple->t_len);
	newTuple->t_len = tuple->t_len;
	newTuple->t_self = tuple->t_self;
	newTuple->t_tableOid = tuple->t_tableOid;
	newTuple->t_data = (LeopardTupleHeader) ((char *) newTuple + LEOPARDTUPLESIZE);
	memcpy((char *) newTuple->t_data, (char *) tuple->t_data, tuple->t_len);
	return newTuple;
}

/* ----------------
 *		leopard_copytuple_with_tuple
 *
 *		copy a tuple into a caller-supplied LeopardTuple management struct
 *
 * Note that after calling this function, the "dest" LeopardTuple will not be
 * allocated as a single palloc() block (unlike with leopard_copytuple()).
 * ----------------
 */
void
leopard_copytuple_with_tuple(LeopardTuple src, LeopardTuple dest)
{
	if (!HeapTupleIsValid(src) || src->t_data == NULL)
	{
		dest->t_data = NULL;
		return;
	}

	dest->t_len = src->t_len;
	dest->t_self = src->t_self;
	dest->t_tableOid = src->t_tableOid;
	dest->t_data = (LeopardTupleHeader) palloc(src->t_len);
	memcpy((char *) dest->t_data, (char *) src->t_data, src->t_len);
}

/*
 * Expand a tuple which has fewer attributes than required. For each attribute
 * not present in the sourceTuple, if there is a missing value that will be
 * used. Otherwise the attribute will be set to NULL.
 *
 * The source tuple must have fewer attributes than the required number.
 *
 * Only one of targetHeapTuple and targetMinimalTuple may be supplied. The
 * other argument must be NULL.
 */
static void
expand_tuple(LeopardTuple *targetHeapTuple,
			 MinimalLeopardTuple *targetMinimalTuple,
			 LeopardTuple sourceTuple,
			 TupleDesc tupleDesc)
{
	AttrMissing *attrmiss = NULL;
	int			attnum;
	int			firstmissingnum;
	bool		hasNulls = LeopardTupleHasNulls(sourceTuple);
	LeopardTupleHeader targetTHeader;
	LeopardTupleHeader sourceTHeader = sourceTuple->t_data;
	int			sourceNatts = LeopardTupleHeaderGetNatts(sourceTHeader);
	int			natts = tupleDesc->natts;
	int			sourceNullLen;
	int			targetNullLen;
	Size		sourceDataLen = sourceTuple->t_len - sourceTHeader->t_hoff;
	Size		targetDataLen;
	Size		len;
	int			hoff;
	bits8	   *nullBits = NULL;
	int			bitMask = 0;
	char	   *targetData;
	uint16	   *infoMask;

	Assert((targetHeapTuple && !targetMinimalTuple)
		   || (!targetHeapTuple && targetMinimalTuple));

	Assert(sourceNatts < natts);

	sourceNullLen = (hasNulls ? BITMAPLEN(sourceNatts) : 0);

	targetDataLen = sourceDataLen;

	if (tupleDesc->constr &&
		tupleDesc->constr->missing)
	{
		/*
		 * If there are missing values we want to put them into the tuple.
		 * Before that we have to compute the extra length for the values
		 * array and the variable length data.
		 */
		attrmiss = tupleDesc->constr->missing;

		/*
		 * Find the first item in attrmiss for which we don't have a value in
		 * the source. We can ignore all the missing entries before that.
		 */
		for (firstmissingnum = sourceNatts;
			 firstmissingnum < natts;
			 firstmissingnum++)
		{
			if (attrmiss[firstmissingnum].am_present)
				break;
			else
				hasNulls = true;
		}

		/*
		 * Now walk the missing attributes. If there is a missing value make
		 * space for it. Otherwise, it's going to be NULL.
		 */
		for (attnum = firstmissingnum;
			 attnum < natts;
			 attnum++)
		{
			if (attrmiss[attnum].am_present)
			{
				Form_pg_attribute att = TupleDescAttr(tupleDesc, attnum);

				targetDataLen = att_align_datum(targetDataLen,
												att->attalign,
												att->attlen,
												attrmiss[attnum].am_value);

				targetDataLen = att_addlength_pointer(targetDataLen,
													  att->attlen,
													  attrmiss[attnum].am_value);
			}
			else
			{
				/* no missing value, so it must be null */
				hasNulls = true;
			}
		}
	}							/* end if have missing values */
	else
	{
		/*
		 * If there are no missing values at all then NULLS must be allowed,
		 * since some of the attributes are known to be absent.
		 */
		hasNulls = true;
	}

	len = 0;

	if (hasNulls)
	{
		targetNullLen = BITMAPLEN(natts);
		len += targetNullLen;
	}
	else
		targetNullLen = 0;

	/*
	 * Allocate and zero the space needed.  Note that the tuple body and
	 * LeopardTupleData management structure are allocated in one chunk.
	 */
	if (targetHeapTuple)
	{
		len += offsetof(LeopardTupleHeaderData, t_bits);
		hoff = len = MAXALIGN(len); /* align user data safely */
		len += targetDataLen;

		*targetHeapTuple = (LeopardTuple) palloc0(LEOPARDTUPLESIZE + len);
		(*targetHeapTuple)->t_data
			= targetTHeader
			= (LeopardTupleHeader) ((char *) *targetHeapTuple + LEOPARDTUPLESIZE);
		(*targetHeapTuple)->t_len = len;
		(*targetHeapTuple)->t_tableOid = sourceTuple->t_tableOid;
		(*targetHeapTuple)->t_self = sourceTuple->t_self;

		targetTHeader->t_infomask = sourceTHeader->t_infomask;
		targetTHeader->t_hoff = hoff;
		LeopardTupleHeaderSetNatts(targetTHeader, natts);
		LeopardTupleHeaderSetDatumLength(targetTHeader, len);
		LeopardTupleHeaderSetTypeId(targetTHeader, tupleDesc->tdtypeid);
		LeopardTupleHeaderSetTypMod(targetTHeader, tupleDesc->tdtypmod);
		/* We also make sure that t_ctid is invalid unless explicitly set */
		ItemPointerSetInvalid(&(targetTHeader->t_ctid));
		if (targetNullLen > 0)
			nullBits = (bits8 *) ((char *) (*targetHeapTuple)->t_data
								  + offsetof(LeopardTupleHeaderData, t_bits));
		targetData = (char *) (*targetHeapTuple)->t_data + hoff;
		infoMask = &(targetTHeader->t_infomask);
	}
	else
	{
		len += SizeofMinimalLeopardTupleHeader;
		hoff = len = MAXALIGN(len); /* align user data safely */
		len += targetDataLen;

		*targetMinimalTuple = (MinimalLeopardTuple) palloc0(len);
		(*targetMinimalTuple)->t_len = len;
		(*targetMinimalTuple)->t_hoff = hoff + MINIMAL_LEOPARD_TUPLE_OFFSET;
		(*targetMinimalTuple)->t_infomask = sourceTHeader->t_infomask;
		/* Same macro works for MinimalTuples */
		LeopardTupleHeaderSetNatts(*targetMinimalTuple, natts);
		if (targetNullLen > 0)
			nullBits = (bits8 *) ((char *) *targetMinimalTuple
								  + offsetof(MinimalLeopardTupleData, t_bits));
		targetData = (char *) *targetMinimalTuple + hoff;
		infoMask = &((*targetMinimalTuple)->t_infomask);
	}

	if (targetNullLen > 0)
	{
		if (sourceNullLen > 0)
		{
			/* if bitmap pre-existed copy in - all is set */
			memcpy(nullBits,
				   ((char *) sourceTHeader)
				   + offsetof(LeopardTupleHeaderData, t_bits),
				   sourceNullLen);
			nullBits += sourceNullLen - 1;
		}
		else
		{
			sourceNullLen = BITMAPLEN(sourceNatts);
			/* Set NOT NULL for all existing attributes */
			memset(nullBits, 0xff, sourceNullLen);

			nullBits += sourceNullLen - 1;

			if (sourceNatts & 0x07)
			{
				/* build the mask (inverted!) */
				bitMask = 0xff << (sourceNatts & 0x07);
				/* Voila */
				*nullBits = ~bitMask;
			}
		}

		bitMask = (1 << ((sourceNatts - 1) & 0x07));
	}							/* End if have null bitmap */

	memcpy(targetData,
		   ((char *) sourceTuple->t_data) + sourceTHeader->t_hoff,
		   sourceDataLen);

	targetData += sourceDataLen;

	/* Now fill in the missing values */
	for (attnum = sourceNatts; attnum < natts; attnum++)
	{

		Form_pg_attribute attr = TupleDescAttr(tupleDesc, attnum);

		if (attrmiss && attrmiss[attnum].am_present)
		{
			fill_val(attr,
					 nullBits ? &nullBits : NULL,
					 &bitMask,
					 &targetData,
					 infoMask,
					 attrmiss[attnum].am_value,
					 false);
		}
		else
		{
			fill_val(attr,
					 &nullBits,
					 &bitMask,
					 &targetData,
					 infoMask,
					 (Datum) 0,
					 true);
		}
	}							/* end loop over missing attributes */
}

/*
 * Fill in the missing values for a minimal LeopardTuple
 */
MinimalLeopardTuple
minimal_expand_leopard_tuple(LeopardTuple sourceTuple, TupleDesc tupleDesc)
{
	MinimalLeopardTuple minimalTuple;

	expand_tuple(NULL, &minimalTuple, sourceTuple, tupleDesc);
	return minimalTuple;
}

/*
 * Fill in the missing values for an ordinary LeopardTuple
 */
LeopardTuple
leopard_expand_tuple(LeopardTuple sourceTuple, TupleDesc tupleDesc)
{
	LeopardTuple	leopardTuple;

	expand_tuple(&leopardTuple, NULL, sourceTuple, tupleDesc);
	return leopardTuple;
}

/* ----------------
 *		leopard_copy_tuple_as_datum
 *
 *		copy a tuple as a composite-type Datum
 * ----------------
 */
Datum
leopard_copy_tuple_as_datum(LeopardTuple tuple, TupleDesc tupleDesc)
{
	LeopardTupleHeader td;

	/*
	 * If the tuple contains any external TOAST pointers, we have to inline
	 * those fields to meet the conventions for composite-type Datums.
	 */
	if (LeopardTupleHasExternal(tuple))
		return toast_flatten_tuple_to_datum(tuple->t_data,
											tuple->t_len,
											tupleDesc);

	/*
	 * Fast path for easy case: just make a palloc'd copy and insert the
	 * correct composite-Datum header fields (since those may not be set if
	 * the given tuple came from disk, rather than from leopard_form_tuple).
	 */
	td = (LeopardTupleHeader) palloc(tuple->t_len);
	memcpy((char *) td, (char *) tuple->t_data, tuple->t_len);

	LeopardTupleHeaderSetDatumLength(td, tuple->t_len);
	LeopardTupleHeaderSetTypeId(td, tupleDesc->tdtypeid);
	LeopardTupleHeaderSetTypMod(td, tupleDesc->tdtypmod);

	return PointerGetDatum(td);
}

/*
 * leopard_form_tuple
 *		construct a tuple from the given values[] and isnull[] arrays,
 *		which are of the length indicated by tupleDescriptor->natts
 *
 * The result is allocated in the current memory context.
 */
LeopardTuple
leopard_form_tuple(TupleDesc tupleDescriptor,
				Datum *values,
				bool *isnull)
{
	LeopardTuple	tuple;			/* return tuple */
	LeopardTupleHeader td;			/* tuple data */
	Size		len,
				data_len;
	int			hoff;
	bool		hasnull = false;
	int			numberOfAttributes = tupleDescriptor->natts;
	int			i;

	if (numberOfAttributes > MaxTupleAttributeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("number of columns (%d) exceeds limit (%d)",
						numberOfAttributes, MaxTupleAttributeNumber)));

	/*
	 * Check for nulls
	 */
	for (i = 0; i < numberOfAttributes; i++)
	{
		if (isnull[i])
		{
			hasnull = true;
			break;
		}
	}

	/*
	 * Determine total space needed
	 */
	len = offsetof(LeopardTupleHeaderData, t_bits);

	if (hasnull)
		len += BITMAPLEN(numberOfAttributes);

	hoff = len = MAXALIGN(len); /* align user data safely */

	data_len = leopard_compute_data_size(tupleDescriptor, values, isnull);

	len += data_len;

	/*
	 * Allocate and zero the space needed.  Note that the tuple body and
	 * LeopardTupleData management structure are allocated in one chunk.
	 */
	tuple = (LeopardTuple) palloc0(LEOPARDTUPLESIZE + len);
	tuple->t_data = td = (LeopardTupleHeader) ((char *) tuple + LEOPARDTUPLESIZE);

	/*
	 * And fill in the information.  Note we fill the Datum fields even though
	 * this tuple may never become a Datum.  This lets HeapTupleHeaderGetDatum
	 * identify the tuple type if needed.
	 */
	tuple->t_len = len;
	ItemPointerSetInvalid(&(tuple->t_self));
	tuple->t_tableOid = InvalidOid;

	LeopardTupleHeaderSetDatumLength(td, len);
	LeopardTupleHeaderSetTypeId(td, tupleDescriptor->tdtypeid);
	LeopardTupleHeaderSetTypMod(td, tupleDescriptor->tdtypmod);
	/* We also make sure that t_ctid is invalid unless explicitly set */
	ItemPointerSetInvalid(&(td->t_ctid));

	LeopardTupleHeaderSetNatts(td, numberOfAttributes);
	td->t_hoff = hoff;

	leopard_fill_tuple(tupleDescriptor,
					values,
					isnull,
					(char *) td + hoff,
					data_len,
					&td->t_infomask,
					(hasnull ? td->t_bits : NULL));

	return tuple;
}

/*
 * leopard_modify_tuple
 *		form a new tuple from an old tuple and a set of replacement values.
 *
 * The replValues, replIsnull, and doReplace arrays must be of the length
 * indicated by tupleDesc->natts.  The new tuple is constructed using the data
 * from replValues/replIsnull at columns where doReplace is true, and using
 * the data from the old tuple at columns where doReplace is false.
 *
 * The result is allocated in the current memory context.
 */
LeopardTuple
leopard_modify_tuple(LeopardTuple tuple,
				  TupleDesc tupleDesc,
				  Datum *replValues,
				  bool *replIsnull,
				  bool *doReplace)
{
	int			numberOfAttributes = tupleDesc->natts;
	int			attoff;
	Datum	   *values;
	bool	   *isnull;
	LeopardTuple	newTuple;

	/*
	 * allocate and fill values and isnull arrays from either the tuple or the
	 * repl information, as appropriate.
	 *
	 * NOTE: it's debatable whether to use leopard_deform_tuple() here or just
	 * leopard_getattr() only the non-replaced columns.  The latter could win if
	 * there are many replaced columns and few non-replaced ones. However,
	 * leopard_deform_tuple costs only O(N) while the leopard_getattr way would cost
	 * O(N^2) if there are many non-replaced columns, so it seems better to
	 * err on the side of linear cost.
	 */
	values = (Datum *) palloc(numberOfAttributes * sizeof(Datum));
	isnull = (bool *) palloc(numberOfAttributes * sizeof(bool));

	leopard_deform_tuple(tuple, tupleDesc, values, isnull);

	for (attoff = 0; attoff < numberOfAttributes; attoff++)
	{
		if (doReplace[attoff])
		{
			values[attoff] = replValues[attoff];
			isnull[attoff] = replIsnull[attoff];
		}
	}

	/*
	 * create a new tuple from the values and isnull arrays
	 */
	newTuple = leopard_form_tuple(tupleDesc, values, isnull);

	pfree(values);
	pfree(isnull);

	/*
	 * copy the identification info of the old tuple: t_ctid, t_self
	 */
	newTuple->t_data->t_ctid = tuple->t_data->t_ctid;
	newTuple->t_self = tuple->t_self;
	newTuple->t_tableOid = tuple->t_tableOid;

	return newTuple;
}

/*
 * leopard_modify_tuple_by_cols
 *		form a new tuple from an old tuple and a set of replacement values.
 *
 * This is like leopard_modify_tuple, except that instead of specifying which
 * column(s) to replace by a boolean map, an array of target column numbers
 * is used.  This is often more convenient when a fixed number of columns
 * are to be replaced.  The replCols, replValues, and replIsnull arrays must
 * be of length nCols.  Target column numbers are indexed from 1.
 *
 * The result is allocated in the current memory context.
 */
LeopardTuple
leopard_modify_tuple_by_cols(LeopardTuple tuple,
						  TupleDesc tupleDesc,
						  int nCols,
						  int *replCols,
						  Datum *replValues,
						  bool *replIsnull)
{
	int			numberOfAttributes = tupleDesc->natts;
	Datum	   *values;
	bool	   *isnull;
	LeopardTuple	newTuple;
	int			i;

	/*
	 * allocate and fill values and isnull arrays from the tuple, then replace
	 * selected columns from the input arrays.
	 */
	values = (Datum *) palloc(numberOfAttributes * sizeof(Datum));
	isnull = (bool *) palloc(numberOfAttributes * sizeof(bool));

	leopard_deform_tuple(tuple, tupleDesc, values, isnull);

	for (i = 0; i < nCols; i++)
	{
		int			attnum = replCols[i];

		if (attnum <= 0 || attnum > numberOfAttributes)
			elog(ERROR, "invalid column number %d", attnum);
		values[attnum - 1] = replValues[i];
		isnull[attnum - 1] = replIsnull[i];
	}

	/*
	 * create a new tuple from the values and isnull arrays
	 */
	newTuple = leopard_form_tuple(tupleDesc, values, isnull);

	pfree(values);
	pfree(isnull);

	/*
	 * copy the identification info of the old tuple: t_ctid, t_self
	 */
	newTuple->t_data->t_ctid = tuple->t_data->t_ctid;
	newTuple->t_self = tuple->t_self;
	newTuple->t_tableOid = tuple->t_tableOid;

	return newTuple;
}

/*
 * leopard_deform_tuple
 *		Given a tuple, extract data into values/isnull arrays; this is
 *		the inverse of leopard_form_tuple.
 *
 *		Storage for the values/isnull arrays is provided by the caller;
 *		it should be sized according to tupleDesc->natts not
 *		LeopardTupleHeaderGetNatts(tuple->t_data).
 *
 *		Note that for pass-by-reference datatypes, the pointer placed
 *		in the Datum will point into the given tuple.
 *
 *		When all or most of a tuple's fields need to be extracted,
 *		this routine will be significantly quicker than a loop around
 *		leopard_getattr; the loop will become O(N^2) as soon as any
 *		noncacheable attribute offsets are involved.
 */
void
leopard_deform_tuple(LeopardTuple tuple, TupleDesc tupleDesc,
				  Datum *values, bool *isnull)
{
	LeopardTupleHeader tup = tuple->t_data;
	bool		hasnulls = LeopardTupleHasNulls(tuple);
	int			tdesc_natts = tupleDesc->natts;
	int			natts;			/* number of atts to extract */
	int			attnum;
	char	   *tp;				/* ptr to tuple data */
	uint32		off;			/* offset in tuple data */
	bits8	   *bp = tup->t_bits;	/* ptr to null bitmap in tuple */
	bool		slow = false;	/* can we use/set attcacheoff? */

	natts = LeopardTupleHeaderGetNatts(tup);

	/*
	 * In inheritance situations, it is possible that the given tuple actually
	 * has more fields than the caller is expecting.  Don't run off the end of
	 * the caller's arrays.
	 */
	natts = Min(natts, tdesc_natts);

	tp = (char *) tup + tup->t_hoff;

	off = 0;

	for (attnum = 0; attnum < natts; attnum++)
	{
		Form_pg_attribute thisatt = TupleDescAttr(tupleDesc, attnum);

		if (hasnulls && att_isnull(attnum, bp))
		{
			values[attnum] = (Datum) 0;
			isnull[attnum] = true;
			slow = true;		/* can't use attcacheoff anymore */
			continue;
		}

		isnull[attnum] = false;

		if (!slow && thisatt->attcacheoff >= 0)
			off = thisatt->attcacheoff;
		else if (thisatt->attlen == -1)
		{
			/*
			 * We can only cache the offset for a varlena attribute if the
			 * offset is already suitably aligned, so that there would be no
			 * pad bytes in any case: then the offset will be valid for either
			 * an aligned or unaligned value.
			 */
			if (!slow &&
				off == att_align_nominal(off, thisatt->attalign))
				thisatt->attcacheoff = off;
			else
			{
				off = att_align_pointer(off, thisatt->attalign, -1,
										tp + off);
				slow = true;
			}
		}
		else
		{
			/* not varlena, so safe to use att_align_nominal */
			off = att_align_nominal(off, thisatt->attalign);

			if (!slow)
				thisatt->attcacheoff = off;
		}

		values[attnum] = fetchatt(thisatt, tp + off);

		off = att_addlength_pointer(off, thisatt->attlen, tp + off);

		if (thisatt->attlen <= 0)
			slow = true;		/* can't use attcacheoff anymore */
	}

	/*
	 * If tuple doesn't have all the atts indicated by tupleDesc, read the
	 * rest as nulls or missing values as appropriate.
	 */
	for (; attnum < tdesc_natts; attnum++)
		values[attnum] = getmissingattr(tupleDesc, attnum + 1, &isnull[attnum]);
}

/*
 * leopard_freetuple
 */
void
leopard_freetuple(LeopardTuple leopardtup)
{
	pfree(leopardtup);
}


/*
 * leopard_form_minimal_tuple
 *		construct a MinimalLeopardTuple from the given values[] and isnull[] arrays,
 *		which are of the length indicated by tupleDescriptor->natts
 *
 * This is exactly like leopard_form_tuple() except that the result is a
 * "minimal" tuple lacking a LeopardTupleData header as well as room for system
 * columns.
 *
 * The result is allocated in the current memory context.
 */
MinimalLeopardTuple
leopard_form_minimal_tuple(TupleDesc tupleDescriptor,
						Datum *values,
						bool *isnull)
{
	MinimalLeopardTuple tuple;			/* return tuple */
	Size		len,
				data_len;
	int			hoff;
	bool		hasnull = false;
	int			numberOfAttributes = tupleDescriptor->natts;
	int			i;

	if (numberOfAttributes > MaxTupleAttributeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("number of columns (%d) exceeds limit (%d)",
						numberOfAttributes, MaxTupleAttributeNumber)));

	/*
	 * Check for nulls
	 */
	for (i = 0; i < numberOfAttributes; i++)
	{
		if (isnull[i])
		{
			hasnull = true;
			break;
		}
	}

	/*
	 * Determine total space needed
	 */
	len = SizeofMinimalLeopardTupleHeader;

	if (hasnull)
		len += BITMAPLEN(numberOfAttributes);

	hoff = len = MAXALIGN(len); /* align user data safely */

	data_len = leopard_compute_data_size(tupleDescriptor, values, isnull);

	len += data_len;

	/*
	 * Allocate and zero the space needed.
	 */
	tuple = (MinimalLeopardTuple) palloc0(len);

	/*
	 * And fill in the information.
	 */
	tuple->t_len = len;
	LeopardTupleHeaderSetNatts(tuple, numberOfAttributes);
	tuple->t_hoff = hoff + MINIMAL_LEOPARD_TUPLE_OFFSET;

	leopard_fill_tuple(tupleDescriptor,
					values,
					isnull,
					(char *) tuple + hoff,
					data_len,
					&tuple->t_infomask,
					(hasnull ? tuple->t_bits : NULL));

	return tuple;
}

/*
 * leopard_free_minimal_tuple
 */
void
leopard_free_minimal_tuple(MinimalLeopardTuple mtup)
{
	pfree(mtup);
}

/*
 * leopard_copy_minimal_tuple
 *		copy a MinimalLeopardTuple
 *
 * The result is allocated in the current memory context.
 */
MinimalLeopardTuple
leopard_copy_minimal_tuple(MinimalLeopardTuple mtup)
{
	MinimalLeopardTuple result;

	result = (MinimalLeopardTuple) palloc(mtup->t_len);
	memcpy(result, mtup, mtup->t_len);
	return result;
}

/*
 * leopard_tuple_from_minimal_tuple
 *		create a LeopardTuple by copying from a MinimalLeopardTuple;
 *		system columns are filled with zeroes
 *
 * The result is allocated in the current memory context.
 * The LeopardTuple struct, tuple header, and tuple data are all allocated
 * as a single palloc() block.
 */
LeopardTuple
leopard_tuple_from_minimal_tuple(MinimalLeopardTuple mtup)
{
	LeopardTuple	result;
	uint32		len = mtup->t_len + MINIMAL_LEOPARD_TUPLE_OFFSET;

	result = (LeopardTuple) palloc(LEOPARDTUPLESIZE + len);
	result->t_len = len;
	ItemPointerSetInvalid(&(result->t_self));
	result->t_tableOid = InvalidOid;
	result->t_data = (LeopardTupleHeader) ((char *) result + LEOPARDTUPLESIZE);
	memcpy((char *) result->t_data + MINIMAL_LEOPARD_TUPLE_OFFSET, mtup, mtup->t_len);
	memset(result->t_data, 0, offsetof(LeopardTupleHeaderData, t_infomask2));
	return result;
}

/*
 * minimal_tuple_from_leopard_tuple
 *		create a MinimalLeopardTuple by copying from a LeopardTuple
 *
 * The result is allocated in the current memory context.
 */
MinimalLeopardTuple
minimal_tuple_from_leopard_tuple(LeopardTuple leopardtup)
{
	MinimalLeopardTuple result;
	uint32		len;

	Assert(leopardtup->t_len > MINIMAL_LEOPARD_TUPLE_OFFSET);
	len = leopardtup->t_len - MINIMAL_LEOPARD_TUPLE_OFFSET;
	result = (MinimalLeopardTuple) palloc(len);
	memcpy(result, (char *) leopardtup->t_data + MINIMAL_LEOPARD_TUPLE_OFFSET, len);
	result->t_len = len;
	return result;
}

/*
 * This mainly exists so JIT can inline the definition, but it's also
 * sometimes useful in debugging sessions.
 */
size_t
varsize_any(void *p)
{
	return VARSIZE_ANY(p);
}
