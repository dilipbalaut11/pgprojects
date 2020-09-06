/*-------------------------------------------------------------------------
 *
 * toast_internals.h
 *	  Internal definitions for the TOAST system.
 *
 * Copyright (c) 2000-2020, PostgreSQL Global Development Group
 *
 * src/include/access/toast_internals.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TOAST_INTERNALS_H
#define TOAST_INTERNALS_H

#include "storage/lockdefs.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

/*
 *	The information at the start of the compressed toast data.
 */
typedef struct toast_compress_header
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	uint32		info;			/* flags (2 bits) and rawsize */
} toast_compress_header;

#define RAWSIZEMASK (0x3FFFFFFFU)

/*
 * Utilities for manipulation of header information for compressed
 * toast entries.
 *
 * Since version 11 TOAST_COMPRESS_SET_RAWSIZE also marks compressed
 * varlenas as custom compressed. Such varlenas will contain 0x02 (0b10) in
 * two highest bits.
 */
#define TOAST_COMPRESS_HDRSZ		((int32) sizeof(toast_compress_header))
#define TOAST_COMPRESS_RAWSIZE(ptr) (((toast_compress_header *)(ptr))->info & RAWSIZEMASK)
#define TOAST_COMPRESS_METHOD(ptr)  (((toast_compress_header *)(ptr))->info >> 30)
#define TOAST_COMPRESS_SIZE(ptr)	((int32) VARSIZE_ANY(ptr) - TOAST_COMPRESS_HDRSZ)
#define TOAST_COMPRESS_RAWDATA(ptr) \
	(((char *) (ptr)) + TOAST_COMPRESS_HDRSZ)
#define TOAST_COMPRESS_SET_RAWSIZE(ptr, len) \
do { \
	Assert(len > 0 && len <= RAWSIZEMASK); \
	((toast_compress_header *) (ptr))->info = (len); \
} while (0)

#define TOAST_COMPRESS_SET_COMPRESSION_METHOD(ptr, cm_method) \
	((toast_compress_header *)(ptr))->info |= ((cm_method) << 30);

#if 0
/*
 * Macro to fetch the possibly-unaligned contents of an EXTERNAL datum
 * into a local "struct varatt_external" toast pointer.  This should be
 * just a memcpy, but some versions of gcc seem to produce broken code
 * that assumes the datum contents are aligned.  Introducing an explicit
 * intermediate "varattrib_1b_e *" variable seems to fix it.
 */

#define VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr)                              \
	do                                                                                \
	{                                                                                 \
		varattrib_1b_e *attre = (varattrib_1b_e *)(attr);                             \
		Assert(VARATT_IS_EXTERNAL(attre));                                            \
		Assert(VARSIZE_EXTERNAL(attre) == sizeof(toast_pointer) + VARHDRSZ_EXTERNAL); \
		memcpy(&(toast_pointer), VARDATA_EXTERNAL(attre), sizeof(toast_pointer));     \
	} while (0)
#endif
extern Datum toast_compress_datum(Datum value, uint8 cm_method);
extern Oid	toast_get_valid_index(Oid toastoid, LOCKMODE lock);

extern void toast_delete_datum(Relation rel, Datum value, bool is_speculative);
extern Datum toast_save_datum(Relation rel, Datum value,
							  struct varlena *oldexternal, int options);

extern int	toast_open_indexes(Relation toastrel,
							   LOCKMODE lock,
							   Relation **toastidxs,
							   int *num_indexes);
extern void toast_close_indexes(Relation *toastidxs, int num_indexes,
								LOCKMODE lock);
extern void init_toast_snapshot(Snapshot toast_snapshot);

/*
 * toast_set_compressed_datum_info -
 *
 * Save metadata in compressed datum
 */
extern void toast_set_compressed_datum_info(struct varlena *val, uint8 cm_method,
											int32 rawsize);

#endif							/* TOAST_INTERNALS_H */
