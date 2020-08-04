/*-------------------------------------------------------------------------
 *
 * detoast.h
 *	  Access to compressed and external varlena values.
 *
 * Copyright (c) 2000-2020, PostgreSQL Global Development Group
 *
 * src/include/access/detoast.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DETOAST_H
#define DETOAST_H

#include "access/attnum.h"
#include "nodes/pg_list.h"

/* Size of an EXTERNAL datum that contains a standard TOAST pointer */
#define TOAST_POINTER_SIZE (VARHDRSZ_EXTERNAL + sizeof(varatt_external))

/* Size of an EXTERNAL datum that contains an indirection pointer */
#define INDIRECT_POINTER_SIZE (VARHDRSZ_EXTERNAL + sizeof(varatt_indirect))

/* ----------
 * detoast_external_attr() -
 *
 *		Fetches an external stored attribute from the toast
 *		relation. Does NOT decompress it, if stored external
 *		in compressed format.
 * ----------
 */
extern struct varlena *detoast_external_attr(struct varlena *attr);

/* ----------
 * detoast_attr() -
 *
 *		Fully detoasts one attribute, fetching and/or decompressing
 *		it as needed.
 * ----------
 */
extern struct varlena *detoast_attr(struct varlena *attr);

/* ----------
 * detoast_attr_slice() -
 *
 *		Fetches only the specified portion of an attribute.
 *		(Handles all cases for attribute storage)
 * ----------
 */
extern struct varlena *detoast_attr_slice(struct varlena *attr,
										  int32 sliceoffset,
										  int32 slicelength);

/* ----------
 * toast_raw_datum_size -
 *
 *	Return the raw (detoasted) size of a varlena datum
 * ----------
 */
extern Size toast_raw_datum_size(Datum value);

/* ----------
 * toast_datum_size -
 *
 *	Return the storage size of a varlena datum
 * ----------
 */
extern Size toast_datum_size(Datum value);

#endif							/* DETOAST_H */
