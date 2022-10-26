/*-------------------------------------------------------------------------
 *
 * hio.h
 *	  POSTGRES leopard access method input/output definitions.
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
 * src/include/access/hio.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LEOPARDIO_H
#define LEOPARDIO_H

#include "access/leopardtup.h"
#include "storage/buf.h"
#include "utils/relcache.h"

/*
 * state for bulk inserts --- private to leopardam.c and hio.c
 *
 * If current_buf isn't InvalidBuffer, then we are holding an extra pin
 * on that buffer.
 *
 * "typedef struct BulkInsertStateData *BulkInsertState" is in leopardam.h
 */



extern void RelationPutLeopardTuple(Relation relation, Buffer buffer,
								 LeopardTuple tuple, bool token);
extern Buffer RelationGetBufferForTuple(Relation relation, Size len,
										Buffer otherBuffer, int options,
										BulkInsertStateData *bistate,
										Buffer *vmbuffer, Buffer *vmbuffer_other);

#endif							/* LEOPARDIO_H */
