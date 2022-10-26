/*-------------------------------------------------------------------------
 *
 * visibilitymap.h
 *		visibility map interface
 *
 *
 * Portions Copyright (c) 2022, EnterpriseDB Corporation. All Rights Reserved.
 *
 *
 *
 *
 *
 * Portions Copyright (c) 2007-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/visibilitymap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LEOPARDVISMAP_H
#define LEOPARDVISMAP_H

#include "access/visibilitymapdefs.h"
#include "access/xlogdefs.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "utils/relcache.h"

/* Macros for visibilitymap test */
#define LEOPARDVM_ALL_VISIBLE(r, b, v) \
	((leopard_vismap_get_status((r), (b), (v)) & LEOPARDVISMAP_ALL_VISIBLE) != 0)
#define LEOPARDVM_ALL_FROZEN(r, b, v) \
	((leopard_vismap_get_status((r), (b), (v)) & LEOPARDVISMAP_ALL_FROZEN) != 0)

extern bool leopard_vismap_clear(Relation rel, BlockNumber leopardBlk,
								Buffer vmbuf, uint8 flags);
extern void leopard_vismap_pin(Relation rel, BlockNumber leopardBlk,
							  Buffer *vmbuf);
extern bool leopard_vismap_pin_ok(BlockNumber leopardBlk, Buffer vmbuf);
extern void leopard_vismap_set(Relation rel, BlockNumber leopardBlk, Buffer leopardBuf,
							  XLogRecPtr recptr, Buffer vmBuf, TransactionId cutoff_xid,
							  uint8 flags);
extern uint8 leopard_vismap_get_status(Relation rel, BlockNumber leopardBlk, Buffer *vmbuf);
extern void leopard_vismap_count(Relation rel, BlockNumber *all_visible, BlockNumber *all_frozen);
extern BlockNumber leopard_vismap_prepare_truncate(Relation rel,
												  BlockNumber nleopardblocks);

#endif							/* LEOPARDVISMAP_H */
