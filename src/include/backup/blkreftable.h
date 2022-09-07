/*-------------------------------------------------------------------------
 *
 * blkreftable.h
 *	  Block reference tables.
 *
 * A block reference table is used to keep track of which blocks have
 * been modified by WAL records within a certain LSN range.
 *
 * For each relation fork, there is a "limit block number". All existing
 * blocks greater than or equal to the limit block number must be
 * considered modified; for those less than the limit block number,
 * we maintain a bitmap. When a relation fork is created or dropped,
 * the limit block number should be set to 0. When it's truncated,
 * the limit block number should be set to the length in blocks to
 * which it was truncated.
 *
 * Portions Copyright (c) 2010-2022, PostgreSQL Global Development Group
 *
 * src/include/backup/blkreftable.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BLKREFTABLE_H
#define BLKREFTABLE_H

#include "nodes/bitmapset.h"
#include "storage/block.h"
#include "storage/fd.h"
#include "storage/relfilelocator.h"

struct BlockRefTable;
typedef struct BlockRefTable BlockRefTable;

extern BlockRefTable *CreateEmptyBlockRefTable(void);

extern void BlockRefTableSetLimitBlock(BlockRefTable *brtab,
									   const RelFileLocator *rlocator,
									   ForkNumber forknum,
									   BlockNumber limit_block);
extern void BlockRefTableMarkBlockModified(BlockRefTable *brtab,
										   const RelFileLocator *rlocator,
										   ForkNumber forknum,
										   BlockNumber blknum);

extern void ReadBlockRefTable(BlockRefTable *brtab, File file);
extern void WriteBlockRefTable(BlockRefTable *brtab, File file);

#endif							/* BLKREFTABLE_H */
