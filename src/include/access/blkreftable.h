
#ifndef BLKREF_TABLE_H
#define BLKREF_TABLE_H

#include "nodes/pg_list.h"
#include "storage/block.h"
#include "storage/fd.h"
#include "storage/relfilelocator.h"
#include "utils/palloc.h"

typedef	struct BlockRefTable BlockRefTable;
typedef	struct BlockRefCombiner BlockRefCombiner;

extern BlockRefTable *CreateEmptyBlockRefTable(MemoryContext mcxt);

extern void BlockRefTableMarkRelationForkNew(BlockRefTable *brtab,
											 RelFileLocator *rlocator,
											 ForkNumber forknum);
extern void BlockRefTableMarkBlockModified(BlockRefTable *brtab,
										   RelFileLocator *rlocator,
										   ForkNumber forknum,
										   BlockNumber blknum);
extern void WriteBlockRefTable(BlockRefTable *brtab, File file);

extern BlockRefTable *ReadBlockRefTable(MemoryContext mcxt, File file);
extern bool BlockRefTableLookupRelationFork(BlockRefTable *brtab,
											RelFileLocator *rlocator,
											ForkNumber forknum,
											bool *is_new,
											void **modified_blocks);
extern BlockRefCombiner *CreateBlockRefCombiner(MemoryContext mcxt);
extern void BlockRefCombinerAddInputFile(BlockRefCombiner *bref, File file);
extern void BlockRefCombinerHashPartition(BlockRefCombiner *bref,
										  size_t size_limit,
										  unsigned *num_partitions,
										  File *partition);
extern void PrintBlockRefTable(BlockRefTable *brtab);

#endif							/* BLKREF_TABLE_H */
