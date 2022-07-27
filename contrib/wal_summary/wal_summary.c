/*-------------------------------------------------------------------------
 *
 * wal_summary.c
 *	  Functions to generate wal summary file
 *
 * Copyright (c) 2016-2022, PostgreSQL Global Development Group
 *
 *	  contrib/wal_summary/wal_summary.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/detoast.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/heaptoast.h"
#include "access/multixact.h"
#include "access/toast_internals.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "access/visibilitymap.h"
#include "blkref_table.h"
#include "catalog/pg_am.h"
#include "catalog/storage_xlog.h"
#include "common/logging.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/pg_lsn.h"

/*
Design:
	extern BlockRefTable *CreateEmptyBlockRefTable(MemoryContext mcxt);
	extern void BlockRefTableMarkRelationForkNew(BlockRefTable *brtab,
												RelFileNode *rlocator,
												ForkNumber forknum);
	extern void BlockRefTableMarkBlockModified(BlockRefTable *brtab,
											RelFileNode *rlocator,
											ForkNumber forknum,
											BlockNumber blknum);
	extern void WriteBlockRefTable(BlockRefTable *brtab, File file);

	BlockRefTableMarkRelationForkNew() is there to avoid filename reuse
	problems. The idea is that we'll call that function for any operation
	that creates a new relfilenode, or truncates an existing file. Once
	that's been called the BlockRefTable won't track individual block
	modifications for that relation fork. It will just remember that the
	entire relation fork is "new". And then for each block reference we
	will call BlockRefTableMarkBlockModified() which will do nothing if
	that relation fork is marked "new" and otherwise remember the block
	number for that relation fork. So internally the data structure would
	have a hash table with RelFileNode+ForkNumber as the key and the value
	would be an is-new Boolean and also a Bitmapset or similar for the
	list of changed blocks.

	Then we need some way to look up blocks when performing an incremental
	backup to see if they have been changed. We'll have to read files back
	from disk and then look things up. It seems like we can use the same
	data structure here, just with some new interface functions:

	extern BlockRefTable *ReadBlockRefTable(MemoryContext mcxt, File file);
	extern void BlockRefTableLookupRelationFork(BlockRefTable *brtab,
												RelFileNode *rlocator,
												bool *is_new,
												Bitmapset **modified_blocks);

	One problem with this scheme is that we don't need to just read and
	write individual files, we also need to combine files. If we have
	created many files by summarizing individual WAL files or ranges of
	WAL files, an incremental backup needs to merge data from all the
	files that cover portions of the WAL range from the old backup's start
	LSN to the new backup's start LSN. The combined data might be too big
	to fit in memory, so I think what we need to do is combine all that
	data but then hash-partition it into an appropriate number of slices
	so that each slice fits in memory. That might be just 1 slice or more.
	So I imagine it like this:

	extern BlockRefCombiner *CreateBlockRefCombiner(MemoryContext mcxt);
	extern void BlockRefCombinerAddInputFile(BlockRefCombiner *bref, File file);
	extern void BlockRefCombinerHashPartition(BlockRefCombiner *bref,
											size_t size_limit,
											unsigned *num_partitions,
											File *partition);

	What you'd do is call BlockRefCombinerAddInputFile() for each
	individual file that covers part of the WAL range you care about. That
	wouldn't read anything, it would just save a reference to the file.
	Then you'd call BlockRefCombinerHashPartition() and it would figure
	out how many partitions are needed based on the size limit and
	generate that many new files. Each of those files could be read using
	ReadBlockRefTable() at a later time. Since each file would be written
	in sorted order, the process of merging and partitioning the data in
	this way can be done with a bounded amount of memory. Multiple merge
	passes would probably be smart if there are more than a few hundred
	files, to avoid thrashing the file descriptor cache. Notice that here
	we don't have any hash table internally.

	I thought of another time you might want to use a BlockRefCombiner (as
	I've called it here). Suppose that the user sets wal_summarization=1TB
	but maintenance_work_mem=1MB. Then you could argue that we ought to
	use no more than 1MB of memory while summarizing WAL, so we ought to
	just generate individual summaries until we fill up 1MB, then write
	that summary and start a new one. Then merge them after using the
	BlockRefCombiner stuff. While we could do that, I'm not sure we need
	to, because maybe WAL summarization doesn't need to respect work_mem.
	Perhaps we can just document that summarizing a bigger range will use
	more memory, and if you are summarizing so much WAL at once that you
	run out of memory, summarize less. I think that's kind of reasonable
	because it seems a bit silly to do combining both while initially
	generating the files and then more combining when using them for
	incremental backup.
 */
PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(generate_wal_summary_file);
PG_FUNCTION_INFO_V1(print_wal_summary_file);
PG_FUNCTION_INFO_V1(read_wal_summary_relinfo);


static void walsummary_process_wal(XLogRecPtr start_lsn, XLogRecPtr end_lsn);
static void walsummary_process_one_record(BlockRefTable *brtab,
										  XLogReaderState *record);

/*
 * Generate wal summary file.
 */
Datum
generate_wal_summary_file(PG_FUNCTION_ARGS)
{
	XLogRecPtr	start_lsn = PG_GETARG_LSN(0);
	XLogRecPtr	end_lsn = PG_GETARG_LSN(1);

	walsummary_process_wal(start_lsn, end_lsn);


	PG_RETURN_LSN(start_lsn);
}

static inline void
WalSummaryFilePath(char *path, XLogRecPtr start_lsn, XLogRecPtr end_lsn)
{
	snprintf(path, MAXPGPATH, XLOGDIR "/%X_%X.%X_%X.walsummary",
			 LSN_FORMAT_ARGS(start_lsn),
			 LSN_FORMAT_ARGS(end_lsn));
}

Datum
print_wal_summary_file(PG_FUNCTION_ARGS)
{
	XLogRecPtr	start_lsn = PG_GETARG_LSN(0);
	XLogRecPtr	end_lsn = PG_GETARG_LSN(1);
	BlockRefTable *brtab;
	File		file;
	char		path[MAXPGPATH];

	WalSummaryFilePath(path, start_lsn, end_lsn);

	file = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (file < 0)
		elog(ERROR, "could not open block reference file ");

	brtab = ReadBlockRefTable(CurrentMemoryContext, file);
	PrintBlockRefTable(brtab);
	CloseTransientFile(file);
	PG_RETURN_LSN(start_lsn);
}

Datum
read_wal_summary_relinfo(PG_FUNCTION_ARGS)
{
	XLogRecPtr	start_lsn = PG_GETARG_LSN(0);
	XLogRecPtr	end_lsn = PG_GETARG_LSN(1);
	Oid			tsid = PG_GETARG_OID(2);
	Oid			dbid = PG_GETARG_OID(3);
	RelFileNumber	relnumber = PG_GETARG_OID(4);
	ForkNumber	fork = PG_GETARG_INT32(5);
	RelFileLocator	rlocator;
	BlockRefTable *brtab;
	File		file;
	bool		isnew;
	char		path[MAXPGPATH];
	void	   *modified_blocks;

	WalSummaryFilePath(path, start_lsn, end_lsn);

	file = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (file < 0)
		elog(ERROR, "could not open block reference file ");

	brtab = ReadBlockRefTable(CurrentMemoryContext, file);

	rlocator.spcOid = tsid;
	rlocator.dbOid = dbid;
	rlocator.relNumber = relnumber;

	BlockRefTableLookupRelationFork(brtab, &rlocator, fork, &isnew,
									&modified_blocks);
	CloseTransientFile(file);
	PG_RETURN_LSN(start_lsn);
}

static void
walsummary_process_wal(XLogRecPtr start_lsn, XLogRecPtr end_lsn)
{
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char	   *errormsg;
	BlockRefTable *brtab;
	File		file;
	char		path[MAXPGPATH];

	brtab = CreateEmptyBlockRefTable(CurrentMemoryContext);

	xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
									XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
											   .segment_open = &wal_segment_open,
											   .segment_close = &wal_segment_close),
									NULL);

	XLogBeginRead(xlogreader, start_lsn);
	while(true)
	{
		record = XLogReadRecord(xlogreader, &errormsg);

		if (record == NULL)
			break;

		walsummary_process_one_record(brtab, xlogreader);

		if (xlogreader->EndRecPtr == end_lsn)
			break;

	}

	/* wal summary file path. */
	WalSummaryFilePath(path, start_lsn, end_lsn);

	file = OpenTransientFile(path, O_RDWR | O_CREAT | PG_BINARY);
	if (file < 0)
		elog(ERROR, "could not open block reference file ");

	WriteBlockRefTable(brtab, file);
	CloseTransientFile(file);
}

/*
 * Extract information on which blocks the current record modifies.
 */
static void
walsummary_process_one_record(BlockRefTable *brtab, XLogReaderState *record)
{
	int			block_id;
	RmgrId		rmid = XLogRecGetRmid(record);
	uint8		info = XLogRecGetInfo(record);
	uint8		rminfo = info & ~XLR_INFO_MASK;

	if (rmid == RM_SMGR_ID && rminfo == XLOG_SMGR_CREATE)
	{
		xl_smgr_create *xlrec = (xl_smgr_create *) XLogRecGetData(record);

		BlockRefTableMarkRelationForkNew(brtab, &xlrec->rlocator,
										 xlrec->forkNum);
	}

	for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
	{
		RelFileLocator rlocator;
		ForkNumber	forknum;
		BlockNumber blkno;

		if (!XLogRecGetBlockTagExtended(record, block_id,
										&rlocator, &forknum, &blkno, NULL))
			continue;

		/* We only care about the main fork; others are copied in toto */
		if (forknum != MAIN_FORKNUM)
			continue;

		BlockRefTableMarkBlockModified(brtab, &rlocator, forknum, blkno);
	}
}

#if 0
/*
 * This callback gets called while we read the WAL in the target, for every
 * block that has changed in the target system.  It decides if the given
 * 'blkno' in the target relfile needs to be overwritten from the source, and
 * if so, records it in 'target_pages_to_overwrite' bitmap.
 *
 * NOTE: All the files on both systems must have already been added to the
 * hash table!
 */
void
process_target_wal_block_change(ForkNumber forknum, RelFileNode rlocator,
								BlockNumber blkno)
{
	char	   *path;
	file_entry_t *entry;
	BlockNumber blkno_inseg;
	int			segno;

	segno = blkno / RELSEG_SIZE;
	blkno_inseg = blkno % RELSEG_SIZE;

	path = datasegpath(rlocator, forknum, segno);
	entry = lookup_filehash_entry(path);
	pfree(path);

	/*
	 * If the block still exists in both systems, remember it. Otherwise we
	 * can safely ignore it.
	 *
	 * If the block is beyond the EOF in the source system, or the file
	 * doesn't exist in the source at all, we're going to truncate/remove it
	 * away from the target anyway. Likewise, if it doesn't exist in the
	 * target anymore, we will copy it over with the "tail" from the source
	 * system, anyway.
	 *
	 * It is possible to find WAL for a file that doesn't exist on either
	 * system anymore. It means that the relation was dropped later in the
	 * target system, and independently on the source system too, or that it
	 * was created and dropped in the target system and it never existed in
	 * the source. Either way, we can safely ignore it.
	 */
	if (entry)
	{
		Assert(entry->isrelfile);

		if (entry->target_exists)
		{
			if (entry->target_type != FILE_TYPE_REGULAR)
				pg_fatal("unexpected page modification for non-regular file \"%s\"",
						 entry->path);

			if (entry->source_exists)
			{
				off_t		end_offset;

				end_offset = (blkno_inseg + 1) * BLCKSZ;
				if (end_offset <= entry->source_size && end_offset <= entry->target_size)
					datapagemap_add(&entry->target_pages_to_overwrite, blkno_inseg);
			}
		}
	}
}

/*
 * Add a block to the bitmap.
 */
void
datapagemap_add(datapagemap_t *map, BlockNumber blkno)
{
	int			offset;
	int			bitno;

	offset = blkno / 8;
	bitno = blkno % 8;

	/* enlarge or create bitmap if needed */
	if (map->bitmapsize <= offset)
	{
		int			oldsize = map->bitmapsize;
		int			newsize;

		/*
		 * The minimum to hold the new bit is offset + 1. But add some
		 * headroom, so that we don't need to repeatedly enlarge the bitmap in
		 * the common case that blocks are modified in order, from beginning
		 * of a relation to the end.
		 */
		newsize = offset + 1;
		newsize += 10;

		map->bitmap = pg_realloc(map->bitmap, newsize);

		/* zero out the newly allocated region */
		memset(&map->bitmap[oldsize], 0, newsize - oldsize);

		map->bitmapsize = newsize;
	}

	/* Set the bit */
	map->bitmap[offset] |= (1 << bitno);
}
#endif

