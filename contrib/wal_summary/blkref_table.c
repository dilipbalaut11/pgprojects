/*
 * contrib/wal_sumarry/blkref_table.c
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>
#include "blkref_table.h"
#include "catalog/storage_xlog.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/pg_lsn.h"

struct BlockRefTable
{
	XLogRecPtr	start_lsn;
	XLogRecPtr	end_lsn;
	HTAB	   *hash;
};

typedef struct BlockRefTableKey
{
	RelFileLocator	rlocator;
	ForkNumber		fork;
} BlockRefTableKey;

typedef struct BlockRefTableEntry
{
	BlockRefTableKey	key;		/* hash key of BlockRefTable */
	bool				isrelnew;	/* is entry for new relation creation*/
	int					nblocks;	/* no. of changed blocks. */
	List			   *chngblk;	/* changed blk list, NULL if isrelnew is
									   true. */
} BlockRefTableEntry;

#define SizeOfFixBlockRefEntry	offsetof(BlockRefTableEntry, chngblk)
#define BLOCKREF_FILE_ENTRY_SZ(nblocks)	\
				SizeOfFixBlockRefEntry + (nblocks) * sizeof(BlockNumber)

struct BlockRefCombiner
{
	int	nfiles;
};

#define PG_BLOCKREF_FILE	"file1"
#define PG_BLOCKREF_BUFSZ	BLCKSZ
#define PG_SINGLE_FLUSH_SZ	sizeof(int32)

/*
 * Create an empty block reference table.
 */
BlockRefTable *
CreateEmptyBlockRefTable(MemoryContext mcxt)
{
	BlockRefTable  *brtab;
	HASHCTL			ctl;

	brtab = MemoryContextAlloc(mcxt, sizeof(BlockRefTable));

	ctl.keysize = sizeof(BlockRefTableKey);
	ctl.entrysize = sizeof(BlockRefTableEntry);
	ctl.hcxt = mcxt;

	brtab->hash = hash_create("buf reference table",
							  8192, /* start small and extend */
							  &ctl,
							  HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
	return brtab;
}

void
BlockRefTableMarkRelationForkNew(BlockRefTable *brtab,
								 RelFileLocator *rlocator,
								 ForkNumber forknum)
{
	BlockRefTableKey	key;
	BlockRefTableEntry *entry;
	bool	found;

	key.rlocator = *rlocator;
	key.fork = forknum;

	entry = (BlockRefTableEntry *) hash_search(brtab->hash,
											   &key, HASH_ENTER, &found);
	entry->isrelnew = true;
	entry->nblocks = 0;
	if (found && entry->chngblk != NIL)
		list_free_deep(entry->chngblk);

	entry->chngblk = NIL;
}

void
BlockRefTableMarkBlockModified(BlockRefTable *brtab,
							   RelFileLocator *rlocator,
							   ForkNumber forknum,
							   BlockNumber blknum)
{
	BlockRefTableKey	key;
	BlockRefTableEntry *entry;
	bool	found;

	key.rlocator = *rlocator;
	key.fork = forknum;

	entry = (BlockRefTableEntry *) hash_search(brtab->hash,
											   &key, HASH_ENTER, &found);

	if (!found)
	{
		entry->chngblk = NULL;
		entry->nblocks = 0;
	}
	else if (entry->isrelnew)
		return;

	entry->chngblk = list_append_unique_int(entry->chngblk, blknum);
	entry->nblocks = list_length(entry->chngblk);
}

/* WriteBlockRefTable
 *
 * --file format--
 *	nentries
 *	entry1{RelFileLocator, ForkNum, isnewrel, nblock, block1, block2,...,blockn}
 *	entry2{RelFileLocator, ForkNum, isnewrel, nblock, block1, block2,...,blockn}
 *	...
 *	entryn{RelFileLocator, ForkNum, isnewrel, nblock, block1, block2,...,blockn}
 */
void
WriteBlockRefTable(BlockRefTable *brtab, File fd)
{
	HASH_SEQ_STATUS			seq_status;
	BlockRefTableEntry	   *entry;
	char	buf[PG_BLOCKREF_BUFSZ];
	int		nentries = 0;
	int		index = PG_SINGLE_FLUSH_SZ;
	int		writeoff = sizeof(uint32);
	
	/*
	 * Scan the complete hash and convert into a flat buffer and flush to file.
	 */
	hash_seq_init(&seq_status, brtab->hash);

	while ((entry = hash_seq_search(&seq_status)) != NULL)
	{
		int			nblocks;
		ListCell   *lc;

		/*
		 * If this entry can not fit into the remaining space of the buffer
		 * then first flush the buffer to the file and reset the buffer.
		 */
		nblocks = list_length(entry->chngblk);
		if (BLOCKREF_FILE_ENTRY_SZ(nblocks) > PG_BLOCKREF_BUFSZ - index)
		{
			/* copy this flush size at the beginning of the buffer. */
			*(int32 *) buf = index - PG_SINGLE_FLUSH_SZ;
			if (pg_pwrite(fd, buf, index, writeoff) != index)
				elog(ERROR, "could not write block reference file ");
			writeoff += index;
			index = PG_SINGLE_FLUSH_SZ;
		}

		/* flatten entry to buffer and increase index */
		memcpy(buf + index, entry, SizeOfFixBlockRefEntry);
		index += SizeOfFixBlockRefEntry;

		/* copy changed block list. */
		foreach(lc, entry->chngblk)
		{
			BlockNumber			blkno = lfirst_int(lc);

			*((BlockNumber *) (buf + index)) = blkno;
			index += sizeof(BlockNumber);
		}
		nentries++;
	}

	/* write remaining data to the file. */
	if (index > 0)
	{
		/* copy this flush size at the beginning of the buffer. */
		*(int32 *) buf = index - PG_SINGLE_FLUSH_SZ;		

		if (pg_pwrite(fd, buf, index, writeoff) != index)
			elog(ERROR, "could not write block reference file ");
	}

	if (pg_pwrite(fd, (char *) (&nentries), sizeof(uint32), 0) != sizeof(uint32))
		elog(ERROR, "could not write block reference file ");

	CloseTransientFile(fd);
}

/* for debugging. */
static void
PrintBlockRefTable(BlockRefTable *brtab)
{
	HASH_SEQ_STATUS			seq_status;
	BlockRefTableEntry	   *entry;
	int		nentries = 0;
	StringInfoData str;

	initStringInfo(&str);

	/*
	 * Scan the complete hash and convert into a flat buffer and flush to file.
	 */
	hash_seq_init(&seq_status, brtab->hash);

	while ((entry = hash_seq_search(&seq_status)) != NULL)
	{
		ListCell   *lc;

		resetStringInfo(&str);
		appendStringInfo(&str, "...Entry Start...\n");

		appendStringInfo(&str, "dboid=%u relnumber=%u isnew=%d nblocks=%u\n",
						entry->key.rlocator.dbOid,
						entry->key.rlocator.relNumber,
						entry->isrelnew, entry->nblocks);

		/* copy changed block list. */
		foreach(lc, entry->chngblk)
		{
			BlockNumber			blkno = lfirst_int(lc);

			appendStringInfo(&str, "block no=%d \n", blkno);
		}

		appendStringInfo(&str, "...Entry End...\n");
		elog(LOG, "%s", str.data);
		nentries++;
	}
}

BlockRefTable *
ReadBlockRefTable(MemoryContext mcxt, File fd)
{
	BlockRefTable *brtab;
	BlockRefTableEntry	*bufentry;
	BlockRefTableEntry	*entry;
	char	buf[PG_BLOCKREF_BUFSZ];
	int		nentries = 0;
	int		index = 0;
	int		bufdatasz = 0;
	int		readoff = 0;

	if (pg_pread(fd, (char *) (&nentries), sizeof(uint32), readoff) != sizeof(uint32))
		elog(ERROR, "could not read block reference file ");

	if (nentries == 0)
		return NULL;

	brtab = CreateEmptyBlockRefTable(mcxt);

	readoff += sizeof(uint32);

	while(nentries-- > 0)
	{
		int 	i = 0;
		bool	found;

		if (bufdatasz == 0)
		{
			if (pg_pread(fd, (char *) (&bufdatasz), sizeof(uint32), readoff) != sizeof(uint32))
				elog(ERROR, "could not read block reference file ");

			readoff += sizeof(uint32);

			if (pg_pread(fd, buf, bufdatasz, readoff) != bufdatasz)
				elog(ERROR, "could not read block reference file ");

			index = 0;
		}

		bufentry = (BlockRefTableEntry *) (buf + index);
		entry = hash_search(brtab->hash, &bufentry->key, HASH_ENTER, &found);
		Assert(!found);

		index += SizeOfFixBlockRefEntry;

		entry->isrelnew = bufentry->isrelnew;
		entry->chngblk = NIL;
		if (entry->isrelnew)
			continue;
		entry->nblocks = bufentry->nblocks;

		/* read changed block list. */
		for (i = 0; i < bufentry->nblocks; i++)
		{
			BlockNumber blknum = *(BlockNumber *) (buf + index);

			entry->chngblk = list_append_unique_int(entry->chngblk, blknum);
			index += sizeof(BlockNumber);
		}
	}

	PrintBlockRefTable(brtab);

	return brtab;
}

bool
BlockRefTableLookupRelationFork(BlockRefTable *brtab,
								RelFileLocator *rlocator,
								ForkNumber forknum,	
								bool *is_new,
								List **modified_blocks)
{
	BlockRefTableKey	key;
	BlockRefTableEntry *entry;
	bool	found;

	key.rlocator = *rlocator;
	key.fork = forknum;

	entry = (BlockRefTableEntry *) hash_search(brtab->hash,
											   &key, HASH_FIND, &found);

	if (entry == NULL)
		return false;

	*is_new = entry->isrelnew;

	*modified_blocks = list_copy_deep(entry->chngblk);

	return true;
}

BlockRefCombiner*
CreateBlockRefCombiner(MemoryContext mcxt)
{
	return NULL;
}

void
BlockRefCombinerAddInputFile(BlockRefCombiner *bref, File file)
{
	return;
}

void
BlockRefCombinerHashPartition(BlockRefCombiner *bref,
							  size_t size_limit,
							  unsigned *num_partitions,
							  File *partition)
{
	return;
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
process_target_wal_block_change(ForkNumber forknum, RelFileLocator rlocator,
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