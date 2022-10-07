/*-------------------------------------------------------------------------
 *
 * blkreftable.c
 *	  Block reference tables.
 *
 * A block reference table is used to keep track of which blocks have
 * been modified by WAL records within a certain LSN range.
 *
 * XXX We should add a CRC to the file format.
 *
 * Portions Copyright (c) 2010-2022, PostgreSQL Global Development Group
 *
 * src/backend/backup/blkreftable.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "backup/blkreftable.h"
#include "common/hashfn.h"
#include "utils/wait_event.h"

/*
 * A block reference table keeps track of the status of each relation
 * fork individually.
 */
typedef struct BlockRefTableKey
{
	RelFileLocator	rlocator;
	ForkNumber		forknum;
} BlockRefTableKey;

/*
 * We keep track of large numbers of modified blocks using an array of chunks.
 * The data for the i'th block is stored in the chunk given
 * by i / NCHUNKELEMENTS.  The chunks can be used either as arrays of offsets
 * or as bitmaps.
 *
 * When used as an array of offsets, each element of the array is a 2-byte
 * offset from the first block number whose status is represented by this
 * chunk.
 *
 * When used as a bitmap, the allocated length must be PG_UINT16_MAX and
 * each array entry represents the status of 16 blocks. The least significant
 * bit of the first array element is the status of the lowest-numbered
 * block covered by this chunk.
 *
 * Chunks aren't self-identifying. A chunk doesn't tell you whether it's
 * being used as an array of offsets or a bitmap; and if it's being used
 * as an array of offsets, it also doesn't tell you the allocated length
 * or how many entries are being used. That information is stored in the
 * BlockRefTableEntry.
 */
#define BLOCKS_PER_CHUNK		(1 << 16)
#define BLOCKS_PER_ENTRY		(BITS_PER_BYTE * sizeof(uint16))
#define MAX_ENTRIES_PER_CHUNK	(BLOCKS_PER_CHUNK / BLOCKS_PER_ENTRY)
#define INITIAL_ENTRIES_PER_CHUNK	16
typedef uint16 *BlockRefTableChunk;

/*
 * State for one relation fork.
 *
 * 'rlocator' and 'forknum' identify the relation fork for which this
 * entry holds the tates.
 *
 * 'limit_block' is the threshold from which all extant blocks should
 * be considered to have been modified. It should be set to 0 if the
 * relation fork is created or dropped. If the relation fork is truncated,
 * it should be set to the number of blocks that remain after truncation.
 * We shouldn't track state for any block whose block number is greater
 * than or equal to this value.
 *
 * 'nchunks' is the allocated length of each of the three arrays that follow.
 * We can only represent the status of block numbers less than nchunks *
 * BLOCKS_PER_CHUNK.
 *
 * 'chunk_size' is an array storing the allocated size of each chunk.
 *
 * 'chunk_usage' is an array storing the number of elements used in each
 * chunk. If that value is less than MAX_ENTIRES_PER_CHUNK, the corresonding
 * chunk is used as an array; else the corresponding chunk is used as a bitmap.
 *
 * 'chunk_data' is the array of chunks.
 */
typedef struct BlockRefTableEntry
{
	BlockRefTableKey	key;
	BlockNumber		limit_block;
	char			status;
	uint32			nchunks;
	uint16		   *chunk_size;
	uint16		   *chunk_usage;
	BlockRefTableChunk *chunk_data;
} BlockRefTableEntry;

/* Declare and define a hash table over type BlockRefTableEntry. */
#define SH_PREFIX blockreftable
#define SH_ELEMENT_TYPE BlockRefTableEntry
#define SH_KEY_TYPE BlockRefTableKey
#define SH_KEY key
#define SH_HASH_KEY(tb, key) \
	hash_bytes((const unsigned char *) &key, sizeof(BlockRefTableKey))
#define SH_EQUAL(tb, a, b) memcmp(&a, &b, sizeof(BlockRefTableKey)) == 0
#define SH_SCOPE static inline
#define SH_DEFINE
#define SH_DECLARE
#include "lib/simplehash.h"

/*
 * A block reference table is basically just the hash table, but we don't
 * want to expose that to outside callers.
 *
 * We keep track of the memory context in use explicitly too, so that it's
 * easy to place all of our allocations in the same context.
 */
struct BlockRefTable
{
	blockreftable_hash *hash;
	MemoryContext		mcxt;
};

/* Magic number for serialization file format. */
#define BLOCKREFTABLE_MAGIC			0x652b137b

/*
 * On-disk serialization format for block reference table entries.
 */
typedef struct BlockRefTableSerializedEntry
{
	RelFileLocator	rlocator;
	ForkNumber		forknum;
	BlockNumber		limit_block;
	uint32			nchunks;
} BlockRefTableSerializedEntry;

/*
 * Buffer size, so that we avoid doing many small I/Os.
 */
#define BUFSIZE					65536

/*
 * Ad-hoc buffer for file I/O.
 */
typedef struct BlockRefTableBuffer
{
	File		file;
	off_t		filepos;
	char		data[BUFSIZE];
	int			used;
	int			cursor;
} BlockRefTableBuffer;

/*
 * State for keeping track of progress while incrementally reading a block
 * table reference file from disk.
 *
 * buffer is an I/O buffer.
 *
 * total_entries means the number of RelFileLocator/ForkNumber combinations
 * that exist in the file, and consumed_entries is the number of those that
 * have been partially or completely read.
 *
 * total_chunks means the number of chunks for the RelFileLocator/ForkNumber
 * combination that is curently being read, and consumed_chunks is the number
 * of those that have been read. (We always read all the information for
 * a single chunk at one time, so we don't need to be able to represent the
 * state where a chunk has been partially read.)
 *
 * chunk_size is the array of chunk sizes. The length is given by total_chunks.
 *
 * chunk_data holds the current chunk.
 *
 * chunk_position helps us figure out how much progress we've made in returning
 * the block numbers for the current chunk to the caller. If the chunk is a
 * bitmap, it's the number of bits we've scanned; otherwise, it's the number
 * of chunk entries we've scanned.
 */
struct BlockRefTableReader
{
	BlockRefTableBuffer	buffer;
	uint32		total_entries;
	uint32		consumed_entries;
	uint32		total_chunks;
	uint32		consumed_chunks;
	uint16	   *chunk_size;
	uint16		chunk_data[MAX_ENTRIES_PER_CHUNK];
	uint32		chunk_position;
};

/* Function prototypes. */
static int BlockRefTableComparator(const void *a, const void *b);
static void BlockRefTableFlush(BlockRefTableBuffer *buffer);
static int BlockRefTableRawRead(BlockRefTableBuffer *buffer, void *data,
								int length);
static void BlockRefTableRawWrite(BlockRefTableBuffer *buffer, void *data,
								  int length);
static void BlockRefTableRead(BlockRefTableBuffer *buffer, void *data,
							  int length);
static void BlockRefTableWrite(BlockRefTableBuffer *buffer, void *data,
							   int length);

/*
 * Create an empty block reference table.
 */
BlockRefTable *
CreateEmptyBlockRefTable(void)
{
	BlockRefTable *brtab = palloc(sizeof(BlockRefTable));

	/*
	 * Even completely empty database has a few hundred relation forks, so it
	 * seems best to size the hash on the assumption that we're going to have
	 * at least a few thousand entries.
	 */
	brtab->mcxt = CurrentMemoryContext;
	brtab->hash = blockreftable_create(brtab->mcxt, 4096, NULL);

	return brtab;
}

/*
 * Set the "limit block" for a relation fork.
 *
 * The "limit block" is the smallest block number such that the block in
 * question and every subsequent block must be considered modified if they
 * exist. See comments for BlockRefTableEntry.
 *
 * If an equal or smaller limit block value is already known, this function
 * does nothing. In other words, the actual limit block value is the smallest
 * value ever set for the relation fork in question within the lifetime of
 * the BlockRefTable.
 */
void
BlockRefTableSetLimitBlock(BlockRefTable *brtab,
						   const RelFileLocator *rlocator,
						   ForkNumber forknum,
						   BlockNumber limit_block)
{
	BlockRefTableEntry *brtentry;
	BlockRefTableKey	key;
	bool	found;
	int		max_chunks;
	int		i;

	memcpy(&key.rlocator, rlocator, sizeof(RelFileLocator));
	key.forknum = forknum;
	brtentry = blockreftable_insert(brtab->hash, key, &found);

	if (!found)
	{
		/*
		 * We have no existing data about this relation fork, so just record
		 * the limit_block value supplied by the caller, and make sure other
		 * parts of the entry are properly initialized.
		 */
		brtentry->limit_block = limit_block;
		brtentry->nchunks = 0;
		brtentry->chunk_size = NULL;
		brtentry->chunk_usage = NULL;
		brtentry->chunk_data = NULL;
		return;
	}

	/* If we already have an equal or lower limit block, do nothing. */
	if (limit_block >= brtentry->limit_block)
		return;

	/* Record the new limit block value. */
	brtentry->limit_block = limit_block;

	/* How many chunks do we still need to store? */
	max_chunks = (limit_block + BLOCKS_PER_CHUNK - 1) / BLOCKS_PER_CHUNK;

	/*
	 * If the number of chunks we're storing is already less than the maximum
	 * number that could be required to cover all blocks less than the new
	 * limit block, we can't free up any memory.
	 */
	if (max_chunks >= brtentry->nchunks)
		return;

	/* Free chunks that are no longer required. */
	for (i = max_chunks; i < brtentry->nchunks; ++i)
	{
		if (brtentry->chunk_data[i] != NULL)
		{
			pfree(brtentry->chunk_data[i]);
			brtentry->chunk_data[i] = NULL;
		}
	}

	/* If the new number of chunks is zero, free the arrays entirely. */
	if (max_chunks == 0)
	{
		if (brtentry->chunk_size != NULL)
		{
			pfree(brtentry->chunk_size);
			brtentry->chunk_size = NULL;
		}
		if (brtentry->chunk_usage != NULL)
		{
			pfree(brtentry->chunk_usage);
			brtentry->chunk_usage = NULL;
		}
		if (brtentry->chunk_data != NULL)
		{
			pfree(brtentry->chunk_data);
			brtentry->chunk_data = NULL;
		}
	}

	/*
	 * Remember how many chunks we think we need. Because the limit block
	 * can never increase, it doesn't matter that we're essentially forgetting
	 * that we've actually allocated memory for a larger number of chunks.
	 *
	 * We could try to save memory here by using repalloc to reduce the
	 * allocated size of the chunk_size[], chunk_usage[], and chunk_data[]
	 * arrays, but it seems unlikely we would free up enough memory to matter.
	 * Partial truncations are performed only by vacuum, which typically
	 * only removes a few blocks from the end of the relation. Even if it
	 * removes a lot more, the allocator is going to round the allocation
	 * sizes up to a convenient value, so there may be no savings in practice.
	 */
	brtentry->nchunks = max_chunks;
}

/*
 * Mark a block as known to have been modified.
 */
void
BlockRefTableMarkBlockModified(BlockRefTable *brtab,
							   const RelFileLocator *rlocator,
							   ForkNumber forknum,
							   BlockNumber blknum)
{
	BlockRefTableEntry *brtentry;
	BlockRefTableKey	key;
	bool	found;
	unsigned	chunkno;
	unsigned	chunkoffset;
	unsigned	i;

	memcpy(&key.rlocator, rlocator, sizeof(RelFileLocator));
	key.forknum = forknum;
	brtentry = blockreftable_insert(brtab->hash, key, &found);

	if (!found)
	{
		/*
		 * We want to set the initial limit block value to something higher
		 * than any legal block number. InvalidBlockNumber fits the bill.
		 */
		brtentry->limit_block = InvalidBlockNumber;
		brtentry->nchunks = 0;
		brtentry->chunk_size = NULL;
		brtentry->chunk_usage = NULL;
		brtentry->chunk_data = NULL;
	}

	/*
	 * Which chunk should store the state of this block? And what is the offset
	 * of this block relative to the start of that chunk?
	 */
	chunkno = blknum / BLOCKS_PER_CHUNK;
	chunkoffset = blknum % BLOCKS_PER_CHUNK;

	/*
	 * If 'nchunks' isn't big enough for us to be able to represent the state
	 * of this block, we need to enlarge our arrays.
	 */
	if (chunkno >= brtentry->nchunks)
	{
		unsigned	max_chunks;
		unsigned	extra_chunks;

		/*
		 * New array size is a power of 2, at least 16, big enough so that
		 * chunkno will be a valid array index.
		 */
		max_chunks = Max(16, brtentry->nchunks);
		while (max_chunks < chunkno + 1)
			chunkno *= 2;
		Assert(max_chunks > chunkno);
		extra_chunks = max_chunks - brtentry->nchunks;

		if (brtentry->nchunks == 0)
		{
			brtentry->chunk_size = MemoryContextAllocZero(brtab->mcxt,
														  sizeof(uint16) * max_chunks);
			brtentry->chunk_usage = MemoryContextAllocZero(brtab->mcxt,
														   sizeof(uint16) * max_chunks);
			brtentry->chunk_data = MemoryContextAllocZero(brtab->mcxt,
														  sizeof(BlockRefTableChunk) * max_chunks);
		}
		else
		{
			brtentry->chunk_size = repalloc(brtentry->chunk_size,
											sizeof(uint16) * max_chunks);
			memset(&brtentry->chunk_size[brtentry->nchunks], 0,
				   extra_chunks * sizeof(uint16));
			brtentry->chunk_usage = repalloc(brtentry->chunk_usage,
											 sizeof(uint16) * max_chunks);
			memset(&brtentry->chunk_usage[brtentry->nchunks], 0,
				   extra_chunks * sizeof(uint16));
			brtentry->chunk_data = repalloc(brtentry->chunk_data,
											sizeof(BlockRefTableChunk) * max_chunks);
			memset(&brtentry->chunk_data[brtentry->nchunks], 0,
				   extra_chunks * sizeof(BlockRefTableChunk));
		}
		brtentry->nchunks = max_chunks;
	}

	/*
	 * If the chunk that covers this block number doesn't exist yet, create
	 * it as an array and add the appropriate offset to it. We make it pretty
	 * small initially, because there might only be 1 or a few block references
	 * in this chunk and we don't want to use up too much memory.
	 */
	if (brtentry->chunk_size[chunkno] == 0)
	{
		brtentry->chunk_data[chunkno] =
			MemoryContextAlloc(brtab->mcxt,
							   sizeof(uint16) * INITIAL_ENTRIES_PER_CHUNK);
		brtentry->chunk_size[chunkno] = INITIAL_ENTRIES_PER_CHUNK;
		brtentry->chunk_data[chunkno][0] = chunkoffset;
		brtentry->chunk_usage[chunkno] = 1;
		return;
	}

	/*
	 * If the number of entries in this chunk is already maximum, it must be
	 * a bitmap. Just set the appropriate bit.
	 */
	if (brtentry->chunk_usage[chunkno] == MAX_ENTRIES_PER_CHUNK)
	{
		BlockRefTableChunk chunk = brtentry->chunk_data[chunkno];

		chunk[chunkoffset / BLOCKS_PER_ENTRY] |=
			1 << (chunkoffset % BLOCKS_PER_ENTRY);
		return;
	}

	/*
	 * There is an existing chunk and it's in array format. Let's find out
	 * whether it already has an entry for this block. If so, we do not need
	 * to do anything.
	 */
	for (i = 0; i < brtentry->chunk_usage[chunkno]; ++i)
	{
		if (brtentry->chunk_data[chunkno][i] == chunkoffset)
			return;
	}

	/*
	 * If the number of entries currently used is one less than the maximum,
	 * it's time to convert to bitmap format.
	 */
	if (brtentry->chunk_usage[chunkno] == MAX_ENTRIES_PER_CHUNK - 1)
	{
		BlockRefTableChunk newchunk;
		unsigned	j;

		/* Allocate a new chunk. */
		newchunk = MemoryContextAlloc(brtab->mcxt,
									  MAX_ENTRIES_PER_CHUNK * sizeof(uint16));

		/* Set the bit for each existing entry. */
		for (j = 0; j < brtentry->chunk_usage[chunkno]; ++j)
		{
			unsigned	coff = brtentry->chunk_data[chunkno][j];

			newchunk[coff / BLOCKS_PER_ENTRY] |=
				1 << (coff % BLOCKS_PER_ENTRY);
		}

		/* Set the bit for the new entry. */
		newchunk[chunkoffset / BLOCKS_PER_ENTRY] |=
			1 << (chunkoffset % BLOCKS_PER_ENTRY);

		/* Swap the new chunk into place and update metadata. */
		pfree(brtentry->chunk_data[chunkno]);
		brtentry->chunk_data[chunkno] = newchunk;
		brtentry->chunk_size[chunkno] = MAX_ENTRIES_PER_CHUNK;
		brtentry->chunk_usage[chunkno] = MAX_ENTRIES_PER_CHUNK;
		return;
	}

	/*
	 * OK, we currently have an array, and we don't need to convert to a
	 * bitmap, but we do need to add a new element. If there's not enough
	 * room, we'll have to expand the array.
	 */
	if (brtentry->chunk_usage[chunkno] == brtentry->chunk_size[chunkno])
	{
		unsigned	newsize = brtentry->chunk_size[chunkno] * 2;

		Assert(newsize <= MAX_ENTRIES_PER_CHUNK);
		brtentry->chunk_data[chunkno] = repalloc(brtentry->chunk_data[chunkno],
												 newsize * sizeof(uint16));
		brtentry->chunk_size[chunkno] = newsize;
	}

	/* Now we can add the new entry. */
	brtentry->chunk_data[chunkno][brtentry->chunk_usage[chunkno]] =
		chunkoffset;
	brtentry->chunk_usage[chunkno]++;
}

/*
 * Prepare to incrementally read a block reference table file.
 */
BlockRefTableReader *
CreateBlockRefTableReader(File file)
{
	BlockRefTableReader *reader;
	uint32 magic;

	/* Initialize data structure. */
	reader = palloc0(sizeof(BlockRefTableReader));
	reader->buffer.file = file;

	/* Verify magic number. */
	BlockRefTableRead(&reader->buffer, &magic, sizeof(uint32));
	if (magic != BLOCKREFTABLE_MAGIC)
		ereport(ERROR,
				errcode(ERRCODE_DATA_CORRUPTED),
				errmsg("file \"%s\" has wrong magic number: expected %u, found %u",
					   FilePathName(file),
					   BLOCKREFTABLE_MAGIC, magic));

	/* Read count of entries. */
	BlockRefTableRead(&reader->buffer, &reader->total_entries, sizeof(uint32));

	return reader;
}

/*
 * Read next relation fork covered by this block reference table file.
 *
 * After calling this function, you must call BlockRefTableReaderGetBlocks
 * until it returns 0 before calling it again.
 */
bool
BlockRefTableReaderNextRelation(BlockRefTableReader *reader,
								RelFileLocator *rlocator,
								ForkNumber *forknum,
								BlockNumber *limit_block)
{
	BlockRefTableSerializedEntry	sentry;

	/*
	 * Sanity check: caller must read all blocks from all chunks before
	 * moving on to the next relation.
	 */
	if (reader->total_chunks != reader->consumed_chunks)
		elog(ERROR, "must consume all chunks before reading next relation");

	/* If we have read all of the entries, we're done. */
	if (reader->total_entries == reader->consumed_entries)
		return false;

	/* Read serialized entry. */
	BlockRefTableRead(&reader->buffer, &sentry,
					  sizeof(BlockRefTableSerializedEntry));
	++reader->consumed_entries;

	/* Read chunk size array. */
	if (reader->chunk_size != NULL)
		pfree(reader->chunk_size);
	reader->chunk_size = palloc(sentry.nchunks * sizeof(uint16));
	BlockRefTableRead(&reader->buffer, reader->chunk_size,
					  sentry.nchunks * sizeof(uint16));

	/* Set up for chunk scan. */
	reader->total_chunks = sentry.nchunks;
	reader->consumed_chunks = 0;

	/* Return data to caller. */
	memcpy(rlocator, &sentry.rlocator, sizeof(RelFileLocator));
	*forknum = sentry.forknum;
	*limit_block = sentry.limit_block;
	return true;
}

/*
 * Get modified blocks associated with the relation fork returned by
 * the most recent call to BlockRefTableReaderNextRelation.
 *
 * On return, block numbers will be written into the 'blocks' array, whose
 * length should be passed via 'nblocks'. The return value is the number of
 * entries actually written into the 'blocks' array, which may be less than
 * 'nblocks' if we run out of modified blocks in tht relation fork before
 * we run out of room in the array.
 */
int
BlockRefTableReaderGetBlocks(BlockRefTableReader *reader,
							 BlockNumber *blocks,
							 int nblocks)
{
	int		blocks_found = 0;

	/* Must provide space for at least one block number to be returned. */
	Assert(nblocks > 0);

	/* Must call BlockRefTableReaderNextRelation before calling this. */
	Assert(reader->consumed_entries > 0);

	/* Loop collecting blocks to return to caller. */
	for (;;)
	{
		uint16	next_chunk_size;

		/*
		 * If we've read at least one chunk, maybe it contains some block
		 * numbers that could satisfy caller's request.
		 */
		if (reader->consumed_chunks > 0)
		{
			uint32	chunkno = reader->consumed_chunks - 1;
			uint16	chunk_size = reader->chunk_size[chunkno];

			if (chunk_size == MAX_ENTRIES_PER_CHUNK)
			{
				/* Bitmap format, so search for bits that are set. */
				while (reader->chunk_position < BLOCKS_PER_CHUNK &&
					   blocks_found < nblocks)
				{
					uint16	chunkoffset = reader->chunk_position;
					uint16	w;

					w = reader->chunk_data[chunkoffset / BLOCKS_PER_ENTRY];
					if ((w & (1u << (chunkoffset % BLOCKS_PER_ENTRY))) != 0)
						blocks[blocks_found++] =
							chunkno * BLOCKS_PER_CHUNK + chunkoffset;
					++reader->chunk_position;
				}
			}
			else
			{
				/* Not in bitmap format, so each entry is a 2-byte offset. */
				while (reader->chunk_position < chunk_size &&
					   blocks_found < nblocks)
				{
					blocks[blocks_found++] = chunkno * BLOCKS_PER_CHUNK
						+ reader->chunk_data[reader->chunk_position];
					++reader->chunk_position;
				}
			}
		}

		/* We found enough blocks, so we're done. */
		if (blocks_found >= nblocks)
			break;

		/*
		 * We didn't find enough blocks, so we must need the next chunk.
		 * If there are none left, though, then we're done anyway.
		 */
		if (reader->consumed_chunks == reader->total_chunks)
			break;

		/*
		 * Read data for next chunk and reset scan position to beginning of
		 * chunk. Note that the next chunk might be empty, in which case we
		 * consume the chunk without actually consuming any bytes from the
		 * underlying file.
		 */
		next_chunk_size = reader->chunk_size[reader->consumed_chunks];
		if (next_chunk_size > 0)
			BlockRefTableRead(&reader->buffer, reader->chunk_data,
							  next_chunk_size * sizeof(uint16));
		++reader->consumed_chunks;
		reader->chunk_position = 0;
	}

	return blocks_found;
}

/*
 * Release memory used while reading a block reference table from a file.
 */
void
DestroyBlockRefTableReader(BlockRefTableReader *reader)
{
	if (reader->chunk_size != NULL)
	{
		pfree(reader->chunk_size);
		reader->chunk_size = NULL;
	}
	pfree(reader);
}

/*
 * Serialize a block reference table to a file.
 */
void
WriteBlockRefTable(BlockRefTable *brtab, File file)
{
	BlockRefTableSerializedEntry *sdata = NULL;
	BlockRefTableBuffer	buffer;
	uint32 magic = BLOCKREFTABLE_MAGIC;

	/* Prepare buffer. */
	memset(&buffer, 0, sizeof(BlockRefTableBuffer));
	buffer.file = file;

	/* Write magic number and entry count. */
	BlockRefTableWrite(&buffer, &magic, sizeof(uint32));
	BlockRefTableWrite(&buffer, &brtab->hash->members, sizeof(uint32));

	/* Assuming there are some entries, we also need to write those. */
	if (brtab->hash->members > 0)
	{
		unsigned i = 0;
		blockreftable_iterator	it;
		BlockRefTableEntry *brtentry;

		/* Extract entries into serializable format and sort them. */
		sdata =
			palloc(brtab->hash->members * sizeof(BlockRefTableSerializedEntry));
		blockreftable_start_iterate(brtab->hash, &it);
		while ((brtentry = blockreftable_iterate(brtab->hash, &it)) != NULL)
		{
			BlockRefTableSerializedEntry *sentry = &sdata[i++];

			sentry->rlocator = brtentry->key.rlocator;
			sentry->forknum = brtentry->key.forknum;
			sentry->limit_block = brtentry->limit_block;
			sentry->nchunks = brtentry->nchunks;
		}
		Assert(i == brtab->hash->members);
		qsort(sdata, i, sizeof(BlockRefTableSerializedEntry),
			  BlockRefTableComparator);

		/* Loop over entries in sorted order and serialize each one. */
		for (i = 0; i < brtab->hash->members; ++i)
		{
			BlockRefTableSerializedEntry *sentry = &sdata[i];
			BlockRefTableEntry *brtentry;
			BlockRefTableKey	key;
			unsigned	j;

			/* XXX this isn't really the right thing to do--we should truncate the chunk length array -- i.e. sentry->nchunks -- if it has trailing zeroes */

			/* Write the serialized entry itself. */
			BlockRefTableWrite(&buffer, sentry,
							   sizeof(BlockRefTableSerializedEntry));

			/* Look up the original entry so we can access the chunks. */
			memcpy(&key.rlocator, &sentry->rlocator, sizeof(RelFileLocator));
			key.forknum = sentry->forknum;
			brtentry = blockreftable_lookup(brtab->hash, key);
			Assert(brtentry != NULL);

			/* Write the chunk length array. */
			if (brtentry->nchunks != 0)
				BlockRefTableWrite(&buffer, brtentry->chunk_usage,
								   brtentry->nchunks * sizeof(uint16));

			/* Write the contents of each chunk. */
			for (j = 0; j < brtentry->nchunks; ++j)
			{
				if (brtentry->chunk_usage[j] == 0)
					continue;
				BlockRefTableWrite(&buffer, brtentry->chunk_data[j],
								   brtentry->chunk_usage[j] * sizeof(uint16));
			}
		}
	}

	/* Flush any leftover data out of our buffer. */
	BlockRefTableFlush(&buffer);
}

/*
 * Comparator for BlockRefTableSerializedEntry objects.
 */
static int
BlockRefTableComparator(const void *a, const void *b)
{
	const BlockRefTableSerializedEntry *sa = a;
	const BlockRefTableSerializedEntry *sb = b;

	if (sa->rlocator.dbOid > sb->rlocator.dbOid)
		return 1;
	if (sa->rlocator.dbOid < sb->rlocator.dbOid)
		return -1;

	if (sa->rlocator.spcOid > sb->rlocator.spcOid)
		return 1;
	if (sa->rlocator.spcOid < sb->rlocator.spcOid)
		return -1;

	if (sa->rlocator.relNumber > sb->rlocator.relNumber)
		return 1;
	if (sa->rlocator.relNumber < sb->rlocator.relNumber)
		return -1;

	if (sa->forknum > sb->forknum)
		return 1;
	if (sa->forknum < sb->forknum)
		return -1;

	return 0;
}

/*
 * Flush any buffered data out of a BlockRefTableBuffer.
 */
static void
BlockRefTableFlush(BlockRefTableBuffer *buffer)
{
	BlockRefTableRawWrite(buffer, buffer->data, buffer->used);
	buffer->used = 0;
}

/*
 * Directly read the underlying file associated with a BlockRefTableBuffer.
 */
static int
BlockRefTableRawRead(BlockRefTableBuffer *buffer, void *data, int length)
{
	int		nbytes;

	nbytes = FileRead(buffer->file, data, length,
					  buffer->filepos,
					  PG_WAIT_EXTENSION); /* XXX FIXME */
	if (nbytes < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m",
						FilePathName(buffer->file))));

	buffer->filepos += nbytes;
	return nbytes;
}

/*
 * Directly write the underlying file associated with a BlockRefTableBuffer.
 */
static void
BlockRefTableRawWrite(BlockRefTableBuffer *buffer, void *data, int length)
{
	int		nbytes;

	nbytes = FileWrite(buffer->file, data, length,
					   buffer->filepos,
					   PG_WAIT_EXTENSION); /* XXX FIXME */
	if (nbytes < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m",
						FilePathName(buffer->file))));
	if (nbytes != length)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": wrote only %d of %d bytes at offset %u",
						FilePathName(buffer->file), nbytes,
						length, (unsigned) buffer->filepos),
				 errhint("Check free disk space.")));

	buffer->filepos += nbytes;
}

/*
 * Read data from a BlockRefTableBuffer.
 */
static void
BlockRefTableRead(BlockRefTableBuffer *buffer, void *data, int length)
{
	/* Loop until read is fully satisfied. */
	while (length > 0)
	{
		if (buffer->cursor < buffer->used)
		{
			/*
			 * If any buffered data is available, use that to satisfy as much
			 * of the request as possible.
			 */
			int	bytes_to_copy = Min(length, buffer->used - buffer->cursor);

			memcpy(data, &buffer->data[buffer->cursor], bytes_to_copy);
			buffer->cursor += bytes_to_copy;
			data = ((char *) data) + bytes_to_copy;
			length -= bytes_to_copy;
		}
		else if (length >= BUFSIZE)
		{
			/*
			 * If the request length is long, read directly into caller's
			 * buffer.
			 */
			int bytes_read;

			bytes_read = BlockRefTableRawRead(buffer, data, length);
			data = ((char *) data) + bytes_read;
			length -= bytes_read;

			/* If we didn't get anything, that's bad. */
			if (bytes_read == 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("unexpected end of file")));
		}
		else
		{
			/*
			 * Refill our buffer.
			 */
			buffer->used = BlockRefTableRawRead(buffer, buffer->data, BUFSIZE);
			buffer->cursor = 0;

			/* If we didn't get anything, that's bad. */
			if (buffer->used == 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("unexpected end of file")));
		}
	}
}

/*
 * Supply data to a BlockRefTableBuffer for write to the underlying File.
 */
static void
BlockRefTableWrite(BlockRefTableBuffer *buffer, void *data, int length)
{
	/* If the new data can't fit into the buffer, flush the buffer. */
	if (buffer->used + length > BUFSIZE)
	{
		BlockRefTableRawWrite(buffer, buffer->data, buffer->used);
		buffer->used = 0;
	}

	/* If the new data would fill the buffer, or more, write it directly. */
	if (length >= BUFSIZE)
	{
		BlockRefTableRawWrite(buffer, data, length);
		return;
	}

	/* Otherwise, copy the new data into the buffer. */
	memcpy(&buffer->data[buffer->used], data, length);
	buffer->used += length;
	Assert(buffer->used <= BUFSIZE);
}
