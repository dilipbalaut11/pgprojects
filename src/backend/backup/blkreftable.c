/*-------------------------------------------------------------------------
 *
 * blkreftable.c
 *	  Block reference tables.
 *
 * A block reference table is used to keep track of which blocks have
 * been modified by WAL records within a certain LSN range.
 *
 * Portions Copyright (c) 2010-2022, PostgreSQL Global Development Group
 *
 * src/include/backup/blkreftable.h
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "backup/blkreftable.h"
#include "common/hashfn.h"

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
	unsigned		nchunks;
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
	unsigned		nchunks;
} BlockRefTableSerializedEntry;

/* Function prototypes. */
static int BlockRefTableComparator(const void *a, const void *b);

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

void
ReadBlockRefTable(BlockRefTable *brtab, File file)
{
	/* XXX */
}

/*
 * Serialize a block reference table to a file.
 */
void
WriteBlockRefTable(BlockRefTable *brtab, File file)
{
	BlockRefTableSerializedEntry *sdata = NULL;
	unsigned	total_block_count = 0;
	unsigned	total_chunk_entries_used = 0;

	elog(LOG, "BEGIN WriteBlockRefTable (%u members)", brtab->hash->members);

	if (brtab->hash->members > 0)
	{
		unsigned i = 0;
		blockreftable_iterator	it;
		BlockRefTableEntry *brtentry;

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

		/* XXX DEBUG */
		for (i = 0; i < brtab->hash->members; ++i)
		{
			BlockRefTableSerializedEntry *sentry = &sdata[i];
			BlockRefTableEntry *brtentry;
			BlockRefTableKey	key;
			unsigned	j;
#if 0
			elog(LOG, "DB %u TS %u RFN %u FORK %u LIMIT %u CHUNKS %u",
				 sentry->rlocator.dbOid,
				 sentry->rlocator.spcOid,
				 sentry->rlocator.relNumber,
				 sentry->forknum,
				 sentry->limit_block,
				 sentry->nchunks);
#endif

			memcpy(&key.rlocator, &sentry->rlocator, sizeof(RelFileLocator));
			key.forknum = sentry->forknum;
			brtentry = blockreftable_lookup(brtab->hash, key);
			Assert(brtentry != NULL);

			for (j = 0; j < brtentry->nchunks; ++j)
			{
				BlockNumber	blknum[BLOCKS_PER_CHUNK];
				unsigned	blkcount = 0;
				if (brtentry->chunk_usage[j] == 0)
				{
					/* Chunk does not exist. */
					continue;
				}
				else if (brtentry->chunk_usage[j] == MAX_ENTRIES_PER_CHUNK)
				{
					unsigned	k;

					/* Chunk is a bitmap. */
					for (k = 0; k < MAX_ENTRIES_PER_CHUNK; ++k)
					{
						uint16	w = brtentry->chunk_data[j][k];
						unsigned	b;

						for (b = 0; b < BITS_PER_BYTE; ++b)
						{
							if ((w & (1 << b)) != 0)
							{
								blknum[blkcount++] = j * BLOCKS_PER_CHUNK +
									k * BITS_PER_BYTE + b;
							}
						}
					}
				}
				else
				{
					unsigned	k;

					/* Chunk is an offset array. */
					for (k = 0; k < brtentry->chunk_usage[j]; ++k)
					{
						uint16	w = brtentry->chunk_data[j][k];

						blknum[k] = j * BLOCKS_PER_CHUNK + w;
					}
					blkcount = k;
				}

				{
#if 0
					StringInfoData str;
					unsigned n;

					initStringInfo(&str);
					appendStringInfo(&str, "chunk %u:", j);
					for (n = 0; n < blkcount; ++n)
						appendStringInfo(&str, " %u", blknum[n]);
					appendStringInfo(&str, " (%u blocks)", blkcount);
					elog(LOG, "%s", str.data);
#endif
					total_block_count += blkcount;
					total_chunk_entries_used += brtentry->chunk_usage[j];
				}
			}
		}
	}

	elog(LOG, "END WriteBlockRefTable (total of %u blocks using %u entries)",
		 total_block_count, total_chunk_entries_used);
	/* XXX */
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
