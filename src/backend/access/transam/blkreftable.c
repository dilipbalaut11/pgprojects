/*-------------------------------------------------------------------------
 *
 * blkreftable.c
 *
 * Maintains in memory hash table for the wal summary and provide interfaces to
 * add data to the hash table.  Also provide interfaces to write hash tables
 * to the summary file and load it back from the summary file.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/blkreftable.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>
#include "access/blkreftable.h"
#include "catalog/storage_xlog.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/pg_lsn.h"
#include "utils/wait_event.h"

/*
 * There could be 2^32 block range within a relation and if we have to store it
 * as a array of block number then we will need 16 GB of memory.  OTOH if we
 * store that as bitmap then also we would need 512 MB of memory.  So instead
 * of storing everything as either array or bitmap we divide the block range
 * into the container wherein each container reresents 65536 blocks so that
 * if we store the blocks within a container as an array then we can just use
 * array of uint16.  So now with this if we need to store the block in a
 * container as a bitmap then we need total 8192 bytes.  Now, the idea is that
 * we will start with storing the block in a container as an array and as soon
 * as it crosses 4096 block i.e. the max container size (8192) then we will
 * convert this particular container from array to a bitmap.
 */
#define BLKREF_CONTAINER_BLOCKS		65536
#define BLKREF_CONTAINER_SIZE		BLKREF_CONTAINER_BLOCKS / 8
#define ARRAY_CONTAINER_THRESHOLD	(BLKREF_CONTAINER_BLOCKS / (sizeof(int16) * 8))

/* Buffer size while reading/writing BlockRefTable to the summary file. */
#define PG_BLOCKREF_BUFSZ			BLKREF_CONTAINER_SIZE * 2

/* Block reference table. */
struct BlockRefTable
{
	int			nentries;	/* total entires in the table. */
	HTAB	   *hash;		/* hash table for storing block references. */
};

/*
 * Container info.
 *
 * using 'containerno' and block number store in 'blkinfo' we can compute the
 * actual relation blockno.
 * 'nblocks' is number of blocks in the container.
 * if 'isarray' is set then block in the container are stored as an array
 * otherwise as bitmap.
 * 'blkinfo' block storage, either in 'blkarray' or in 'blkmap'
 */
typedef struct ContainerInfo
{
	uint32		containerno;
	uint32		nblocks;
	bool		isarray;
	union
	{
		uint16	   *blkarray;
		Bitmapset  *blkmap;
	} blkinfo;
} ContainerInfo;

/* Size of the fixed part of the ContainerInfo. */
#define SizeOfFixedContainer	(offsetof(ContainerInfo, isarray) + sizeof(bool))

/* BlockRefTable key */
typedef struct BlockRefTableKey
{
	RelFileLocator	rlocator;
	ForkNumber		fork;
} BlockRefTableKey;

typedef struct BlockRefTableEntry
{
	BlockRefTableKey	key;			/* hash key of BlockRefTable */
	bool				isrelnew;		/* is entry for new relation creation*/
	uint32				ncontainer;		/* no. of containers */
	List			   *containers;		/* container list */
} BlockRefTableEntry;

/* Size of the fixed part of the BlockRefTableEntry. */
#define SizeOfFixedBlockRefEntry	(offsetof(BlockRefTableEntry, ncontainer) + sizeof(uint32))

typedef struct BlockRefFileHandle
{
	char	buffer[PG_BLOCKREF_BUFSZ];	/* read/write buffer*/
	int		index;						/* current read/write index in buffer */
	int		writeoff;					/* write offset in file if writing */
	int		readoff;					/* read offset in file if reading */
	int		bufdatasz;					/* total data size in buffer when reading */
	File	fd;							/* file descriptor */
} BlockRefFileHandle;

/* TODO: block combiner for combining the files */
struct BlockRefCombiner
{
	int	nfiles;
};

/* file static function declarations. */
static void FreeContainerList(BlockRefTableEntry *entry);
static ContainerInfo *FindOrAllocateContainer(BlockRefTableEntry *entry, int containerno);
static BlockRefFileHandle *BlockRefAllocateHandle(File fd);
static void BlockRefFilePutData(BlockRefFileHandle *handle, char *data, int size);
static void BlockRefFileGetData(BlockRefFileHandle *handle, char **data, int size);
static void BlockRefFileFlush(BlockRefFileHandle *handle);
static ContainerInfo *ContainerAllocateNew(int containerno);
static void ContainerAddBlock(ContainerInfo *container, uint16 blkno);
static void ContainerSwitchToBitmap(ContainerInfo *container);

/*
 * Create an empty block reference table.
 */
BlockRefTable *
CreateEmptyBlockRefTable(MemoryContext mcxt)
{
	BlockRefTable  *brtab;
	HASHCTL			ctl;

	brtab = MemoryContextAlloc(mcxt, sizeof(BlockRefTable));
	brtab->nentries = 0;

	ctl.keysize = sizeof(BlockRefTableKey);
	ctl.entrysize = sizeof(BlockRefTableEntry);
	ctl.hcxt = mcxt;

	brtab->hash = hash_create("buf reference table",
							  8192, /* start small and extend */
							  &ctl,
							  HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
	return brtab;
}

/*
 * Add an new table entry into the block reference table.
 */
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
	if (!found)
		brtab->nentries++;
	entry->isrelnew = true;
	entry->ncontainer = 0;
	if (found && entry->containers != NIL)
		FreeContainerList(entry);

	entry->containers = NIL;
}

/*
 * Add new block entry into the block reference table for input relation.
 */
void
BlockRefTableMarkBlockModified(BlockRefTable *brtab,
							   RelFileLocator *rlocator,
							   ForkNumber forknum,
							   BlockNumber blknum)
{
	BlockRefTableKey	key;
	BlockRefTableEntry *entry;
	ContainerInfo		   *container;
	bool		found;
	uint16		blkno_incontainer;
	int			containerno;

	key.rlocator = *rlocator;
	key.fork = forknum;

	/*
	 * Add the entry to the hash, if it is new then initialize the entry,
	 * otherwise if this relation is marked as isrelnew then we can ignore
	 * the block information for the relation.
	 */
	entry = (BlockRefTableEntry *) hash_search(brtab->hash,
											   &key, HASH_ENTER, &found);
	if (!found)
	{
		entry->ncontainer = 0;
		entry->containers = NIL;
		entry->isrelnew = false;
		brtab->nentries++;
	}
	else if (entry->isrelnew)
	{
		return;
	}

	/* compute containerno and block no in container. */
	containerno = blknum / BLKREF_CONTAINER_BLOCKS;
	blkno_incontainer = blknum % BLKREF_CONTAINER_BLOCKS;

	container = FindOrAllocateContainer(entry, containerno);
	ContainerAddBlock(container, blkno_incontainer);
}

/*
 * WriteBlockRefTable
 *
 *  ---file format--
 *	nentries
 *		entry-1(RelFileLocator, ForkNum, isnewrel, ncontainer)
 *			container-1(containerno, nblocks, isarray)
 *				container block data (array or BitmapSet)
 *			container-2(containerno, nblocks, isarray)
 *				....
 *			container-n(containerno, nblocks, isarray)
 *		entry-2(RelFileLocator, ForkNum, isnewrel, ncontainer)
 *			....
 *		entry-n(RelFileLocator, ForkNum, isnewrel, ncontainer)
 *	---------------
 */
void
WriteBlockRefTable(BlockRefTable *brtab, File fd)
{
	HASH_SEQ_STATUS			seq_status;
	BlockRefTableEntry	   *entry;
	BlockRefFileHandle	   *handle;

	if (brtab->nentries == 0)
		return;

	handle = BlockRefAllocateHandle(fd);

	/* write number of entries */
	BlockRefFilePutData(handle, (char *) &brtab->nentries, sizeof(uint32));

	/*
	 * Scan the complete hash and write the entries to the file.
	 */
	hash_seq_init(&seq_status, brtab->hash);

	while ((entry = hash_seq_search(&seq_status)) != NULL)
	{
		ListCell	*lc;

		/* write entry header. */
		BlockRefFilePutData(handle, (char *) entry, SizeOfFixedBlockRefEntry);

		/* write container list. */
		foreach(lc, entry->containers)
		{
			ContainerInfo *container = lfirst(lc);

			/* write container header. */
			BlockRefFilePutData(handle, (char *) container, SizeOfFixedContainer);

			/* write chaged block info. */
			if (container->isarray)
				BlockRefFilePutData(handle,
									(char *) container->blkinfo.blkarray,
									sizeof(uint16) * container->nblocks);
			/* XXX instead of BLKREF_CONTAINER_SIZE copy actual bitmap size. */
			else
				BlockRefFilePutData(handle,
									(char *) container->blkinfo.blkmap,
									BLKREF_CONTAINER_SIZE);
		}
	}

	/* flush any remaining data in the buffer. */
	BlockRefFileFlush(handle);

	pfree(handle);
}

BlockRefTable *
ReadBlockRefTable(MemoryContext mcxt, File fd)
{
	BlockRefTable *brtab;
	BlockRefTableEntry	*bufentry;
	BlockRefTableEntry	*entry;
	BlockRefFileHandle	*handle;
	char	*data;
	int		 nentries = 0;

	brtab = CreateEmptyBlockRefTable(mcxt);
	handle = BlockRefAllocateHandle(fd);

	/* write number of entries */
	BlockRefFileGetData(handle, (char **) &data, sizeof(uint32));
	nentries = *(int*) data;

	if (nentries == 0)
		return NULL;

	brtab = CreateEmptyBlockRefTable(mcxt);
	brtab->nentries = nentries;

	while(nentries-- > 0)
	{
		int 	i = 0;
		bool	found;

		BlockRefFileGetData(handle, (char **) &bufentry, SizeOfFixedBlockRefEntry);

		entry = hash_search(brtab->hash, &bufentry->key, HASH_ENTER, &found);
		Assert(!found);
		entry->containers = NIL;

		entry->isrelnew = bufentry->isrelnew;

		if (entry->isrelnew)
		{
			entry->ncontainer = 0;
			continue;
		}

		entry->ncontainer = bufentry->ncontainer;

		/* read container list. */
		for (i = 0; i < entry->ncontainer; i++)
		{
			ContainerInfo	*bufcontainer;
			ContainerInfo	*container;

			/* read container header. */
			BlockRefFileGetData(handle, (char **) &bufcontainer,
								SizeOfFixedContainer);

			container = ContainerAllocateNew(bufcontainer->containerno);
			container->isarray = bufcontainer->isarray;
			container->nblocks = bufcontainer->nblocks;

			/* write chaged block info. */
			if (container->isarray)
			{
				int	size = sizeof(uint16) * container->nblocks;

				BlockRefFileGetData(handle, (char **) &data, size);
				container->blkinfo.blkarray = palloc(size);
				memcpy(container->blkinfo.blkarray, data, size);
			}
			else
			{
				BlockRefFileGetData(handle,
									(char **) &data,
									BLKREF_CONTAINER_SIZE);
				container->blkinfo.blkmap = palloc(BLKREF_CONTAINER_SIZE);
				memcpy(container->blkinfo.blkmap, data, BLKREF_CONTAINER_SIZE);
				container->nblocks = bms_num_members(container->blkinfo.blkmap);
			}

			entry->containers = lappend(entry->containers, container);
		}
	}

	return brtab;
}

bool
BlockRefTableLookupRelationFork(BlockRefTable *brtab,
								RelFileLocator *rlocator,
								ForkNumber forknum,
								bool *is_new,
								void **modified_blocks)
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

	return true;
}

/* TODO */
BlockRefCombiner*
CreateBlockRefCombiner(MemoryContext mcxt)
{
	return NULL;
}

/* TODO */
void
BlockRefCombinerAddInputFile(BlockRefCombiner *bref, File file)
{
	return;
}

/* TODO */
void
BlockRefCombinerHashPartition(BlockRefCombiner *bref,
							  size_t size_limit,
							  unsigned *num_partitions,
							  File *partition)
{
	return;
}

static void
FreeContainerList(BlockRefTableEntry *entry)
{
	ListCell	*lc;

	foreach(lc, entry->containers)
	{
		ContainerInfo *container = lfirst(lc);

		pfree(container->blkinfo.blkarray);
	}

	list_free_deep(entry->containers);
}

/* Find or allocate a new container. */
static ContainerInfo *
FindOrAllocateContainer(BlockRefTableEntry *entry, int containerno)
{
	ListCell	*lc;
	ContainerInfo *container;

	foreach(lc, entry->containers)
	{
		container = lfirst(lc);

		if (container->containerno == containerno)
			return container;
	}

	container = ContainerAllocateNew(containerno);

	entry->containers = lappend(entry->containers, container);

	entry->ncontainer++;

	return container;
}

static BlockRefFileHandle *
BlockRefAllocateHandle(File fd)
{
	BlockRefFileHandle *handle;

	handle = palloc0(sizeof(BlockRefFileHandle));
	handle->fd = fd;

	return handle;
}

static void
BlockRefFilePutData(BlockRefFileHandle *handle, char *data, int size)
{
	/* Leave space for the buffer flush size. */
	if (handle->index == 0)
		handle->index += sizeof(int32);

	if (size > PG_BLOCKREF_BUFSZ - handle->index)
	{
		*(int32 *) handle->buffer = handle->index;

		if (FileWrite(handle->fd, handle->buffer, handle->index,
					  handle->writeoff, WAIT_EVENT_WAL_WRITE) != handle->index)
				elog(ERROR, "could not write block reference file ");
		handle->writeoff += handle->index;
		handle->index = sizeof(int32);
	}

	memcpy(handle->buffer + handle->index, data, size);
	handle->index += size;
}

static void
BlockRefFileGetData(BlockRefFileHandle *handle, char **data, int size)
{
	/* If not enough data in buffer then read from file. */
	if (handle->bufdatasz - handle->index < size)
	{
		if (FileRead(handle->fd, (char *) (&handle->bufdatasz),
					 sizeof(uint32), handle->readoff, WAIT_EVENT_WAL_READ) != sizeof(uint32))
			elog(ERROR, "could not read block reference file ");

		if (FileRead(handle->fd, handle->buffer, handle->bufdatasz,
					 handle->readoff, WAIT_EVENT_WAL_READ) != handle->bufdatasz)
			elog(ERROR, "could not read block reference file ");

		handle->readoff += handle->bufdatasz;
		handle->index = sizeof(int32);

	}

	*data = handle->buffer + handle->index;
	handle->index += size;
}

static void
BlockRefFileFlush(BlockRefFileHandle *handle)
{
	if (handle->index == 0)
		return;

	*(int32 *) handle->buffer = handle->index;
	if (FileWrite(handle->fd, handle->buffer, handle->index, handle->writeoff,
				  WAIT_EVENT_WAL_WRITE) != handle->index)
		elog(ERROR, "could not write block reference file ");

	handle->writeoff += handle->index;
	handle->index = 0;
}

static ContainerInfo *
ContainerAllocateNew(int containerno)
{
	ContainerInfo	*container;

	container = palloc(sizeof(ContainerInfo));

	container->nblocks = 0;
	container->isarray = true;
	container->containerno = containerno;

	return container;
}

static void
ContainerAddBlock(ContainerInfo *container, uint16 blkno)
{
	if (container->isarray)
	{
		int i;

		for (i = 0; i < container->nblocks; i++)
		{
			/*
			 * Immediately return if block is already exists in the
			 * container.
			 */
			if (container->blkinfo.blkarray[i] == blkno)
				return;
		}

		/* block no found so add into the array. */
		if (container->nblocks == 0)
			container->blkinfo.blkarray = palloc0(BLKREF_CONTAINER_SIZE);

		if (container->nblocks == ARRAY_CONTAINER_THRESHOLD)
		{
			ContainerSwitchToBitmap(container);
			container->blkinfo.blkmap =
					bms_add_member(container->blkinfo.blkmap, blkno);
		}
		else
			container->blkinfo.blkarray[container->nblocks] = blkno;

		container->nblocks++;
	}
	else
		container->blkinfo.blkmap = bms_add_member(container->blkinfo.blkmap,
												   blkno);
}

static void
ContainerSwitchToBitmap(ContainerInfo *container)
{
	int i;
	Bitmapset	*bms = NULL;

	Assert(container->isarray);

	/*
	 * Insert array element to the bitmap, for copying we need array data so
	 * we can not convert inplace.
	 */
	for (i = 0; i < container->nblocks; i++)
		bms = bms_add_member(bms, container->blkinfo.blkarray[i]);

	pfree(container->blkinfo.blkarray);

	container->blkinfo.blkmap = bms;
	container->isarray = false;
}

/* Debug function to print the BlockRefTabeEntry. */
static void
PrintBlockRefTabeEntry(BlockRefTableEntry *entry)
{
	ListCell	*lc;
	StringInfoData str;
	BlockNumber		blkno;

	initStringInfo(&str);
	resetStringInfo(&str);
	appendStringInfo(&str, "...Entry Start...\n");

	appendStringInfo(&str, "dboid=%u relnumber=%u isnew=%d ncontainer=%u\n",
					entry->key.rlocator.dbOid,
					entry->key.rlocator.relNumber,
					entry->isrelnew, entry->ncontainer);

	/* write container list. */
	foreach(lc, entry->containers)
	{
		ContainerInfo *container = lfirst(lc);

		appendStringInfo(&str, "container=%d:", container->containerno);
		if (container->isarray)
		{
			int i;

			for (i = 0; i < container->nblocks; i++)
			{
				blkno = container->containerno * BLKREF_CONTAINER_BLOCKS
						+ container->blkinfo.blkarray[i];
				appendStringInfo(&str, "block=%d ", blkno);
			}
		}
		else
		{
			int i;

			for (i = 0; i < BLKREF_CONTAINER_BLOCKS; i++)
			{
				if (bms_is_member(i, container->blkinfo.blkmap))
				{
					blkno = container->containerno * BLKREF_CONTAINER_BLOCKS + i;
					appendStringInfo(&str, "block=%d ", blkno);
				}
			}

		}
		appendStringInfo(&str, "\n");
	}

	appendStringInfo(&str, "...Entry End...\n");
	elog(LOG, "%s", str.data);
}

/* Debug function to print the BlockRefTabe. */
void
PrintBlockRefTable(BlockRefTable *brtab)
{
	HASH_SEQ_STATUS			seq_status;
	BlockRefTableEntry	   *entry;

	/*
	 * Scan the complete hash and convert into a flat buffer and flush to file.
	 */
	hash_seq_init(&seq_status, brtab->hash);

	while ((entry = hash_seq_search(&seq_status)) != NULL)
		PrintBlockRefTabeEntry(entry);
}
