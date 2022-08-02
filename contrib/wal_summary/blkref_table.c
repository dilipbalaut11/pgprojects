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

/*
 * Number of blocks in one container, keep it low so that while
 * using array we can represent it as 16 uint16.
 */
#define BLKREF_CONTAINER_BLOCKS				1024	/* 2 ^ 16 */
#define BLKREF_CONTAINER_SIZE				BLKREF_CONTAINER_BLOCKS / 8

/*
 * Bitmap size to store 65536 block is 8192(8kb).  So after 4096 number of
 * blocks we will switch the container from array to bitmap.
 */

/* Threshold after these many blocks we will switch from array to bitmap. */
#define ARRAY_CONTAINER_THRESHOLD	(BLKREF_CONTAINER_BLOCKS / (sizeof(int16) * 8))


#define PG_BLOCKREF_FILE	"file1"
#define PG_BLOCKREF_BUFSZ	BLKREF_CONTAINER_SIZE * 2
#define PG_SINGLE_FLUSH_SZ	sizeof(int32)

struct BlockRefTable
{
	XLogRecPtr	start_lsn;
	XLogRecPtr	end_lsn;
	int			nentries;
	HTAB	   *hash;
};

typedef struct BlockRefTableKey
{
	RelFileLocator	rlocator;
	ForkNumber		fork;
} BlockRefTableKey;

typedef struct ContainerInfo
{
	uint32		containerno;
	uint32			nblocks;
	bool		isarray;
	union
	{
		uint16	   *blkarray;
		Bitmapset  *blkmap;
	} blkinfo;
} ContainerInfo;

#define SizeOfFixedContainer	(offsetof(ContainerInfo, isarray) + sizeof(bool))

typedef struct BlockRefTableEntry
{
	BlockRefTableKey	key;		/* hash key of BlockRefTable */
	bool				isrelnew;	/* is entry for new relation creation*/
	uint32				ncontainer;	/* no. of containers */
	List			   *containers;		/* Block container list */
} BlockRefTableEntry;

#define SizeOfFixedBlockRefEntry	(offsetof(BlockRefTableEntry, ncontainer) + sizeof(uint32))

typedef struct BlockRefFileHandle
{
	char	buffer[PG_BLOCKREF_BUFSZ];
	int		index;
	int		writeoff;
	int		readoff;
	int		bufdatasz;
	File	fd;
} BlockRefFileHandle;

#define BLOCKREF_FILE_ENTRY_SZ(nblocks)	\
				SizeOfFixBlockRefEntry + (nblocks) * sizeof(BlockNumber)

struct BlockRefCombiner
{
	int			nfiles;		/* current number of files */
	int			maxfiles;
	File	   *files;		/* palloc'd array with nfiles entries */
};



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

/* WriteBlockRefTable
 *
 * --file format--
 *	nentries
 *	entry1{RelFileLocator, ForkNum, isnewrel, ncontainer,}
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
			else
				BlockRefFilePutData(handle,
									(char *) container->blkinfo.blkmap,
									BLKREF_CONTAINER_SIZE);
		}
	}
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

/* for debugging. */
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
	{
		ListCell	*lc;

		PrintBlockRefTabeEntry(entry);
	}
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
	PrintBlockRefTabeEntry(entry);

	return true;
}

BlockRefCombiner*
CreateBlockRefCombiner(MemoryContext mcxt)
{
	BlockRefCombiner  *bref;
	HASHCTL			ctl;

	bref = MemoryContextAlloc(mcxt, sizeof(BlockRefCombiner));
	bref->nfiles = 0;
	bref->maxfiles = 10;
	bref->files = (File *) palloc(bref->maxfiles * sizeof(File));

	return bref;
}

void
BlockRefCombinerAddInputFile(BlockRefCombiner *bref, File file)
{
	if (bref->nfiles >= bref->maxfiles)
	{
		bref->maxfiles = bref->maxfiles * 2;
		bref->files = (File *) repalloc(bref->files,
										bref->maxfiles * sizeof(File));
	}

	bref->files[bref->nfiles++] = file;

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
	static int i=0;

	/* Leave space for the buffer flush size. */
	if (handle->index == 0)
		handle->index += sizeof(int32);

	if (size > PG_BLOCKREF_BUFSZ - handle->index)
	{
		*(int32 *) handle->buffer = handle->index;
		if (pg_pwrite(handle->fd, handle->buffer, handle->index, handle->writeoff) != handle->index)
				elog(ERROR, "could not write block reference file ");
		elog(NOTICE, "FLUSH: index:%d, writeoff=%d", handle->index, handle->writeoff);
		handle->writeoff += handle->index;
		handle->index = sizeof(int32);
	}

	elog(NOTICE, "WRITE(%d): index:%d, size=%d", i++,handle->index, size);
	memcpy(handle->buffer + handle->index, data, size);
	handle->index += size;
}

static void
BlockRefFileGetData(BlockRefFileHandle *handle, char **data, int size)
{
	static int i=0;

	/* If not enough data in buffer then read from file. */
	if (handle->bufdatasz - handle->index < size)
	{
		if (pg_pread(handle->fd, (char *) (&handle->bufdatasz),
					 sizeof(uint32), handle->readoff) != sizeof(uint32))
			elog(ERROR, "could not read block reference file ");

		if (pg_pread(handle->fd, handle->buffer, handle->bufdatasz,
					 handle->readoff) != handle->bufdatasz)
			elog(ERROR, "could not read block reference file ");
		elog(NOTICE, "LOAD: index:%d, readoff=%d", handle->index, handle->readoff);

		handle->readoff += handle->bufdatasz;
		handle->index = sizeof(int32);

	}

	elog(NOTICE, "READ(%d): index:%d, size=%d", i++,handle->index, size);
	*data = handle->buffer + handle->index;
	handle->index += size;
}

static void
BlockRefFileFlush(BlockRefFileHandle *handle)
{
	if (handle->index == 0)
		return;

	*(int32 *) handle->buffer = handle->index;
	if (pg_pwrite(handle->fd, handle->buffer, handle->index, handle->writeoff) != handle->index)
		elog(ERROR, "could not write block reference file ");

	handle->writeoff += handle->index;
	handle->index = 0;
}

/****************Container code***************************/

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
