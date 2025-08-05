/*-------------------------------------------------------------------------
 *
 * check_stale_read.c
 * 		Check stale page read whenever page is read from disk
 *
 *		Whenever page is flushed out to the disk a mapping from
 *		complete block path to a LSN is stored into a table. 
 *		And whenever a page is read back
 *		into the memory it is verified and if LSN is not same
 *		a WARNING will be issued.
 *
 *	IDENTIFICATION
 *		contrib/check_stale_read/check_stale_read.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/hash.h"
#include "access/xlog.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "storage/ipc.h"
#include "storage/lwlock.h"
#if PG_VERSION_NUM >= 100000
#include "storage/shmem.h"
#endif

#include "storage/relfilelocator.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/pg_lsn.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(check_stale_read_stats);

bool	check_stale_read_enabled = true;
Oid		csr_table_oid = InvalidOid;

#define CSR_TABLE_COLS 6

typedef struct CSRState
{
	pg_atomic_uint64	block_registered;
	pg_atomic_uint64	block_validated;
	pg_atomic_uint64	block_stale;
	pg_atomic_uint64	live_entries;
	pg_atomic_uint64	insert_skipped;
	XLogRecPtr	last_flush_lsn;
} CSRState;

CSRState *csr;

static page_flush_hook_type prev_page_flush_hook = NULL;
static page_validate_hook_type prev_page_validate_hook = NULL;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

#if PG_VERSION_NUM >= 150000
static void csr_shmem_request(void);
#endif
static void csr_shmem_startup(void);
static bool csr_enabled(void);
static void csr_page_flush(RelFileLocator rellocator, ForkNumber forknum,
						   BlockNumber blocknum, XLogRecPtr recptr);
static void csr_page_validate(RelFileLocator rellocator, ForkNumber forknum,
							  BlockNumber blocknum, Page page);
static Size csr_state_shmem_size(void);
static Size csr_shmem_size(void);

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		elog(ERROR, "Must be added in shared preload library");

#if PG_VERSION_NUM >= 150000
		prev_shmem_request_hook = shmem_request_hook;
		shmem_request_hook = csr_shmem_request;
#endif
		prev_shmem_startup_hook = shmem_startup_hook;
		shmem_startup_hook = csr_shmem_startup;

	prev_page_flush_hook = page_flush_hook;
	page_flush_hook = csr_page_flush;
	prev_page_validate_hook = page_validate_hook;
	page_validate_hook = csr_page_validate;

	DefineCustomBoolVariable("check_stale_read.enabled",
							 "Enable / Disable check_stale_read",
							 NULL,
							 &check_stale_read_enabled,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);
}

#if PG_VERSION_NUM >= 150000
static void
csr_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(csr_shmem_size());
	RequestNamedLWLockTranche("check_stale_read", NUM_PARTITIONS);
}
#endif

static void
csr_shmem_startup(void)
{
	HASHCTL		info;
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	csr = (CSRState *) ShmemInitStruct("csr hash map",
										csr_state_shmem_size(),
										&found);
	Assert(found || !IsUnderPostmaster);

	if (!found)
	{
		int			i;

		memset(csr, 0, sizeof(CSRState));

		pg_atomic_init_u64(&csr->block_registered, 0);
		pg_atomic_init_u64(&csr->block_validated, 0);
		pg_atomic_init_u64(&csr->block_stale, 0);
		csr->last_flush_lsn = InvalidXLogRecPtr;
		pg_atomic_init_u64(&csr->live_entries, 0);
		pg_atomic_init_u64(&csr->insert_skipped, 0);
	}

	LWLockRelease(AddinShmemInitLock);
}

/*
 * TODO: This feature can be enabled/disabled via a GUC parameter
 * so need to implement a new guc parameter.
 */
static bool
csr_enabled()
{
	return check_stale_read_enabled;
}

/*
 * csr_page_flush - register a new page entry in hash with given LSN
 *
 * TODO: If hash table is full remove old entries
 */
static void
csr_page_flush(RelFileLocator rellocator, ForkNumber forknum,
			   BlockNumber blocknum, XLogRecPtr recptr)
{
	CSRLookupKey	key;
	CSRLookupEnt   *hentry;
	bool			found;
	LWLock		   *lock;

	if (prev_page_flush_hook)
		prev_page_flush_hook(rellocator, forknum, blocknum, recptr);

	/*
	 * If this is not enabled just return.  But before that cleanup the hash
	 * so that we don't report false stale read.
	 */
	if (!csr_enabled())
	{
		return;
	}


}

static void
csr_lookup_relid()
{
	Oid			relid;
	Oid			schema_oid;

	schema_oid = get_namespace_oid("csr", false);

	relid = get_relname_relid("check_stale_read_table", schema_oid);

	if (!relid)
		elog(ERROR, "cache lookup failed for relation %s.%s",
			 get_namespace_name(schema_oid), relname);

	csr_table_oid = relid;
}

/*
 * csr_page_flush - register a new page entry in hash with given LSN
 *
 * TODO: If hash table is full remove old entries
 */
static void
csr_page_flush(RelFileLocator rellocator, ForkNumber forknum,
			   BlockNumber blocknum, XLogRecPtr recptr)
{
	Datum		values[CSR_TABLE_COLS];
	bool		nulls[CSR_TABLE_COLS];
	int			attno;
	Relation	rel;
	HeapTuple	tup;
	TupleTableSlot *slot;
	EState	   *log_estate;
	ResultRelInfo *relinfo = makeNode(ResultRelInfo);

	if (prev_page_flush_hook)
		prev_page_flush_hook(rellocator, forknum, blocknum, recptr);

	/*
	 * If this is not enabled just return.  But before that cleanup the hash
	 * so that we don't report false stale read.
	 */
	if (!csr_enabled())
	{
		return;
	}
	else if (OidIsValid(csr_table_oid))
	{
		csr_lookup_relid();
	}

	memset(nulls, 0, sizeof(bool) * CSR_TABLE_COLS);
	memset(values, 0, sizeof(Datum) * CSR_TABLE_COLS);

	attno = 0;
	values[attno++] = ObjectIdGetDatum(rellocator.spcOid);
	values[attno++] = ObjectIdGetDatum(rellocator.dbOid);
	values[attno++] = ObjectIdGetDatum(rellocator.relNumber);
	values[attno++] = Int32GetDatum(forknum);
	values[attno++] = Int32GetDatum(blocknum);
	values[attno++] = LSNGetDatum(recptr);

	rel = table_open(csr_table_oid, RowExclusiveLock);
	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	simple_heap_insert(rel, tup);
	table_close(rel, RowExclusiveLock);
}

/*
 * csr_page_validate - Lookup the page entry in hash and validate the LSN
 *
 * This function should be called whenever we are reading a page into the
 * shared buffer.
 */
static void
csr_page_validate(RelFileLocator rellocator, ForkNumber forknum,
			  BlockNumber blocknum, Page page)
{
	CSRLookupKey	key;
	CSRLookupEnt   *hentry;
	bool			found;
	XLogRecPtr		recptr;
	LWLock		   *lock;

	if (prev_page_validate_hook)
		prev_page_validate_hook(rellocator, forknum, blocknum, page);

	/* If we don't need to validate, just return */
	if (PageIsNew(page) || !csr_enabled())
		return;

	key.locator = rellocator;
	key.forknum = forknum;
	key.blocknum = blocknum;

	/* Acquire hash partition lock in shared mode before lookup. */
	lock = csr_get_partition_lock(&key);
	LWLockAcquire(lock, LW_SHARED);
	hentry = (CSRLookupEnt *) hash_search(block_lsn_map, &key,
																				HASH_FIND, &found);
	if (hentry == NULL)
	{
		/* TODO: Remove this log, this is just for debugging purpose in POC. */
		elog(DEBUG2, "entry not found for block %u of relation %s;",
			 blocknum,
			 relpathperm(rellocator, forknum));
		/* Release hash partition lock. */
		LWLockRelease(lock);
		return;
	}

	recptr = hentry->recptr;

	/*
	 * XXX We have got the block back in shared buffer so now we can remove
	 * entry from the hash.  But for that we will need an exclusive lock
	 * so for now just don't do anything.
	 */

	/* Release hash partition lock. */
	LWLockRelease(lock);

	/*
	 * Compare lsn of the block store in the hash entry with the LSN of the
	 * page we just read.
	 */
	if (PageGetLSN(page) != recptr)
	{
		pg_atomic_add_fetch_u64(&csr->block_stale, 1);
		ereport(WARNING,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid LSN in block %u of relation %s;",
					blocknum, relpathperm(rellocator, forknum))),
				 errdetail("Disk page has lsn %X/%X which is not same as last flushed page lsn=%X/%X",
						   LSN_FORMAT_ARGS(PageGetLSN(page)),
						   LSN_FORMAT_ARGS(recptr)));
	}

	pg_atomic_add_fetch_u64(&csr->block_validated, 1);
}

/*
 * qsort comparator functions
 */
static int
compare_entries(const void *va, const void *vb)
{
	const CSRLookupEnt	*a = ((const CSRLookupEnt *) va);
	const CSRLookupEnt	*b = ((const CSRLookupEnt *) vb);

	if (a->counter == b->counter)
		return 0;
	return (a > b) ? 1 : -1;
}

/*
 * csr_cleanup - Remove old entries.
 *
 * This function should be called whenever the hash table is full.
 */
static void
csr_cleanup()
{
	HASH_SEQ_STATUS status;
	CSRLookupEnt   *hentry;
	CSRLookupEnt  **entries;
	int				nvictims;
	int				i = 0;

	entries = palloc(hash_get_num_entries(block_lsn_map) * sizeof(CSRLookupEnt *));

	hash_seq_init(&status, block_lsn_map);
	while ((hentry = hash_seq_search(&status)) != NULL)
		entries[i++] = hentry;

	/* Sort into increasing order by usage */
	qsort(entries, i, sizeof(CSRLookupEnt *), compare_entries);

	/* Now remove 10% of oldest entries */
	nvictims = Max(10, i * 0.1);
	nvictims = Min(nvictims, i);

	for (i = 0; i < nvictims; i++)
		hash_search(block_lsn_map, &entries[i]->key, HASH_REMOVE, NULL);
	
	pg_atomic_sub_fetch_u64(&csr->live_entries, nvictims);

	pfree(entries);

}

/*
 * csr_state_shmem_size - Compute size for CSRState.
 */
static Size
csr_state_shmem_size()
{
	Size		size = 0;

	size = MAXALIGN(sizeof(CSRState));
	size += MAXALIGN(NUM_PARTITIONS * sizeof(LWLockPadded *));

	return size;
}
/*
 * csr_shmem_size - Compute size for LSN validation hash.
 */
static Size
csr_shmem_size()
{
	Size	size = csr_state_shmem_size();

	size = add_size(size, hash_estimate_size(check_stale_read_max_entries,
									sizeof(CSRLookupEnt)));
	return size;
}

/*
 * check_stale_read_stats - Returns the stats for check_stale_read.
 *
 * This function returns the following stats:
 * 1. block_registered - Number of blocks registered in the hash table.
 * 2. block_validated - Number of blocks validated in the hash table.
 * 3. block_stale - Number of blocks with stale LSN.
 * 4. last_flush_lsn - LSN of the last flush.
 */
Datum
check_stale_read_stats(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum			values[6] = {0};
	bool			nulls[6] = {0};

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");


	values[0] = pg_atomic_add_fetch_u64(&csr->block_registered, 0);
	values[1] = pg_atomic_add_fetch_u64(&csr->block_validated, 0);
	values[2] = pg_atomic_add_fetch_u64(&csr->block_stale, 0);
	values[3] = pg_atomic_add_fetch_u64(&csr->live_entries, 0);
	values[4] = pg_atomic_add_fetch_u64(&csr->insert_skipped, 0);
	values[6] = LSNGetDatum(csr->last_flush_lsn);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}
