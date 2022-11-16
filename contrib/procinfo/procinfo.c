/*-------------------------------------------------------------------------
 *
 * procinfo.c
 *		plugin to get current subtransaction status of each backend
 *
 * IDENTIFICATION
 *	  contrib/procinfo/procinfo.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "funcapi.h"

#include "access/multixact.h"
#include "storage/proc.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_get_all_procinfo);
PG_FUNCTION_INFO_V1(pg_get_next_multixact);

/*
 * store the proc information.
 */
typedef struct procinfo
{
	int				pid;
	TransactionId	backend_xid;
	int				nsubxact;
	bool			overflow;
} procinfo;

TupleDesc	tupdesc;
procinfo   *currentinfo = NULL;
int			nproccount = 0;

/*
 * pg_get_all_procinfo
 */
Datum
pg_get_all_procinfo(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	MemoryContext oldcontext;
	TupleDesc	tupledesc;
	TupleDesc	expected_tupledesc;

	if (SRF_IS_FIRSTCALL())
	{
		int			i;

		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (get_call_result_type(fcinfo, NULL, &expected_tupledesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		/* Construct a tuple descriptor for the result rows. */
		tupledesc = CreateTemplateTupleDesc(expected_tupledesc->natts);

		TupleDescInitEntry(tupledesc, (AttrNumber) 1, "pid",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 2, "xid",
						   XIDOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 3, "subxact_count",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 4, "isoverflow",
						   BOOLOID, -1, 0);
		tupdesc = BlessTupleDesc(tupledesc);

		/* Get shared lock on ProcArrayLock */
		LWLockAcquire(ProcArrayLock, LW_SHARED);
		currentinfo = palloc0(ProcGlobal->allProcCount * sizeof(procinfo));
		nproccount = 0;

		for (i = 0; i < ProcGlobal->allProcCount; i++)
		{
			PGPROC *proc = &ProcGlobal->allProcs[i];
			PGXACT *xact = &ProcGlobal->allPgXact[proc->pgprocno];

			/* if no process w.r.t. this slot. */
			if (proc->pid == 0)
				continue;

			currentinfo[nproccount].pid = proc->pid;
			currentinfo[nproccount].backend_xid = xact->xid;
			currentinfo[nproccount].nsubxact = xact->nxids;
			currentinfo[nproccount].overflow = xact->overflowed;
			nproccount++;
		}
		funcctx->max_calls = nproccount;

		LWLockRelease(ProcArrayLock);

		/* Return back to the original context  */
		MemoryContextSwitchTo(oldcontext);

	}

	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		HeapTuple	tuple;
		Datum		values[4];
		bool		nulls[4];
		Datum		result;

		uint32		i = funcctx->call_cntr;

		values[0] = Int32GetDatum(currentinfo[i].pid);
		nulls[0] = false;
		values[1] = TransactionIdGetDatum(currentinfo[i].backend_xid);
		nulls[1] = false;
		values[2] = Int32GetDatum(currentinfo[i].nsubxact);
		nulls[2] = false;
		values[3] = BoolGetDatum(currentinfo[i].overflow);
		nulls[3] = false;

		/* Build and return the tuple. */
		tuple = heap_form_tuple(tupdesc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
		SRF_RETURN_DONE(funcctx);
}

/*
 * pg_get_next_multixact
 */
Datum
pg_get_next_multixact(PG_FUNCTION_ARGS)
{
	return TransactionIdGetDatum(ReadNextMultiXactId());
}
