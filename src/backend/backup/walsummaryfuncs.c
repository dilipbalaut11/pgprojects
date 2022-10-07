/*-------------------------------------------------------------------------
 *
 * walsummaryfuncs.c
 *	  SQL-callable functions for accessing WAL summary data.
 *
 * Portions Copyright (c) 2010-2022, PostgreSQL Global Development Group
 *
 * src/backend/backup/walsummaryfuncs.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "backup/blkreftable.h"
#include "backup/walsummary.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/fmgrprotos.h"
#include "utils/pg_lsn.h"

#define NUM_WS_ATTS			3
#define NUM_SUMMARY_ATTS	6
#define MAX_BLOCKS_PER_CALL	256

Datum
pg_available_wal_summaries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsi;
	List *wslist;
	ListCell *lc;
	Datum	values[NUM_WS_ATTS];
	bool	nulls[NUM_WS_ATTS];

	SetSingleFuncCall(fcinfo, 0);
	rsi = (ReturnSetInfo *) fcinfo->resultinfo;

	memset(nulls, 0, sizeof(nulls));

	wslist = GetWalSummaries(0, InvalidXLogRecPtr, InvalidXLogRecPtr);
	foreach(lc, wslist)
	{
		WalSummaryFile *ws = (WalSummaryFile *) lfirst(lc);
		HeapTuple	tuple;

		CHECK_FOR_INTERRUPTS();

		values[0] = Int64GetDatum((int64) ws->tli);
		values[1] = LSNGetDatum(ws->start_lsn);
		values[2] = LSNGetDatum(ws->end_lsn);

		tuple = heap_form_tuple(rsi->setDesc, values, nulls);
		tuplestore_puttuple(rsi->setResult, tuple);
	}

	return (Datum) 0;
}

Datum
pg_wal_summary_contents(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsi;
	Datum	values[NUM_SUMMARY_ATTS];
	bool	nulls[NUM_SUMMARY_ATTS];
	WalSummaryFile	ws;
	File	wsfile;
	BlockRefTableReader *reader;
	int64	raw_tli;
	RelFileLocator	rlocator;
	ForkNumber	forknum;
	BlockNumber	limit_block;

	SetSingleFuncCall(fcinfo, 0);
	rsi = (ReturnSetInfo *) fcinfo->resultinfo;
	memset(nulls, 0, sizeof(nulls));

	raw_tli = PG_GETARG_INT64(0);
	if (raw_tli < 1 || raw_tli > PG_INT32_MAX)
		elog(ERROR, "tli sux");

	ws.tli = (TimeLineID) raw_tli;
	ws.start_lsn = PG_GETARG_LSN(1);
	ws.end_lsn = PG_GETARG_LSN(2);
	wsfile = OpenWalSummaryFile(&ws, false);
	reader = CreateBlockRefTableReader(wsfile);

	while (BlockRefTableReaderNextRelation(reader, &rlocator, &forknum, &limit_block))
	{
		BlockNumber	blocks[MAX_BLOCKS_PER_CALL];
		HeapTuple	tuple;

		CHECK_FOR_INTERRUPTS();

		values[0] = ObjectIdGetDatum(rlocator.relNumber);
		values[1] = ObjectIdGetDatum(rlocator.spcOid);
		values[2] = ObjectIdGetDatum(rlocator.dbOid);
		values[3] = Int16GetDatum((int16) forknum);
		values[5] = BoolGetDatum(false);

		while (true)
		{
			int		nblocks;
			int		i;

			CHECK_FOR_INTERRUPTS();

			nblocks = BlockRefTableReaderGetBlocks(reader, blocks, MAX_BLOCKS_PER_CALL);
			if (nblocks == 0)
				break;

			for (i = 0; i < nblocks; ++i)
			{
				values[4] = Int64GetDatum((int64) blocks[i]);

				tuple = heap_form_tuple(rsi->setDesc, values, nulls);
				tuplestore_puttuple(rsi->setResult, tuple);
			}

			if (BlockNumberIsValid(limit_block))
			{
				values[4] = Int64GetDatum((int64) limit_block);
				values[5] = BoolGetDatum(true);

				tuple = heap_form_tuple(rsi->setDesc, values, nulls);
				tuplestore_puttuple(rsi->setResult, tuple);
			}
		}
	}

	DestroyBlockRefTableReader(reader);
	FileClose(wsfile);

	return (Datum) 0;
}
