/*-------------------------------------------------------------------------
 *
 * edb_wait_states_reader.c
 *		Code for UDFs in edb_wait_states extension.
 *
 * Copyright (c) 2018, EnterpriseDB
 *
 * IDENTIFICATION
 *	  contrib/edb_wait_states/edb_wait_states_reader.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "datatype/timestamp.h"
#include "edb_wait_states.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "pgstat.h"
#include "port.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(edb_wait_states_samples);
PG_FUNCTION_INFO_V1(edb_wait_states_queries);
PG_FUNCTION_INFO_V1(edb_wait_states_sessions);
PG_FUNCTION_INFO_V1(edb_wait_states_purge);

static void read_edb_wait_states_samples(Tuplestorestate *tupstore,
							 TupleDesc tupdesc, TimestampTz start_ts,
							 TimestampTz end_ts);
static void read_edb_wait_states_one_sample_file(char *sample_file_name,
									 Tuplestorestate *tupstore,
									 TupleDesc tupdesc, TimestampTz start_ts,
									 TimestampTz end_ts);
static void read_edb_wait_states_queries(Tuplestorestate *tupstore,
							 TupleDesc tupdesc, TimestampTz start_ts,
							 TimestampTz end_ts);
static void read_edb_wait_states_one_query_file(char *query_file_name,
									Tuplestorestate *tupstore,
									TupleDesc tupdesc, TimestampTz file_start_ts,
									TimestampTz file_end_ts);
static void read_edb_wait_states_sessions(Tuplestorestate *tupstore,
							  TupleDesc tupdesc, TimestampTz start_ts,
							  TimestampTz end_ts);
static void read_edb_wait_states_one_session_file(char *session_file_name,
									  Tuplestorestate *tupstore,
									  TupleDesc tupdesc,
									  TimestampTz file_start_ts,
									  TimestampTz file_end_ts);


#define Natts_sample 7
#define Anum_sample_queryid 0
#define Anum_sample_sessionid 1
#define Anum_sample_query_start_ts 2
#define Anum_sample_ts 3
#define Anum_sample_wait_event_type 4
#define Anum_sample_wait_event 5
#define Anum_sample_interval 6


#define Natts_query 4
#define Anum_query_id 0
#define Anum_query_text 1
#define Anum_query_ref_start_ts 2
#define Anum_query_ref_end_ts 3

#define Natts_session 5
#define Anum_session_id 0
#define Anum_session_db 1
#define Anum_session_user 2
#define Anum_session_ref_start_ts 3
#define Anum_session_ref_end_ts 4

#define START_TS_ARGNO 0
#define END_TS_ARGNO 1


/*
 * edb_wait_states_samples
 *
 * C interface to SQL callable function to access the edb_wait_states
 * samples files.
 *
 * The function outputs the records in samples files that are collected in the
 * given time interval. This function only implements the C interface to SQL
 * callable UDF, but the real work is carried out in
 * read_edb_wait_states_samples(). See prologue of that function for more
 * details.
 */
Datum
edb_wait_states_samples(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	TimestampTz start_ts = PG_GETARG_TIMESTAMPTZ(START_TS_ARGNO);
	TimestampTz end_ts = PG_GETARG_TIMESTAMPTZ(END_TS_ARGNO);

	/* Check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	Assert(tupdesc->natts == Natts_sample);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	read_edb_wait_states_samples(tupstore, tupdesc, start_ts, end_ts);

	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * edb_wait_states_queries
 *
 * C interface to SQL callable function to access the edb_wait_states
 * queries files.
 *
 * The function outputs the records in queries files which correspond to the
 * given time interval. This function only implements the C interface to SQL
 * callable UDF, but the real work is carried out in
 * read_edb_wait_states_queries(). See prologue of that function for more
 * details.
 */
Datum
edb_wait_states_queries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	TimestampTz start_ts = PG_GETARG_TIMESTAMPTZ(START_TS_ARGNO);
	TimestampTz end_ts = PG_GETARG_TIMESTAMPTZ(END_TS_ARGNO);

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	Assert(tupdesc->natts == Natts_query);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	read_edb_wait_states_queries(tupstore, tupdesc, start_ts, end_ts);

	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * edb_wait_states_sessions
 *
 * C interface to SQL callable function to access the information in
 * edb_wait_states sessions files.
 *
 * The function outputs the records in sessions files which correspond to the
 * given time interval. This function only implements the C interface to SQL
 * callable UDF, but the real work is carried out in
 * read_edb_wait_states_sessions(). See prologue of that function for more
 * details.
 */
Datum
edb_wait_states_sessions(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	TimestampTz start_ts = PG_GETARG_TIMESTAMPTZ(START_TS_ARGNO);
	TimestampTz end_ts = PG_GETARG_TIMESTAMPTZ(END_TS_ARGNO);

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	Assert(tupdesc->natts == Natts_session);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	read_edb_wait_states_sessions(tupstore, tupdesc, start_ts, end_ts);

	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * edb_wait_states_purge
 *
 * C interface to SQL callable function to delete edb_wait_states
 * files in given time interval.
 *
 * This function only implements the C interface to SQL callable
 * UDF, but the real work is carried out in delete_edb_wait_states_files(). See
 * prologue of that function for more details.
 */
Datum
edb_wait_states_purge(PG_FUNCTION_ARGS)
{
	TimestampTz start_ts = PG_GETARG_TIMESTAMPTZ(START_TS_ARGNO);
	TimestampTz end_ts = PG_GETARG_TIMESTAMPTZ(END_TS_ARGNO);
#ifdef USE_ASSERT_CHECKING
	Oid			result_type;
#endif							/* USE_ASSERT_CHECKING */

#ifdef USE_ASSERT_CHECKING
	(void) get_call_result_type(fcinfo, &result_type, NULL);
	Assert(result_type == VOIDOID);
#endif							/* USE_ASSERT_CHECKING */

	delete_edb_wait_states_files(start_ts, end_ts);

	PG_RETURN_VOID();
}

/*
 * is_edb_wait_states_file
 *
 * If the given file name looks like a edb_wait_states file's name,
 * return true. Otherwise false. The file name should start with the given
 * prefix and end with two timestamps separated by a '-'. The two timestamps
 * are returned in the given output arguments.
 */
bool
is_edb_wait_states_file(const char *filename, const char *prefix,
						TimestampTz *file_start_ts, TimestampTz *file_end_ts)
{
	if (strstr(filename, prefix) != filename)
		return false;

	if (sscanf(filename + strlen(prefix), INT64_FORMAT "_" INT64_FORMAT,
		file_start_ts, file_end_ts) != 2)
		return false;

	return true;
}

/*
 * read_edb_wait_states_samples
 *
 * Work-horse function implementing the guts of edb_wait_states_samples().
 *
 * User configurable directory contains files containing edb_wait_states
 * data. From those files choose the files that contain wait event samples i.e.
 * the files with prefix EDB_WAIT_STATES_SAMPLES_FILE_PREFIX. The names of each
 * of these files contains the interval, the samples collected within which,
 * are saved in that file. Choose the files that contain the samples belonging
 * to the given interval. The samples in each such file are then read by
 * read_edb_wait_states_one_sample_file().
 */
static void
read_edb_wait_states_samples(Tuplestorestate *tupstore,
							 TupleDesc tupdesc, TimestampTz start_ts,
							 TimestampTz end_ts)
{
	DIR		   *samples_dir;
	struct dirent *samples_ent;
	TimestampTz file_start_ts;
	TimestampTz file_end_ts;

	samples_dir = AllocateDir(get_edb_wait_states_directory());
	while ((samples_ent = ReadDir(samples_dir,
								  get_edb_wait_states_directory())) != NULL)
	{
		char		sample_file_name[MAXPGPATH];

		/* Ignore current and previous directories. */
		if (strcmp(samples_ent->d_name, ".") == 0 ||
			strcmp (samples_ent->d_name, "..") == 0)
			continue;

		/*
		 * Ignore the deleted files, refer comments in
		 * delete_edb_wait_states_files().
		 */
#ifdef WIN32
		if (strstr(samples_ent->d_name, ".deleted") != NULL)
			continue;
#endif

		if (!is_edb_wait_states_file(samples_ent->d_name,
									 EDB_WAIT_STATES_SAMPLES_FILE_PREFIX,
									 &file_start_ts, &file_end_ts))
		{
			elog(DEBUG3, "skipping a non-sample file %s", samples_ent->d_name);
			continue;
		}

		/* Ignore any file whose timestamps are beyond the requested range. */
		if ((timestamptz_cmp_internal(file_start_ts, end_ts) > 0) ||
			(timestamptz_cmp_internal(file_end_ts, start_ts) <= 0))
		{
			elog(DEBUG3, "skipping a sample file %s outside given time interval",
				 samples_ent->d_name);
			continue;
		}

		snprintf(sample_file_name, MAXPGPATH, "%s/%s",
				 get_edb_wait_states_directory(), samples_ent->d_name);

		read_edb_wait_states_one_sample_file(sample_file_name, tupstore,
											 tupdesc, start_ts, end_ts);
	}

	FreeDir(samples_dir);
}

/*
 * read_edb_wait_states_one_sample_file
 *
 * The function reads given file containing wait event samples and inserts the
 * records in it in the given tuple store. Each sample is assumed to be written
 * as EDBWaitStatesSample structure. The function ignores any samples which do
 * not belong to the given time interval.
 */
static void
read_edb_wait_states_one_sample_file(char *sample_file_name,
									 Tuplestorestate *tupstore,
									 TupleDesc tupdesc, TimestampTz start_ts,
									 TimestampTz end_ts)
{
	EDBWaitStatesSample sample;
	Datum		values[Natts_sample];
	bool		nulls[Natts_sample];
	FILE	   *samples_file;

	samples_file = AllocateFile(sample_file_name, PG_BINARY_R);
	if (samples_file == NULL)
	{
		/*
		 * Couldn't open the file. The reader is supposed to provide as much
		 * data as possible, so do not throw an error, which would cause the
		 * transaction to abort. Instead give a warning and move on.
		 */
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("can not open file %s for reading samples: %m",
						sample_file_name)));
		return;
	}

	/*
	 * Read one sample and add it to the tuple store.  We might do better by
	 * reading a bunch of samples e.g. whatever fits in 1KB memory and add
	 * them to the tuple store one at a time. But probably fread() already
	 * does the buffering so efficiency we will get may not be worth two
	 * loops, one to read the file in chunks and other to add one tuple at a
	 * time to the tuple store.
	 */
	while (fread(&sample, sizeof(sample), 1, samples_file) == 1)
	{
		const char *wait_event_type;
		const char *wait_event;

		/* initialize nulls as if we have none of them */
		memset(nulls, 0, sizeof(nulls));

		/* Ignore samples which are not within the given range. */
		if (timestamptz_cmp_internal(sample.sample_ts, start_ts) < 0 ||
			timestamptz_cmp_internal(sample.sample_ts, end_ts) >= 0)
			continue;

		wait_event_type = pgstat_get_wait_event_type(sample.wait_event_id);
		if (wait_event_type)
			values[Anum_sample_wait_event_type] = CStringGetTextDatum(wait_event_type);
		else
			nulls[Anum_sample_wait_event_type] = true;

		wait_event = pgstat_get_wait_event(sample.wait_event_id);
		if (wait_event)
			values[Anum_sample_wait_event] = CStringGetTextDatum(wait_event);
		else
			nulls[Anum_sample_wait_event] = true;

		values[Anum_sample_queryid] = Int64GetDatumFast(sample.query_id);
		values[Anum_sample_sessionid] = Int32GetDatum(sample.session_id);
		values[Anum_sample_ts] = TimestampTzGetDatum(sample.sample_ts);
		values[Anum_sample_query_start_ts] = TimestampTzGetDatum(sample.query_start_ts);
		values[Anum_sample_interval] = Int32GetDatum(sample.sample_interval);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		if (wait_event_type)
			pfree(DatumGetPointer(values[Anum_sample_wait_event_type]));
		if (wait_event)
			pfree(DatumGetPointer(values[Anum_sample_wait_event]));
	}

	FreeFile(samples_file);
}

/*
 * read_edb_wait_states_queries
 *
 * Work-horse function implementing the guts of edb_wait_states_queries().
 *
 * User configurable directory contains files containing edb_wait_states
 * data. From those files choose the files that contain information about
 * sampled queries i.e.  the files with prefix
 * EDB_WAIT_STATES_QUERIES_FILE_PREFIX. The names of each of these files contains
 * the interval, the queries collected within which are saved in that file.
 * Choose the files that contain the queries belonging to the given interval.
 * The queries in each such file are then read by
 * read_edb_wait_states_one_query_file().
 */
static void
read_edb_wait_states_queries(Tuplestorestate *tupstore,
							 TupleDesc tupdesc, TimestampTz start_ts,
							 TimestampTz end_ts)
{
	DIR		   *queries_dir;
	struct dirent *queries_ent;
	TimestampTz file_start_ts;
	TimestampTz file_end_ts;

	queries_dir = AllocateDir(get_edb_wait_states_directory());
	while ((queries_ent = ReadDir(queries_dir,
								  get_edb_wait_states_directory())) != NULL)
	{
		char		query_file_name[MAXPGPATH];

		/* Ignore current and previous directories. */
		if (strcmp(queries_ent->d_name, ".") == 0 ||
			strcmp (queries_ent->d_name, "..") == 0)
			continue;

		/*
		 * Ignore the deleted files, refer comments in
		 * delete_edb_wait_states_files().
		 */
#ifdef WIN32
		if (strstr(queries_ent->d_name, ".deleted") != NULL)
			continue;
#endif

		/* Ignore files other than queries file. */
		if (!is_edb_wait_states_file(queries_ent->d_name,
									 EDB_WAIT_STATES_QUERIES_FILE_PREFIX,
									 &file_start_ts, &file_end_ts))
		{
			elog(DEBUG3, "skipping a non-query file %s", queries_ent->d_name);
			continue;
		}

		/* Ignore any file whose timestamps are beyond the requested range. */
		if ((timestamptz_cmp_internal(file_start_ts, end_ts) > 0) ||
			(timestamptz_cmp_internal(file_end_ts, start_ts) <= 0))
		{
			elog(DEBUG3, "skipping a query file %s outside given time interval",
				 queries_ent->d_name);
			continue;
		}

		snprintf(query_file_name, MAXPGPATH, "%s/%s",
				 get_edb_wait_states_directory(), queries_ent->d_name);

		read_edb_wait_states_one_query_file(query_file_name, tupstore, tupdesc,
											file_start_ts, file_end_ts);
	}

	FreeDir(queries_dir);
}

/*
 * read_edb_wait_states_one_query_file
 *
 * The function reads given file containing sampled queries and inserts the
 * records in it in the given tuple store. Each query record is assumed to
 * contain triplet: 64 bit query id, length of query, actual query text.  The
 * function also accepts time interval, the queries sampled within which are
 * saved in the file. This time interval is saved in each record. This
 * reference time interval can then be used to identify the wait event samples,
 * the query ids in which can be found in this file.
 */
static void
read_edb_wait_states_one_query_file(char *query_file_name,
									Tuplestorestate *tupstore,
									TupleDesc tupdesc, TimestampTz file_start_ts,
									TimestampTz file_end_ts)
{
	FILE	   *query_file;
	Datum		values[Natts_query];
	bool		nulls[Natts_query];
	char	   *buffer;

	memset(nulls, 0, sizeof(nulls));

	query_file = AllocateFile(query_file_name, PG_BINARY_R);
	if (query_file == NULL)
	{
		/*
		 * Couldn't open the file. The reader is supposed to provide as much
		 * data as possible, so do not throw an error, which would cause the
		 * transaction to abort. Instead give a warning and move on.
		 */
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("can not open file %s for reading queries: %m",
						query_file_name)));
		return;
	}

	/* Read the file till end. */
	buffer = palloc(pgstat_track_activity_query_size + 1);
	while (true)
	{
		uint64		query_id;
		int			query_len;

		/*
		 * Read the next record from query file. While doing so, if we
		 * encounter anything unexpected, return whatever sane information we
		 * gathered and stop. That is more desired than throwing an error in
		 * case of error.
		 */
		if (ews_read_next_query_record(query_file, &query_id, &query_len,
									   buffer) <= 0)
			break;

		values[Anum_query_id] = Int64GetDatumFast(query_id);
		values[Anum_query_text] = CStringGetTextDatum(buffer);

		values[Anum_query_ref_start_ts] = TimestampTzGetDatum(file_start_ts);
		values[Anum_query_ref_end_ts] = TimestampTzGetDatum(file_end_ts);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		pfree(DatumGetPointer(values[Anum_query_text]));
	}

	pfree(buffer);
	FreeFile(query_file);
}

/*
 * read_edb_wait_states_sessions
 *
 * Work-horse function implementing the guts of edb_wait_states_sessions().
 *
 * User configurable directory contains files containing edb_wait_states
 * data. From those files choose the files that contain information about
 * sampled sessions i.e.  the files with prefix
 * EDB_WAIT_STATES_SESSIONS_FILE_PREFIX. The names of each of these files
 * contains the interval, the sessions sampled within which are saved in that
 * file.  Choose the files that contain the sessions belonging to the given
 * interval.  The information about sessions in each such file are then read by
 * read_edb_wait_states_one_session_file().
 */
static void
read_edb_wait_states_sessions(Tuplestorestate *tupstore,
							  TupleDesc tupdesc, TimestampTz start_ts,
							  TimestampTz end_ts)
{
	DIR		   *sessions_dir;
	struct dirent *sessions_ent;
	TimestampTz file_start_ts;
	TimestampTz file_end_ts;

	sessions_dir = AllocateDir(get_edb_wait_states_directory());
	while ((sessions_ent = ReadDir(sessions_dir,
								   get_edb_wait_states_directory())) != NULL)
	{
		char		session_file_name[MAXPGPATH];

		/* Ignore current and previous directories. */
		if (strcmp(sessions_ent->d_name, ".") == 0 ||
			strcmp (sessions_ent->d_name, "..") == 0)
			continue;

		/*
		 * Ignore the deleted files, refer comments in
		 * delete_edb_wait_states_files().
		 */
#ifdef WIN32
		if (strstr(sessions_ent->d_name, ".deleted") != NULL)
			continue;
#endif

		/* Ignore files other than sessions files. */
		if (!is_edb_wait_states_file(sessions_ent->d_name,
									 EDB_WAIT_STATES_SESSIONS_FILE_PREFIX,
									 &file_start_ts, &file_end_ts))
		{
			elog(DEBUG3, "skipping a non-session file %s", sessions_ent->d_name);
			continue;
		}

		/* Ignore any file whose timestamps are beyond the requested range. */
		if ((timestamptz_cmp_internal(file_start_ts, end_ts) > 0) ||
			(timestamptz_cmp_internal(file_end_ts, start_ts) <= 0))
		{
			elog(DEBUG3, "skipping a session file %s outside given time interval",
				 sessions_ent->d_name);
			continue;
		}

		snprintf(session_file_name, MAXPGPATH, "%s/%s",
				 get_edb_wait_states_directory(), sessions_ent->d_name);

		read_edb_wait_states_one_session_file(session_file_name, tupstore, tupdesc,
											  file_start_ts, file_end_ts);
	}

	FreeDir(sessions_dir);
}

/*
 * read_edb_wait_states_one_query_file
 *
 * The function reads given file containing sampled queries and inserts the
 * records in it in the given tuple store. Each query record is assumed to
 * contain 32 bit session id, length of database name, database name, length of
 * user name, user name.  The function also accepts time interval, the sessions
 * sampled within which are saved in the file. This time interval is saved in
 * each record. This reference time interval can then be used to identify the
 * wait event samples, the session ids in which can be found in this file.
 */
static void
read_edb_wait_states_one_session_file(char *session_file_name,
									  Tuplestorestate *tupstore,
									  TupleDesc tupdesc,
									  TimestampTz file_start_ts,
									  TimestampTz file_end_ts)
{
	FILE	   *session_file;
	Datum		values[Natts_session];
	bool		nulls[Natts_session];

	memset(nulls, 0, sizeof(nulls));

	session_file = AllocateFile(session_file_name, PG_BINARY_R);
	if (session_file == NULL)
	{
		/*
		 * Couldn't open the file. The reader is supposed to provide as much
		 * data as possible, so do not throw an error, which would cause the
		 * transaction to abort. Instead give a warning and move on.
		 */
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("can not open file %s for reading sessions: %m",
						session_file_name)));
		return;
	}

	/* Read the file till the end. */
	while (true)
	{
		int32		sessionid;
		char		dbname[NAMEDATALEN];
		char		username[NAMEDATALEN];

		/*
		 * Read the next record from session file. While doing so, if we
		 * encounter anything unexpected, return whatever sane information we
		 * gathered and stop.  That is more desired than throwing an error in
		 * case of error.
		 */
		if (ews_read_next_session_record(session_file,
										 &sessionid, dbname, username) <= 0)
			break;

		values[Anum_session_id] = Int64GetDatumFast(sessionid);
		values[Anum_session_db] = CStringGetTextDatum(dbname);
		values[Anum_session_user] = CStringGetTextDatum(username);

		values[Anum_session_ref_start_ts] = TimestampTzGetDatum(file_start_ts);
		values[Anum_session_ref_end_ts] = TimestampTzGetDatum(file_end_ts);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		pfree(DatumGetPointer(values[Anum_session_db]));
		pfree(DatumGetPointer(values[Anum_session_user]));
	}

	FreeFile(session_file);
}

/*
 * delete_edb_wait_states_files
 *
 * Given a time interval, delete all the files containing edb_wait_states
 * data that were created in that time interval. The files which
 * have their starting and ending timestamps completely within the given time
 * interval are deleted.
 */
void
delete_edb_wait_states_files(TimestampTz start_ts,
							 TimestampTz end_ts)
{
	DIR		   *ews_dir;
	struct dirent *file_ent;
	TimestampTz file_start_ts;
	TimestampTz file_end_ts;

	ews_dir = AllocateDir(get_edb_wait_states_directory());
	while ((file_ent = ReadDir(ews_dir,
							   get_edb_wait_states_directory())) != NULL)
	{
		char		file_name[MAXPGPATH];
#ifdef WIN32
		char		new_file_name[MAXPGPATH];
#endif
		int 		ret;

		/* Ignore current and previous directories. */
		if (strcmp(file_ent->d_name, ".") == 0 ||
			strcmp (file_ent->d_name, "..") == 0)
			continue;

		/*
		 * On windows we might have deleted log files lingering around, which
		 * are marked by appending '.deleted' in their names, ignore such
		 * files.
		 */
#ifdef WIN32
		if (strstr(file_ent->d_name, ".deleted") != NULL)
			continue;
#endif

		/*
		 * Verify known prefixes and obtain the time interval associated with
		 * files using those prefixes. Any file which has a different prefix
		 * will be ignored.
		 */
		if (!is_edb_wait_states_file(file_ent->d_name,
									 EDB_WAIT_STATES_SESSIONS_FILE_PREFIX,
									 &file_start_ts, &file_end_ts) &&
			!is_edb_wait_states_file(file_ent->d_name,
									 EDB_WAIT_STATES_QUERIES_FILE_PREFIX,
									 &file_start_ts, &file_end_ts) &&
			!is_edb_wait_states_file(file_ent->d_name,
									 EDB_WAIT_STATES_SAMPLES_FILE_PREFIX,
									 &file_start_ts, &file_end_ts))
			continue;

		/* Ignore any file which does not fit the given interval completely. */
		if ((timestamptz_cmp_internal(file_start_ts, start_ts) < 0) ||
			(timestamptz_cmp_internal(file_end_ts, end_ts) > 0))
		{
			elog(DEBUG3, "skipping edb_wait_states file %s outside given time interval",
				 file_ent->d_name);
			continue;
		}

		snprintf(file_name, MAXPGPATH, "%s/%s",
				 get_edb_wait_states_directory(), file_ent->d_name);

		/*
		 * On Windows, if another process holds the file (in this case
		 * edb_wait_states collector) open in FILE_SHARE_DELETE mode, unlink
		 * will succeed, but the file will still show up in directory listing
		 * until the last handle is closed. To avoid confusion due to the
		 * presence of this lingering file mark it as deleted.
		 */
#ifdef WIN32
		snprintf(new_file_name, MAXPGPATH, "%s.deleted", file_name);
		if (rename(file_name, new_file_name) != 0)
		{
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not rename old edb_wait_states log file \"%s\": %m",
							file_name)));
			return;
		}
		ret = unlink(new_file_name);
#else
		ret = unlink(file_name);
#endif

		if (ret != 0)
#ifdef WIN32
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not remove file \"%s\": %m",
							new_file_name)));
#else
            ereport(LOG,
                    (errcode_for_file_access(),
                     errmsg("could not remove file \"%s\": %m",
                            file_name)));
#endif
	}

	FreeDir(ews_dir);
}

/*
 * ews_read_next_session_record
 *
 * Reads the next session record from the session log file. Assumes that
 * session_file is currently pointing to start of next session record, where
 * record is of the form: sessionid, dbname length, dbname, username length,
 * username. If a next record is read successfully session_id, db_name and
 * user_name are set to appropriate field values. If db_name/user_name passed
 * were NULL, then the file pointer is moved dblen/userlen number of bytes
 * forward, so that file pointer points to start of next record. If a corrupted
 * or half cooked record is detected the file pointer is positioned back to the
 * start of the record.
 *
 * Returns 1 if next record is read successfully, 0 if no record found and -1
 * in case of an error.
 */
int
ews_read_next_session_record(FILE *session_file, int32 *session_id,
							 char *db_name, char *user_name)
{
	int			dblen;
	int			userlen;
	long		start_position = ftell(session_file);

	Assert((session_file != NULL) && (session_id != NULL));

	/*
	 * Read session id, dblen, "dblen" long database name, userlen, "userlen"
	 * long user name in the same sequence they were written.
	 */
	if (fread(session_id, sizeof(int32), 1, session_file) < 1)
	{
		/*
		 * If we are at the end of the file, either the file is empty or we
		 * have read all the records successfully from session log file.
		 */
		if (feof(session_file) && start_position == ftell(session_file))
			return 0;
		else
			goto error;
	}

	/* Read the length of the database name. */
	if (fread(&dblen, sizeof(dblen), 1, session_file) < 1)
		goto error;
	if (dblen <= 0 || dblen >= NAMEDATALEN)
		goto error;

	/*
	 * If database name is not requested to be sent back, just skip dblen
	 * number of bytes and position the pointer to next field i.e. userlen,
	 * else read and the database name in db_name buffer.
	 */
	if (db_name == NULL)
	{
		int			len = 0;

		while ((len != dblen) && fgetc(session_file) != EOF)
			len++;

		/*
		 * EOF reached before we could read and skip the database name of
		 * length dblen.
		 */
		if (len != dblen)
			goto error;
	}
	else
	{
		if (fread(db_name, 1, dblen, session_file) < dblen)
			goto error;

		db_name[dblen] = '\0';
	}

	/* Read the length of the username. */
	if (fread(&userlen, sizeof(userlen), 1, session_file) < 1)
		goto error;
	if (userlen <= 0 || userlen >= NAMEDATALEN)
		goto error;

	/*
	 * Similar to dbname if username not requested skip userlen bytes, else
	 * read the username in user_name buffer.
	 */
	if (user_name == NULL)
	{
		int			len = 0;

		while ((len != userlen) && fgetc(session_file) != EOF)
			len++;

		/*
		 * EOF reached before we could read and skip the user name of length
		 * userlen.
		 */
		if (len != userlen)
			goto error;
	}
	else
	{
		if (fread(user_name, 1, userlen, session_file) < userlen)
			goto error;

		user_name[userlen] = '\0';
	}

	/* Read a session record successfully. */
	return 1;

error:

	/*
	 * The session record seems to be corrupted, move the file pointer back to
	 * the start position.
	 */
	fseek(session_file, start_position, SEEK_SET);
	return -1;
}

/*
 * ews_read_next_query_record
 *
 * Reads the next query record from the query log file. Assumes that query_file
 * is currently pointing to start of next query record, where record is of the
 * form: queryid, query length, query text. If a next record is read successfully
 * query_id, query_len and query_buf are set to appropriate field values. If
 * query_buf passed in was NULL, then query_len number of bytes are skipped
 * after query_len is read, and the file pointer is moved to start of next
 * record. If a corrupted or half cooked record is detected the file pointer
 * is positioned back to the last valid record.
 *
 * Returns 1 if next record is read successfully, 0 if no record found and -1 in
 * case of an error.
 */
int
ews_read_next_query_record(FILE *query_file, uint64 *query_id,
						   int *query_len, char *query_buf)
{
	long		start_position = ftell(query_file);

	Assert((query_file != NULL) && (query_id != NULL) && (query_len != NULL));

	/*
	 * Read query id, query_len, "query_len" long query text in the same
	 * sequence they were written.
	 */
	if (fread(query_id, sizeof(uint64), 1, query_file) < 1)
	{
		/*
		 * If we are at the end of the file, either the file is empty or we
		 * have read all the records successfully from query log file.
		 */
		if (feof(query_file) && start_position == ftell(query_file))
			return 0;
		else
			goto error;
	}

	/* Read the length of the query text. */
	if (fread(query_len, sizeof(int), 1, query_file) < 1)
		goto error;
	if (*query_len <= 0 || *query_len > pgstat_track_activity_query_size)
		goto error;

	/*
	 * If query text is not requested, move the file pointer to start of next
	 * query record by skipping the query text.
	 */
	if (query_buf == NULL)
	{
		int			qlen = 0;

		while ((qlen != *query_len) && fgetc(query_file) != EOF)
			qlen++;

		/*
		 * We can do only length based validation as we dont have any checksum
		 * stored anywhere to verify if the query text got corrupted. Ideally
		 * the file is not supposed to be manually edited, and only the last
		 * record in the event of crash might have been partitially written to
		 * log file.
		 */
		if (qlen != *query_len)
			goto error;
	}
	else
	{
		/* Read the query text. */
		if (fread(query_buf, 1, *query_len, query_file) < *query_len)
			goto error;

		query_buf[*query_len] = '\0';
	}

	/* Read a query record successfully. */
	return 1;

error:

	/*
	 * The query record seems to be corrupted, move the file pointer back to
	 * the start position.
	 */
	fseek(query_file, start_position, SEEK_SET);
	return -1;
}
