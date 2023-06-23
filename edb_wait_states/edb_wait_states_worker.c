/*-------------------------------------------------------------------------
 *
 * edb_wait_states_worker.c
 *			edb_wait_states sample collection worker.
 *
 * Copyright (c) 2018, EnterpriseDB
 *
 * IDENTIFICATION
 *		  contrib/edb_wait_states/edb_wait_states_worker.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/hash.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "edb_wait_states.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "parser/analyze.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker.h"
#include "replication/walsender.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/fd.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "tcop/utility.h"
#include "utils/timestamp.h"

/* Default location of the directory containing edb_wait_states log files. */
#define EDB_WAIT_STATES_DEFAULT_DIRECTORY "edb_wait_states"

/* Constants for timestamp calculations. */
#define SECS_PER_WEEK (7 * SECS_PER_DAY)
#define MILLISECONDS_PER_SEC 1000
#define MILLISECONDS_PER_HOUR (SECS_PER_HOUR * MILLISECONDS_PER_SEC)
#define MICROSECONDS_PER_MILLISEC 1000

/* Hash table names. */
#define QUERY_ID_HASH "edb_wait_states QueryId Hash"
#define SESSION_ID_HASH "edb_wait_states SessionId Hash"

/* GUC variables. */
static int	edb_wait_states_sampling_interval;	/* in seconds */
static char *edb_wait_states_directory = EDB_WAIT_STATES_DEFAULT_DIRECTORY;
static int	edb_wait_states_retention_period;
static bool edb_wait_states_enable_collection = true;

/*
 * Constants controlling the duration for which the log files are actively
 * written logs to.
 */
#define SAMPLE_LOG_ROTATION_AGE (SECS_PER_HOUR * MILLISECONDS_PER_SEC)
#define QUERY_LOG_ROTATION_AGE (SECS_PER_DAY * MILLISECONDS_PER_SEC)
#define SESSION_LOG_ROTATION_AGE (SECS_PER_DAY * MILLISECONDS_PER_SEC)

/* Structure holding metadata about log files. */
typedef struct EdbWaitStatesLogFile
{
	FILE	   *fp;
	char		file_name[MAXPGPATH];
	HTAB	   *id_hash;		/* NULL for sample file. */
	TimestampTz rotation_time;
} EdbWaitStatesLogFile;

/*
 * Shared memory structure for storing query information such as a generated
 * query id, normalized query text and the time when the query was started.
 */
typedef struct EdbWaitStatesQueryInfo
{
	/*
	 * To avoid locking overhead while the fields of EdbWaitStatesQueryInfo
	 * are being written and read, we use a protocol where the reader will
	 * check if the changecount is even, if yes the fields read are valid.
	 * This is based on the protocol used in PgBackendStatus, for details
	 * please refer to the comment for st_changecount in pgstat.h.
	 */
	int			ews_info_changecount;

	uint64		queryId;

	/* Following fields are valid only when queryId > 0. */

	/* Timestamp when the query started executing */
	TimestampTz query_start_ts;

	/*
	 * Normalized text form of a query. The max size of the array is defined
	 * by pgstat_track_activity_query_size GUC.
	 */
	char		normalized_query[FLEXIBLE_ARRAY_MEMBER];
} EdbWaitStatesQueryInfo;

/* Shared state information for edb_wait_states bgworker. */
typedef struct EdbWaitStatesSharedState
{
	LWLock		lock;			/* mutual exclusion for bgworker_pid */
	pid_t		bgworker_pid;

	/* There are MaxBackends EdbWaitStatesQueryInfo in this array. */
	EdbWaitStatesQueryInfo *ews_info[FLEXIBLE_ARRAY_MEMBER];
} EdbWaitStatesSharedState;

/*
 * These macros help reading fields of EdbWaitStatesQueryInfo to ensure validity.
 * This is similar to pgstat_increment_changecount_* and
 * pgstat_save_changecount_* macros in pgstat.h, please refer to the comment
 * for these macros in pgstat.h for more details.
 */
#define ews_increment_changecount_before(ewsinfo)	\
	do {	\
		(ewsinfo)->ews_info_changecount++;	\
		pg_write_barrier(); \
	} while (0)

#define ews_increment_changecount_after(ewsinfo) \
	do {	\
		pg_write_barrier(); \
		(ewsinfo)->ews_info_changecount++;	\
		Assert(((ewsinfo)->ews_info_changecount & 1) == 0); \
	} while (0)

#define ews_save_changecount_before(ewsinfo, save_changecount)	\
	do {	\
		save_changecount = ewsinfo->ews_info_changecount; \
		pg_read_barrier();	\
	} while (0)

#define ews_save_changecount_after(ewsinfo, save_changecount)	\
	do {	\
		pg_read_barrier();	\
		save_changecount = ewsinfo->ews_info_changecount; \
	} while (0)

void		_PG_init(void);
void		_PG_fini(void);
PGDLLEXPORT void edb_wait_states_main(Datum main_arg);

/* Pointer to shared-memory state. */
static EdbWaitStatesSharedState *ews_worker_state = NULL;

/* Current nesting depth of ExecutorRun + ProcessUtility calls */
static int	nested_level = 0;

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

/* Flag set by signal handler */
static volatile sig_atomic_t got_sighup = false;

static Size ews_shmem_size(Size *ews_info_size);
static void ews_init_shmem(void);
#if PG_VERSION_NUM >= 150000
static void ews_shmem_request(void);
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static void ews_detach_shmem(int code, Datum arg);
#if PG_VERSION_NUM >= 140000
static void ews_post_parse_analyze(ParseState *pstate, Query *query,
								   JumbleState *jstate);
#else
static void ews_post_parse_analyze(ParseState *pstate, Query *query);
#endif
static void ews_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void ews_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
#if PG_VERSION_NUM >= 100000
				uint64 count, bool execute_once);
#else
				uint64 count);
#endif
static void ews_ExecutorFinish(QueryDesc *queryDesc);
static void ews_ExecutorEnd(QueryDesc *queryDesc);
#if PG_VERSION_NUM >= 140000
static void
ews_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
				   bool readOnlyTree, ProcessUtilityContext context,
				   ParamListInfo params, QueryEnvironment *queryEnv,
				   DestReceiver *dest, QueryCompletion *completionTag);
#elif PG_VERSION_NUM >= 130000
static void
ews_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
				   ProcessUtilityContext context, ParamListInfo params,
				   QueryEnvironment *queryEnv,
				   DestReceiver *dest, QueryCompletion *completionTag);
#elif PG_VERSION_NUM >= 100000
static void ews_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
				   ProcessUtilityContext context, ParamListInfo params,
				   QueryEnvironment *queryEnv,
				   DestReceiver *dest, char *completionTag);
#else
static void ews_ProcessUtility(Node *pstmt, const char *queryString,
				   ProcessUtilityContext context, ParamListInfo params,
				   DestReceiver *dest, char *completionTag);
#endif
static void ews_sigterm_handler(SIGNAL_ARGS);
static void ews_sighup_handler(SIGNAL_ARGS);
static void ews_log_events(TimestampTz ts, EdbWaitStatesLogFile *session_file,
			   EdbWaitStatesLogFile *query_file, EdbWaitStatesLogFile *sample_file,
			   char *session_info_buffer, char *query_info_buffer,
			   EDBWaitStatesSample *sample_info_items);
static void ews_init_directory(void);
static void ews_init_log_files(EdbWaitStatesLogFile *session_file,
				   EdbWaitStatesLogFile *query_file,
				   EdbWaitStatesLogFile *sample_file);
static void create_log_file(TimestampTz start_ts, EdbWaitStatesLogFile *log_file,
				char *file_prefix);
static void ews_get_latest_log_files(TimestampTz startup_log_ts,
						 EdbWaitStatesLogFile *session_file,
						 EdbWaitStatesLogFile *query_file,
						 EdbWaitStatesLogFile *sample_file);
static void ews_open_latest_log_file(EdbWaitStatesLogFile *log_file,
						 char *file_name,
						 TimestampTz startup_log_ts,
						 TimestampTz file_start_ts,
						 TimestampTz rotation_age);
static TimestampTz ews_rotate_files(EdbWaitStatesLogFile *session_file,
				 EdbWaitStatesLogFile *query_file,
				 EdbWaitStatesLogFile *sample_file);
static HTAB *ews_create_hash(char *table_name, size_t keysize,
				size_t entrysize);
static void ews_populate_query_hash(EdbWaitStatesLogFile *query_file);
static void ews_populate_session_hash(EdbWaitStatesLogFile *session_file);
extern uint64 getQueryId(ParseState *pstate, Query *query, char **norm_query,
		   int *norm_query_len);


/*
 * _PG_init
 *
 * Module load callback.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;
#if PG_VERSION_NUM < 150000
	Size            ews_info_size;
#endif

	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomStringVariable("edb_wait_states.directory",
							   "The edb_wait_states logs are stored in this directory.",
							   "Default directory is $PGDATA/edb_wait_states.",
							   &edb_wait_states_directory,
							   EDB_WAIT_STATES_DEFAULT_DIRECTORY,
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomIntVariable("edb_wait_states.sampling_interval",
							"The interval between two sampling cycles.",
							NULL,
							&edb_wait_states_sampling_interval,
							1,
							1,
							INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("edb_wait_states.retention_period",
							"Log files will be automatically deleted after retention period.",
							NULL,
							&edb_wait_states_retention_period,
							SECS_PER_WEEK,
							SECS_PER_DAY,
							INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("edb_wait_states.enable_collection",
							"Enable/Disable wait states data collection",
							NULL,
							&edb_wait_states_enable_collection,
							true,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	EmitWarningsOnPlaceholders("edb_wait_states");
#if PG_VERSION_NUM < 150000
	RequestAddinShmemSpace(ews_shmem_size(&ews_info_size));
#endif

	/* Install hooks. */
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = ews_shmem_request;
#endif
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = ews_init_shmem;
	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = ews_post_parse_analyze;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = ews_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = ews_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = ews_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = ews_ExecutorEnd;
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = ews_ProcessUtility;

	/* Do not start the worker from the child processes. */
	if (IsUnderPostmaster)
		return;

	/* Create the edb_wait_states log directory if does not exist already. */
	ews_init_directory();

	/* Prepare to launch the background worker process. */
	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags =
		BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;

	/*
	 * In case of an unrecoverable error the worker would keep on restarting
	 * and every time log the error in server log. This might fill up the
	 * server log. To reduce the number of messages logged keep the restart
	 * time to 1 minute.
	 */
	worker.bgw_restart_time = SECS_PER_MINUTE;

	strcpy(worker.bgw_library_name, "edb_wait_states");
	strcpy(worker.bgw_function_name, "edb_wait_states_main");
	strcpy(worker.bgw_name, "edb_wait_states collector");
#if PG_VERSION_NUM >= 110000
	strcpy(worker.bgw_type, "edb_wait_states collector");
#endif

	/* Load as member of shared_preload_libraries */
	RegisterBackgroundWorker(&worker);
}

/*
 * _PG_fini
 *
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;
	ProcessUtility_hook = prev_ProcessUtility;
}

/*
 * ews_shmem_size
 *
 * Calculates and returns the size of required shared memory and sets the size
 * for EdbWaitStatesQueryInfo in ews_info_size.
 */
static Size
ews_shmem_size(Size *ews_info_size)
{
	Size		shm_size;
	int 		MaxBackends;

	/*
	 * Calculate the MaxBackends as global MaxBackends will be
	 * initialized by InitializeMaxBackends() and postmaster will call
	 * InitializeMaxBackends() once all libraries mentioned in
	 * shared_preload_libraries are processed. We are good to refer to
	 * local variable here for calculating the size.
	 */
	MaxBackends = MaxConnections + autovacuum_max_workers + 1 +
		max_worker_processes + max_wal_senders;
	/* First calculate the size of EdbWaitStatesQueryInfo. */
	*ews_info_size = offsetof(EdbWaitStatesQueryInfo, normalized_query);
	/* Get the size of normalized_query. */
	*ews_info_size = add_size(*ews_info_size,
							  pgstat_track_activity_query_size + 1);
	*ews_info_size = MAXALIGN(*ews_info_size);

	/*
	 * Now calculate the total size of shared memory required. fixed members +
	 * MaxBackends pointers to EdbWaitStatesQueryInfo + MaxBackends instances
	 * of EdbWaitStatesQueryInfo.
	 */
	shm_size = offsetof(EdbWaitStatesSharedState, ews_info);
	shm_size = add_size(shm_size, mul_size(MaxBackends,
										   sizeof(EdbWaitStatesQueryInfo *)));
	shm_size = MAXALIGN(shm_size);
	shm_size = add_size(shm_size, mul_size(MaxBackends, *ews_info_size));

	return shm_size;
}

#if PG_VERSION_NUM >= 150000
/*
 * Requests any additional shared memory required for dbms_pipe.
 */
static void
ews_shmem_request(void)
{
	Size            ews_info_size;

	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(ews_shmem_size(&ews_info_size));
}
#endif

/*
 * ews_init_shmem
 *
 * shmem_startup_hook to allocate or attach to a shared memory. Basically,
 * initialize the structure EdbWaitStatesSharedState in shared memory that will
 * hold information regarding queries being run on different backends. Every
 * backend will fill it's own slot of information via different hooks
 * installed, and then edb_wait_states worker would read them by
 * iterating over it for each backend.
 */
static void
ews_init_shmem(void)
{
	bool		found;
	int			i;
	Size		ews_info_size;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	/* Create or attach to the shared EdbWaitStatesSharedState array */
	ews_worker_state = ShmemInitStruct("edb_wait_states Worker State",
									   ews_shmem_size(&ews_info_size),
									   &found);
	if (!found)
	{
		EdbWaitStatesQueryInfo *ews_info;

		LWLockInitialize(&ews_worker_state->lock, LWLockNewTrancheId());
		ews_worker_state->bgworker_pid = InvalidPid;

		ews_info = (EdbWaitStatesQueryInfo *)
			((char *) ews_worker_state +
			 MAXALIGN(offsetof(EdbWaitStatesSharedState, ews_info) +
					  MaxBackends * sizeof(EdbWaitStatesQueryInfo *)));

		/*
		 * Initialize the ews_info members and ews_worker_state->ews_info
		 * array.
		 */
		for (i = 0; i < MaxBackends; i++)
		{
			ews_info->ews_info_changecount = 0;
			ews_info->queryId = UINT64CONST(0);
			TIMESTAMP_NOBEGIN(ews_info->query_start_ts);
			MemSet(ews_info->normalized_query, 0,
				   pgstat_track_activity_query_size + 1);

			ews_worker_state->ews_info[i] = ews_info;

			ews_info = (EdbWaitStatesQueryInfo *) ((char *) ews_info +
												   ews_info_size);
		}

	}

	LWLockRelease(AddinShmemInitLock);
}

/*
 * ews_detach_shmem
 *
 * Clear our PID from EdbWaitStatesSharedState.
 */
static void
ews_detach_shmem(int code, Datum arg)
{
	LWLockAcquire(&ews_worker_state->lock, LW_EXCLUSIVE);

	/*
	 * Only edb_wait_states worker sets the bgworker_pid hence while
	 * invalidating it we do not need to cross check if it was correctly set.
	 */
	ews_worker_state->bgworker_pid = InvalidPid;

	LWLockRelease(&ews_worker_state->lock);
}

/*
 * ews_post_parse_analyze
 *
 * A post_parse_analyze_hook: Parses the query string, generates a query
 * id and normalized query text, stores them in respective slot belonging to
 * the backend in shared memory array i.e. ews_worker_state->ews_info.
 */
static void
#if PG_VERSION_NUM >= 140000
ews_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate)
#else
ews_post_parse_analyze(ParseState *pstate, Query *query)
#endif
{
	char	   *norm_query = NULL;
	int			norm_query_len;
	uint64		queryId;
	EdbWaitStatesQueryInfo *ews_info;

	ews_info = ews_worker_state->ews_info[MyProc->pgprocno];

	if (prev_post_parse_analyze_hook)
#if PG_VERSION_NUM >= 140000
		prev_post_parse_analyze_hook(pstate, query, jstate);
#else
		prev_post_parse_analyze_hook(pstate, query);
#endif

	/* Generate queryId and normalization only for the top level query. */
	if (nested_level != 0)
		return;

	/*
	 * Do not process query and collect the stats if the backend have opted
	 * not to collect information about currently executing command by setting
	 * GUC 'track_activities' to OFF.
	 */
	if (!pgstat_track_activities)
		queryId = UINT64CONST(0);
	else
		/* Generate query ID here, and also get normalized query text */
		queryId = getQueryId(pstate, query, &norm_query, &norm_query_len);

	/*
	 * changecount increment need to do after the queryId generation, as
	 * in case queryId generation throws an unexpected error, we don't end
	 * up into the inconsistent state.
	 */
	ews_increment_changecount_before(ews_info);
	ews_info->queryId = queryId;

	if (queryId > 0)
	{
		/*
		 * Truncate the query string if it's longer than the
		 * pgstat_track_activity_query_size.
		 */
		if (norm_query_len > pgstat_track_activity_query_size)
		{
			norm_query_len = pgstat_track_activity_query_size + 1;
			norm_query[norm_query_len - 1] = '\0';
		}

		/*
		 * Copy the normalized query in shared buffer including the null
		 * terminating character.
		 */
		memcpy(ews_info->normalized_query, norm_query, norm_query_len + 1);
	}

	ews_increment_changecount_after(ews_info);

	/* getQueryId() might have allocated the query buffer, free the memory. */
	if (norm_query)
		pfree(norm_query);
}

/*
 * ews_ExecutorStart
 *
 * An ExecutorStart_hook: Stores the execution start time of the query in
 * shared memory.
 */
static void
ews_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	EdbWaitStatesQueryInfo *ews_info;

	ews_info = ews_worker_state->ews_info[MyProc->pgprocno];

	ews_increment_changecount_before(ews_info);
	ews_info->query_start_ts = GetCurrentTimestamp();
	ews_increment_changecount_after(ews_info);

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

/*
 * ExecutorRun
 *
 * An ExecutorRun_hook: All we need to do is track nesting depth.
 */
static void
ews_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
#if PG_VERSION_NUM >= 100000
				uint64 count, bool execute_once)
#else
				uint64 count)
#endif
{
	nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
		{
#if PG_VERSION_NUM >= 100000
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
#else
			prev_ExecutorRun(queryDesc, direction, count);
#endif
		}
		else
		{
#if PG_VERSION_NUM >= 100000
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
#else
			standard_ExecutorRun(queryDesc, direction, count);
#endif
		}
		nested_level--;
	}
	PG_CATCH();
	{
		nested_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ExecutorFinish
 *
 * An ExecutorFinish_hook: All we need to do is track nesting depth.
 */
static void
ews_ExecutorFinish(QueryDesc *queryDesc)
{
	nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
		nested_level--;
	}
	PG_CATCH();
	{
		nested_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ews_ExecutorEnd
 *
 * An ExecutorEnd_hook: Invalidates the query id for the backend, indicating
 * the background worker should ignore this backend slot in shared memory when
 * logging the wait states.
 */
static void
ews_ExecutorEnd(QueryDesc *queryDesc)
{
	EdbWaitStatesQueryInfo *ews_info;

	ews_info = ews_worker_state->ews_info[MyProc->pgprocno];

	if (nested_level == 0)
	{
		ews_increment_changecount_before(ews_info);

		/*
		 * Reset the queryId for this backend. Intentionally we do not reset
		 * the normalized_query to avoid memset overhead, instead always check
		 * queryId before trying to use normalized_query.
		 */
		ews_info->queryId = UINT64CONST(0);
		ews_increment_changecount_after(ews_info);
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

/*
 * ews_ProcessUtility
 *
 * A ProcessUtility_hook: Stores the execution start time of the utility query
 * in shared memory.
 */
#if PG_VERSION_NUM >= 140000
static void
ews_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
				   bool readOnlyTree, ProcessUtilityContext context,
				   ParamListInfo params, QueryEnvironment *queryEnv,
				   DestReceiver *dest, QueryCompletion *completionTag)
#elif PG_VERSION_NUM >= 130000
static void
ews_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
				   ProcessUtilityContext context, ParamListInfo params,
				   QueryEnvironment *queryEnv,
				   DestReceiver *dest, QueryCompletion *completionTag)
#elif PG_VERSION_NUM >= 100000
static void
ews_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
				   ProcessUtilityContext context, ParamListInfo params,
				   QueryEnvironment *queryEnv,
				   DestReceiver *dest, char *completionTag)
#else
static void
ews_ProcessUtility(Node *pstmt, const char *queryString,
				   ProcessUtilityContext context, ParamListInfo params,
				   DestReceiver *dest, char *completionTag)
#endif
{
	int			is_top_level = (nested_level == 0);
	EdbWaitStatesQueryInfo *ews_info;

	ews_info = ews_worker_state->ews_info[MyProc->pgprocno];

	if (is_top_level)
	{
		ews_increment_changecount_before(ews_info);
		ews_info->query_start_ts = GetCurrentTimestamp();
		ews_increment_changecount_after(ews_info);
	}

	nested_level++;
	PG_TRY();
	{
		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString,
#if PG_VERSION_NUM >= 140000
								readOnlyTree,
#endif
								context, params,
#if PG_VERSION_NUM >= 100000
								queryEnv,
#endif
								dest, completionTag);
		else
			standard_ProcessUtility(pstmt, queryString,
#if PG_VERSION_NUM >= 140000
									readOnlyTree,
#endif
									context, params,
#if PG_VERSION_NUM >= 100000
									queryEnv,
#endif
									dest, completionTag);
		nested_level--;
	}
	PG_CATCH();
	{
		/* Reset the queryId for the backend in case of an error. */
		if (is_top_level)
		{
			ews_increment_changecount_before(ews_info);
			ews_info->queryId = UINT64CONST(0);
			ews_increment_changecount_after(ews_info);
		}

		nested_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * Reset the queryId for this backend once the utility command is
	 * processed.
	 */
	if (is_top_level)
	{
		ews_increment_changecount_before(ews_info);
		ews_info->queryId = UINT64CONST(0);
		ews_increment_changecount_after(ews_info);
	}
}

/*
 * edb_wait_states_main
 *
 * Main entry point for the master edb_wait_states worker. Loops infinitely
 * unless a SIGTERM is received. Collects the diagnostics data after every
 * edb_wait_states.sampling_interval is hit. Further, it takes care of rotating
 * i.e. creating new log files after a predefined rotation interval, also
 * performs a cleanup check at intervals to delete the log files older than
 * edb_wait_states.retention_period.
 */
void
edb_wait_states_main(Datum main_arg)
{
	/* Log files. */
	EdbWaitStatesLogFile sample_file;
	EdbWaitStatesLogFile session_file;
	EdbWaitStatesLogFile query_file;

	/* Timestamp when the next set of samples will be collected. */
	TimestampTz sampling_dump_time = 0;

	/* Timestamp when deletion of log files will be triggered. */
	TimestampTz log_files_deletion_time;

	/*
	 * Buffers for holding the query, session and sample records before they
	 * can be written onto their respective files.
	 */
	EDBWaitStatesSample *sample_info_items;
	char	   *session_info_buffer;
	char	   *query_info_buffer;

	/* Establish signal handlers; once that's done, unblock signals. */
	pqsignal(SIGTERM, ews_sigterm_handler);
	pqsignal(SIGHUP, ews_sighup_handler);
	BackgroundWorkerUnblockSignals();

	/* Set on-detach hook so that our PID will be cleared on exit. */
	on_shmem_exit(ews_detach_shmem, 0);

	/*
	 * Store our PID in the shared memory area --- unless there's already
	 * another worker running, in which case just exit.
	 */
	LWLockAcquire(&ews_worker_state->lock, LW_EXCLUSIVE);
	if (ews_worker_state->bgworker_pid != InvalidPid)
	{
		pid_t		pid = ews_worker_state->bgworker_pid;

		LWLockRelease(&ews_worker_state->lock);
		ereport(LOG,
				(errmsg("edb_wait_states worker is already running under PID %d",
						pid)));
		return;
	}
	ews_worker_state->bgworker_pid = MyProcPid;
	LWLockRelease(&ews_worker_state->lock);

	ereport(LOG, (errmsg("edb_wait_states collector started")));

	/*
	 * Connect background worker to postgres database, so that we can access
	 * syscache to retrieve information from catalogs such as username,
	 * database name etc.
	 */
#if PG_VERSION_NUM >= 110000
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);
#else
	BackgroundWorkerInitializeConnection("postgres", NULL);
#endif

	/* Get the log file handles ready for writing logs. */
	ews_init_log_files(&session_file, &query_file, &sample_file);

	/*
	 * Allocate buffers sufficient for holding the logs for all backends
	 * locally.
	 *
	 * Session file record format: sessionId, dbname_len, dbname,
	 * dbusername_len, dbuser
	 */
	session_info_buffer = (char *) palloc((sizeof(int) + sizeof(int) +
										   NAMEDATALEN + sizeof(int) +
										   NAMEDATALEN) * MaxBackends);

	/*
	 * Query log record length is (queryid i.e. uint64 + query_text_len i.e.
	 * int + query_text)
	 */
	query_info_buffer = (char *) palloc((sizeof(uint64) + sizeof(int) +
										 pgstat_track_activity_query_size +
										 1) * MaxBackends);

	/* Now allocate MaxBackends of EDBWaitStatesSample. */
	sample_info_items = (EDBWaitStatesSample *)
		palloc(sizeof(EDBWaitStatesSample) * MaxBackends);

	/* Initialize sampling time to first edb_wait_states.sampling_interval. */
	sampling_dump_time = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
													 edb_wait_states_sampling_interval * MILLISECONDS_PER_SEC);

	/*
	 * Initialize the deletion time to current time. This will take care of
	 * cleaning any files that are beyond the retention period at start-up if
	 * an existing edb_wait_states directory was found.
	 */
	log_files_deletion_time = GetCurrentTimestamp();

	/*
	 * Periodically dump the active queries, the backend info it's running on
	 * and the wait event in log files.
	 */
	for (;;)
	{
		long		secs = 0;
		int			usecs = 0;
		TimestampTz current_time;

		/* Earliest time of all rotation, deletion and sampling time. */
		TimestampTz next_activity_time;

		/* Calling CHECK_FOR_INTERRUPTS() can allow us to handle interrupts. */
		CHECK_FOR_INTERRUPTS();

		/* In case of a SIGHUP, just reload the configuration. */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (!edb_wait_states_enable_collection)
		{
			/*
			 * It's better to wait than loop continuously while data collection
			 * is turned off.
			 */
			/* Update dump time for the next cycle. */
			current_time = GetCurrentTimestamp();
			sampling_dump_time = TimestampTzPlusMilliseconds(current_time,
															 edb_wait_states_sampling_interval * MILLISECONDS_PER_SEC);
			next_activity_time = sampling_dump_time;
			goto wait;
		}

		/* Rotate the files if needed. */
		current_time = ews_rotate_files(&session_file, &query_file,
										&sample_file);

		/* Is it the time to dump samples? */
		if (timestamptz_cmp_internal(sampling_dump_time, current_time) <= 0)
		{

			ews_log_events(current_time, &session_file, &query_file,
						   &sample_file, session_info_buffer,
						   query_info_buffer, sample_info_items);

			/* Update dump time for the next cycle. */
			sampling_dump_time = TimestampTzPlusMilliseconds(sampling_dump_time,
															 edb_wait_states_sampling_interval * MILLISECONDS_PER_SEC);

			current_time = GetCurrentTimestamp();

			/*
			 * Ideally sampling_dump_time should be in future. The
			 * sampling_interval should be set such that it does not trigger
			 * the logging too frequently causing significant slow down of the
			 * database server.
			 */
			if (timestamptz_cmp_internal(sampling_dump_time, current_time) <= 0)
				ereport(LOG,
						(errmsg_plural("edb_wait_states sample collection is occurring too frequently (%d second apart)",
									   "edb_wait_states sample collection is occurring too frequently (%d seconds apart)",
									   edb_wait_states_sampling_interval,
									   edb_wait_states_sampling_interval),
						 errhint("Consider increasing the configuration parameter \"edb_wait_states.sampling_interval\".")));
		}

		/*
		 * Delete any files that are beyond retention period if
		 * log_files_deletion_time is hit.
		 */
		if (timestamptz_cmp_internal(log_files_deletion_time, current_time) <= 0)
		{
			TimestampTz dt_nobegin;
			TimestampTz del_end_ts;

			/*
			 * We want to delete only files that have hit the retention_period
			 * with respect to current time.
			 */
			del_end_ts = TimestampTzPlusMilliseconds(current_time,
													 0 - (edb_wait_states_retention_period *
														  MILLISECONDS_PER_SEC));

			TIMESTAMP_NOBEGIN(dt_nobegin);
			delete_edb_wait_states_files(dt_nobegin, del_end_ts);

			/* Schedule next time for log files clean-up. */
			log_files_deletion_time = TimestampTzPlusMilliseconds(log_files_deletion_time,
																  HOURS_PER_DAY * MILLISECONDS_PER_HOUR);

			current_time = GetCurrentTimestamp();
		}

		/* Find the nearest time until which worker should sleep. */
		next_activity_time = sampling_dump_time;
		if (timestamptz_cmp_internal(sample_file.rotation_time,
									 next_activity_time) <= 0)
			next_activity_time = sample_file.rotation_time;
		if (timestamptz_cmp_internal(session_file.rotation_time,
									 next_activity_time) <= 0)
			next_activity_time = session_file.rotation_time;
		if (timestamptz_cmp_internal(query_file.rotation_time,
									 next_activity_time) <= 0)
			next_activity_time = query_file.rotation_time;
		if (timestamptz_cmp_internal(log_files_deletion_time,
									 next_activity_time) <= 0)
			next_activity_time = log_files_deletion_time;

wait:
		/*
		 * If it is not yet past the scheduled next activity, sleep until
		 * next_activity_time.
		 */
		TimestampDifference(current_time, next_activity_time,
							&secs, &usecs);
		if (secs > 0 || usecs > 0)
		{
			int			rc;
			long		delay_in_ms;

			delay_in_ms = (secs * MILLISECONDS_PER_SEC) +
				(usecs / MICROSECONDS_PER_MILLISEC);

			/* Sleep until the next dump time. */
			rc = WaitLatch(&MyProc->procLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
#if PG_VERSION_NUM >= 100000
						   delay_in_ms,
						   PG_WAIT_EXTENSION);
#else
						   delay_in_ms);
#endif

			/* Reset the latch, bail out if postmaster died, otherwise loop. */
			ResetLatch(&MyProc->procLatch);
			if (rc & WL_POSTMASTER_DEATH)
				proc_exit(1);
		}
	}

	ereport(LOG, (errmsg("edb_wait_states collector shutting down")));
	return;
}

/*
 * ews_sigterm_handler
 *
 * Signal handler for SIGTERM.
 */
static void
ews_sigterm_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	/* Don't joggle the elbow of proc_exit */
	if (!proc_exit_inprogress)
	{
		InterruptPending = true;
		ProcDiePending = true;
	}

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * ews_sighup_handler
 *
 * Signal handler for SIGHUP.
 */
static void
ews_sighup_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * ews_log_events
 *
 * Logs the session information, the query being run and the wait events if any
 * the backend is waiting on for all the backends at present. ts indicates the
 * reference timestamp when the logs are being collected. Note that there is
 * always going to be some degree of error when the sample is actually
 * collected and when the timestamp for it is noted.
 */
static void
ews_log_events(TimestampTz ts, EdbWaitStatesLogFile *session_file,
			   EdbWaitStatesLogFile *query_file, EdbWaitStatesLogFile *sample_file,
			   char *session_info_buffer, char *query_info_buffer,
			   EDBWaitStatesSample *sample_info_items)
{
	int			num_backends = pgstat_fetch_stat_numbackends();
	int			curr_backend;
	int			cnt_sampling_items = 0;
	int			query_info_index = 0;
	int			session_info_index = 0;
	char	   *query_text;

	/* Allocate a buffer for query text */
	query_text = (char *) palloc(pgstat_track_activity_query_size);

	/* 1-based index */
	for (curr_backend = 1; curr_backend <= num_backends; curr_backend++)
	{
		PGPROC	   *proc;
		uint64		queryId;
		TimestampTz query_start_ts;
		int			query_len;
		EDBWaitStatesSample *samplingItem;
		LocalPgBackendStatus *local_beentry;
		bool		found;
		int			procpid;
#if PG_VERSION_NUM >= 130000
		BackendType backendType = B_INVALID;
#else
		BackendType backendType = -1;
#endif
		BackendState backendState = STATE_UNDEFINED;
		Oid			databaseid = InvalidOid;
		Oid			userid = InvalidOid;

		CHECK_FOR_INTERRUPTS();

		local_beentry = pgstat_fetch_stat_local_beentry(curr_backend);
		if (!local_beentry)
			continue;

		for (;;)
		{
			/*
			 * Follow the protocol of retrying if st_changecount changes while
			 * we copy the entry. For details refer to comment in similar loop
			 * in pgstat_read_current_status().
			 */
			PgBackendStatus *beentry;
			int			before_changecount;
			int			after_changecount;

			beentry = &local_beentry->backendStatus;

#if PG_VERSION_NUM >= 120000
			pgstat_begin_read_activity(beentry, before_changecount);
#else
			pgstat_save_changecount_before(beentry, before_changecount);
#endif

			procpid = beentry->st_procpid;

			if (procpid > 0)
			{
#if PG_VERSION_NUM >= 100000
				backendType = beentry->st_backendType;
#endif
				backendState = beentry->st_state;
				databaseid = beentry->st_databaseid;
				userid = beentry->st_userid;
			}

#if PG_VERSION_NUM >= 120000
			pgstat_end_read_activity(beentry, after_changecount);
#else
			pgstat_save_changecount_after(beentry, after_changecount);
#endif
#if PG_VERSION_NUM >= 120000
			if (pgstat_read_activity_complete(before_changecount, after_changecount))
#else
			if (before_changecount == after_changecount &&
				(before_changecount & 1) == 0)
#endif
				break;

			/* Make sure we can break out of loop if stuck... */
			CHECK_FOR_INTERRUPTS();
		}

		proc = BackendPidGetProc(procpid);

#if PG_VERSION_NUM >= 100000
		/* We are interested in collecting statistics of backends only */
		if (proc == NULL || backendType != B_BACKEND)
			continue;
#else
		if (proc == NULL)
			continue;
#endif

		for (;;)
		{
			/*
			 * Follow the protocol of retrying if ews_info_changecount changes
			 * while reading the fields of EdbWaitStatesQueryInfo in local
			 * variables. For details refer to comment in similar loop in
			 * pgstat_read_current_status() for st_changecount.
			 */
			EdbWaitStatesQueryInfo *ews_info;
			int			before_changecount;
			int			after_changecount;

			ews_info = ews_worker_state->ews_info[proc->pgprocno];

			ews_save_changecount_before(ews_info, before_changecount);

			/* Locally store the fields of edb_info. */
			queryId = ews_info->queryId;
			query_start_ts = ews_info->query_start_ts;
			query_len = strlen(ews_info->normalized_query);
			memcpy(query_text, ews_info->normalized_query, query_len);

			ews_save_changecount_after(ews_info, after_changecount);

			/* Check if we have got valid copies of fields. */
			if (before_changecount == after_changecount &&
				(before_changecount & 1) == 0)
				break;

			/* Make sure we can break out of loop if stuck... */
			CHECK_FOR_INTERRUPTS();
		}

		/*
		 * We want to log events only for active queries and only for the
		 * backends having track_activities set to ON, continue otherwise. If
		 * queryId is 0, it means track_activities is OFF.
		 */
		if (queryId == UINT64CONST(0) || backendState == STATE_IDLE)
			continue;

		/* Enter the queryId in the hash table */
		hash_search(query_file->id_hash, &queryId, HASH_ENTER, &found);

		/* If this is a new query, we log this to the query log file */
		if (!found)
		{
			/* query file record format: queryid, query_text_len, query_text */
			memcpy(query_info_buffer + query_info_index, &queryId,
				   sizeof(queryId));
			query_info_index += sizeof(queryId);
			memcpy(query_info_buffer + query_info_index, &query_len,
				   sizeof(query_len));
			query_info_index += sizeof(query_len);
			memcpy(query_info_buffer + query_info_index, query_text,
				   query_len);
			query_info_index += query_len;
		}

		/* Enter the session id in the hash table */
		hash_search(session_file->id_hash, &procpid, HASH_ENTER, &found);

		/* If this is a new session, we log this to the session log file */
		if (!found)
		{
			char	   *username;
			char	   *dbname;
			int			uname_len;
			int			db_len;

			StartTransactionCommand();

			/* Get the database name */
			Assert(OidIsValid(databaseid));
			dbname = get_database_name(databaseid);
			db_len = strlen(dbname);

			Assert(OidIsValid(userid));
			username = GetUserNameFromId(userid, false);
			uname_len = strlen(username);

			/*
			 * session file record format: sessionId, dbname_len, dbname,
			 * dbusername_len, dbuser
			 */
			memcpy(&session_info_buffer[session_info_index], &procpid,
				   sizeof(procpid));
			session_info_index += sizeof(procpid);

			memcpy(&session_info_buffer[session_info_index], &db_len,
				   sizeof(db_len));
			session_info_index += sizeof(db_len);

			memcpy(&session_info_buffer[session_info_index], dbname, db_len);
			session_info_index += db_len;

			memcpy(&session_info_buffer[session_info_index], &uname_len,
				   sizeof(uname_len));
			session_info_index += sizeof(uname_len);

			memcpy(&session_info_buffer[session_info_index], username,
				   uname_len);
			session_info_index += uname_len;

			CommitTransactionCommand();
		}

		/* Fill up sampling item */
		samplingItem = &sample_info_items[cnt_sampling_items++];
		samplingItem->query_id = queryId;
		samplingItem->session_id = procpid;
		samplingItem->sample_ts = ts;
		samplingItem->wait_event_id = proc->wait_event_info;
		samplingItem->sample_interval = edb_wait_states_sampling_interval;
		samplingItem->query_start_ts = query_start_ts;
	}
	/* Throw away the current stats snapshot. */
	pgstat_clear_snapshot();

	/* Write to the query info log file. */
	if (query_info_index > 0)
	{
		if (fwrite(query_info_buffer, query_info_index, 1,
				   query_file->fp) != 1)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("can not write query info to edb_wait_states query log file \"%s\": %m",
								   query_file->file_name)));

		fflush(query_file->fp);
	}

	/* Write to the session info log file. */
	if (session_info_index > 0)
	{
		if (fwrite(session_info_buffer, session_info_index, 1,
				   session_file->fp) != 1)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("can not write session info to edb_wait_states session log file \"%s\": %m",
								   session_file->file_name)));

		fflush(session_file->fp);
	}

	/*
	 * The samples are written after query and session logs are written,
	 * because sample record contains both queryid and sessionid. If we write
	 * sample record before query/session logs are written and in case there
	 * is an error writing query or session log file, then there may not be a
	 * record in query or session file that would have a row containing the
	 * queryid or sessionid for corresponding sample.
	 */
	if (cnt_sampling_items > 0)
	{
		if (fwrite(sample_info_items, sizeof(EDBWaitStatesSample),
				   cnt_sampling_items, sample_file->fp) != cnt_sampling_items)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("can not write sampling records to sample log file \"%s\": %m",
								   sample_file->file_name)));

		fflush(sample_file->fp);
	}

	pfree(query_text);
}

/*
 * ews_init_directory
 *
 * Creates a directory specified by GUC 'edb_wait_states.directory'. If the
 * directory already exists logs a message indicating directory already
 * exists.
 */
static void
ews_init_directory()
{
	/* create directory where we will later create log files */
	if (mkdir(edb_wait_states_directory, S_IRWXU) < 0)
	{
		struct stat st;

		if (errno == EEXIST &&
			(lstat(edb_wait_states_directory, &st) == 0 &&
			 S_ISDIR(st.st_mode)))
			ereport(LOG,
					(errmsg("directory \"%s\" already exists",
							edb_wait_states_directory)));
		else
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("error creating the samples directory \"%s\": %m",
								   edb_wait_states_directory),
							errhint("Make sure you have sufficient privileges to create the directory or a similarly named file does not already exist.")));
	}
}

/*
 * get_edb_wait_states_directory
 *
 * Returns the directory path where the log files are stored.
 */
char *
get_edb_wait_states_directory(void)
{
	return edb_wait_states_directory;
}

/*
 * ews_init_log_files
 *
 * Allocates the log files, Creates new or opens existing query, session and
 * sample log files as appropriate and sets their respective file handles,
 * rotation times in their respective EdbWaitStatesLogFile structure. If an
 * existing session or query file is opened, populates their respective hash
 * tables.
 */
static void
ews_init_log_files(EdbWaitStatesLogFile *session_file,
				   EdbWaitStatesLogFile *query_file,
				   EdbWaitStatesLogFile *sample_file)
{
	TimestampTz startup_ts = GetCurrentTimestamp();

	/* Initialize the file pointers. */
	session_file->fp = query_file->fp = sample_file->fp = NULL;

	/*
	 * Initialize hash tables. Create hash tables for query and session log
	 * files to maintain unqiue queryids and sessionids respectively. Sample
	 * file does not have any unique id, so mark its hash table as NULL.
	 */
	session_file->id_hash = ews_create_hash(SESSION_ID_HASH, sizeof(int),
											sizeof(int));
	query_file->id_hash = ews_create_hash(QUERY_ID_HASH,
										  sizeof(uint64),
										  sizeof(uint64));
	sample_file->id_hash = NULL;

	/*
	 * Now check if there are any existing log files that can be used for
	 * logging.
	 */
	ews_get_latest_log_files(startup_ts, session_file, query_file,
							 sample_file);

	/*
	 * Found an existing active session file, populate a sessionid hash table
	 * from the file to avoid duplicate entries logged in session log file.
	 */
	if (session_file->fp)
		ews_populate_session_hash(session_file);
	else
	{
		/*
		 * Did not find any existing session file, we need to create a new
		 * one. For that we set the session_file_rotation_time per the
		 * rotation age and append that time as end_ts to session log file
		 * name. The next session file will be created after
		 * session_file_rotation_time is hit.
		 */
		session_file->rotation_time = TimestampTzPlusMilliseconds(startup_ts,
																  SESSION_LOG_ROTATION_AGE);
		create_log_file(startup_ts, session_file,
						EDB_WAIT_STATES_SESSIONS_FILE_PREFIX);
	}

	/*
	 * Similar to session file, if found an active query file build a queryid
	 * hash table from it.
	 */
	if (query_file->fp)
		ews_populate_query_hash(query_file);
	else
	{
		/*
		 * No existing query file was found, create a new one. For details
		 * refer comment for session file creation.
		 */
		query_file->rotation_time = TimestampTzPlusMilliseconds(startup_ts,
																QUERY_LOG_ROTATION_AGE);
		create_log_file(startup_ts, query_file,
						EDB_WAIT_STATES_QUERIES_FILE_PREFIX);
	}

	/*
	 * If an existing sample log file is being used, make sure that the last
	 * sample record was completely written, if not then move the file pointer
	 * at the end of the last completely written record.
	 */
	if (sample_file->fp)
	{
		long		filesize;
		long		incomplete_record_size;
		bool		result = true;

		if (fseek(sample_file->fp, 0, SEEK_END) != 0)
			result = false;

		if (result && ((filesize = ftell(sample_file->fp)) == -1))
			result = false;

		if (result)
		{
			incomplete_record_size = filesize % sizeof(EDBWaitStatesSample);
			if (fseek(sample_file->fp, filesize - incomplete_record_size,
					  SEEK_SET) != 0)
				result = false;
		}

		if (!result)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("can not open sample file \"%s\" for appending logs: %m",
								   sample_file->file_name)));
	}
	else
	{
		/*
		 * No existing sample file was found, create a new one. For details
		 * refer comment for session file creation.
		 */
		sample_file->rotation_time = TimestampTzPlusMilliseconds(startup_ts,
																 SAMPLE_LOG_ROTATION_AGE);
		create_log_file(startup_ts, sample_file,
						EDB_WAIT_STATES_SAMPLES_FILE_PREFIX);
	}
}

/*
 * create_log_file
 *
 * Creates a log file with name per format <file_prefix>-<start_ts>-<end_ts>,
 * sets the appropriate file pointer fp, and name in log_file.
 */
static void
create_log_file(TimestampTz start_ts, EdbWaitStatesLogFile *log_file,
				char *file_prefix)
{
	snprintf(log_file->file_name, MAXPGPATH, "%s/%s" INT64_FORMAT "_"
			 INT64_FORMAT, edb_wait_states_directory, file_prefix, start_ts,
			 log_file->rotation_time);

	if (log_file->fp)
		fclose(log_file->fp);

	log_file->fp = fopen(log_file->file_name, "a");
	if (log_file->fp == NULL)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("can not open file \"%s\" for writing edb_wait_states logs: %m",
							   log_file->file_name)));
}

/*
 * ews_get_latest_log_files
 *
 * For all type of log files, for each of them finds if there exists any log
 * file such that its next rotation timestamp is > startup_log_ts. If such
 * files exists, opens the files in append mode, sets their respective file
 * pointers <file_type>_file->fp to the opened log file, sets their
 * <file_type>_file->rotation_time correctly to rotation time per rotation ages
 * and copies file name in <file_type>_file->file_name for error reporting. If
 * no such file exist for any of the log file type, then it's pointer
 * <file_type>_file->fp is set to NULL.
 */
static void
ews_get_latest_log_files(TimestampTz startup_log_ts,
						 EdbWaitStatesLogFile *session_file,
						 EdbWaitStatesLogFile *query_file,
						 EdbWaitStatesLogFile *sample_file)
{
	TimestampTz latest_sample_file_start_ts;
	TimestampTz latest_session_file_start_ts;
	TimestampTz latest_query_file_start_ts;
	DIR		   *log_dir;
	struct dirent *log_dir_ent;
	char		latest_sample_file[MAXPGPATH];
	char		latest_session_file[MAXPGPATH];
	char		latest_query_file[MAXPGPATH];

	/* Initialize the start timestamp for all type of log files to. */
	TIMESTAMP_NOBEGIN(latest_sample_file_start_ts);
	TIMESTAMP_NOBEGIN(latest_session_file_start_ts);
	TIMESTAMP_NOBEGIN(latest_query_file_start_ts);

	log_dir = AllocateDir(get_edb_wait_states_directory());

	/*
	 * Iterate over the log directory and find the log files with the latest
	 * start_ts for each type of log file.
	 */
	while ((log_dir_ent = ReadDir(log_dir,
								  get_edb_wait_states_directory())) != NULL)
	{
		TimestampTz file_start_ts;
		TimestampTz file_end_ts;

		/* Ignore current and previous directories. */
		if (strcmp(log_dir_ent->d_name, ".") == 0 ||
			strcmp (log_dir_ent->d_name, "..") == 0)
			continue;

		/*
		 * Ignore the deleted files, refer comments in
		 * delete_edb_wait_states_files().
		 */
#ifdef WIN32
		if (strstr(log_dir_ent->d_name, ".deleted") != NULL)
			continue;
#endif

		/*
		 * Check which type of log file it is, and for respective log file
		 * check if it's start_ts is later than already detected
		 * latest_<file_type>_file_start_ts. If the current log_dir_ent is
		 * latest then update the latest directory entry i.e.
		 * latest_<file_type>_ent.
		 */
		if (is_edb_wait_states_file(log_dir_ent->d_name,
									EDB_WAIT_STATES_SAMPLES_FILE_PREFIX,
									&file_start_ts, &file_end_ts))
		{
			if (timestamptz_cmp_internal(latest_sample_file_start_ts,
										 file_start_ts) < 0)
			{
				latest_sample_file_start_ts = file_start_ts;
				strcpy(latest_sample_file, log_dir_ent->d_name);
			}
		}
		else if (is_edb_wait_states_file(log_dir_ent->d_name,
										 EDB_WAIT_STATES_SESSIONS_FILE_PREFIX,
										 &file_start_ts, &file_end_ts))
		{
			if (timestamptz_cmp_internal(latest_session_file_start_ts,
										 file_start_ts) < 0)
			{
				latest_session_file_start_ts = file_start_ts;
				strcpy(latest_session_file, log_dir_ent->d_name);
			}
		}
		else if (is_edb_wait_states_file(log_dir_ent->d_name,
										 EDB_WAIT_STATES_QUERIES_FILE_PREFIX,
										 &file_start_ts, &file_end_ts))
		{
			if (timestamptz_cmp_internal(latest_query_file_start_ts,
										 file_start_ts) < 0)
			{
				latest_query_file_start_ts = file_start_ts;
				strcpy(latest_query_file, log_dir_ent->d_name);
			}
		}
		else
			/* The entry is not a edb_wait_states log file, ignore this entry. */
			continue;
	}

	/*
	 * Check if the latest files found are still usable for writing logs per
	 * their respective rotation ages and startup_log_ts, if yes then open
	 * them for appending logs.
	 */
	ews_open_latest_log_file(query_file, latest_query_file,
							 startup_log_ts, latest_query_file_start_ts,
							 QUERY_LOG_ROTATION_AGE);

	ews_open_latest_log_file(session_file, latest_session_file,
							 startup_log_ts, latest_session_file_start_ts,
							 SESSION_LOG_ROTATION_AGE);

	ews_open_latest_log_file(sample_file, latest_sample_file,
							 startup_log_ts, latest_sample_file_start_ts,
							 SAMPLE_LOG_ROTATION_AGE);

	FreeDir(log_dir);
}

/*
 * ews_open_latest_log_file
 *
 * Check if given file pointer by directory entry latest_ent has it's rotation
 * time per it's file_start_ts and rotation_age is later than the
 * startup_log_ts. If yes, then this file can be used for appending the logs.
 * Open this file in append mode, assign the file pointer to log_file->fp and
 * update log_file->rotation_time; else set log_file->fp to NULL.
 */
static void
ews_open_latest_log_file(EdbWaitStatesLogFile *log_file,
						 char *file_name,
						 TimestampTz startup_log_ts,
						 TimestampTz file_start_ts,
						 TimestampTz rotation_age)
{
	TimestampTz file_rotation_time;

	Assert(log_file != NULL);

	/* Calculate the rotation time for the given log file. */
	file_rotation_time =
		TimestampTzPlusMilliseconds(file_start_ts, rotation_age);

	/*
	 * If rotation time of the given log file i.e. file_rotation_time is later
	 * than the startup_log_ts, then we can use this file to append logs.
	 */
	if (timestamptz_cmp_internal(file_rotation_time, startup_log_ts) > 0)
	{
		/* Get the path of the log file. */
		snprintf(log_file->file_name, MAXPGPATH, "%s/%s",
				 get_edb_wait_states_directory(), file_name);

		/*
		 * We want to position the file pointer at the end of the last valid
		 * record in the log file for writing the new logs. If the file is
		 * opened in 'a+' mode then the bytes can be written only at the end
		 * of the file, hence we open the file in 'r+' mode.
		 */
		log_file->fp = fopen(log_file->file_name, "r+");
		if (log_file->fp == NULL)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("can not open file \"%s\" for appending logs: %m",
								   log_file->file_name)));

		/* Set the rotation time correctly. */
		log_file->rotation_time = file_rotation_time;
	}
	else
		log_file->fp = NULL;
}

/*
 * ews_rotate_files
 *
 * Checks if it is the time for rotation of log files, rotates the log files
 * if needed, destroys old query/session hash tables if the file is rotated
 * and sets the next rotation time per rotation age.
 */
static TimestampTz
ews_rotate_files(EdbWaitStatesLogFile *session_file,
				 EdbWaitStatesLogFile *query_file,
				 EdbWaitStatesLogFile *sample_file)
{
	TimestampTz current_time = GetCurrentTimestamp();

	/* Is it time to rotate sampling file? */
	if (timestamptz_cmp_internal(sample_file->rotation_time, current_time) <= 0)
	{
		sample_file->rotation_time = TimestampTzPlusMilliseconds(sample_file->rotation_time,
																 SAMPLE_LOG_ROTATION_AGE);

		create_log_file(current_time, sample_file,
						EDB_WAIT_STATES_SAMPLES_FILE_PREFIX);

		current_time = GetCurrentTimestamp();
	}

	/* Is it time to rotate session file? */
	if (timestamptz_cmp_internal(session_file->rotation_time, current_time) <= 0)
	{
		/*
		 * We want to log the sessionIds that are unique across the session
		 * log file, to maintain the logged session ids create a hash table.
		 * As we are going to have a new session log file destroy the old hash
		 * table and start fresh.
		 */
		hash_destroy(session_file->id_hash);
		session_file->id_hash = ews_create_hash(SESSION_ID_HASH, sizeof(int),
												sizeof(int));

		session_file->rotation_time = TimestampTzPlusMilliseconds(session_file->rotation_time,
																  SESSION_LOG_ROTATION_AGE);

		create_log_file(current_time, session_file,
						EDB_WAIT_STATES_SESSIONS_FILE_PREFIX);

		current_time = GetCurrentTimestamp();
	}

	/* Is it time to rotate query file? */
	if (timestamptz_cmp_internal(query_file->rotation_time, current_time) <= 0)
	{
		/*
		 * For the same reasons explained above for sessionId hash, create new
		 * hash for query ids.
		 */
		hash_destroy(query_file->id_hash);
		query_file->id_hash = ews_create_hash(QUERY_ID_HASH, sizeof(uint64),
											  sizeof(uint64));

		query_file->rotation_time = TimestampTzPlusMilliseconds(query_file->rotation_time,
																QUERY_LOG_ROTATION_AGE);

		create_log_file(current_time, query_file,
						EDB_WAIT_STATES_QUERIES_FILE_PREFIX);

		current_time = GetCurrentTimestamp();
	}

	return current_time;
}

/*
 * ews_create_hash
 *
 * A generic function to create hash tables for queryId or sessionId. Returns
 * a hash table.
 */
static HTAB *
ews_create_hash(char *table_name, size_t keysize,
				size_t entrysize)
{
#define NUM_HASH_ELEMENTS 1024
	HASHCTL		ctl;

	ctl.keysize = keysize;
	ctl.entrysize = entrysize;

	return hash_create(table_name, NUM_HASH_ELEMENTS, &ctl,
					   HASH_ELEM | HASH_BLOBS);
}

/*
 * ews_populate_query_hash
 *
 * This function reads the query_file and builds initial query_file->id_hash
 * table using the queryid field in the file. Note that this function should be
 * called only during the start of the background worker in case we have
 * found an existing query log file which can still be used for logging further
 * queries. Building this hash is important to avoid duplicate entries.
 */
static void
ews_populate_query_hash(EdbWaitStatesLogFile *query_file)
{
	/* Read the file till the end. */
	while (true)
	{
		uint64		query_id;
		bool		found;
		int			query_len;
		int			read_next_record;

		read_next_record = ews_read_next_query_record(query_file->fp, &query_id,
													  &query_len, NULL);

		/*
		 * If there was an error reading the query file, it means the query
		 * file might be corrupted, log a warning.
		 */
		if (read_next_record == -1)
			ereport(WARNING,
					(errmsg("query log file \"%s\" is truncated",
							query_file->file_name),
					 errdetail("A corrupted record is found, file truncated to the last valid record.")));

		/* No more records to be read from the query file. */
		if (read_next_record != 1)
			break;

		/* Enter the query_id in hash table, ignore other fields. */
		hash_search(query_file->id_hash, &query_id, HASH_ENTER, &found);
	}
}

/*
 * ews_populate_session_hash
 *
 * Similar to ews_populate_query_hash() this function reads session_file and builds
 * initial session_file->id_hash table using sessionid field in the file. Refer
 * to ews_populate_query_hash() prologue for details.
 */
static void
ews_populate_session_hash(EdbWaitStatesLogFile *session_file)
{
	/* Read the file till the end. */
	while (true)
	{
		int32		sessionid;
		bool		found;
		int			read_next_record;

		read_next_record = ews_read_next_session_record(session_file->fp, &sessionid,
														NULL, NULL);

		/*
		 * If there was an error reading the session file, it means the
		 * session file might be corrupted, log a warning.
		 */
		if (read_next_record == -1)
			ereport(WARNING,
					(errmsg("session log file \"%s\" is truncated",
							session_file->file_name),
					 errdetail("A corrupted record is found, file truncated to the last valid record.")));

		/* No more records to be read from the session file. */
		if (read_next_record != 1)
			break;

		/* Enter the sessionid in hash table. */
		hash_search(session_file->id_hash, &sessionid, HASH_ENTER, &found);
	}
}
