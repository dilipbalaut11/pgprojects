/*-------------------------------------------------------------------------
 *
 * edb_wait_states.h
 * 			Common data structures and macros to be shared between the
 * 			edb_wait_states background worker and the udf reader.
 *
 * Copyright (c) 2018, EnterpriseDB Corporation. All Rights Reserved.
 *
 * IDENTIFICATION
 *	  contrib/edb_wait_states/edb_wait_states.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EDB_WAIT_STATES_H
#define EDB_WAIT_STATES_H

#include "postgres.h"

/* File name prefixes for various edb_wait_states log files. */
#define EDB_WAIT_STATES_SAMPLES_FILE_PREFIX "edb_ws_samples_"
#define EDB_WAIT_STATES_QUERIES_FILE_PREFIX "edb_ws_queries_"
#define EDB_WAIT_STATES_SESSIONS_FILE_PREFIX "edb_ws_sessions_"
#define EDB_WAIT_STATES_HEADER_FILE_PREFIX "edb_ws_header_"

#define EDB_WAIT_STATES_DEFAULT_DIRECTORY "edb_wait_states"

/*
 * Structure to hold one edb_wait_states sample data.
 *
 * Note: Do not change the order of the structure members. The 8-byte and
 * 4-byte members are clustered together so as we do not end-up having
 * additional padding bytes resulting in larger size of structure run-time.
 */
typedef struct EDBWaitStatesSample
{
	uint64		query_id;		/* internally generated query identifier */
	TimestampTz sample_ts;		/* timestamp when this sample was collected */
	uint32		wait_event_id;	/* backend's wait information when the sample
								 * was collected */
	int32		session_id;		/* backend process id on which the query was
								 * running */
	int32		sample_interval; /* sampling interval when this sample was taken */

	TimestampTz query_start_ts; /* timestamp when the query began it's
								 * execution */
} EDBWaitStatesSample;

typedef struct EDBWaitStatesHeader
{
	char hostname[256];
	char dbuptime[64];
	char cpuinfo[512];
	char meminfo[512];
	char dbinfo[512];
} EDBWaitStatesHeader;


extern char *get_edb_wait_states_directory(void);
extern void delete_edb_wait_states_files(TimestampTz start_ts,
							 TimestampTz end_ts);
extern int ews_read_next_query_record(FILE *query_file, uint64 *query_id,
						   int *query_len, char *query_buf);
extern int ews_read_next_session_record(FILE *session_file, int32 *session_id,
							 char *db_name, char *user_name);
extern bool is_edb_wait_states_file(const char *filename, const char *prefix,
						TimestampTz *file_start_ts, TimestampTz *file_end_ts);
#endif
