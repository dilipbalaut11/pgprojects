/*-------------------------------------------------------------------------
 *
 * pg_iceberg_copy.c
 *		PostgreSQL extension to copy data blocks, WAL, and SLRU to disk
 *		in Apache Iceberg table format.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/pg_iceberg_copy/pg_iceberg_copy.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include <time.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

/* GUC variables */
static char *pg_iceberg_copy_dir = NULL;
static int	pg_iceberg_copy_naptime = 10;
static bool	pg_iceberg_copy_enabled = true;

/* Status tracking */
static TimestampTz last_export_timestamp = 0;
static int64 total_files_exported = 0;
static int64 total_bytes_written = 0;

/* Function declarations */
void		_PG_init(void);
PGDLLEXPORT void pg_iceberg_copy_worker_main(Datum main_arg);

PG_FUNCTION_INFO_V1(pg_iceberg_copy_run);
PG_FUNCTION_INFO_V1(pg_iceberg_copy_status);

static void create_dir_if_not_exists(const char *path);
static void perform_iceberg_export(const char *base_dir, int64 *files_out, int64 *bytes_out);
static void copy_data_blocks(const char *base_dir, int64 *files_out, int64 *bytes_out);
static void copy_wal_segments(const char *base_dir, int64 *files_out, int64 *bytes_out);
static void copy_slru_segments(const char *base_dir, int64 *files_out, int64 *bytes_out);
static void write_iceberg_metadata(const char *base_dir, int64 total_files, int64 total_bytes);

/*
 * Create directory recursively if it doesn't already exist.
 */
static void
create_dir_if_not_exists(const char *path)
{
	struct stat st;

	if (stat(path, &st) == 0)
	{
		if (S_ISDIR(st.st_mode))
			return;
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("path \"%s\" exists but is not a directory", path)));
	}

	if (mkdir(path, 0700) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m", path)));
}

/*
 * Copy heap data blocks for user tables in the current database.
 */
static void
copy_data_blocks(const char *base_dir, int64 *files_out, int64 *bytes_out)
{
	char		datablocks_dir[MAXPGPATH];
	int			ret;

	snprintf(datablocks_dir, sizeof(datablocks_dir), "%s/data/datablocks", base_dir);
	create_dir_if_not_exists(datablocks_dir);

	ret = SPI_execute("SELECT c.oid, c.relname FROM pg_class c "
					  "JOIN pg_namespace n ON n.oid = c.relnamespace "
					  "WHERE c.relkind IN ('r', 'm') "
					  "AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast');",
					  true, 0);

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		uint64		i;

		for (i = 0; i < SPI_processed; i++)
		{
			Oid			reloid;
			bool		isnull;
			Relation	rel;
			BlockNumber nblocks;
			BlockNumber blk;

			reloid = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i],
													SPI_tuptable->tupdesc,
													1, &isnull));
			if (isnull)
				continue;

			rel = table_open(reloid, AccessShareLock);
			nblocks = RelationGetNumberOfBlocks(rel);

			for (blk = 0; blk < nblocks; blk++)
			{
				Buffer		buf;
				Page		page;
				char		block_path[MAXPGPATH];
				FILE	   *fp;

				buf = ReadBufferExtended(rel, MAIN_FORKNUM, blk, RBM_NORMAL, NULL);
				LockBuffer(buf, BUFFER_LOCK_SHARE);
				page = BufferGetPage(buf);

				snprintf(block_path, sizeof(block_path),
						 "%s/db_%u_rel_%u_blk_%u.bin",
						 datablocks_dir, MyDatabaseId, reloid, blk);

				fp = fopen(block_path, "wb");
				if (fp)
				{
					size_t written = fwrite(page, 1, BLCKSZ, fp);
					fclose(fp);

					(*files_out)++;
					(*bytes_out) += written;
				}

				UnlockReleaseBuffer(buf);
			}

			table_close(rel, AccessShareLock);
		}
	}
}

/*
 * Copy current WAL segment.
 */
static void
copy_wal_segments(const char *base_dir, int64 *files_out, int64 *bytes_out)
{
	char		wal_dir[MAXPGPATH];
	XLogRecPtr	current_lsn;
	XLogSegNo	segno;
	char		wal_filename[MAXFNAMELEN];
	char		src_wal_path[MAXPGPATH];
	char		dst_wal_path[MAXPGPATH];
	FILE	   *src;
	FILE	   *dst;

	snprintf(wal_dir, sizeof(wal_dir), "%s/data/wal", base_dir);
	create_dir_if_not_exists(wal_dir);

	current_lsn = GetFlushRecPtr(NULL);
	XLByteToSeg(current_lsn, segno, wal_segment_size);
	XLogFileName(wal_filename, 1, segno, wal_segment_size);

	snprintf(src_wal_path, sizeof(src_wal_path), "pg_wal/%s", wal_filename);
	snprintf(dst_wal_path, sizeof(dst_wal_path), "%s/wal_%s.bin", wal_dir, wal_filename);

	src = fopen(src_wal_path, "rb");
	if (src)
	{
		dst = fopen(dst_wal_path, "wb");
		if (dst)
		{
			char	buffer[8192];
			size_t	bytes_read;

			while ((bytes_read = fread(buffer, 1, sizeof(buffer), src)) > 0)
			{
				fwrite(buffer, 1, bytes_read, dst);
				(*bytes_out) += bytes_read;
			}
			fclose(dst);
			(*files_out)++;
		}
		fclose(src);
	}
}

/*
 * Copy SLRU segments from pg_xact directory.
 */
static void
copy_slru_segments(const char *base_dir, int64 *files_out, int64 *bytes_out)
{
	char		slru_dir[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;

	snprintf(slru_dir, sizeof(slru_dir), "%s/data/slru", base_dir);
	create_dir_if_not_exists(slru_dir);

	dir = opendir("pg_xact");
	if (dir == NULL)
		return;

	while ((de = readdir(dir)) != NULL)
	{
		char	src_path[MAXPGPATH];
		char	dst_path[MAXPGPATH];
		FILE   *src;
		FILE   *dst;

		if (de->d_name[0] == '.')
			continue;

		snprintf(src_path, sizeof(src_path), "pg_xact/%s", de->d_name);
		snprintf(dst_path, sizeof(dst_path), "%s/pg_xact_%s.bin", slru_dir, de->d_name);

		src = fopen(src_path, "rb");
		if (src)
		{
			dst = fopen(dst_path, "wb");
			if (dst)
			{
				char	buffer[8192];
				size_t	bytes_read;

				while ((bytes_read = fread(buffer, 1, sizeof(buffer), src)) > 0)
				{
					fwrite(buffer, 1, bytes_read, dst);
					(*bytes_out) += bytes_read;
				}
				fclose(dst);
				(*files_out)++;
			}
			fclose(src);
		}
	}
	closedir(dir);
}

/*
 * Write Apache Iceberg table format metadata JSON & manifests.
 */
static void
write_iceberg_metadata(const char *base_dir, int64 total_files, int64 total_bytes)
{
	char		metadata_dir[MAXPGPATH];
	char		v1_json_path[MAXPGPATH];
	char		manifest_list_path[MAXPGPATH];
	char		manifest_file_path[MAXPGPATH];
	FILE	   *fp;
	TimestampTz	now = GetCurrentTimestamp();
	int64		epoch_ms = (now - SetEpochTimestamp()) / 1000;

	snprintf(metadata_dir, sizeof(metadata_dir), "%s/metadata", base_dir);
	create_dir_if_not_exists(metadata_dir);

	/* 1. Write v1.metadata.json */
	snprintf(v1_json_path, sizeof(v1_json_path), "%s/v1.metadata.json", metadata_dir);
	fp = fopen(v1_json_path, "w");
	if (fp)
	{
		fprintf(fp, "{\n"
				"  \"format-version\": 2,\n"
				"  \"table-uuid\": \"e85a21bc-94ef-4c91-a12b-postgresql0001\",\n"
				"  \"location\": \"%s\",\n"
				"  \"last-updated-ms\": %ld,\n"
				"  \"last-column-id\": 6,\n"
				"  \"current-schema-id\": 0,\n"
				"  \"schemas\": [\n"
				"    {\n"
				"      \"type\": \"struct\",\n"
				"      \"schema-id\": 0,\n"
				"      \"fields\": [\n"
				"        { \"id\": 1, \"name\": \"db_oid\", \"required\": true, \"type\": \"long\" },\n"
				"        { \"id\": 2, \"name\": \"rel_oid\", \"required\": true, \"type\": \"long\" },\n"
				"        { \"id\": 3, \"name\": \"block_no\", \"required\": true, \"type\": \"long\" },\n"
				"        { \"id\": 4, \"name\": \"type\", \"required\": true, \"type\": \"string\" },\n"
				"        { \"id\": 5, \"name\": \"lsn\", \"required\": true, \"type\": \"long\" },\n"
				"        { \"id\": 6, \"name\": \"file_path\", \"required\": true, \"type\": \"string\" }\n"
				"      ]\n"
				"    }\n"
				"  ],\n"
				"  \"default-spec-id\": 0,\n"
				"  \"partition-specs\": [ { \"spec-id\": 0, \"fields\": [] } ],\n"
				"  \"current-snapshot-id\": 1,\n"
				"  \"snapshots\": [\n"
				"    {\n"
				"      \"snapshot-id\": 1,\n"
				"      \"timestamp-ms\": %ld,\n"
				"      \"manifest-list\": \"metadata/snap-1.manifest_list.json\",\n"
				"      \"summary\": {\n"
				"        \"operation\": \"append\",\n"
				"        \"total-data-files\": \"%ld\",\n"
				"        \"total-bytes\": \"%ld\"\n"
				"      }\n"
				"    }\n"
				"  ]\n"
				"}\n",
				base_dir, (long) epoch_ms, (long) epoch_ms, (long) total_files, (long) total_bytes);
		fclose(fp);
	}

	/* 2. Write snap-1.manifest_list.json */
	snprintf(manifest_list_path, sizeof(manifest_list_path), "%s/snap-1.manifest_list.json", metadata_dir);
	fp = fopen(manifest_list_path, "w");
	if (fp)
	{
		fprintf(fp, "{\n"
				"  \"manifests\": [\n"
				"    {\n"
				"      \"manifest-path\": \"metadata/manifest-1.json\",\n"
				"      \"partition-spec-id\": 0,\n"
				"      \"added-snapshot-id\": 1,\n"
				"      \"added-data-files-count\": %ld\n"
				"    }\n"
				"  ]\n"
				"}\n", (long) total_files);
		fclose(fp);
	}

	/* 3. Write manifest-1.json */
	snprintf(manifest_file_path, sizeof(manifest_file_path), "%s/manifest-1.json", metadata_dir);
	fp = fopen(manifest_file_path, "w");
	if (fp)
	{
		fprintf(fp, "{\n"
				"  \"manifest-format\": 2,\n"
				"  \"schema-id\": 0,\n"
				"  \"status\": \"ADDED\",\n"
				"  \"total-files\": %ld,\n"
				"  \"total-bytes\": %ld\n"
				"}\n", (long) total_files, (long) total_bytes);
		fclose(fp);
	}
}

/*
 * Main export controller.
 */
static void
perform_iceberg_export(const char *base_dir, int64 *files_out, int64 *bytes_out)
{
	int64	files = 0;
	int64	bytes = 0;
	bool	started_tx = false;

	create_dir_if_not_exists(base_dir);

	if (!IsTransactionState())
	{
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		started_tx = true;
	}

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	copy_data_blocks(base_dir, &files, &bytes);
	copy_wal_segments(base_dir, &files, &bytes);
	copy_slru_segments(base_dir, &files, &bytes);
	write_iceberg_metadata(base_dir, files, bytes);

	PopActiveSnapshot();
	SPI_finish();

	if (started_tx)
		CommitTransactionCommand();

	last_export_timestamp = GetCurrentTimestamp();
	total_files_exported = files;
	total_bytes_written = bytes;

	if (files_out)
		*files_out = files;
	if (bytes_out)
		*bytes_out = bytes;
}

/*
 * Background worker main entry point.
 */
void
pg_iceberg_copy_worker_main(Datum main_arg)
{
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	while (!ShutdownRequestPending)
	{
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 pg_iceberg_copy_naptime * 1000L,
						 PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (pg_iceberg_copy_enabled && pg_iceberg_copy_dir && pg_iceberg_copy_dir[0] != '\0')
		{
			PG_TRY();
			{
				perform_iceberg_export(pg_iceberg_copy_dir, NULL, NULL);
			}
			PG_CATCH();
			{
				EmitErrorReport();
				FlushErrorState();
			}
			PG_END_TRY();
		}
	}
}

/*
 * Extension initialization.
 */
void
_PG_init(void)
{
	BackgroundWorker worker = {0};

	DefineCustomStringVariable("pg_iceberg_copy.export_dir",
							   "Target directory for Iceberg export.",
							   NULL,
							   &pg_iceberg_copy_dir,
							   "iceberg_export",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomIntVariable("pg_iceberg_copy.naptime",
							"Interval in seconds between Iceberg exports.",
							NULL,
							&pg_iceberg_copy_naptime,
							10,
							1,
							3600,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_iceberg_copy.enabled",
							 "Enable or disable automatic background Iceberg export.",
							 NULL,
							 &pg_iceberg_copy_enabled,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	MarkGUCPrefixReserved("pg_iceberg_copy");

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 10;
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_iceberg_copy background worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "pg_iceberg_copy background worker");
	sprintf(worker.bgw_library_name, "pg_iceberg_copy");
	sprintf(worker.bgw_function_name, "pg_iceberg_copy_worker_main");
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);
}

/*
 * SQL function to trigger export on demand.
 */
Datum
pg_iceberg_copy_run(PG_FUNCTION_ARGS)
{
	char	   *target_dir;
	int64		files = 0;
	int64		bytes = 0;
	char		result_str[256];

	if (PG_NARGS() > 0 && !PG_ARGISNULL(0))
		target_dir = text_to_cstring(PG_GETARG_TEXT_PP(0));
	else
		target_dir = pg_iceberg_copy_dir;

	perform_iceberg_export(target_dir, &files, &bytes);

	snprintf(result_str, sizeof(result_str),
			 "Successfully exported to Iceberg format at '%s': %ld files, %ld bytes",
			 target_dir, (long) files, (long) bytes);

	PG_RETURN_TEXT_P(cstring_to_text(result_str));
}

/*
 * SQL function to get export status.
 */
Datum
pg_iceberg_copy_status(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[6];
	bool		nulls[6];
	HeapTuple	tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context that cannot accept type record")));

	memset(nulls, false, sizeof(nulls));

	values[0] = Int32GetDatum(MyProcPid);
	values[1] = BoolGetDatum(pg_iceberg_copy_enabled);
	values[2] = CStringGetTextDatum(pg_iceberg_copy_dir ? pg_iceberg_copy_dir : "");

	if (last_export_timestamp == 0)
		nulls[3] = true;
	else
		values[3] = TimestampTzGetDatum(last_export_timestamp);

	values[4] = Int64GetDatum(total_files_exported);
	values[5] = Int64GetDatum(total_bytes_written);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
