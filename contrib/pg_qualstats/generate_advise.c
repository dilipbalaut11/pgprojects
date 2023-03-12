/*-------------------------------------------------------------------------
 *
 * generate_advise.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "utils/builtins.h"

#include "include/hypopg.h"
#include "include/hypopg_index.h"
#include "include/pg_qualstats.h"
#include "include/generate_advise.h"

/*---- Function declarations ----*/
extern PGDLLEXPORT Datum index_advisor_get_advise(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(index_advisor_get_advise);

#define DEBUG_INDEX_ADVISOR

/*
 * If 'pgqs_cost_track_enable' is set then the planner will start tracking the
 * cost of queries being planned and store plan cost in 'pgqs_plan_cost'.
 */
bool pgqs_cost_track_enable = false;
static float pgqs_plan_cost = 0.0;
planner_hook_type prev_planner_hook = NULL;

/*
 * Store information for a index combination.
 */
typedef struct IndexCombination
{
	double	cost;		/* total cost of queries for the index combination */
	int		nindices;	/* number of indexes in this combination */
	double	overhead;	/* total overhead of this combination */
	int		indices[FLEXIBLE_ARRAY_MEMBER];	/* array of indexes in
	    									   IndexCandidate array */
} IndexCombination;

/* Index candidate information. */
typedef struct IndexCandidate
{
	Oid		relid;			/* relation id */
	Oid		amoid;			/* candidate's index access method oid */
	char   *amname;			/* candidate's index access method name */
	int		nattrs;			/* number of key attributes */
	int	   *attnum;			/* key attribute number array */
	int		nqueryids;		/* number of queryids related to this index */
	int64  *queryids;		/* queryids array */
	int64	nupdates;		/* nupdates done on key attributes involved in this
							   candididate */
	int		nupdatefreq;	/* frequency of the updates on candidate keys */
	double	benefit;		/* total cost benefit gained by this candidate */
	double	overhead;		/* overhead of the candidate */
	char   *indexstmt;		/* sql query for creating index for this candidate */
	bool	isvalid;		/* is candidate still valid (not rejected)*/
	bool	isselected;		/* is candidate already selected in final list */
} IndexCandidate;

/*
 * Store information for a query, i.e. querystring, its base cost and execution
 * frequency.
 */
typedef struct QueryInfo
{
	double	cost;
	int		frequency;
	char   *query;
} QueryInfo;

/*
 * Index advisor context, store various intermediate information while
 * evaluating value of candidate indexes and finding out the best combination.
 */
typedef struct IndexAdvisorContext
{
	int		nqueries;			/* total number of queries */
	int		ncandidates;		/* total numbed of index candidates */
	int		maxcand;
	double		  **benefitmat;	/* index to query benefit matrix (n x m)*/
	IndexCandidate *candidates;	/* array IndexCandidate for all candidates */
	QueryInfo	   *queryinfos;	/* array of QueryInfo for all queries */
	Bitmapset	   *memberattr;	
	MemoryContext	queryctx;	/* reference to per query context */
} IndexCombContext;

/* store index advises for SRF call */
typedef struct IndexAdvises
{
	int		next;
	int		count;
	char  **index_array;
} IndexAdvises;

/*
 * query qual stats from pg_qualstats and group them by based on the relationid
 * index amname and qualid.
 *
 * XXX instead of doing union of 2 simmilar queries is there any other way to
 * output two independent rows for lrelid and rrelid.
 *
 * TODO: We should consider BRIN index only on very large tables
 *
 * TODO: We should consider GIN/GIST indexes, for that we need to create
 * support for them in hypopg.
 */
char *query =
"WITH pgqs AS ("
"\n          (SELECT dbid, min(am.oid) amoid, amname, qualid, qualnodeid,"
"\n            (lrelid, lattnum,"
"\n            opno, eval_type)::qual AS qual, queryid,"
"\n            round(avg(execution_count)) AS execution_count,"
"\n            sum(occurences) AS occurences,"
"\n            round(sum(nbfiltered)::numeric / sum(occurences)) AS avg_filter,"
"\n            CASE WHEN sum(execution_count) = 0"
"\n              THEN 0"
"\n              ELSE round(sum(nbfiltered::numeric) / sum(execution_count) * 100)"
"\n            END AS avg_selectivity"
"\n          FROM pg_qualstats() q"
"\n          JOIN pg_catalog.pg_database d ON q.dbid = d.oid"
"\n          JOIN pg_catalog.pg_operator op ON op.oid = q.opno"
"\n          JOIN pg_catalog.pg_amop amop ON amop.amopopr = op.oid"
"\n          JOIN pg_catalog.pg_am am ON am.oid = amop.amopmethod"
"\n          WHERE d.datname = current_database()"
"\n          AND eval_type = 'f'"
"\n			 AND lrelid IS NOT NULL"
"\n          AND lrelid != 0"
"\n          GROUP BY dbid, amname, qualid, qualnodeid, lrelid,"
"\n            lattnum, rattnum, opno, eval_type, queryid ORDER BY lattnum)"
"\n			UNION ALL "
"\n          (SELECT dbid, min(am.oid) amoid, amname, qualid, qualnodeid,"
"\n            (rrelid, rattnum,"
"\n            opno, eval_type)::qual AS qual, queryid,"
"\n            round(avg(execution_count)) AS execution_count,"
"\n            sum(occurences) AS occurences,"
"\n            round(sum(nbfiltered)::numeric / sum(occurences)) AS avg_filter,"
"\n            CASE WHEN sum(execution_count) = 0"
"\n              THEN 0"
"\n              ELSE round(sum(nbfiltered::numeric) / sum(execution_count) * 100)"
"\n            END AS avg_selectivity"
"\n          FROM pg_qualstats() q"
"\n          JOIN pg_catalog.pg_database d ON q.dbid = d.oid"
"\n          JOIN pg_catalog.pg_operator op ON op.oid = q.opno"
"\n          JOIN pg_catalog.pg_amop amop ON amop.amopopr = op.oid"
"\n          JOIN pg_catalog.pg_am am ON am.oid = amop.amopmethod"
"\n          WHERE d.datname = current_database()"
"\n          AND eval_type = 'f'"
"\n          AND rrelid != 0"
"\n			 AND rrelid IS NOT NULL"
"\n          GROUP BY dbid, amname, qualid, qualnodeid, rrelid,"
"\n            lattnum, rattnum, opno, eval_type, queryid ORDER BY rattnum)"
"\n        ),"
"\n        -- apply cardinality and selectivity restrictions"
"\n        filtered AS ("
"\n          SELECT (qual).relid, min(amoid) amoid, amname, coalesce(qualid, qualnodeid) AS parent,"
"\n            count(*) AS weight,"
"\n            array_agg(DISTINCT((qual).attnum) ORDER BY ((qual).attnum)) AS attnumlist,"
"\n            array_agg(qualnodeid) AS qualidlist,"
"\n            array_agg(DISTINCT(queryid)) AS queryidlist"
"\n          FROM pgqs"
"\n          GROUP BY (qual).relid, amname, parent"
"\n        )"
"\nSELECT * FROM filtered where amname='btree' OR amname='brin' ORDER BY relid, amname DESC, cardinality(attnumlist);";

/* static function declarations */
static IndexAdvises *index_advisor_generate_advise(MemoryContext per_query_ctx,
												   bool exhaustive);
static char **index_advisor_advise_one_rel(char **prevarray,
											 IndexCandidate *candidates,
											 int ncandidates, int *nindexes,
											 MemoryContext per_query_ctx,
											 bool iterative);
static QueryInfo *index_advisor_get_queries(IndexCandidate *candidates,
											int ncandidates, int *nqueries);
static bool pg_qulstat_is_queryid_exists(int64 *queryids, int nqueryids,
										 int64 queryid, int *idx);
static char *index_advisor_get_query(int64 queryid, int *freq);
static IndexCandidate *index_advisor_get_index_combination(IndexCandidate *candidates,
								   						   int *ncandidates);
static IndexCandidate *index_advisor_add_candidate_if_not_exists(
										IndexCandidate *candidates,
										IndexCandidate *cand,
										int *ncandidates, int *nmaxcand);
static void index_advisor_get_updates(IndexCandidate *candidates,
									  int ncandidates);

static bool index_advisor_generate_index_queries(IndexCandidate *candidates,
												 int ncandidates);
static void index_advisor_set_basecost(QueryInfo *queryinfos, int nqueries);
static void index_advisor_plan_query(const char *query);
static void index_advisor_compute_index_benefit(IndexCombContext *context,
												int nqueries, int *queryidxs);
static double index_advisor_get_index_overhead(IndexCandidate *cand, Oid idxid);
static int index_advisor_get_best_candidate(IndexCombContext *context);
static IndexCombination * index_advisor_exhaustive(IndexCombContext *context);

/* planner hook */
PlannedStmt *
pgqs_planner(Query *parse, const char *query_string,
			 int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result;

	/* Invoke the planner, possibly via a previous hook user */
	if (prev_planner_hook)
		result = prev_planner_hook(parse, query_string, cursorOptions,
								   boundParams);
	else
		result = standard_planner(parse, query_string, cursorOptions,
								  boundParams);

	/* If enabled, delay by taking and releasing the specified lock */
	if (pgqs_cost_track_enable)
	{
		pgqs_plan_cost = result->planTree->total_cost;
	}

	return result;
}

/* Location of permanent stats file (valid when database is shut down) */
#define PGQS_DUMP_FILE	"pg_qualstats.stat"
/* Magic number identifying the stats file format */
static const uint32 PGQS_FILE_HEADER = 0x20220408;

/* PostgreSQL major version number, changes in which invalidate all entries */
static const uint32 PGQS_PG_MAJOR_VERSION = PG_VERSION_NUM / 100;

#ifdef DEBUG_INDEX_ADVISOR

static void
print_candidates(IndexCandidate *candidates, int ncandidates)
{
	int			i;

	for (i = 0; i < ncandidates; i++)
	{
		elog(NOTICE, "Index: %s: update: %lld freq: %d valid:%d", candidates[i].indexstmt,
			 (long long int) candidates[i].nupdates, candidates[i].nupdatefreq, candidates[i].isvalid);
	}
}

static void
print_benefit_matrix(IndexCombContext *context)
{
	double		  **benefitmat = context->benefitmat;
	int 	ncandidates = context->ncandidates;
	int		i;
	StringInfoData	row;

	initStringInfo(&row);

	appendStringInfo(&row, "======Benefit Matrix Start=======\n");
	for (i = 0; i < ncandidates; i++)
	{
		int	j;

		for (j = 0; j < context->nqueries; j++)
			appendStringInfo(&row, "%f\t", benefitmat[j][i]);
		appendStringInfo(&row, "\n");
	}
	appendStringInfo(&row, "======Benefit Matrix End=======\n");
	elog(NOTICE, "%s", row.data);
	pfree(row.data);
}
#endif

/* 
 * Main index advisor function,  this will generate the advise based on the
 * predicate and workload stats collected so far.
 */
static IndexAdvises *
index_advisor_generate_advise(MemoryContext per_query_ctx, bool exhaustive)
{
	int			ret;
	int			i;
	int			ncandidates = 0;
	int			nrelcand = 0;
	int			nindexes = 0;
	int			idxcand = 0;
	Oid			prevrelid = InvalidOid;
	char	  **index_array;
	TupleDesc	tupdesc;
	IndexAdvises   *advise;	
	IndexCandidate *candidates;
	MemoryContext 	oldcontext;

	if ((ret = SPI_connect()) < 0)
		elog(ERROR, "pg_qualstat: SPI_connect returned %d", ret);

	/*
	 * Execute query to get list of all quals grouped by relation id and
	 * index amname.
	 */
	ret = SPI_execute(query, true, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
	}

	tupdesc = SPI_tuptable->tupdesc;
	ncandidates = SPI_processed;
	candidates = palloc0(sizeof(IndexCandidate) * ncandidates);

	/* 
	 * Read all the entires and prepare a index candidate array for further
	 * processing.
	 */
	for (i = 0; i < ncandidates; i++)
	{
		HeapTuple		tup = SPI_tuptable->vals[i];
		Datum			dat;
		bool			isnull;
		ArrayType	   *r;
		IndexCandidate *cand;

		cand = &(candidates[i]);

		dat = SPI_getbinval(tup, tupdesc, 1, &isnull);
		cand->relid = DatumGetObjectId(dat);

		dat = SPI_getbinval(tup, tupdesc, 2, &isnull);
		cand->amoid = DatumGetObjectId(dat);

		cand->amname = pstrdup(SPI_getvalue(tup, tupdesc, 3));

		dat = SPI_getbinval(tup, tupdesc, 6, &isnull);
		r = DatumGetArrayTypePCopy(dat);

		cand->attnum = (int *) ARR_DATA_PTR(r);
		cand->nattrs = ARR_DIMS(r)[0];

		dat = SPI_getbinval(tup, tupdesc, 8, &isnull);
		r = DatumGetArrayTypePCopy(dat);

		cand->queryids = (int64 *) ARR_DATA_PTR(r);
		cand->nqueryids = ARR_DIMS(r)[0];

		cand->isvalid = true;
		cand->isselected = false;
	}

	/* process candidates for rel by rel and generate index advises. */
	for (i = 0; i < ncandidates; i++)
	{
		if (OidIsValid(prevrelid) && prevrelid != candidates[i].relid)
		{
			index_array = index_advisor_advise_one_rel(index_array,
													   &candidates[idxcand],
													   nrelcand, &nindexes,
													   per_query_ctx,
													   !exhaustive);
			nrelcand = 0;
			idxcand = i;
		}
		prevrelid = candidates[i].relid;
		nrelcand++;
	}

	/* process the last relation */
	index_array = index_advisor_advise_one_rel(index_array,
											   &candidates[idxcand],
											   nrelcand, &nindexes,
											   per_query_ctx,
											   !exhaustive);

	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	advise = (IndexAdvises *) palloc(sizeof(IndexAdvises));
	MemoryContextSwitchTo(oldcontext);

	SPI_finish();

	advise->count = nindexes;
	advise->next = 0;
	advise->index_array = index_array;

	return advise;
}

/*
 * Index advisor entry function, this will returns the index advises in form
 * of create index queries.
 */
Datum
index_advisor_get_advise(PG_FUNCTION_ARGS)
{
	char	  **index_array;
	IndexAdvises   *advise;	
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		bool exhaustive = PG_GETARG_BOOL(0);
		MemoryContext per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;

		funcctx = SRF_FIRSTCALL_INIT();

		/* generate index advises and save it for subsequent calls */
		funcctx->user_fctx = index_advisor_generate_advise(per_query_ctx,
														   exhaustive);
	}
	funcctx = SRF_PERCALL_SETUP();
	advise = (IndexAdvises *) funcctx->user_fctx;

	index_array = advise->index_array;
	if (advise->next < advise->count)
		SRF_RETURN_NEXT(funcctx, CStringGetTextDatum(index_array[advise->next++]));

	SRF_RETURN_DONE(funcctx);
}

/*
 * Iterative approach for finding the best set of indexes.
 */
static IndexCombination *
index_advisor_iterative(IndexCombContext *context)
{
	IndexCombination   *comb;
	int			i;
	int			ncandidates = context->ncandidates;
	int			nqueries = context->nqueries;
	int		   *queryidxs = NULL;
	QueryInfo  *queryinfos = context->queryinfos;
	IndexCandidate *candidates = context->candidates;

	/* allocate memory for benefit matrix */
	context->benefitmat = (double **) palloc0(nqueries * sizeof(double *));
	for (i = 0; i < nqueries; i++)
		context->benefitmat[i] = (double *) palloc0(ncandidates * sizeof(double));

	comb = palloc0(sizeof(IndexCombination) + ncandidates * sizeof(int));

	/*
	 * The main iterative algorithm for selecting the best candidate indexes.
	 *
	 * Step1: At first plan all the queries without creating any new index and
	 * set that as a base cost for each query.
	 *
	 * Step2: Now replan all the queries with each index and prepare a
	 * index-query benefit matrix.  Each element in this matrix will represent
	 * the cost benefit for a given query if that particular index exists.
	 *
	 * Step3: In this step we will compute the total benefit for each index
	 * i.e. (Sum of each query benefit * query execution frequency).
	 *
	 * Step4: Shortlist the index which is giving the maximum benefit and
	 * assume that this index is now selected.  So update the base cost of each
	 * query which got benefitted with this index.
	 *
	 * Step5: Go to step2 and repeat the process to select the next best
	 * candidate.  This time instead of replanning all the queries, only replan
	 * the queries which got benefitted by the previous best candidate. And
	 * also note that in this round the previously selected candidate are out
	 * of the selection process.
	 */
	index_advisor_set_basecost(queryinfos, nqueries);
	while (true)
	{
		int		bestcand;
		int		i;

		/*
		 * Create each index one at a time and replan every query and fill
		 * index-query benefit matrix.
		 */
		index_advisor_compute_index_benefit(context, nqueries, queryidxs);

#ifdef DEBUG_INDEX_ADVISOR
		print_benefit_matrix(context);
#endif

		/*
		 * Compute the overall benefit of all the candidate and get the  best
		 * candidate.
		 */
		bestcand = index_advisor_get_best_candidate(context);
		if (bestcand == -1)
			break;

		/*
		 * Add best candidates to the path and create the hypoindex for this
		 * candidate and reiterate for the next round.  For this index we
		 * create hypoindex and do not drop so that next round assume this
		 * index is already exist now and check benefit of each candidate by
		 * assuming this candidate is already finalized.
		 */
		hypo_create_index(candidates[bestcand].indexstmt, NULL);

		/*
		 * Allocate the memory to remember the query indexes which got
		 * benefitted by this index so that while selecting the next best
		 * candidate we can only plan these queries because benefit matrix for
		 * other queries should not be impacted.
		 */
		if (queryidxs == NULL)
			queryidxs = (int *) palloc (nqueries * sizeof (int));

		nqueries = 0;

		/* 
		 * Next best candidate is seleted so update the query base cost as if this
		 * index exists before going for next iteration.
		 */
		for (i = 0; i < context->nqueries; i++)
		{
			if (context->benefitmat[i][bestcand] > 0)
			{
				queryinfos[i].cost -= context->benefitmat[i][bestcand];

				/* 
				 * Remember the queries which got benefited by this index and
				 * in the next round only plan these queries.
				 */
				queryidxs[nqueries++] = i;
			}
		}

		/* Add candidate to the selected index combination. */
		comb->indices[comb->nindices++] = bestcand;
		candidates[bestcand].isselected = true;
	}

	hypo_index_reset();

	return comb;
}

/*
 * Index advisor function for one relation, this will process all the
 * input index candidates.  Generate all possible single coulmn and two columns
 * indexes and check their value and finally return the list of indexes which
 * can get the best value.
 */
static char **
index_advisor_advise_one_rel(char **prevarray, IndexCandidate *candidates,
							   int ncandidates, int *nindexes,
							   MemoryContext per_query_ctx, bool iterative)
{
	char	  **index_array = NULL;
	QueryInfo  *queryinfos;
	int			nqueries;
	int			i;
	int			prev_indexes = *nindexes;
	IndexCandidate *finalcand;
	IndexCombination   *comb = NULL;
	IndexCombContext		context;
	MemoryContext oldcontext;

#ifdef DEBUG_INDEX_ADVISOR
	elog(NOTICE, "candidate Relation %d", candidates[0].relid);
#endif

	/*
	 * Process all candidate and get the list of all the unique queryids and
	 * also the actual queries.
	 */
	queryinfos = index_advisor_get_queries(candidates, ncandidates, &nqueries);

	/*
	 * Process the candidate and from those genrate all possible distinct one
	 * and two length index candidates.
	 */
	finalcand = index_advisor_get_index_combination(candidates, &ncandidates);

	/*
	 * Process all the candidates and get the total update count we are
	 * performing on key attributes for those candidate indexes.
	 */
	index_advisor_get_updates(finalcand, ncandidates);

	/*
	 * Generate index creation statement for each candidate.  We need to output
	 * the index statements so store in per query context.
	 */
	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	if (!index_advisor_generate_index_queries(finalcand, ncandidates))
		return prevarray;

	MemoryContextSwitchTo(oldcontext);
#ifdef DEBUG_INDEX_ADVISOR
	print_candidates(finalcand, ncandidates);
#endif

	/* prepare context for index advisor processing */
	context.candidates = finalcand;
	context.ncandidates = ncandidates;
	context.queryinfos = queryinfos;
	context.nqueries = nqueries;
	context.maxcand = ncandidates;
	context.memberattr = NULL;
	context.queryctx = per_query_ctx;

	/*
	 * Process index candidate with iterative approach and find out the list
	 * of indexes which can produce least cost for the given set of workload
	 * queries.
	 */
	if (iterative)
		comb = index_advisor_iterative(&context);
	else
		comb = index_advisor_exhaustive(&context);

	if (comb == NULL || comb->nindices == 0)
		return prevarray;

	prev_indexes = *nindexes;
	(*nindexes) += comb->nindices;

	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	if (prev_indexes == 0)
		index_array = (char **) palloc(comb->nindices * sizeof(char *));
	else
		index_array = (char ** ) repalloc(prevarray, (*nindexes) * sizeof(char *));
	MemoryContextSwitchTo(oldcontext);

	/* construct and return array. */
	for (i = 0; i < comb->nindices; i++)
		index_array[prev_indexes + i] = finalcand[comb->indices[i]].indexstmt;

	return index_array;
}

/*
 * Process all candidate and get the list of all the unique queryids from the
 * queryids stored in each candidate and also fetch the actual query from the
 * workload hash and store in query info.
 */
static QueryInfo *
index_advisor_get_queries(IndexCandidate *candidates, int ncandidates,
						  int *nqueries)
{
	int			i;
	int			j;
	int			nids = 0;
	int			maxids = ncandidates;
	int64	   *queryids;
	QueryInfo  *queryinfos;

	queryids = palloc(maxids * sizeof(int64));

	/*
	 * Process through the queryids stored in each candidate and prepare a
	 * array of all distinct queryids. 
	 */
	for (i = 0; i < ncandidates; i++)
	{
		IndexCandidate *cand = &candidates[i];

		for (j = 0; j < cand->nqueryids ; j++)
		{
			int index = -1;

			if (pg_qulstat_is_queryid_exists(queryids, nids, cand->queryids[j],
											 &index))
				continue;

			if (nids >= maxids)
			{
				maxids *= 2;
				queryids = repalloc(queryids, maxids * sizeof(int64));
			}
			queryids[nids] = cand->queryids[j];
			nids++;
		}
	}

	queryinfos = (QueryInfo *) palloc(nids * sizeof(QueryInfo));

	/* get the actual query and execution frequency for each queryid */
	for (i = 0; i < nids; i++)
	{
		queryinfos[i].query = index_advisor_get_query(queryids[i],
													 &queryinfos[i].frequency);
#ifdef DEBUG_INDEX_ADVISOR
		elog(NOTICE, "query %d: %s-freq:%d", i, queryinfos[i].query, queryinfos[i].frequency);
#endif
	}

	pfree(queryids);

	*nqueries = nids;

	return queryinfos;
}

static bool
pg_qulstat_is_queryid_exists(int64 *queryids, int nqueryids, int64 queryid,
							 int *idx)
{
	int			i;

	for (i = 0; i < nqueryids ; i++)
	{
		if (queryids[i] == queryid)
		{
			*idx = i;
			return true;
		}
	}

	return false;
}

static char *
index_advisor_get_query(int64 queryid, int *freq)
{
	pgqsQueryStringEntry   *entry;
	pgqsQueryStringHashKey	queryKey;
	char   *query = NULL;
	bool	found;

	queryKey.queryid = queryid;

	LWLockAcquire(pgqs->querylock, LW_SHARED);
	entry = (pgqsQueryStringEntry *) dshash_find(pgqs_query_dshash,
												 &queryKey, false);									
	if (entry == NULL)
	{
		LWLockRelease(pgqs->querylock);
		return NULL;
	}

	if (entry->isExplain)
	{
		query = palloc0(entry->qrylen);
		strcpy(query, entry->querytext);
	}
	else
	{
		int		explainlen = strlen("EXPLAIN ");

		query = palloc0(explainlen + entry->qrylen);
		strncpy(query, "EXPLAIN ", explainlen);
		strcpy(query + explainlen, entry->querytext);
	}

	*freq = entry->frequency;
	dshash_release_lock(pgqs_query_dshash, entry);

	LWLockRelease(pgqs->querylock);

	return query;
}

/*
 * From given index candidate list generate single column and two columns
 * index candidate array.
 */
static IndexCandidate *
index_advisor_get_index_combination(IndexCandidate *candidates,
								   int *ncandidates)
{
	IndexCandidate *finalcand;
	IndexCandidate	cand;
	int				nfinalcand = 0;
	int				nmaxcand = *ncandidates;
	int				i;
	int				j;
	int				k = 0;

	finalcand = palloc(sizeof(IndexCandidate) * nmaxcand);

	/* genrate all one and tow length index combinations. */
	for (i = 0; i < *ncandidates; i++)
	{
		for (j = 0; j < candidates[i].nattrs; j++)
		{
			/* generae one column index */
			memcpy(&cand, &candidates[i], sizeof(IndexCandidate));
			cand.nattrs = 1;
			cand.attnum = (int *) palloc0(sizeof(int));
			cand.attnum[0] = candidates[i].attnum[j];

			finalcand = index_advisor_add_candidate_if_not_exists(finalcand,
																 &cand,
																 &nfinalcand,
																 &nmaxcand);

			/* generate two column indexes. */
			for (k = 0; k < candidates[i].nattrs; k++)
			{
				if (k == j)
					continue;

				cand.nattrs = 2;
				cand.attnum = (int *) palloc0(sizeof(int) * 2);

				cand.attnum[0] = candidates[i].attnum[j];
				cand.attnum[1] = candidates[i].attnum[k];

				/*
				 * TODO: for brin index order of column doesn't matter so if
				 * the index with all column same as new candidate exists then
				 * we can consider this as duplicate (no need to check strict
				 * column order)
				 */
				finalcand = index_advisor_add_candidate_if_not_exists(finalcand,
												&cand, &nfinalcand, &nmaxcand);
			}
		}
	}

	*ncandidates = nfinalcand;

	return finalcand;
}

/*
 * Add 'cand' index candidate to the input 'candidates' array if its not
 * already present in the array and return the update array.
 */
static IndexCandidate *
index_advisor_add_candidate_if_not_exists(IndexCandidate *candidates,
										 IndexCandidate *cand,
										 int *ncandidates, int *nmaxcand)
{
	IndexCandidate *finalcand = candidates;
	int		i;
	int		j;
	bool	found = false;

	for (i = 0; i < *ncandidates; i++)
	{
		IndexCandidate *oldcand = &candidates[i];

		if (oldcand->nattrs != cand->nattrs)
			continue;
		if (oldcand->amoid != cand->amoid)
			continue;

		for (j = 0; j < oldcand->nattrs; j++)
		{
			if (oldcand->attnum[j] != cand->attnum[j])
				break;
		}

		if (j == oldcand->nattrs)
			found = true;
	}

	if (!found)
	{
		if (*ncandidates == *nmaxcand)
		{
			(*nmaxcand) *= 2;
			finalcand = repalloc(finalcand,
								 sizeof(IndexCandidate) * (*nmaxcand));
		}
		memcpy(&finalcand[*ncandidates], cand, sizeof(IndexCandidate));
		(*ncandidates)++;
	}

	return finalcand;
}

/*
 * Process each index candidate and compute the number of updated tuple for
 * each index candidates based on the index column update counts.
 */
static void
index_advisor_get_updates(IndexCandidate *candidates, int ncandidates)
{
	dshash_seq_status	hash_seq;
	int64	   *qrueryid_done;
	int64		nupdates = 0;
	int			nupdatefreq = 0;
	int			nqueryiddone = 0;
	int			maxqueryids = 50;
	int			i;

	qrueryid_done = palloc(sizeof(int64) * maxqueryids);

	LWLockAcquire(pgqs->querylock, LW_SHARED);

	for (i = 0; i < ncandidates; i++)
	{
		pgqsUpdateHashEntry	   *entry;
		IndexCandidate		   *cand = &candidates[i];

		dshash_seq_init(&hash_seq, pgqs_update_dshash, false);

		while ((entry = dshash_seq_next(&hash_seq)) != NULL)
		{
			int			i;

			if (entry->key.dbid != MyDatabaseId)
				continue;
			if (entry->key.relid != cand->relid)
				continue;
			for (i = 0; i < cand->nattrs; i++)
			{
				if (entry->key.attnum == cand->attnum[i])
					break;
			}

			if (i == cand->nattrs)
				continue;

			for (i = 0; i < nqueryiddone; i++)
			{
				if (entry->key.queryid == qrueryid_done[i])
					break;
			}
			if (i < nqueryiddone)
				continue;

			if (nqueryiddone == maxqueryids)
			{
				maxqueryids *= 2;
				qrueryid_done = repalloc(qrueryid_done, sizeof(int64) * maxqueryids);
			}
			qrueryid_done[nqueryiddone++] = entry->key.queryid;

			/* average update per query. */
			nupdates += entry->updated_rows;
			nupdatefreq += entry->frequency;
		}

		dshash_seq_term(&hash_seq);

		if (nupdates > 0)
		{
			cand->nupdates = nupdates / nupdatefreq;
			cand->nupdatefreq = nupdatefreq;
		}

		nupdates = 0;
		nupdatefreq = 0;
		nqueryiddone = 0;
	}

	LWLockRelease(pgqs->querylock);
}

/*
 * Generate index creation statement for each candidate and store in the index
 * candidate structure for later use.
 */
static bool
index_advisor_generate_index_queries(IndexCandidate *candidates, int ncandidates)
{
	int		i;
	int		j;
	Relation	relation;
	StringInfoData buf;

	relation = RelationIdGetRelation(candidates[0].relid);
	if (relation == NULL)
		return false;

	initStringInfo(&buf);

	for (i = 0; i < ncandidates; i++)
	{
		IndexCandidate *cand = &candidates[i];

		appendStringInfo(&buf, "CREATE INDEX ON %s USING %s (",
						 relation->rd_rel->relname.data,
						 cand->amname);

		for (j = 0; j < cand->nattrs; j++)
		{
			if (j > 0)
				appendStringInfo(&buf, ",");
			appendStringInfo(&buf, "%s", relation->rd_att->attrs[cand->attnum[j] - 1].attname.data);
		}

		appendStringInfo(&buf, ");");
		cand->indexstmt = palloc(buf.len + 1);
		strcpy(cand->indexstmt, buf.data);
		resetStringInfo(&buf);
	}
	RelationClose(relation);
	return true;
}

/*
 * Plan all given queries without any new index and fill base cost for each
 * query.
 */
static void
index_advisor_set_basecost(QueryInfo *queryinfos, int nqueries)
{
	int		i;

	/* enable cost tracking before planning queries */
	pgqs_cost_track_enable = true;

	/* 
	 * plan each query and get its cost
	 */
	for (i = 0; i < nqueries; i++)
	{
		index_advisor_plan_query(queryinfos[i].query);
		queryinfos[i].cost = pgqs_plan_cost;
	}

	/* disable cost tracking */
	pgqs_cost_track_enable = false;
}

/* Plan a given query */
static void
index_advisor_plan_query(const char *query)
{
	StringInfoData	explainquery;

	if (query == NULL)
		return;

	/*
	 * Enable hypo index injection so that we can see the cost with the
	 * hypothetical indexes we have created.
	 */
	hypo_is_enabled = true;

	initStringInfo(&explainquery);
	appendStringInfoString(&explainquery, query);
	SPI_execute(query, false, 0);

	/* diable hypo index injection */
	hypo_is_enabled = false;
}

/*
 * Plan given set of input queries with each valid and non-selected indexes and
 * fill the index-query benefit matrix.
 */
static void
index_advisor_compute_index_benefit(IndexCombContext *context,
									int nqueries, int *queryidxs)
{
	IndexCandidate *candidates = context->candidates;
	QueryInfo  *queryinfos = context->queryinfos;
	double	  **benefit = context->benefitmat;
	int 		ncandidates = context->ncandidates;
	int			i;

	pgqs_cost_track_enable = true;

	for (i = 0; i < ncandidates; i++)
	{
		IndexCandidate *cand = &candidates[i];
		Oid			idxid;
		BlockNumber	relpages;
		int		j;

		/* 
		 * If candiate is already identified as it has no value or it is
		 * seletced in the final combination then in the recheck its benefits.
		 */
		if (!cand->isvalid || cand->isselected)
			continue;

		idxid = hypo_create_index(cand->indexstmt, &relpages);

		/* If candidate overhead is not yet computed then do it now */
		if (cand->overhead == 0)
			cand->overhead = index_advisor_get_index_overhead(cand, relpages);

		/* 
		 * Replan each query and compute the total weighted cost by multiplying
		 * each query cost with its frequency.
		 */
		for (j = 0; j < nqueries; j++)
		{
			int		index = (queryidxs != NULL) ? queryidxs[j] : j;

			index_advisor_plan_query(queryinfos[index].query);

			/*
			 * If the index reduces the cost at least by 5% and cost with index
			 * including index overhead is lesser than cost without index, then
			 * consider this index useful and update the benefit matrix
			 */
			if ((pgqs_plan_cost < queryinfos[index].cost * 0.95) &&
				(pgqs_plan_cost + cand->overhead < queryinfos[index].cost))
			{
				benefit[index][i] = queryinfos[index].cost - pgqs_plan_cost;
			}
			else
				benefit[index][i] = 0;
		}

		hypo_index_remove(idxid);
	}

	pgqs_cost_track_enable = false;
}

static double
index_advisor_get_index_overhead(IndexCandidate *cand, BlockNumber relpages)
{
	double		T = relpages;
	double		index_pages;
	double		update_io_cost;
	double		update_cpu_cost;
	double		overhead;
	int			navgupdate = cand->nupdates;
	int			nfrequency = cand->nupdatefreq;

	/* 
	 * We are commputing the page acccess cost and tuple cost based on total
	 * accumulated tuple count so we don't need to use update query frequency.
	 */
	index_pages = (2 * T * navgupdate) / (2 * T + navgupdate);
	update_io_cost = (index_pages * nfrequency) * random_page_cost;
	update_cpu_cost = (navgupdate * nfrequency) * cpu_tuple_cost;

	overhead = update_io_cost + update_cpu_cost;

	/* XXX overhead of index based on number of size and number of columns. */
	overhead += T;
	overhead += (cand->nattrs * 1000);

	return overhead;
}

/*
 * Compute total benefit of each candidate and return the index of the
 * candidate which is giving maximum total benefit.
 */
static int
index_advisor_get_best_candidate(IndexCombContext *context)
{
	IndexCandidate *candidates = context->candidates;
	QueryInfo	   *queryinfos = context->queryinfos;
	double		  **benefitmat = context->benefitmat;
	int 	ncandidates = context->ncandidates;
	int		i;
	int		bestcandidx = -1;
	double	max_benefit = 0;
	double	benefit;

	/*
	 * Loop through all index candidate and all queries and compute weighted
	 * sum of benefit for each candidate.
	 * total_benefit = Sum(benefit(Qi) * frequency(Qi)).
	 */
	for (i = 0; i < ncandidates; i++)
	{
		int	j;

		if (!candidates[i].isvalid || candidates[i].isselected)
			continue;

		benefit = 0;

		for (j = 0; j < context->nqueries; j++)
			benefit += (benefitmat[j][i] * queryinfos[j].frequency);

		if (benefit == 0)
		{
			candidates[i].isvalid = false;
			continue;
		}

		if (benefit > max_benefit)
		{
			max_benefit = benefit;
			bestcandidx = i;
		}
	}

	return bestcandidx;
}

#if 1 //exhaustive
static void
index_advisor_bms_add_candattr(IndexCombContext *context, IndexCandidate *cand)
{
	int		i;

	for (i = 0; i < cand->nattrs; i++)
	{
		context->memberattr = bms_add_member(context->memberattr,
											 cand->attnum[i]);
	}
}

static void
index_advisor_bms_remove_candattr(IndexCombContext *context,
								 IndexCandidate *cand)
{
	int		i;

	for (i = 0; i < cand->nattrs; i++)	
	{
		context->memberattr = bms_del_member(context->memberattr,
											 cand->attnum[i]);
	}
}

static bool
index_advisor_is_cand_overlaps(Bitmapset *bms, IndexCandidate *cand)
{
	int		i;

	if (bms == NULL)
		return false;

	for (i = 0; i < cand->nattrs; i++)
	{
		if (bms_is_member(cand->attnum[i], bms))
			return true;
	}
	return false;
}

/*
 * compare and retutn cheaper path and free non selected path
 *
 */
static IndexCombination *
index_advisor_compare_path(IndexCombination *path1,
						  IndexCombination *path2)
{
	double	cost1 = path1->cost + path1->overhead;
	double	cost2 = path2->cost + path2->overhead;

	if (cost1 <= cost2)
	{
		pfree(path2);
		return path1;
	}

	pfree(path1);
	return path2;
}

/*
 * Plan all give queries to compute the total cost.
 */
static double
index_advisor_get_cost(QueryInfo *queryinfos, int nqueries, int *queryidxs)
{
	int		i;
	double	cost = 0.0;

	pgqs_cost_track_enable = true;

	/* 
	 * replan each query and compute the total weighted cost by multiplying
	 * each query cost with its frequency.
	 */
	for (i = 0; i < nqueries; i++)
	{
		int		index = (queryidxs != NULL) ? queryidxs[i] : i;

		index_advisor_plan_query(queryinfos[index].query);
		cost += (pgqs_plan_cost * queryinfos[index].frequency);
	}

	pgqs_cost_track_enable = false;

	return cost;
}

/*
 * Perform a exhaustive search with different index combinations.
 */
static IndexCombination *
index_advisor_compare_comb(IndexCombContext *context, int cur_cand,
						  int selected_cand)
{
	Oid						idxid;
	BlockNumber				pages;
	double					overhead;
	int						max_cand = context->maxcand;
	IndexCombination   *path1;
	IndexCombination   *path2;
	IndexCandidate		   *cand = context->candidates;

	if (cur_cand == max_cand || selected_cand == max_cand)
	{
		path1 = palloc0(sizeof(IndexCombination) +
						context->ncandidates * sizeof(int));

		path1->cost = index_advisor_get_cost(context->queryinfos,
											context->nqueries, NULL);
		path1->nindices = 0;

		return path1;
	}

	/* compute total cost excluding this index */
	path1 = index_advisor_compare_comb(context, cur_cand + 1, selected_cand);

	/*
	 * If any of the attribute of this candidate is overlapping with any
	 * existing candidate in this combindation then don't add this candidate
	 * in the combination.
	 */
	if (!cand[cur_cand].isvalid ||
		index_advisor_is_cand_overlaps(context->memberattr, &cand[cur_cand]))
		return path1;

	index_advisor_bms_add_candattr(context, &cand[cur_cand]);

	/* compare cost with and without this index */
	idxid = hypo_create_index(cand[cur_cand].indexstmt, &pages);

	/* compute total cost including this index */
	path2 = index_advisor_compare_comb(context, cur_cand + 1, selected_cand + 1);
	path2->indices[path2->nindices++] = cur_cand;

	overhead = index_advisor_get_index_overhead(&cand[cur_cand], pages);
	path2->overhead += overhead;

	hypo_index_remove(idxid);
	index_advisor_bms_remove_candattr(context, &cand[cur_cand]);

	return index_advisor_compare_path(path1, path2);
}

/*
 * try missing candidate in selected path with greedy approach
 */
static IndexCombination *
index_advisor_complete_comb(IndexCombContext *context,
						   IndexCombination *path)
{
	int		i;
	int 	ncand = context->ncandidates;	
	IndexCandidate		   *cand = context->candidates;
	IndexCombination   *finalpath = path;

	for (i = 0; i < path->nindices; i++)
	{
		hypo_create_index(cand[path->indices[i]].indexstmt, NULL);
	}

	for (i = 0; i < ncand; i++)
	{
		Oid			idxid;
		int			j;

		if (!cand->isvalid)
			continue;

		for (j = 0; j < finalpath->nindices; j++)
		{
			if (finalpath->indices[j] == i)
				break;
		}
		/* 
		 * if candidate not found in path then try to add in the path and
		 * compare cost
		 */
		if (j == finalpath->nindices)
		{
			IndexCombination *newpath;
			int		size;
			double	overhead;
			BlockNumber		relpages;
			
			size = sizeof(IndexCombination) + ncand * sizeof(int);

			newpath = (IndexCombination *) palloc0(size);
			memcpy(newpath, finalpath, size);
			newpath->nindices += 1;
			idxid = hypo_create_index(cand[i].indexstmt, &relpages);
			overhead = index_advisor_get_index_overhead(&cand[i], relpages);
			newpath->overhead += overhead;
			newpath->cost = index_advisor_get_cost(context->queryinfos,
												  context->nqueries, NULL);
#ifdef DEBUG_INDEX_ADVISOR
			if (newpath->cost > finalpath->cost)
			{
				elog(NOTICE, "cost increased in greedy");
			}
#endif
			finalpath = index_advisor_compare_path(finalpath, newpath);
			/* drop this index if this index is not selected. */
			if (finalpath != newpath)
				hypo_index_remove(idxid);
#ifdef DEBUG_INDEX_ADVISOR
			else
				elog(NOTICE, "path selected in greedy");
#endif				
		}
	}

	/* reset all hypo indexes. */
	hypo_index_reset();

	return finalpath;
}

/*
 * check individiual index usefulness, if not reducing the cost by some margin
 * then mark invalid.
 */
static void
index_advisor_mark_useless_candidate(IndexCombContext *context)
{
	int		i;

	for (i = 0; i < context->ncandidates; i++)
	{
		IndexCandidate *cand = &context->candidates[i];
		double	cost1;
		double	cost2;
		Oid		idxid;

		cost1 = index_advisor_get_cost(context->queryinfos, context->nqueries, NULL);
		idxid = hypo_create_index(cand->indexstmt, NULL);
		cost2 = index_advisor_get_cost(context->queryinfos, context->nqueries, NULL);
		hypo_index_remove(idxid);

		if (cost2 < cost1 - 100)
			cand->isvalid = true;
		else
			cand->isvalid = false;
	}
}

static IndexCombination *
index_advisor_exhaustive(IndexCombContext *context)
{
	IndexCombination   *path;

	index_advisor_mark_useless_candidate(context);
	path = index_advisor_compare_comb(context, 0, 0);

	/* 
	 * If path doesn't include all the candidates then try to add missing
	 * candidates with greedy approach.
	 * TODO TRY iterative for remaining candidates.
	 */
	if (path->nindices < context->ncandidates)
		index_advisor_complete_comb(context, path);

	return path;
}

#endif //exhaustive

#if 0 /* we do not need to survive restart*/
static void pgqs_shmem_shutdown(int code, Datum arg);

static void
pgqs_read_dumpfile(void)
{
	bool		found;
	HASHCTL		info;
	FILE	   *file = NULL;
	FILE	   *qfile = NULL;
	uint32		header;
	int32		num;
	int32		pgver;
	int32		i;
	int			buffer_size;
	char	   *buffer = NULL;

	//TODO: Acquire lock
	/*
	 * If we're in the postmaster (or a standalone backend...), set up a shmem
	 * exit hook to dump the statistics to disk.
	 */
	//if (!IsUnderPostmaster)
		on_shmem_exit(pgqs_shmem_shutdown, (Datum) 0);

	/*
	 * Attempt to load old statistics from the dump file.
	 */
	file = AllocateFile(PGQS_DUMP_FILE, PG_BINARY_R);
	if (file == NULL)
	{
		if (errno != ENOENT)
			goto read_error;
		/* No existing persisted stats file, so we're done */
		return;
	}

	if (fread(&header, sizeof(uint32), 1, file) != 1 ||
		fread(&pgver, sizeof(uint32), 1, file) != 1 ||
		fread(&num, sizeof(int32), 1, file) != 1)
		goto read_error;

//	if (header != PQSS_FILE_HEADER ||
//		pgver != PGQS_PG_MAJOR_VERSION)
//		goto data_error;

	for (i = 0; i < num; i++)
	{
		pgqsEntry	temp;
		pgqsEntry  *entry;
		Size		query_offset;

		if (fread(&temp, sizeof(pgqsEntry), 1, file) != 1)
			goto read_error;

		entry = (pgqsEntry *) hash_search(pgqs_hash,
										  &temp.key,
										  HASH_ENTER, &found);
		if (!found)
			memcpy(entry, &temp, sizeof(pgqsEntry));
	}
	FreeFile(file);

	unlink(PGQS_DUMP_FILE);

	return;

read_error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read file \"%s\": %m",
					PGQS_DUMP_FILE)));
	goto fail;
data_error:
	ereport(LOG,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("ignoring invalid data in file \"%s\"",
					PGQS_DUMP_FILE)));
	goto fail;
fail:
	if (file)
		FreeFile(file);

	/* If possible, throw away the bogus file; ignore any error */
	unlink(PGQS_DUMP_FILE);
}

/*
 * shmem_shutdown hook: Dump statistics into file.
 *
 * Note: we don't bother with acquiring lock, because there should be no
 * other processes running when this is called.
 */
static void
pgqs_shmem_shutdown(int code, Datum arg)
{
	FILE	   *file;
	char	   *qbuffer = NULL;
	Size		qbuffer_size = 0;
	HASH_SEQ_STATUS hash_seq;
	int32		num_entries;
	pgqsEntry  *entry;
	int i=1;

	while(i);
	/* Don't try to dump during a crash. */
	if (code)
		return;

	/* Safety check ... shouldn't get here unless shmem is set up. */
	if (!pgqs || !pgqs_hash)
		return;

	/* Don't dump if told not to. */
//	if (!pgqs_save)
//		return;

	file = AllocateFile(PGQS_DUMP_FILE ".tmp", PG_BINARY_W);
	if (file == NULL)
		goto error;

	if (fwrite(&PGQS_FILE_HEADER, sizeof(uint32), 1, file) != 1)
		goto error;
	if (fwrite(&PGQS_PG_MAJOR_VERSION, sizeof(uint32), 1, file) != 1)
		goto error;
	num_entries = hash_get_num_entries(pgqs_hash);
	if (fwrite(&num_entries, sizeof(int32), 1, file) != 1)
		goto error;

	/*
	 * When serializing to disk, we store query texts immediately after their
	 * entry data.  Any orphaned query texts are thereby excluded.
	 */
	hash_seq_init(&hash_seq, pgqs_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (fwrite(entry, sizeof(pgqsEntry), 1, file) != 1)
		{
			/* note: we assume hash_seq_term won't change errno */
			hash_seq_term(&hash_seq);
			goto error;
		}
	}

	if (FreeFile(file))
	{
		file = NULL;
		goto error;
	}

	/*
	 * Rename file into place, so we atomically replace any old one.
	 */
	(void) durable_rename(PGQS_DUMP_FILE ".tmp", PGQS_DUMP_FILE, LOG);

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not write file \"%s\": %m",
					PGQS_DUMP_FILE ".tmp")));
	free(qbuffer);
	if (file)
		FreeFile(file);
	unlink(PGQS_DUMP_FILE ".tmp");
}

#endif