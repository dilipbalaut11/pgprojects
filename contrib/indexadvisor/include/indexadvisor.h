/*-------------------------------------------------------------------------
 *
 * generate_advise.h: 
 *
 *
 *-------------------------------------------------------------------------
*/
#ifndef _GENERATE_ADVISE_H_
#define _GENERATE_ADVISE_H_

extern planner_hook_type prev_planner_hook;
extern bool advisor_disable_stats;
extern PlannedStmt *advisor_planner(Query *parse,
#if PG_VERSION_NUM >= 130000
					 const char *query_string,
#endif
				 int cursorOptions,
				 ParamListInfo boundParams);
#endif
