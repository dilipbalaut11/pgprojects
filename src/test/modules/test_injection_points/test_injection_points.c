/*--------------------------------------------------------------------------
 *
 * test_injection_points.c
 *		Code for testing injection points.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/test/modules/test_injection_points/test_injection_points.c
 *
 * Injection points are able to trigger user-defined callbacks in pre-defined
 * code paths.
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/injection_point.h"

PG_MODULE_MAGIC;

extern PGDLLEXPORT void test_injection_error(const char *name);
extern PGDLLEXPORT void test_injection_notice(const char *name);

/* Set of callbacks available at point creation */
void
test_injection_error(const char *name)
{
	elog(ERROR, "error triggered for injection point %s", name);
}

void
test_injection_notice(const char *name)
{
	elog(NOTICE, "notice triggered for injection point %s", name);
}

/*
 * SQL function for creating an injection point.
 */
PG_FUNCTION_INFO_V1(test_injection_points_attach);
Datum
test_injection_points_attach(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *mode = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	   *function;

	if (strcmp(mode, "error") == 0)
		function = "test_injection_error";
	else if (strcmp(mode, "notice") == 0)
		function = "test_injection_notice";
	else
		elog(ERROR, "incorrect mode \"%s\" for injection point creation", mode);

	InjectionPointAttach(name, "test_injection_points", function);

	PG_RETURN_VOID();
}

/*
 * SQL function for triggering an injection point.
 */
PG_FUNCTION_INFO_V1(test_injection_points_run);
Datum
test_injection_points_run(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	INJECTION_POINT(name);

	PG_RETURN_VOID();
}

/*
 * SQL function for dropping an injection point.
 */
PG_FUNCTION_INFO_V1(test_injection_points_detach);
Datum
test_injection_points_detach(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	InjectionPointDetach(name);

	PG_RETURN_VOID();
}
