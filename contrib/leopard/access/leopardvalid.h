/*-------------------------------------------------------------------------
 *
 * valid.h
 *	  POSTGRES tuple qualification validity definitions.
 *
 *
 * Portions Copyright (c) 2022, EnterpriseDB Corporation. All Rights Reserved.
 *
 *
 *
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/valid.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LEOPARD_VALID_H
#define LEOPARD_VALID_H

/*
 *		LeopardKeyTest
 *
 *		Test a leopard tuple to see if it satisfies a scan key.
 */
#define LeopardKeyTest(tuple, \
					tupdesc, \
					nkeys, \
					keys, \
					result) \
do \
{ \
	/* Use underscores to protect the variables passed in as parameters */ \
	int			__cur_nkeys = (nkeys); \
	ScanKey		__cur_keys = (keys); \
 \
	(result) = true; /* may change */ \
	for (; __cur_nkeys--; __cur_keys++) \
	{ \
		Datum	__atp; \
		bool	__isnull; \
		Datum	__test; \
 \
		if (__cur_keys->sk_flags & SK_ISNULL) \
		{ \
			(result) = false; \
			break; \
		} \
 \
		__atp = leopard_getattr((tuple), \
							 __cur_keys->sk_attno, \
							 (tupdesc), \
							 &__isnull); \
 \
		if (__isnull) \
		{ \
			(result) = false; \
			break; \
		} \
 \
		__test = FunctionCall2Coll(&__cur_keys->sk_func, \
								   __cur_keys->sk_collation, \
								   __atp, __cur_keys->sk_argument); \
 \
		if (!DatumGetBool(__test)) \
		{ \
			(result) = false; \
			break; \
		} \
	} \
} while (0)

#endif							/* LEOPARD_VALID_H */
