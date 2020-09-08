/*-------------------------------------------------------------------------
 *
 * pg_compression.h
 *	  definition of the "trigger" system catalog (pg_compression)
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_compression.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_COMPRESSION_H
#define PG_COMPRESSION_H

#include "catalog/genbki.h"
#include "catalog/pg_compression_d.h"

/* ----------------
 *		pg_compression definition.  cpp turns this into
 *		typedef struct FormData_pg_compression
 * ----------------
 */
CATALOG(pg_compression,5555,CompressionRelationId)
{
	Oid			oid;			/* oid */
	NameData	cmname;			/* compression method name */
	regproc 	cmhandler BKI_LOOKUP(pg_proc); /* handler function */
} FormData_pg_compression;

/* ----------------
 *		Form_pg_compression corresponds to a pointer to a tuple with
 *		the format of pg_compression relation.
 * ----------------
 */
typedef FormData_pg_compression *Form_pg_compression;

#endif							/* PG_COMPRESSION_H */
