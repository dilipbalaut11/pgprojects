/*-------------------------------------------------------------------------
 *
 * pg_attr_compression.h
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_attr_compression.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ATTR_COMPRESSION_H
#define PG_ATTR_COMPRESSION_H

#include "catalog/genbki.h"
#include "catalog/pg_attr_compression_d.h"

/* ----------------
 *		pg_attr_compression definition.  cpp turns this into
 *		typedef struct FormData_pg_attr_compression
 * ----------------
 */
CATALOG(pg_attr_compression,5555,AttrCompressionRelationId)
{
	Oid			acoid;						/* attribute compression oid */
	char		acmethod;					/* compression method */
	Oid			acrelid BKI_DEFAULT(0);		/* attribute relation */
	int16		acattnum BKI_DEFAULT(0);	/* attribute number in the relation */
} FormData_pg_attr_compression;

/* ----------------
 *		Form_pg_attr_compresssion corresponds to a pointer to a tuple with
 *		the format of pg_attr_compression relation.
 * ----------------
 */
typedef FormData_pg_attr_compression *Form_pg_attr_compression;


#endif							/* PG_ATTR_COMPRESSION_H */
