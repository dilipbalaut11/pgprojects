/*-------------------------------------------------------------------------
 *
 * cmapi.h
 *	  API for Postgres compression methods.
 *
 * Copyright (c) 2015-2017, PostgreSQL Global Development Group
 *
 * src/include/access/cmapi.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CMAPI_H
#define CMAPI_H

#include "postgres.h"
#include "catalog/pg_am.h"
#include "catalog/pg_attribute.h"
#include "nodes/pg_list.h"

/* Built-in compression methods */
#define PGLZ_COMPRESSION			'p'
#define ZLIB_COMPRESSION			'z'
#define InvalidCompressionMethod	'i'

/*
 * Built-in compression method-id.  This is direct index into the built-in
 * array.
 */
typedef enum PGCompressionID
{
	PGLZ_COMPRESSION_ID,
	ZLIB_COMPRESSION_ID
} PGCompressionID;

#define MAX_BUILTIN_COMPRESSION_METHOD 2

#define DefaultCompressionMethod PGLZ_COMPRESSION
#define IsValidCompression(cm)  ((cm) != InvalidCompressionMethod)

typedef struct CompressionRoutine CompressionRoutine;

typedef struct varlena *(*cmcompress_function) (const struct varlena *value);
typedef struct varlena *(*cmdecompress_slice_function)
						(const struct varlena *value, int32 slicelength);

/*
 * API struct for a compression routine.
 *
 * 'cmcompress' and 'cmdecompress' - varlena compression functions.
 */
struct CompressionRoutine
{
	char	cmname[64];
	cmcompress_function cmcompress;
	cmcompress_function cmdecompress;
	cmdecompress_slice_function cmdecompress_slice;
};

char GetCompressionMethod(Form_pg_attribute att, char *compression);
char GetCompressionMethodFromCMID(PGCompressionID cmid);
PGCompressionID GetCompressionMethodID(char cm_method);
char *GetCompressionName(char cm_method);
CompressionRoutine *GetCompressionRoutine(PGCompressionID cmid);

/*
 * buit-in compression method handler function declaration
 * XXX we can add header files for pglz and zlib and move these
 * declarations to specific header files.
 */
struct varlena *pglz_cmcompress(const struct varlena *value);
struct varlena *pglz_cmdecompress(const struct varlena *value);
struct varlena *pglz_cmdecompress_slice(const struct varlena *value,
						int32 slicelength);
struct varlena *zlib_cmcompress(const struct varlena *value);
struct varlena *zlib_cmdecompress(const struct varlena *value);
#endif							/* CMAPI_H */
