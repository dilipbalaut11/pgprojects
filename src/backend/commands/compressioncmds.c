/*-------------------------------------------------------------------------
 *
 * compressioncmds.c
 *	  Routines for SQL commands for attribute compression methods
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/compressioncmds.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/compressionapi.h"
#include "commands/defrem.h"


/*
 * CheckCompressionMethodsPreserved - Compare new compression method list with
 * 									  the existing list.
 *
 * Check whether all the previous compression methods are preserved in the new
 * compression methods or not.
 */
static bool
CheckCompressionMethodsPreserved(char *previous_cm, char *new_cm)
{
	int i = 0;

	while (previous_cm[i] != 0)
	{
		if (strchr(new_cm, previous_cm[i]) == NULL)
			return false;
		i++;
	}

	return true;
}

/*
 * GetAttributeCompression - Get compression for given attribute
 */
char *
GetAttributeCompression(Form_pg_attribute att, ColumnCompression *compression,
						bool *need_rewrite)
{
	ListCell   *cell;
	char	   *cm;
	int			preserve_idx = 0;

	cm = palloc0(NAMEDATALEN);

	/* no compression for the plain storage */
	if (att->attstorage == TYPSTORAGE_PLAIN)
		return NULL;

	/* fallback to default compression if it's not specified */
	if (compression == NULL)
	{
		cm[preserve_idx] = DefaultCompressionMethod;
		return cm;
	}

	/* current compression method */
	cm[preserve_idx] = GetCompressionMethod(compression->cmname);
	if (!IsValidCompression(cm[preserve_idx]))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("compression type \"%s\" not recognized", compression->cmname)));
	preserve_idx++;

	foreach (cell, compression->preserve)
	{
		char *cmname_p = strVal(lfirst(cell));
		char cm_p = GetCompressionMethod(cmname_p);

		/*
		 * The compression method given in the preserve list must present in
		 * the existing compression methods for the attribute.
		 */
		if (strchr(NameStr(att->attcompression), cm_p) == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("\"%s\" compression method cannot be preserved", cmname_p)));
		cm[preserve_idx++] = cm_p;
	}

	/*
	 * If all the previous compression methods are preserved then we don't need
	 * to rewrite the table otherwise we need to.
	 */
	if (need_rewrite)
	{
		if (CheckCompressionMethodsPreserved(NameStr(att->attcompression), cm))
			*need_rewrite = false;
		else
			*need_rewrite = true;
	}

	return cm;
}

/*
 * InitAttributeCompression - initialize compression method in pg_attribute
 *
 * If attribute storage is plain then initialize with the invalid compression
 * otherwise initialize it with the default compression method.
 */
void
InitAttributeCompression(Form_pg_attribute att)
{
	char *attcompression = NameStr(att->attcompression);

	MemSet(NameStr(att->attcompression), 0, NAMEDATALEN);
	if (att->attstorage != TYPSTORAGE_PLAIN)
		attcompression[0] = DefaultCompressionMethod;
}

/*
 * MakeColumnCompression - Construct ColumnCompression node.
 */
ColumnCompression *
MakeColumnCompression(NameData *compression)
{
	ColumnCompression *node;
	char	cm = *(NameStr(*compression));

	if (!IsValidCompression(cm))
		return NULL;

	node = makeNode(ColumnCompression);
	node->cmname = GetCompressionName(cm);

	return node;
}
