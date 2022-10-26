/*-------------------------------------------------------------------------
 *
 * leopard_tableam.h
 *	  EnterpriseDB leopard table access method declarations.
 *
 * Copyright (c) 2022, EnterpriseDB Inc.
 * Portions Copyright (c) 2022, EnterpriseDB Corporation. All Rights Reserved.
 *
 *
 *
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * leopard/access/leopard_tableam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LEOPARD_TABLEAM_H
#define LEOPARD_TABLEAM_H

#include "access/tableam.h"

/* leopardam interface */
extern Datum leopard_tableam_handler(PG_FUNCTION_ARGS);
extern const TableAmRoutine *GetLeopardamTableAmRoutine(void);

#endif							/* LEOPARD_TABLEAM_H */
