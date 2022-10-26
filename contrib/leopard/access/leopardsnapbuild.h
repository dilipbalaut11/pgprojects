/*-------------------------------------------------------------------------
 *
 * snapbuild.h
 *	  Exports from replication/logical/snapbuild.c.
 *
 * Portions Copyright (c) 2022, EnterpriseDB Corporation. All Rights Reserved.
 *
 *
 *
 *
 *
 * Copyright (c) 2012-2021, PostgreSQL Global Development Group
 *
 * src/include/replication/snapbuild.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LEOPARDSNAPBUILD_H
#define LEOPARDSNAPBUILD_H

#include "access/xlogdefs.h"
#include "utils/snapmgr.h"



/* forward declare so we don't have to expose the struct to the public */

typedef struct SnapBuild SnapBuild;

/* forward declare so we don't have to include reorderbuffer.h */


/* forward declare so we don't have to include leopardam_xlog.h */
struct xl_leopard_new_cid;


















extern XLogRecPtr SnapBuildInitialConsistentPoint(SnapBuild *builder);



extern void LeopardSnapBuildProcessNewCid(SnapBuild *builder, TransactionId xid,
								   XLogRecPtr lsn, struct xl_leopard_new_cid *cid);



extern void SnapBuildXidSetCatalogChanges(SnapBuild *builder, TransactionId xid,
										  int subxcnt, TransactionId *subxacts,
										  XLogRecPtr lsn);
#endif							/* LEOPARDSNAPBUILD_H */
