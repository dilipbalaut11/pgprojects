/*-------------------------------------------------------------------------
 *
 * module.c
 *		EnterpriseDB leopard plugin.
 *
 * Portions Copyright (c) 2022, EnterpriseDB Corporation. All Rights Reserved.
 *
 * IDENTIFICATION
 *	  leopard/module.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "access/xlog_internal.h"
#include "access/leopardam_xlog.h"
#include "access/leoparddecode.h"

PG_MODULE_MAGIC;

/*---- GUC variables ----*/


/*---- static variables ----*/

#if PG_VERSION_NUM >= 150000
static RmgrData leopard_rmgr = {
    .rm_name = "RM_LEOPARD",
    .rm_redo = leopard_redo,
    .rm_desc = leopard_desc,
    .rm_identify = leopard_identify,
    .rm_startup = NULL,
    .rm_cleanup = NULL,
    .rm_mask = leopard_mask,
    .rm_decode = leopard_decode
};

static RmgrData leopard2_rmgr = {
    .rm_name = "RM_LEOPARD2",
    .rm_redo = leopard2_redo,
    .rm_desc = leopard2_desc,
    .rm_identify = leopard2_identify,
    .rm_startup = NULL,
    .rm_cleanup = NULL,
    .rm_mask = NULL,
    .rm_decode = NULL
};
#endif

/*---- Private function declarations ----*/
static void RegisterLeopardHooks(void);

/*---- Function declarations ----*/

void		_PG_init(void);
void		_PG_fini(void);

/*---- Function definitions ----*/

/*
 * Module load callback
 */
void
_PG_init(void)
{
	/*
	 * Custom resource managers are only supported in postgres and epas version
	 * 15 and above.
	 *
	 * If you intend to make *any* feature depend on the use of a custom Rmgr,
	 * be sure to make the feature optional, or you'll break compatibility with
	 * versions 13 and 14, which (so far) we're still supporting.
	 */
#if PG_VERSION_NUM >= 150000
	RegisterCustomRmgr(
		/*
		 * Replace RM_EXPERIMENTAL_ID with a unique ID for our leopard TAM
		 * before release.
		 *
		 * See https://wiki.postgresql.org/wiki/CustomWALResourceManagers
		 */
		RM_LEOPARD_ID,
		&leopard_rmgr
	);

	RegisterCustomRmgr(
		/*
		 * Replace RM_EXPERIMENTAL_ID with a unique ID for our leopard TAM
		 * before release.
		 *
		 * See https://wiki.postgresql.org/wiki/CustomWALResourceManagers
		 */
		RM_LEOPARD2_ID,
		&leopard2_rmgr
	);
#endif

	/*
	 * Set up Leopard specific hooks, if any.
	 */
	RegisterLeopardHooks();
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
}

/*
 * Register all hooks for this TAM
 */
void
RegisterLeopardHooks(void)
{
}
