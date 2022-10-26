/*-------------------------------------------------------------------------
 *
 * visibilitymapdefs.h
 *		macros for accessing contents of visibility map pages
 *
 *
 * Portions Copyright (c) 2022, EnterpriseDB Corporation. All Rights Reserved.
 *
 *
 *
 *
 *
 * Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * src/include/access/visibilitymapdefs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LEOPARDVISMAPDEFS_H
#define LEOPARDVISMAPDEFS_H

/* Number of bits for one leopard page */
#define BITS_PER_LEOPARDBLOCK 2

/* Flags for bit map */
#define LEOPARDVISMAP_ALL_VISIBLE	0x01
#define LEOPARDVISMAP_ALL_FROZEN	0x02
#define LEOPARDVISMAP_VALID_BITS	0x03	/* OR of all valid visibilitymap
											 * flags bits */

#endif							/* LEOPARDVISMAPDEFS_H */
