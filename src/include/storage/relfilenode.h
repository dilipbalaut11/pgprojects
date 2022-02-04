/*-------------------------------------------------------------------------
 *
 * relfilenode.h
 *	  Physical access information for relations.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/relfilenode.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELFILENODE_H
#define RELFILENODE_H

#include "common/relpath.h"
#include "storage/backendid.h"

/*
 * RelNodeId:
 *
 * this is a storage type for RelFileNode.relNode.  Instead of directly using
 * uint64 we use 2 uint32 for avoiding the alignment padding.
 */
typedef struct RelNodeId
{
	uint32	rn_hi;
	uint32	rn_lo;
} RelNodeId;

/*
 * RelFileNode must provide all that we need to know to physically access
 * a relation, with the exception of the backend ID, which can be provided
 * separately. Note, however, that a "physical" relation is comprised of
 * multiple files on the filesystem, as each fork is stored as a separate
 * file, and each fork can be divided into multiple segments. See md.c.
 *
 * spcNode identifies the tablespace of the relation.  It corresponds to
 * pg_tablespace.oid.
 *
 * dbNode identifies the database of the relation.  It is zero for
 * "shared" relations (those common to all databases of a cluster).
 * Nonzero dbNode values correspond to pg_database.oid.
 *
 * relNode identifies the specific relation and its fork number.  High 8 bits
 * represent the fork number and the remaining 56 bits represent the
 * relation.  relNode corresponds to pg_class.relfilenode (NOT pg_class.oid).
 * Notice that relNode is unique within a cluster.
 *
 * Note: When RelFileNode is part of the BufferTag only then the first 8 bits
 * of the relNode will represent the fork number otherwise those will be
 * cleared.
 *
 * Note: spcNode must be GLOBALTABLESPACE_OID if and only if dbNode is
 * zero.  We support shared relations only in the "global" tablespace.
 *
 * Note: in pg_class we allow reltablespace == 0 to denote that the
 * relation is stored in its database's "default" tablespace (as
 * identified by pg_database.dattablespace).  However this shorthand
 * is NOT allowed in RelFileNode structs --- the real tablespace ID
 * must be supplied when setting spcNode.
 *
 * Note: in pg_class, relfilenode can be zero to denote that the relation
 * is a "mapped" relation, whose current true filenode number is available
 * from relmapper.c.  Again, this case is NOT allowed in RelFileNodes.
 *
 * Note: various places use RelFileNode in hashtable keys.  Therefore,
 * there *must not* be any unused padding bytes in this struct.  That
 * should be safe as long as all the fields are of type Oid.
 *
 * We use RelNodeId in order to avoid the alignment padding.
 */
typedef struct RelFileNode
{
	Oid			spcNode;		/* tablespace */
	Oid			dbNode;			/* database */
	RelNodeId	relNode;		/* relation */
} RelFileNode;

/*
 * Augmenting a relfilenode with the backend ID provides all the information
 * we need to locate the physical storage.  The backend ID is InvalidBackendId
 * for regular relations (those accessible to more than one backend), or the
 * owning backend's ID for backend-local relations.  Backend-local relations
 * are always transient and removed in case of a database crash; they are
 * never WAL-logged or fsync'd.
 */
typedef struct RelFileNodeBackend
{
	RelFileNode node;
	BackendId	backend;
} RelFileNodeBackend;

#define RelFileNodeBackendIsTemp(rnode) \
	((rnode).backend != InvalidBackendId)

/*
 * Note: RelFileNodeEquals and RelFileNodeBackendEquals compare relNode first
 * since that is most likely to be different in two unequal RelFileNodes.  It
 * is probably redundant to compare spcNode if the other fields are found equal,
 * but do it anyway to be sure.  Likewise for checking the backend ID in
 * RelFileNodeBackendEquals.
 */
#define RelFileNodeEquals(node1, node2) \
	((RelFileNodeGetRel((node1)) == RelFileNodeGetRel((node2))) && \
	 (node1).dbNode == (node2).dbNode && \
	 (node1).spcNode == (node2).spcNode)

#define RelFileNodeBackendEquals(node1, node2) \
	(RelFileNodeGetRel((node1)) == RelFileNodeGetRel((node2)) && \
	 (node1).node.dbNode == (node2).node.dbNode && \
	 (node1).backend == (node2).backend && \
	 (node1).node.spcNode == (node2).node.spcNode)

/*
 * These macros define the "relation" stored in the RelFileNode.relNode.  Its
 * remaining 8 high-order bits identify the relation's fork number.
 */
#define RELFILENODE_RELNODE_BITS	56
#define RELFILENODE_RELNODE_MASK	((((uint64) 1) << RELFILENODE_RELNODE_BITS) - 1)
#define MAX_RELFILENODE				RELFILENODE_RELNODE_MASK

/* Retrieve the RelNode from a RelNodeId. */
#define RelNodeIDGetRelNode(rnodeid) \
	(uint64) (((uint64) (rnodeid).rn_hi << 32) | ((uint32) (rnodeid).rn_lo))

/* Store the given value in RelNodeId. */
#define RelNodeIDSetRelNode(rnodeid, val) \
( \
	(rnodeid).rn_hi = (val) >> 32, \
	(rnodeid).rn_lo = (val) & 0xffffffff \
)

/* Gets the relfilenode stored in rnode.relNode. */
#define RelFileNodeGetRel(rnode) \
	(RelNodeIDGetRelNode((rnode).relNode) & RELFILENODE_RELNODE_MASK)

/* Gets the fork number stored in rnode.relNode. */
#define RelFileNodeGetFork(rnode) \
	(RelNodeIDGetRelNode((rnode).relNode) >> RELFILENODE_RELNODE_BITS)

/* Sets input val in the relfilenode part of the rnode.relNode. */
#define RelFileNodeSetRel(rnode, val) \
	RelNodeIDSetRelNode((rnode).relNode, (val) & RELFILENODE_RELNODE_MASK)

/* Sets input val in the fork number part of the rnode.relNode. */
#define RelFileNodeSetFork(rnode, val) \
	RelNodeIDSetRelNode((rnode).relNode, \
						  (RelNodeIDGetRelNode((rnode).relNode)) | \
						  ((uint64) (val) << RELFILENODE_RELNODE_BITS))

#endif							/* RELFILENODE_H */
