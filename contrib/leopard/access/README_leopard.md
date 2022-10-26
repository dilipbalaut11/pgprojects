= Leopard Table Access Method (TAM)

Leopard adds an important capability: the ability
to archive recently dead tuples as a mechanism
to avoid performing an expensive multi-block non-HOT UPDATE,
which causes heap and index bloat. PostgreSQL does
not currently provide a mechanism to repair index bloat,
other than a full REINDEX, so this is especially important
to avoid.

Thus, Leopard is optimized for frequently updated tables,
or cases where the UPDATEs are much more prevalent than
INSERTs and DELETEs.

With this use case in mind, Leopard's default fillfactor
is 95 (percent).

Leopard will provide no benefit for INSERT-only workloads,
but nor will it give an overhead either.

Leopard will provide no benefit for INSERT/DELETE workloads
such as queue tables.

Leopard is designed to work with EDB Postgres Distributed.

== Archiving of Recently Dead tuples

Once archived, there is a higher cost to bring the
rows back, so we want to archive as few rows as
possible.

Leopard follows a number of heuristics, which will be
subject to some degree of further tuning.

* Leopard can archive rows that are Recently Dead,
but only ever does this as a last resort, if
pruning is not possible. This is possible because
archiving happens in a later transaction, so is
not performed eagerly.

* When forced to archive, Leopard aims to archive as
many rows as needed to free enough space for 2x the
space required for the current UPDATE, or a minimum of 2.
This gives space in case archiving is not possible later,
which can occur if we cannot obtain a block cleanup lock.
These rows will be naturally clustered in the RDA, thus
improving dearchive performance.

* Leopard will preferentially try to archive rows on
the update chain that is about to be UPDATEd, rather
than pick recently dead rows at random.

* Leopard will archive never-visible rows, such as xmin=xmax,
but in this case will choose not to store the main tuple,
since it will never be visible.

We archive only recently dead tuples on the root end of
the chain, always leaving the last tuple.

Note that when we archive a tuple, it is removed completely
just as if it had been pruned when dead. Thus we get the
full benefit of removing even small rows, and the huge
benefit of not needing to alter the block format at all to
make Leopard work. Just to restate this: there is no pointer
or metadata left on-block to help locate any archived rows.
The only keys we have are the table oid and the root ItemPointer.

Archiving is covered by two WAL records:

* The WAL record covering the RDA
* The prune WAL record, identical to the normal prune, except
  that the latestRemovedXid is always InvalidTransactionId,
  which is a valid, supported setting.

Note that there is no additional WAL record type required
for Leopard, so no separate rmgr is required either.

Note also that the Leopard page format is identical to the
standard Heap page format; yes, completely identical. As a result
all standard utilities, such as page inspect, amcheck, etc
will work without modification. Which also means that you
cannot tell by looking at a block whether it was written by
Leopard, or by Heap.

== Dearchival of rows during scans

If Leopard scans an all-visible block then it has nothing
else to do since there cannot be any archived tuples from
the block that would be visible to the current scan.

If Leopard follows an index TID to the heap it will then
follow the chain looking for visible tuples. If the chain
starts with an updated tuple where the first tuple is in
the future, this is taken as an indication that recently
dead rows have been archived. In this case we scan the
archive for a visible row, if any exist.

If we attempt a page scan and find that it is not all-visible,
we first find the root tuples on the page, then scan each chain
individually, potentially retrieving rows for each.
So there is no page-level scan where we treat each tuple as
potentially visible, as we do with normal heap; the only
access path is via HOT chains.

We archive only recently dead tuples on the root end of the chain,
always leaving the last tuple. This ensures that we have at least
one tuple in the chain, which then allows us to judge whether that
tuple is in the past or the future, visibility-wise. If a chain
has a future-visible tuple that has been HOT UPDATEd, but no currently
visible tuple, we infer that an archived tuple may exist that is
visible to us and so choose to scan the archive for a visible tuple.
It might be the case that this is actually just a
recently INSERTed and then UPDATEd tuple, but that is considered
to be less likely for the stated use-case of frequent updates,
and so it is acceptable that we might occaisionally waste a call
to dearchive a row when there isn't one. This choice is made
explicitly by the user when they select the Leopard TAM.

Dearchival only occurs by the supported API, which implements
concurrent actions correctly.

== Supported scans

Leopard does not support Bitmap or Tid range scans, since
these do not map easily onto the Leopard concept.

Leopard index build scans work without change, though currently
always set ii_BrokenHotChain=true, though this might be
optimized in future if the RDA has no entries for the table.
