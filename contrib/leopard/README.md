= Leopard Table Access Method (TAM)

Leopard is a Table Access Method (TAM) extension that is
tuned specifically for frequently updated tables. Leopard
tables will bloat significantly less during UPDATE workloads,
especially when longer running scans are in progress. MVCC
semantics are not affected.

Specifically, if HOT updates are more than 90% of the workload,
in comparison to INSERTs and DELETEd, then a table may benefit
from using Leopard, especially in a mixed workload server.
Temporary tables do not benefit from using Leopard.

Leopard's default fillfactor is 95 (percent), but can be
adjusted to other values as needed.

Leopard will provide no benefit for these workload types

* Read-mostly workloads
* INSERT-mostly workloads
* INSERT/DELETE (queue) workloads
* Infrequently updated tables

Thus, Leopard is not recommended as the default TAM.

Leopard is designed to work with EDB Postgres Distributed.

== Installing Leopard

	CREATE EXTENSION leopard;

== Using Leopard

Tables can be created to use Leopard like this

	CREATE TABLE tab (
	 ....
	)
	USING leopard;

There are no additional parameters to use with Leopard.

== Usage details

Leopard is tuned for frequently updated tables, for cases where
an index is used to locate rows before they are updated.
SeqScan updates may not be optimized as well.

SeqScans of the table are possible with full MVCC, but they may
be measurably slower, depending upon the update rate, the size of
the table and the age of the snapshot used for the scan.

Leopard will not throw ERRORs for old snapshots.

Leopard does not support Bitmap or Tid range scans.

CREATE INDEX, VACUUM, ANALYZE are unaffected for Leopard.

CLUSTER and VACUUM FULL are not possible with Leopard.
