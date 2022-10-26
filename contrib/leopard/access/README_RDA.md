= Leopard Recently Dead Archive (RDA) notes

== Basic Overview

Leopard will send recently dead rows to the RDA
as a last resort before it needs to switch to
another data block when doing an UPDATE.

The general notes here are independent of the
implementation details of the RDA module,
hence why they exist in a separate README.

The optimization of the RDA module is very important
to the success of the Leopard concept, so likely
much fine tuning will be needed over time.
As a result, we may need specially designed
diagnostic and cumulative statistics for RDA.

== API

* rda_insert() - adds a new tuple to the RDA
* rda_get() - retrieves one visible tuple from RDA
* rda_trim() - truncates rows from RDA, once DEAD

The RDA is optimized for OLTP, rather than whole-page
processing. Functions for multi-row insert/get
might be added later.

Repeated rda_insert() is likely to be clustered
anyway, so not sure of the additional benefit of a
multi-row rda_insert() other than reduction in WAL
traffic, which might be significant.

== Critical Sections

Insertions into the RDA should be WAL-logged, so
that the data is visible on read replicas/standbys.
This is optional, but the option to SKIP_WAL is
set at table level in Postgres heaps, so we need
to make a big decision whether to use it or not,
which really means we must choose WAL-logged for
practicality.

Removal of rows from data block are also WAL-logged.

That gives us a general flow like this:

START_CRIT_SECTION();

	START_CRIT_SECTION();

	WAL-log changes to RDA heap
	Insert into RDA heap

	WAL-log changes to RDA index
	Insert into RDA index (if any)

	END_CRIT_SECTION();

	START_CRIT_SECTION();

	(This is identical OR very similar to pruning:)
	WAL-log removal of rows from data block
	Remove rows from data block
	Repair fragmentation

	END_CRIT_SECTION();

END_CRIT_SECTION();

== Aftermath of a crash

It is then important that the scan
code doesn't request RDA rows for an update chain for which it already
has a visible tuple, otherwise we might see multiple visible tuples in
the aftermath of a crash. And it is also important that we only scan
the RDA using the index, so that any crash that leaves an RDA row but
no index row would never be found by rda_get(). 
