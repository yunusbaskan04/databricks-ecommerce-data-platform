# 0001. Medallion layers as Unity Catalog managed tables, Silver split by grain

- **Status:** Accepted
- **Date:** 2026-10-25 (records decisions made between 2026-10-11 and 2026-10-25)
- **Decided by:** Yunus Başkan

## Context
The first version of the pipeline stored every layer as path-based Delta folders
under one UC Volume and kept a single Silver table at order-item grain that also
carried an order-level payment total. Two problems followed:

- Path-based tables are invisible to Unity Catalog governance (permissions,
  lineage, discovery), and the dashboard needed tables registered by hand.
- `total_payment_value` was repeated on every item row of an order, so summing
  it at Silver grain silently double-counted.

The incremental-processing demo also ran `MERGE` against Bronze and polluted it
(see `docs/incidents/2026-10-11-bronze-merge-pollution.md`).

## Decision
1. Raw files stay in the UC Volume; every Delta table is a UC managed table in
   `workspace.olist`, named `<layer>_<entity>`.
2. Bronze uses explicit schemas, keeps unparseable values in `_rescued_data` and
   adds `_ingested_at` and `_source_file`.
3. Silver is split by grain: `silver_orders` (one row per order, payments
   aggregated) and `silver_order_items` (one row per item).
4. `MERGE` belongs in Silver only. Bronze is overwritten while the source is a
   full CSV snapshot and becomes append-only once incremental ingestion exists.

## Rationale
Governance and lineage come for free with managed tables, and a table whose grain
is obvious from its name cannot be aggregated at the wrong level by accident.
Keeping Bronze as an untouched copy of the source means it can always be replayed.

## Consequences
- Notebooks address tables by name through `olist_platform.config`, not by path.
- Gold reads order-level facts from `silver_orders` and item-level facts from
  `silver_order_items`.
- The old path-based folders under the Volume can be deleted once the new tables
  are verified.
