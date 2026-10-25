# 0002. Silver data-quality rules: what happens to bad rows

- **Status:** Accepted
- **Date:** 2026-10-25
- **Decided by:** Yunus Başkan

## Context
Silver is the first layer that downstream consumers trust. Bronze keeps every
source row as-is, including rows that cannot be used:

- primary key is null (e.g. `order_id`, `order_item_id`),
- the same key appears more than once (duplicate snapshot rows),
- a value did not fit the schema and was moved to `_rescued_data`,
- a foreign key points nowhere (an item whose `order_id` has no order).

Some "gaps" are **not** errors and must stay: an order that was never delivered has
a null `order_delivered_customer_date`; a canceled order may have no items;
about 600 products have no category.

The question is what Silver does with rows that break a rule.

## Options
### A. Drop silently
Filter the rows out; Silver only contains valid rows.
- Pros: simplest code; Silver is always clean.
- Cons: data disappears without a trace; a source problem (e.g. a broken export)
  looks like "fewer sales", not like an incident.

### B. Quarantine table
Valid rows go to `silver_<entity>`; invalid rows go to `silver_<entity>_quarantine`
with a `_dq_reason` column. A validation cell reports counts per reason.
- Pros: Silver stays clean **and** nothing is lost; bad rows can be inspected,
  fixed upstream and replayed; counts are easy to alert on later (Phase 3).
  This mirrors Lakeflow expectations' "drop + keep a record" pattern.
- Cons: one extra table per entity; slightly more code.

### C. Keep and flag
Keep every row in Silver with `_is_valid` / `_dq_issues` columns.
- Pros: nothing is lost; one table.
- Cons: every consumer must remember to filter `_is_valid`; one forgotten filter
  in Gold or the dashboard reintroduces bad data.

### Sub-decision: products without a category
- keep `null`, or
- replace with `"unknown"` so they form one visible group in category reports.

## Decision
**B. Quarantine.** Rows that break a rule are moved out of Silver into a
quarantine table with the reason. Products without a category get `"unknown"`.

## Rationale
Silver must be safe to consume without extra filters, but losing rows silently
hides source problems. Quarantine gives both: a clean table and an auditable record
of what was rejected and why, which Phase 3 can turn into alerts. `"unknown"`
makes uncategorised sales visible as one group instead of an empty label.

## Consequences
- One shared table, `silver_quarantine`, holds rejected rows of every entity:
  `entity`, `_dq_reason`, `record` (the rejected row as JSON), `_ingested_at`,
  `_source_file`. One schema for all entities keeps it queryable in one place
  (a "dead-letter table").
- Rules run in this order; a row gets the first reason it fails:
  1. `null_key`: a key column is null.
  2. `rescued_data`: `_rescued_data` is not null.
  3. `duplicate_key`: another row with the same key was kept (latest
     `_ingested_at` wins).
  4. `orphan_order`: an order item whose order is not in `silver_orders`.
- Missing timestamps (e.g. not yet delivered), orders without items and
  orders without payments are **valid** and stay in Silver.
- Category names fall back to `"unknown"`. The English name falls back to the
  Portuguese name when no translation exists.
- Implemented in `src/olist_platform/silver.py`, tested in `tests/test_silver.py`.
