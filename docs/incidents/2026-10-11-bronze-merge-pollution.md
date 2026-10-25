# Incident: Synthetic rows polluted Bronze `orders` via non-idempotent MERGE demo

**Date detected:** 2026-10-11 · **Severity:** Low (dev data) · **Status:** Resolved

## Summary
The incremental-processing demo (notebook 05) inserted 10 synthetic orders into
Bronze `orders` across repeated runs, although it was intended to be idempotent.

## Impact
- Bronze `orders`: 99,441 → 99,451 rows.
- Silver/Gold: not affected (0 synthetic rows in `silver/order_details`).


## Timeline
| Version | Date | Operation | Effect |
|---|---|---|---|
| 0 | 2026-09-21 | WRITE (overwrite) | 99,441 rows |
| 1 | 2026-09-29 | MERGE | +5 `incremental_*` rows |
| 2 | 2026-09-29 | MERGE | +5 `incremental_incremental_*` rows |
| 3 | 2026-10-11 | MERGE | 5 rows updated, 0 inserted |
| 4 | 2026-10-11 | DELETE | −10 synthetic rows → 99,441 |

## Root cause
1. The source batch was derived from the target table itself, so each run
   changed the input of the next run.
2. `limit(5)` is non-deterministic; it picked previously inserted synthetic rows.

## Resolution
- `RESTORE ... VERSION AS OF 0` failed: version older than
  `deletedFileRetentionDuration` (7 days).
- Removed synthetic rows with a targeted `DELETE`; verified count = 99,441.

## Lessons learned
- MERGE is idempotent only for a fixed, deterministic input.
- Incremental sources must be independent of the target (new files, CDC, events).
- MERGE belongs in Silver; Bronze stays append-only.
- Delta time travel is short-term undo, not a backup.

## Follow-ups
- [x] Remove the Bronze MERGE demo (`05_incremental_processing.py`), 2026-10-25.
- [ ] Redesign incremental processing with an independent source (Phase 2:
      Kafka events → Auto Loader → append-only Bronze → `MERGE` in Silver).