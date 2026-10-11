# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Databricks / PySpark medallion pipeline (Bronze → Silver → Gold) over the Olist Brazilian E-Commerce dataset, ending in a Databricks SQL dashboard (`docs/dashboard.png`). The README tracks implemented vs. planned work (incremental processing, data quality, Jobs/Workflows, Kafka + Structured Streaming); keep its checklist and Project Structure section in sync when adding stages.

## Running code

There is no local build, lint or test tooling. `src/`, `tests/` and `data/` hold only `.gitkeep` placeholders. All code is Databricks notebooks that run on a Databricks cluster/serverless and rely on the globals the runtime injects (`spark`, `display`, `dbutils`, `%fs`, and `delta.tables`). They will not run as plain Python scripts. To "run" something, the user executes the notebook in their Databricks workspace. Commits are made locally and pushed to GitHub.

Notebooks run in numeric order, and each one reads the previous layer's output:

1. `02_bronze_ingestion`: CSV → Bronze Delta (`orders`, `customers`, `products`, `order_items`, `payments`), using `load_to_bronze(file_name, table_name)`. There is no `01_` notebook.
2. `03_silver_transformation`: Bronze → `silver/order_details`.
3. `04_gold_analytics`: Silver → `gold/daily_sales`, `gold/category_sales`, `gold/order_status`.
4. `05_incremental_processing`: Delta `MERGE` upsert demo on Bronze `orders`.

## Storage layout

Everything is **path-based Delta** under a single Unity Catalog Volume. No `saveAsTable` is called in the repo. The UC tables the dashboard queries were registered outside these notebooks.

```
/Volumes/workspace/default/olist_data/
  olist_*_dataset.csv          # raw source files
  bronze/<table>/              # raw, inferSchema, mode("overwrite")
  silver/order_details/
  gold/<metric_table>/
```

Each notebook re-declares `DATA_PATH = "/Volumes/workspace/default/olist_data"` at the top. If you change that path, change it in every notebook. Every layer writes with `mode("overwrite")` (full rebuild); only notebook 05 uses `MERGE`.

## Data-model points that span notebooks

- **Silver grain is one row per order item**, not per order. Orders are left-joined to customers, then order_items, then products. That's why Gold uses `countDistinct("order_id")` for order counts and `sum("price")` for sales. Don't switch these to `count`, and don't use `total_payment_value` for sales.
- **Payments are aggregated to one row per `order_id`** (`total_payment_value`, `payment_count`) *before* the join, to avoid multiplying rows. `total_payment_value` is therefore repeated on every item row of the same order. Summing it at the Silver grain double-counts.
- The Silver output is a fixed column list (`select(...)` near the end of 03). New downstream fields need to be added there.
- Notebook 05 **mutates the real Bronze `orders` table**: it merges 5 synthetic rows whose `order_id` is prefixed `incremental_`. Those rows flow into Silver/Gold if 03/04 run afterwards. Re-running 02 overwrites Bronze and removes them. The notebook itself says the realistic incremental ingestion strategy is still to come.

## Notebook formats

02–04 are `.ipynb` files exported from Databricks; their cell metadata (`application/vnd.databricks.v1+cell`, `nuid`) is Databricks-specific. 05 is in Databricks **source format** (`.py` starting with `# Databricks notebook source`, cells separated by `# COMMAND ----------`, markdown as `# MAGIC %md`). Keep the existing format of whichever file you edit. Each notebook opens with a markdown cell explaining the layer's purpose and architecture, written in English.

## Commit style

Conventional-commit prefixes, one commit per pipeline stage: `feat: implement <stage>`, `docs: ...`.


## Working agreement

- Work in small steps. Propose a plan first and wait for approval before editing.
- Leave design-critical logic (join keys, grain decisions, MERGE conditions,
  business rules like revenue definition) as `TODO(human)` for the user to write.
- Never run `git commit` or `git push`. When a commit point is reached, say so
  and suggest a conventional commit message.
- Never run notebooks or modify data in Databricks; the user runs them.

## Target architecture (refactor in progress)

The sections above describe the *current* state. New work moves toward this target,
one notebook at a time:

- Raw files stay in the Volume; all Delta tables become UC managed tables
  under `workspace.olist` via `saveAsTable` (no path-based tables).
- Bronze is append-only, with explicit schemas and `_ingested_at`, `_source_file` columns.
- Silver is split by grain: `silver_orders` (1 row per order, payments aggregated)
  and `silver_order_items` (1 row per item).
- `MERGE` belongs in Silver, never in Bronze.
- Notebooks will be converted to Databricks source format (`.py`); shared logic moves to `src/`.

When current state and target conflict, follow the target for new code.
