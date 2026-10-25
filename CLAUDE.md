# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Databricks / PySpark medallion pipeline (Bronze → Silver → Gold) over the Olist Brazilian E-Commerce dataset, ending in a Databricks SQL dashboard (`docs/dashboard.png`). The platform is **Databricks Free Edition** (serverless only, restricted outbound internet). The README tracks implemented vs. planned work; keep its checklist and Project Structure section in sync when adding stages.

## Layout

- `src/olist_platform/`: all logic, as pure `DataFrame -> DataFrame` functions where possible.
  `config.py` (catalog, schema, Volume path, `table_name()`), `schemas.py` (explicit Bronze schemas),
  `bronze.py`, `silver.py`, `gold.py`.
- `notebooks/`: thin Databricks entry points in **source format** (`.py`, `# Databricks notebook source`,
  `# COMMAND ----------`, markdown as `# MAGIC %md`). Each opens with an English markdown cell explaining
  the layer, then `sys.path.insert(0, os.path.abspath("../src"))`, imports from `olist_platform`, reads
  tables, calls the functions and writes with `saveAsTable`. Notebooks rely on runtime globals
  (`spark`, `display`, `dbutils`).
- `tests/`: pytest with a local `SparkSession` (`conftest.py`, `bronze_df` fixture). No I/O in tests.
- `databricks.yml`: Asset Bundle with the `olist_medallion` job (bronze → silver → gold, serverless).
- `docs/decisions/`: ADRs. `docs/incidents/`: postmortems.

## Running code

- Local venv lives at `C:\Users\ybask\.venvs\olist` (outside the repo: the `ü` in the repo path breaks
  Spark's Java classpath on Windows). Install with `pip install -e ".[dev]"`.
- Lint: `ruff check .` and `ruff format --check .`. Tests: `pytest` (slow on Windows, ~90 s).
- CI (`.github/workflows/ci.yml`) runs the same on every PR.
- Databricks CLI: `databricks bundle validate|deploy|run`. The user authenticates with `databricks auth login`.

## Storage layout

Raw CSVs stay in the Volume `/Volumes/workspace/default/olist_data/`. Every Delta table is a UC managed
table `workspace.olist.<layer>_<entity>` (ADR-0001), addressed only through `olist_platform.config`.

- Bronze: `bronze_orders`, `bronze_customers`, `bronze_products`, `bronze_order_items`, `bronze_payments`,
  `bronze_category_translation`; explicit schema, `_rescued_data`, `_ingested_at`, `_source_file`; overwrite.
- Silver: `silver_orders`, `silver_order_items`, `silver_quarantine`; overwrite.
- Gold: `gold_daily_sales`, `gold_category_sales`, `gold_order_status`; overwrite.

Old path-based folders (`bronze/`, `silver/`, `gold/` under the Volume) are obsolete.

## Data-model points that span layers

- **Grain is in the table name.** `silver_orders` = one row per order; `silver_order_items` = one row per
  item. Order counts are `count` on `silver_orders` but `countDistinct("order_id")` on items.
- Payments are aggregated per order **before** joining (`total_payment_value`, `payment_count`) and live
  only on `silver_orders`.
- Data quality (ADR-0002): rows with null key, `_rescued_data`, duplicate key (latest `_ingested_at` wins)
  or orphan item go to `silver_quarantine` (`entity`, `_dq_reason`, `record` JSON). Missing timestamps,
  orders without items/payments are valid. Missing category → `"unknown"`.
- Revenue (ADR-0003): `sum(price)` over items whose order status is not in
  `gold.REVENUE_EXCLUDED_STATUSES` (`canceled`, `unavailable`). Freight is a separate metric.
- Silver and Gold outputs use explicit `select` lists; new downstream fields must be added there.

## Commit style

Conventional commits, one commit per step: `feat:`, `refactor:`, `chore:`, `ci:`, `docs:`, `fix:`.

## Working agreement

- Work in small steps. Propose a plan first and wait for approval before editing.
- Claude writes the code; the user owns design-critical decisions (grain, join
  keys, MERGE conditions, data-quality rules, business rules like revenue).
  Do not leave `TODO(human)` code. Instead, write an ADR in `docs/decisions/`
  (copy `0000-template.md`) with context, options and trade-offs, ask the user to
  choose, then record their choice and rationale and implement it.
- If the user has already made a design decision in chat, record it and implement it.
- Claude commits and pushes: one conventional commit per step, on a feature
  branch per phase, merged to `main` through a PR after CI passes. Ask the user
  before merging.
- Never run notebooks, run jobs or modify data in Databricks; the user runs them.
  Read-only checks through the Databricks CLI are fine. Ask before every
  `databricks bundle deploy`.
- Mirror learnings into the user's Obsidian vault (`C:\Users\ybask\OneDrive\Masaüstü\Proje\Learning`),
  following its `Vault rehberi.md`.

## Roadmap

- Phase 2: Kafka (Docker locally) → events land in the Volume → Auto Loader → append-only Bronze →
  `MERGE` in Silver. Free Edition likely cannot reach external brokers; spike first (ADR-0004).
- Phase 3: data-quality monitoring and alerts on `silver_quarantine`.
- Phase 4: performance (liquid clustering, `OPTIMIZE`).
