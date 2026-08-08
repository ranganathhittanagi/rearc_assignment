# Architecture

## Sources

1. **BLS productivity data** (`https://download.bls.gov/pub/time.series/pr/`):
   directory of tab-separated files, ingested as one file per bronze table.
2. **US population** (DataUSA API, JSON): single API response with a nested
   `data` array of yearly records.

## Medallion layers

### Bronze
- One Delta streaming table per landed file, via Auto Loader (`cloudFiles`).
- All columns kept as STRING (no schema inference) to avoid ingestion-time
  type failures on malformed/irregular source files.
- Column names stripped of whitespace so tables are queryable.
- Audit columns added: `source_file`, `ingested_at`.
- Delta column mapping (`delta.columnMapping.mode = name`) enabled to
  tolerate special characters in some BLS headers.

### Silver
- `pr_data_current` / `pr_data_alldata`: typed (`year` int, `value` double),
  trimmed, with row-level expectations (`series_id`/`year` not null).
- `pr_series`: dimension table, bronze columns retained as-is (trimmed) -
  human-readable labels are derived downstream in gold via dimension joins.
- `population`: JSON `data` string parsed with an explicit schema and
  exploded into one row per year.
- Remaining reference/dimension tables (`pr_class`, `pr_sector`,
  `pr_measure`, `pr_footnote`, `pr_duration`, `pr_seasonal`, `pr_period`,
  `pr_contacts`, `pr_txt`): trimmed, audit/rescue columns dropped.

### Gold
- `population_stats`: mean and sample stddev of population, 2013-2018
  (years hard-coded per assignment).
- `pr_data_best_year`: for each `series_id`, the year with the largest
  summed value, joined with dimension tables for human-readable labels.
- `PRS30006032_Q01_yearly_population`: yearly Q01 values for series
  PRS30006032, left-joined with that year's population.

## Design decisions

- Managed Delta tables in dedicated Unity Catalog schemas
  (`workspace.bronze`, `workspace.silver`, `workspace.gold`) rather than
  external tables on the Volume, for governance and simpler lifecycle.
- Ingestion job writes to a Unity Catalog Volume with idempotent
  add/update/remove semantics so pipeline re-runs are safe.
- PySpark is the primary implementation for gold tables; the equivalent
  Spark SQL is included as a comment for readability/explainability.
