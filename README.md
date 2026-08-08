# Rearc Assignment - Databricks Medallion Pipeline

Production-grade Databricks pipeline that ingests BLS productivity data and
US population data, then processes it through a Bronze -> Silver -> Gold
medallion architecture using Lakeflow Declarative Pipelines (DLT).

## Project layout

```
databricks.yml                      Asset Bundle root config (dev/prod targets)
resources/
  jobs/ingestion_job.yml            Ingestion job definition (schedule, cluster)
  pipelines/medallion_pipeline.yml  DLT pipeline definition (bronze/silver/gold)
src/rearc_assignment/
  ingestion/
    job.py                         Ingestion entrypoint
    utils.py                       HTTP/parsing/idempotency helpers
  pipelines/
    bronze.py                      Raw landing tables (Auto Loader)
    silver.py                      Cleaned, typed, trimmed tables
    gold.py                        Business-ready analytical tables
tests/
  unit/                            Fast, Spark-optional tests
  integration/                     End-to-end tests against a test catalog
conf/
  dev.yml / prod.yml               Environment config (non-secret)
```

## Architecture

- **Bronze**: one raw Delta table per landed file via Auto Loader
  (`cloudFiles`), all columns kept as STRING, audit columns added
  (`source_file`, `ingested_at`).
- **Silver**: cleaned and typed tables. Key tables (`pr_data_current`,
  `pr_data_alldata`, `population`) are explicitly typed/parsed; dimension
  tables are trimmed and pass through as-is.
- **Gold**: business-ready answers:
  - `population_stats`: mean/stddev of US population, 2013-2018.
  - `pr_data_best_year`: best year per BLS series by summed value, with
    human-readable labels from dimension joins.
  - `PRS30006032_Q01_yearly_population`: yearly Q01 values for series
    PRS30006032 joined with population.

## Deploy

```bash
databricks bundle deploy -t dev
databricks bundle run rearc_ingestion_job -t dev
databricks bundle run rearc_medallion_pipeline -t dev
```

## Tests

```bash
pip install -e ".[dev]"
pytest tests/unit
```
