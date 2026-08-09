# Rearc Assignment — Databricks Medallion Pipeline

A production-style Databricks pipeline that ingests BLS productivity data and US population data, then processes it through Bronze, Silver, and Gold layers using Databricks Asset Bundles and Lakeflow Declarative Pipelines (DLT).

## What it does

- **Ingestion job**: lands raw files from the BLS website and the DataUSA API into a Unity Catalog Volume.
- **Bronze**: stores every file as-is with Auto Loader.
- **Silver**: cleans, types, and joins the data into reusable tables.
- **Gold**: answers three analytical questions (population stats, best year per series, PRS30006032 Q01 yearly values).

## Quick start

```bash
databricks bundle deploy -t dev
databricks bundle run rearc_ingestion_job -t dev
databricks bundle run rearc_medallion_pipeline -t dev
```

See [PROCESS.md](PROCESS.md) for architecture, data model, trade-offs, and retrospective notes.
