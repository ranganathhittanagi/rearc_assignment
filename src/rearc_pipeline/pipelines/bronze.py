# Bronze layer - one raw Delta table per landed file via Auto Loader (incremental).
# Values stay STRING; column names are stripped so tables are cleanly queryable.

import os

import dlt
from pyspark.sql.functions import col, current_timestamp

# Pipeline configuration. Set `volume_path` in the pipeline settings; the
# default is here for convenience during development.
VOLUME_PATH = spark.conf.get("volume_path", "/Volumes/workspace/default/rearc_raw")
PR_DIR = f"{VOLUME_PATH}/pr"


def _table_name(file_name):
    """Turn a source file name into a valid table name.

    e.g. "pr.data.0.Current" -> "pr_data_0_current"
    """
    return file_name.replace(".", "_").lower()


def _clean_column_names(df):
    # Strip whitespace padding from BLS headers so columns are queryable.
    for name in df.columns:
        df = df.withColumnRenamed(name, name.strip())
    return df


def _add_audit_columns(df):
    return (
        df
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ingested_at", current_timestamp())
    )


def _make_pr_bronze_table(file_name):
    """Define one bronze streaming table for a single BLS pr/ file."""

    @dlt.table(
        name=_table_name(file_name),
        comment=f"Raw BLS file '{file_name}' ingested as-is (all columns string).",
        # Column mapping allows any leftover special chars in non-tabular files.
        table_properties={"delta.columnMapping.mode": "name"},
    )
    def _bronze_table():
        return _add_audit_columns(_clean_column_names(
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("header", "true")
            .option("sep", "\t")
            # Ingest only this one file into this table.
            .option("pathGlobFilter", file_name)
            .load(PR_DIR)
        ))


# Register one bronze table per file in the pr/ directory. The ingestion job
# runs before this pipeline, so the files already exist.
for file_name in sorted(os.listdir(PR_DIR)):
    _make_pr_bronze_table(file_name)


@dlt.table(
    name="population_raw",
    comment="Raw DataUSA population API response, kept as a single nested row.",
)
def population_raw():
    return _add_audit_columns(_clean_column_names(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("multiLine", "true")
        # Match population.json at the Volume root.
        .option("pathGlobFilter", "population.json")
        .load(VOLUME_PATH)
    ))
