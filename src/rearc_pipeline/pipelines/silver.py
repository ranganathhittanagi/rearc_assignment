# Silver layer - Lakeflow Declarative Pipeline (DLT).
#

import dlt
from pyspark.sql.functions import col, trim, explode, from_json
from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


# Auto Loader rescue column + bronze audit columns, dropped from every silver table.
DROP_UNWANTED_COLUMNS = ["_rescued_data", "source_file", "ingested_at"]


def _trim_all_values(df):
    """Drop unwanted columns and trim every string value."""
    df = df.drop(*DROP_UNWANTED_COLUMNS)
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))
    return df


# --- Silver: BLS productivity data ------------------------------------------

PR_DATA_RULES = {
    "series_id_not_null": "series_id IS NOT NULL",
    "year_not_null": "year IS NOT NULL"
}


def _clean_pr_data(df):
    df = df.drop(*DROP_UNWANTED_COLUMNS)
    return df.select(
        trim(col("series_id")).alias("series_id"),
        trim(col("year")).cast("int").alias("year"),
        trim(col("period")).alias("period"),
        trim(col("value")).cast("double").alias("value"),
        trim(col("footnote_codes")).alias("footnote_codes"),
    )


@dlt.table(
    name="workspace.silver.pr_data_current",
    comment="Cleaned BLS productivity series (current file): typed year/value.",
)
@dlt.expect_all_or_drop(PR_DATA_RULES)
def pr_data_current():
    return _clean_pr_data(dlt.read_stream("pr_data_0_current"))


@dlt.table(
    name="workspace.silver.pr_data_alldata",
    comment="Cleaned BLS productivity series (full history): typed year/value.",
)
@dlt.expect_all_or_drop(PR_DATA_RULES)
def pr_data_alldata():
    return _clean_pr_data(dlt.read_stream("pr_data_1_alldata"))


# --- Silver: BLS series dimension -------------------------------------------
#
# Keep all original columns from the bronze pr_series file as-is (trimmed).
# Meaningful series labels are derived in the gold layer by joining this table
# with the sector and measure dimension tables.

@dlt.table(
    name="workspace.silver.pr_series",
    comment="BLS series dimension: all original columns retained and trimmed.",
)
@dlt.expect_all_or_drop({"series_id_not_null": "series_id IS NOT NULL"})
def pr_series():
    return _trim_all_values(dlt.read("pr_series"))


# --- Silver: US population by year ------------------------------------------

POPULATION_RULES = {
    "year_not_null": "year IS NOT NULL",
    "population_not_null": "population IS NOT NULL",
}

# In bronze the `data` array arrived as a JSON STRING (schema inference was off),
# so we parse it with this schema before exploding.
POPULATION_DATA_SCHEMA = ArrayType(
    StructType([
        StructField("Nation ID", StringType()),
        StructField("Nation", StringType()),
        StructField("Year", IntegerType()),
        StructField("Population", DoubleType()),
    ])
)


@dlt.table(
    name="workspace.silver.population",
    comment="US national population, one row per year (flattened from raw JSON).",
)
@dlt.expect_all_or_drop(POPULATION_RULES)
def population():
    # bronze `population_raw` holds the whole API response in one row; the yearly
    # records live in the nested `data` array (stored as a JSON string), so parse
    # then explode it into one row/year.
    parsed = dlt.read_stream("population_raw").select(
        from_json(col("data"), POPULATION_DATA_SCHEMA).alias("data")
    )
    rows = parsed.select(explode(col("data")).alias("row"))
    return rows.select(
        col("row.Nation ID").alias("nation_id"),
        col("row.Year").cast("int").alias("year"),
        col("row.Nation").alias("nation"),
        col("row.Population").cast("long").alias("population"),
    )


# --- Silver: remaining reference tables -------------------------------------
# Loaded from bronze with all string values trimmed and audit/rescue columns
# dropped. Column mapping covers non-tabular files (pr_txt/pr_contacts).

DIM_TABLES = [
    "pr_class",
    "pr_contacts",
    "pr_duration",
    "pr_footnote",
    "pr_measure",
    "pr_period",
    "pr_seasonal",
    "pr_sector",
    "pr_txt",
]


def _make_dim_table(bronze_name):
    @dlt.table(
        name=f"workspace.silver.{bronze_name}",
        comment=f"Cleaned {bronze_name} from bronze.",
        table_properties={"delta.columnMapping.mode": "name"},
    )
    def _reference():
        return _trim_all_values(dlt.read_stream(bronze_name))
    return _reference


for _name in DIM_TABLES:
    _make_dim_table(_name)
