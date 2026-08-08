# Gold layer - Lakeflow Declarative Pipeline (DLT).
#
# Business-ready answers to the required analytical questions.
# Implemented so far: population_stats.


import dlt
from pyspark.sql.functions import col, avg, stddev, sum, desc, rank, coalesce, lit
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Q1: Mean and standard deviation of the annual US population, 2013-2018 inclusive
#
# Spark SQL:
#   SELECT
#       avg(population)          AS mean_population,
#       stddev_samp(population)  AS stddev_population
#   FROM workspace.silver.population
#   WHERE year BETWEEN 2013 AND 2018
# ---------------------------------------------------------------------------
@dlt.table(
    name="workspace.gold.population_stats",
    comment="Mean and sample stddev of annual US population, 2013-2018 inclusive.",
)
def population_stats():
    pop = dlt.read("workspace.silver.population").filter(
        (col("year") >= 2013) & (col("year") <= 2018)
    )
    return pop.agg(
        avg("population").alias("mean_population"),
        stddev("population").alias("stddev_population"),
    )


# ---------------------------------------------------------------------------
# Q2: Best year per BLS productivity series (largest annual sum of values)
#
# Spark SQL:
#   WITH yearly_sums AS (
#       SELECT
#           series_id,
#           year,
#           SUM(value) AS summed_value
#       FROM workspace.silver.pr_data_current d
#       GROUP BY series_id, year
#   ),
#   ranked AS (
#       SELECT
#           series_id,
#           year,
#           summed_value,
#           RANK() OVER (PARTITION BY series_id ORDER BY summed_value DESC) AS rnk
#       FROM yearly_sums
#   )
#   SELECT
#       r.series_id,
#       r.year AS best_year,
#       r.summed_value AS value,
#       s.sector_name,
#       m.measure_text,
#       COALESCE(f.footnote_text, 'Unknown') AS footnote,
#       c.class_text,
#       d.duration_text,
#       COALESCE(s.Seasonal_text, 'Unknown') AS seasonal
#   FROM ranked r
#   LEFT JOIN workspace.silver.pr_series ps
#       ON ps.series_id = r.series_id
#   LEFT JOIN workspace.silver.pr_sector s
#       ON s.sector_code = ps.sector_code
#   LEFT JOIN workspace.silver.pr_measure m
#       ON m.measure_code = ps.measure_code
#   LEFT JOIN workspace.silver.pr_class c
#       ON c.class_code = ps.class_code
#   LEFT JOIN workspace.silver.pr_footnote f
#       ON f.footnote_code = ps.footnote_codes
#   LEFT JOIN workspace.silver.pr_seasonal s
#       ON ps.sector_code = s.Seasonal_code
#   LEFT JOIN workspace.silver.pr_duration d
#       ON ps.duration_code = d.duration_code
#   WHERE r.rnk = 1;
# ---------------------------------------------------------------------------
@dlt.table(
    name="workspace.gold.pr_data_best_year",
    comment="Best year for each BLS productivity series (largest annual sum of values).",
)
def pr_data_best_year():
    yearly = (
        dlt.read("workspace.silver.pr_data_current")
        .groupBy("series_id", "year")
        .agg(sum("value").alias("summed_value"))
    )

    ranked = yearly.withColumn(
        "rnk",
        rank().over(Window.partitionBy("series_id").orderBy(desc("summed_value")))
    )

    ps = dlt.read("workspace.silver.pr_series")
    sec = dlt.read("workspace.silver.pr_sector")
    meas = dlt.read("workspace.silver.pr_measure")
    cls = dlt.read("workspace.silver.pr_class")
    ftn = dlt.read("workspace.silver.pr_footnote")
    seas = dlt.read("workspace.silver.pr_seasonal")
    dur = dlt.read("workspace.silver.pr_duration")

    return (
        ranked.filter(col("rnk") == 1)
        .join(ps, "series_id", "left")
        .join(sec, "sector_code", "left")
        .join(meas, "measure_code", "left")
        .join(cls, "class_code", "left")
        .join(ftn, col("footnote_codes") == col("footnote_code"), "left")
        .join(seas, col("sector_code") == col("Seasonal_code"), "left")
        .join(dur, "duration_code", "left")
        .select(
            col("series_id"),
            col("year").alias("best_year"),
            col("summed_value").alias("value"),
            col("sector_name"),
            col("measure_text"),
            coalesce(col("footnote_text"), lit("Unknown")).alias("footnote"),
            col("class_text"),
            col("duration_text"),
            coalesce(col("Seasonal_text"), lit("Unknown")).alias("seasonal"),
        )
    )


# ---------------------------------------------------------------------------
# Q3: PRS30006032 Q01 value per year, joined with that year's population
#     where available.
#
# Spark SQL:
#   SELECT
#       d.year,
#       d.value,
#       coalesce(CAST(p.population AS STRING),'not available') as population
#   FROM workspace.silver.pr_data_current d
#   LEFT JOIN workspace.silver.population p
#       ON p.year = d.year
#   WHERE d.series_id = 'PRS30006032'
#     AND d.period = 'Q01'
#   ORDER BY d.year
# ---------------------------------------------------------------------------
@dlt.table(
    name="workspace.gold.PRS30006032_Q01_yearly_population",
    comment="Yearly Q01 values for series PRS30006032 joined with US population where available.",
)
def PRS30006032_Q01_yearly_population():
    d = dlt.read("workspace.silver.pr_data_current").filter(
        (col("series_id") == "PRS30006032") & (col("period") == "Q01")
    )
    p = dlt.read("workspace.silver.population")
    return (
        d.join(p, "year", "left")
        .select(
            col("year").cast("int").alias("year"),
            col("value"),
            coalesce(col("population").cast("string"), lit("not available")).alias("population"),
        )
        .orderBy("year")
    )
