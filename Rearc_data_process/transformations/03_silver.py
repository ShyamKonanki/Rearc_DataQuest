# Databricks notebook source
# MAGIC %md
# MAGIC # Silver (Spark Declarative Pipeline)
# MAGIC
# MAGIC Cleans up the bronze tables: trim the whitespace, cast the numeric columns,
# MAGIC drop duplicates on the natural key, and drop rows missing a key.
# MAGIC
# MAGIC Silver stays close to the source - one cleaned table per bronze table, no
# MAGIC joins. Building the readable series dimension (joining the code/text lookups
# MAGIC and deriving the label) is Gold's job.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim

catalog = spark.conf.get("catalog", "rearc_dev")
bronze_schema = spark.conf.get("bronze_schema", "dataquest_bronze")
silver_schema = spark.conf.get("silver_schema", "dataquest_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean the BLS data
# MAGIC Trim everything, cast year to int and value to double, and dedupe on
# MAGIC (series_id, year, period). Drop any row missing one of those keys.

# COMMAND ----------

@dp.materialized_view(name=f"{catalog}.{silver_schema}.silver_pr_data")
@dp.expect_all_or_drop({
    "valid_series_id": "series_id IS NOT NULL",
    "valid_year": "year IS NOT NULL",
    "valid_period": "period IS NOT NULL",
})
def silver_pr_data():
    df = spark.read.table(f"{catalog}.{bronze_schema}.bronze_pr_data")
    return (df
            .select(
                trim("series_id").alias("series_id"),
                trim("year").cast("int").alias("year"),
                trim("period").alias("period"),
                trim("value").cast("double").alias("value"),
                trim("footnote_codes").alias("footnote_codes"),
            )
            .dropDuplicates(["series_id", "year", "period"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean the series codes
# MAGIC Just trim the series attributes. The lookups are cleaned separately below;
# MAGIC Gold joins them into the readable dimension.

# COMMAND ----------

@dp.materialized_view(name=f"{catalog}.{silver_schema}.silver_pr_series")
@dp.expect_all_or_drop({"valid_series_id": "series_id IS NOT NULL"})
def silver_pr_series():
    return (spark.read.table(f"{catalog}.{bronze_schema}.bronze_pr_series")
            .select(
                trim("series_id").alias("series_id"),
                trim("sector_code").alias("sector_code"),
                trim("class_code").alias("class_code"),
                trim("measure_code").alias("measure_code"),
                trim("duration_code").alias("duration_code"),
                trim("seasonal").alias("seasonal_code"),
            ))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean the lookup tables
# MAGIC Trimmed code/text lookups. Gold joins these onto the series to build the
# MAGIC readable dimension.

# COMMAND ----------

@dp.materialized_view(name=f"{catalog}.{silver_schema}.silver_pr_measure")
def silver_pr_measure():
    return (spark.read.table(f"{catalog}.{bronze_schema}.bronze_pr_measure")
            .select(trim("measure_code").alias("measure_code"),
                    trim("measure_text").alias("measure_text")))


@dp.materialized_view(name=f"{catalog}.{silver_schema}.silver_pr_sector")
def silver_pr_sector():
    return (spark.read.table(f"{catalog}.{bronze_schema}.bronze_pr_sector")
            .select(trim("sector_code").alias("sector_code"),
                    trim("sector_name").alias("sector_name")))


@dp.materialized_view(name=f"{catalog}.{silver_schema}.silver_pr_seasonal")
def silver_pr_seasonal():
    return (spark.read.table(f"{catalog}.{bronze_schema}.bronze_pr_seasonal")
            .select(trim("seasonal_code").alias("seasonal_code"),
                    trim("seasonal_text").alias("seasonal_text")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean the population data
# MAGIC Keep the US national rows, cast the types, and dedupe by year.

# COMMAND ----------

@dp.materialized_view(name=f"{catalog}.{silver_schema}.silver_population")
@dp.expect_all_or_drop({
    "valid_year": "year IS NOT NULL",
    "valid_population": "population IS NOT NULL",
})
def silver_population():
    df = spark.read.table(f"{catalog}.{bronze_schema}.bronze_population")
    return (df
            .filter(col("Nation") == "United States")
            .select(
                col("Year").cast("int").alias("year"),
                col("Population").cast("long").alias("population"),
                col("Nation").alias("nation"),
            )
            .dropDuplicates(["year"]))