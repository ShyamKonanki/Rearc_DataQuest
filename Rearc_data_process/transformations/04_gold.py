# Databricks notebook source
# MAGIC %md
# MAGIC # Gold (Spark Declarative Pipeline)
# MAGIC
# MAGIC Gold is where the modeling happens: it conforms the cleaned Silver tables
# MAGIC into a business-ready star - a series dimension (built by joining the code/
# MAGIC text lookups and deriving a readable label), a productivity fact, and the
# MAGIC population reference.
# MAGIC
# MAGIC The analytical questions are answered by views on top of these tables in the
# MAGIC separate `05_gold_serving` notebook - not baked into the pipeline - so new
# MAGIC metrics can be added without changing the pipeline or reprocessing data.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import concat_ws

catalog = spark.conf.get("catalog", "rearc_dev")
silver_schema = spark.conf.get("silver_schema", "dataquest_silver")
gold_schema = spark.conf.get("gold_schema", "dataquest_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Series dimension
# MAGIC Join the cleaned lookups onto the series and derive the readable label.
# MAGIC One row per series with its codes, texts, and label.

# COMMAND ----------

@dp.materialized_view(name=f"{catalog}.{gold_schema}.gold_dim_series")
def gold_dim_series():
    series = spark.read.table(f"{catalog}.{silver_schema}.silver_pr_series")
    measure = spark.read.table(f"{catalog}.{silver_schema}.silver_pr_measure")
    sector = spark.read.table(f"{catalog}.{silver_schema}.silver_pr_sector")
    seasonal = spark.read.table(f"{catalog}.{silver_schema}.silver_pr_seasonal")

    return (series
            .join(measure, "measure_code", "left")
            .join(sector, "sector_code", "left")
            .join(seasonal, "seasonal_code", "left")
            .withColumn("series_label",
                        concat_ws(" - ", "measure_text", "sector_name", "seasonal_text"))
            .select(
                "series_id",
                "series_label",
                "sector_code", "sector_name",
                "measure_code", "measure_text",
                "seasonal_code", "seasonal_text",
                "class_code",
                "duration_code",
            ))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Productivity fact
# MAGIC One row per observation (series_id, year, period).

# COMMAND ----------

@dp.materialized_view(name=f"{catalog}.{gold_schema}.gold_fact_productivity")
def gold_fact_productivity():
    return (spark.read.table(f"{catalog}.{silver_schema}.silver_pr_data")
            .select("series_id", "year", "period", "value", "footnote_codes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Population reference
# MAGIC US population by year, joined to the fact on year.

# COMMAND ----------

@dp.materialized_view(name=f"{catalog}.{gold_schema}.gold_population")
def gold_population():
    return (spark.read.table(f"{catalog}.{silver_schema}.silver_population")
            .select("year", "population"))