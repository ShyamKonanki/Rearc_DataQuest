# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold serving layer - analytical questions
# MAGIC
# MAGIC The three quest answers, built as views on top of the Gold star tables.
# MAGIC Each question is implemented in Spark SQL (the view that serves the answer)
# MAGIC and again in PySpark as a documented alternative.
# MAGIC
# MAGIC This notebook runs on its own (not part of the declarative pipeline), so new
# MAGIC metrics can be added as views without touching the pipeline or reprocessing.
# MAGIC The views live in a dedicated serving schema so a read-only analyst can be
# MAGIC granted access to just this schema.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window

catalog = "rearc_dev"
gold_schema = "dataquest_gold"
serving_schema = "dataquest_serving"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{serving_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1: US population mean & standard deviation (2013-2018)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{serving_schema}.vw_population_stats AS
SELECT avg(population)    AS mean_population,
       stddev(population) AS stddev_population
FROM   {catalog}.{gold_schema}.gold_population
WHERE  year BETWEEN 2013 AND 2018
""")

display(spark.table(f"{catalog}.{serving_schema}.vw_population_stats"))

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark alternative:

# COMMAND ----------

pop = spark.table(f"{catalog}.{gold_schema}.gold_population")
q1 = (pop
      .filter((col("year") >= 2013) & (col("year") <= 2018))
      .agg(
          F.avg("population").alias("mean_population"),
          F.stddev("population").alias("stddev_population"),
      ))
display(q1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2: best year per series (largest sum of value across its quarters)
# MAGIC
# MAGIC We sum Q01-Q04 only (Q05 is the annual average, not a quarter) and pick the
# MAGIC top year per series, with the readable label from the series dimension.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{serving_schema}.vw_series_best_year AS
WITH quarterly AS (
  SELECT series_id, year, SUM(value) AS summed_value
  FROM   {catalog}.{gold_schema}.gold_fact_productivity
  WHERE  period IN ('Q01','Q02','Q03','Q04')
  GROUP BY series_id, year
),
ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY series_id
                            ORDER BY summed_value DESC, year DESC) AS rn
  FROM quarterly
)
SELECT r.series_id, d.series_label, r.year, r.summed_value
FROM   ranked r
LEFT JOIN {catalog}.{gold_schema}.gold_dim_series d USING (series_id)
WHERE  r.rn = 1
""")

display(spark.table(f"{catalog}.{serving_schema}.vw_series_best_year"))

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark alternative:

# COMMAND ----------

fact = spark.table(f"{catalog}.{gold_schema}.gold_fact_productivity")
dim = spark.table(f"{catalog}.{gold_schema}.gold_dim_series")

quarterly = (fact
             .filter(col("period").isin("Q01", "Q02", "Q03", "Q04"))
             .groupBy("series_id", "year")
             .agg(F.sum("value").alias("summed_value")))

w = Window.partitionBy("series_id").orderBy(col("summed_value").desc(), col("year").desc())
best = (quarterly
        .withColumn("rn", F.row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn"))

q2 = (best
      .join(dim.select("series_id", "series_label"), "series_id", "left")
      .select("series_id", "series_label", "year", "summed_value"))
display(q2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3: PRS30006032 / Q01 value per year, joined to population

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{serving_schema}.vw_pr30006032_q01 AS
SELECT f.series_id, f.year, f.period, f.value, p.population
FROM   {catalog}.{gold_schema}.gold_fact_productivity f
LEFT JOIN {catalog}.{gold_schema}.gold_population p USING (year)
WHERE  f.series_id = 'PRS30006032' AND f.period = 'Q01'
ORDER BY f.year
""")

display(spark.table(f"{catalog}.{serving_schema}.vw_pr30006032_q01"))

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark alternative:

# COMMAND ----------

fact = spark.table(f"{catalog}.{gold_schema}.gold_fact_productivity")
pop = spark.table(f"{catalog}.{gold_schema}.gold_population")

q3 = (fact
      .filter((col("series_id") == "PRS30006032") & (col("period") == "Q01"))
      .select("series_id", "year", "period", "value")
      .join(pop.select("year", "population"), "year", "left")
      .select("series_id", "year", "period", "value", "population")
      .orderBy("year"))
display(q3)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from rearc_dev.dataquest_serving.vw_population_stats;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from rearc_dev.dataquest_serving.vw_pr30006032_q01;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from rearc_dev.dataquest_serving.vw_series_best_year;

# COMMAND ----------

