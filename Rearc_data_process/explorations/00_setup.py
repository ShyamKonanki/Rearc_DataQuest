# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS rearc_dev
# MAGIC   COMMENT 'Rearc Data Quest catalog (v2)';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS rearc_dev.dataquest_raw    COMMENT 'Raw landing: BLS pr files + population API JSON';
# MAGIC CREATE SCHEMA IF NOT EXISTS rearc_dev.dataquest_bronze COMMENT 'Bronze: raw ingested tables';
# MAGIC CREATE SCHEMA IF NOT EXISTS rearc_dev.dataquest_silver COMMENT 'Silver: cleaned/typed tables';
# MAGIC CREATE SCHEMA IF NOT EXISTS rearc_dev.dataquest_gold   COMMENT 'Gold: curated star model';
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS rearc_dev.dataquest_raw.landing
# MAGIC   COMMENT 'Raw landing zone for BLS pr files and population API JSON';
# MAGIC
# MAGIC SHOW SCHEMAS IN rearc_dev;
# MAGIC SHOW VOLUMES IN rearc_dev.dataquest_raw;

# COMMAND ----------

