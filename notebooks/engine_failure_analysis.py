# Databricks notebook source
# MAGIC %md
# MAGIC # Engine Failure Analysis — Medallion Architecture
# MAGIC **INNIO Data Engineering Intern Assignment**  
# MAGIC **Author:** Muhammad Muzammal Hussain  
# MAGIC **Date:** March 2026  
# MAGIC
# MAGIC This notebook implements a Bronze → Silver → Gold medallion pipeline 
# MAGIC for engine failure data. Each layer applies progressively stricter 
# MAGIC quality rules, delivering a trusted dataset for business analysis.
# MAGIC
# MAGIC | Layer | Purpose | Expected Rows |
# MAGIC |-------|---------|---------------|
# MAGIC | Bronze | Raw ingestion — no filtering | 319 |
# MAGIC | Silver | Cleaned, validated, renamed | 314 |
# MAGIC | Gold | Business rules applied | 313 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup and Configuration
# MAGIC All imports and settings are defined here in one place.  
# MAGIC If file paths or table names change, only this cell needs updating.

# COMMAND ----------

# functions from spark
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# this is the configuration all settings, update only here not everywhere in the code
SOURCE_PATH  = "/Volumes/innio_workspace/default/my_volume/source_data.csv"
BRONZE_TABLE = "innio_workspace.default.bronze_engine_raw"
SILVER_TABLE = "innio_workspace.default.silver_engine_clean"
GOLD_TABLE   = "innio_workspace.default.gold_engine_semantic"
OPH_MAX      = 120_000
print("my Configuration has loaded.")
print(f"Source : {SOURCE_PATH}")
print(f"Bronze : {BRONZE_TABLE}")
print(f"Silver table : {SILVER_TABLE}")
print(f"Gold table   : {GOLD_TABLE}")
print(f"OPH max rule : {OPH_MAX:,}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze Layer — Raw Ingestion
# MAGIC
# MAGIC **Rule: ingest everything exactly as it arrived. Touch nothing.**
# MAGIC
# MAGIC Bronze is a raw copy of the source. We load all 319 rows and all 15 
# MAGIC columns without any filtering or transformation. Every column is kept 
# MAGIC as a string — we do not let Spark guess types because guessing can 
# MAGIC silently lose data.
# MAGIC
# MAGIC The only thing we ADD is an `ingested_at` timestamp so we always know 
# MAGIC when this batch of data was loaded.
# MAGIC
# MAGIC **Why keep broken records in Bronze?**  
# MAGIC If we find a bug in Silver logic later, we can always fix and re-run 
# MAGIC from Bronze. We never lose the original data.

# COMMAND ----------

# Load CSV with all columns as strings (inferSchema=false)
# This is intentional — Bronze preserves raw data exactly as received

df_bronze = (
    spark.read.option("header","true")
    .option("inferSchema","false")
    .csv(SOURCE_PATH)
)

# this is the only transformation in Bronze
df_bronze = df_bronze.withColumn("ingested_at",F.current_timestamp())

# Save as Delta table — overwrite ensures idempotency (safe to re-run)
(
df_bronze.write.format("delta").mode("overwrite").option("overwriteSchema","true")
.saveAsTable(BRONZE_TABLE)
)

# we would verify this 
print("bronze complete")
print(f"rows : {df_bronze.count()}")
print(f"cloumns : {len(df_bronze.columns)}")
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver Layer — Cleaning and Validation
# MAGIC
# MAGIC **Rule: keep only full, valid, correctly-typed records.**
# MAGIC
# MAGIC Silver applies three categories of quality checks:
# MAGIC
# MAGIC **Completeness** — drop any row that has a missing value in any column.
# MAGIC A record with missing fields cannot be trusted for analysis.
# MAGIC
# MAGIC **Type validity** — cast each column to its correct type.
# MAGIC We use try_cast instead of direct cast because try_cast returns null
# MAGIC for invalid values like "kkkkk" instead of crashing the pipeline.
# MAGIC The subsequent dropna then removes those rows cleanly.
# MAGIC
# MAGIC **Value validity** — check that values belong to their allowed set:
# MAGIC - issue_type must be one of: typical, atypical, non-related, non-symptomatic
# MAGIC - past_dmg must be 0 or 1 only
# MAGIC - resting_analysis_results must be 0, 1, or 2 only
# MAGIC
# MAGIC **Column naming** — rename all abbreviated column names to full
# MAGIC descriptive names so the Gold layer is self-documenting.
# MAGIC
# MAGIC **Zero-information column** — op_set_2 is 100% empty across all rows.
# MAGIC It is dropped here because it carries no information whatsoever.

# COMMAND ----------

# read from the bronze table and dropping the ingested_at timestamp creates an immutable audit trail - what is the current business truth?
df = spark.read.table(BRONZE_TABLE).drop("ingested_at")
# Step 1: Trim whitespace from all string columns 
for col_name in df.columns:
    df = df.withColumn(col_name,F.trim(F.col(col_name)))
# step 2: here in this we change the empty space with the null because dropna() only catches the null values, not emppty strings ""
for col_name in df.columns:
    df = df.withColumn(
        col_name,
        F.when(F.col(col_name)=="",None).otherwise(F.col(col_name))
    )

# step 3 in this we can see the col3 is empty so remove this 
df = df.drop("op_set_2")

# step 4 completeness filtering
before = df.count()
df = df.dropna()
after = df.count()
print(f"completeness: removed {before - after} rows and then after {after} rows remained")