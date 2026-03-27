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
from pyspark.sql.window import Window

# this is the configuration all settings, update only here not everywhere in the code
SOURCE_PATH  = "/Volumes/innio_workspace/default/my_volume/source_data.csv"
BRONZE_TABLE = "innio_workspace.default.bronze_engine_raw"
SILVER_TABLE = "innio_workspace.default.silver_engine_clean"
GOLD_TABLE = "innio_workspace.default.gold_engine_semantic"
GOLD_VIEW    = "innio_workspace.default.vw_gold_engine"
OPH_MAX = 120_000
VALID_ISSUE_TYPES = ["typical", "atypical", "non-related", "non-symptomatic"]
VALID_PAST_DMG    = [0, 1]
VALID_RESTING     = [0, 1, 2]
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
print(f"columns : {len(df_bronze.columns)}")
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

def get_row_nums(df):
    """
    Return a Python set of all row_num values currently in df.
    We compare before/after sets to find exactly which rows were dropped.
    """
    return set(r["row_num"] for r in df.select("row_num").collect())

def report_dropped(before_set, after_set, reason):
    """
    Print which row numbers disappeared between two steps, and why.
    """
    # Set subtraction: rows that were in before but not in after
    dropped = sorted(before_set - after_set)
    if dropped:
        print(f"  Dropped {len(dropped)} row(s) — {reason}")
        print(f"  Row numbers: {dropped}")
    else:
        print(f"  No rows dropped — {reason}")
df = spark.read.table(BRONZE_TABLE).drop("ingested_at")

# this gives use row numbers which will be sequential and unique large integer 
# here i used the F.lit(1) because for the partition it was giving me the warning because so i used this because my data is small 
df = df.withColumn(
    "row_num",
    F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.monotonically_increasing_id()))
)
# Step 1: Trim whitespace from all string columns 
for col_name in df.columns:
    if col_name == "row_num":
        continue           # row_num is an integer — skip it
    df = df.withColumn(col_name,F.trim(F.col(col_name)))
# step 2: here in this we change the empty space with the null because dropna() only catches the null values, not emppty strings ""
for col_name in df.columns:
    if col_name == "row_num":
        continue
    df = df.withColumn(
        col_name,
        F.when(F.col(col_name)=="",None).otherwise(F.col(col_name))
    )

# step 3 in this we can see the col3 is empty so remove this 
df = df.drop("op_set_2")

# step 4 completeness filtering
# Snapshot row numbers before this step
before_set = get_row_nums(df)
# dropna removes null values in the columns
df = df.dropna()
after_set = get_row_nums(df)
report_dropped(before_set, after_set, "missing values in one or more columns")
print(f"  Rows remaining: {df.count()}")
 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Step 5 — Type Casting
# MAGIC
# MAGIC Every column arrived as a string from Bronze.
# MAGIC We now cast each column to its correct type.
# MAGIC
# MAGIC We use try_cast instead of direct cast for a critical reason:
# MAGIC direct cast crashes the entire pipeline if it finds one bad value.
# MAGIC try_cast returns null for invalid values like "kkkkk" or "999abc"
# MAGIC and the pipeline continues. The dropna after removes those null rows.
# MAGIC
# MAGIC Numeric columns and their expected types:
# MAGIC - oph, pist_m, past_dmg, resting_analysis_results : integer
# MAGIC - rpm_max, full_load_issues, number_up, number_tc  : integer
# MAGIC - op_set_1, op_set_3, high_breakdown_risk          : integer
# MAGIC - bmep, ng_imp                                     : double (decimal)

# COMMAND ----------

int_cols = ["oph", "pist_m", "past_dmg", "resting_analysis_results",
    "rpm_max", "full_load_issues", "number_up", "number_tc",
    "op_set_1", "op_set_3", "high_breakdown_risk"
]
double_cols = ["bmep", "ng_imp"]
# try_cast returns null for bad values instead of crashing like in pandas coerce 
for c in int_cols:
  df = df.withColumn(c,F.expr(f"try_cast(`{c}` as  INT)"))
for c in double_cols:
  df = df.withColumn(c,F.expr(f"try_cast(`{c}` as DOUBLE)"))

# so any value that could not be cast is now null.
# dropna(subset=...) drops rows where null appears in ANY of the listed columns.
before_set = get_row_nums(df)
df = df.dropna(subset=int_cols + double_cols)
after_set = get_row_nums(df)
report_dropped(before_set, after_set, "type error — non-numeric value in numeric column")
print(f"  Rows remaining: {df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Step 6 — Value Validity Checks
# MAGIC
# MAGIC Even after type casting, some values may be technically valid numbers
# MAGIC but not in the allowed set defined by the business description.
# MAGIC
# MAGIC We check three columns:
# MAGIC - issue_type must be one of 4 categories defined in business_description.txt
# MAGIC - past_dmg must be 0 or 1 only — it is a boolean flag
# MAGIC - resting_analysis_results must be 0, 1, or 2 only
# MAGIC
# MAGIC Rows with values outside these sets are invalid records.
# MAGIC They are removed from Silver and will never reach Gold.

# COMMAND ----------

# Even after casting, a number can be outside its allowed set.
# For example: past_dmg=2 is a valid integer but NOT a valid boolean flag.
 
before_set = get_row_nums(df)

df = df.filter(F.col("issue_type").isin(VALID_ISSUE_TYPES))
 
# past_dmg is now an integer after Step 5 casting.
# Only 0 (no past damage) and 1 (has past damage) are valid.
df = df.filter(F.col("past_dmg").isin(VALID_PAST_DMG))
 
# resting_analysis_results: 0=normal, 1=abnormal, 2=critical only
df = df.filter(F.col("resting_analysis_results").isin(VALID_RESTING))
 
after_set = get_row_nums(df)
report_dropped(
    before_set, after_set,
    "value outside allowed set (issue_type / past_dmg / resting_analysis_results)"
)
print(f"step 6  Rows remaining: {df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Step 7 — Column Renaming
# MAGIC
# MAGIC All abbreviated column names are expanded to full descriptive names.
# MAGIC This makes the Gold layer self-documenting — an analyst reading the
# MAGIC table 6 months from now understands every column without a dictionary.
# MAGIC
# MAGIC Examples:
# MAGIC - oph      → operating_hours
# MAGIC - pist_m   → piston_material  
# MAGIC - bmep     → brake_mean_effective_pressure
# MAGIC - ng_imp   → natural_gas_impurities_nmol
# MAGIC - past_dmg → has_past_damage

# COMMAND ----------

column_rename_map = {
    "oph"                      : "operating_hours",
    "pist_m"                   : "piston_material",
    "issue_type"               : "combustion_issue_type",
    "bmep"                     : "brake_mean_effective_pressure",
    "ng_imp"                   : "natural_gas_impurities_nmol",
    "past_dmg"                 : "has_past_damage",
    "resting_analysis_results" : "resting_analysis_result",
    "rpm_max"                  : "max_rotations_per_minute",
    "full_load_issues"         : "has_full_load_issues",
    "number_up"                : "unplanned_events_count",
    "number_tc"                : "turbocharger_count",
    "op_set_1"                 : "operational_setting_1",
    "op_set_3"                 : "operational_setting_3",
    "high_breakdown_risk"      : "high_breakdown_risk",
}

for old, new in column_rename_map.items():
  df= df.withColumnRenamed(old,new)

# Drop row_num before saving — it was a pipeline helper only.
# It must not appear in the Silver table as business data.
df = df.drop("row_num")
# save silver as delta table
(
  df.write.format("delta").mode("overwrite").option("overwriteSchema","true")
  .saveAsTable(SILVER_TABLE)
)

print("Silver Complete")
print(f"rows : {df.count()}")
print(f"columns : {len(df.columns)}")
print(f"columns : {df.columns}")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold Layer — Business Ready Semantic Model
# MAGIC
# MAGIC **Rule: apply business tests and remove zero-information columns.**
# MAGIC
# MAGIC Gold is the final layer. Only records that pass ALL quality checks
# MAGIC reach here. Analysts and dashboards query Gold exclusively.
# MAGIC
# MAGIC Two things happen in Gold:
# MAGIC
# MAGIC **Business test — operating hours rule**
# MAGIC The business description states engines must have operating_hours <= 120,000.
# MAGIC One engine had oph = 1,000,000,000 which is physically impossible.
# MAGIC This rule removes that record.
# MAGIC
# MAGIC **Zero-information columns removed**
# MAGIC After Silver cleaning, two columns have only one unique value:
# MAGIC - operational_setting_1 — constant value of 1 in every row
# MAGIC - operational_setting_3 — constant value of 0 in every row
# MAGIC A constant column has zero variance. It cannot explain why some
# MAGIC engines break and others do not. Keeping it misleads analysts.

# COMMAND ----------

# read from silver 
df_gold = spark.read.table(SILVER_TABLE)
df_gold = df_gold.withColumn(
    "row_num",
    F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.monotonically_increasing_id()))
)
before_set = get_row_nums(df_gold)
df_gold = df_gold.filter(F.col("operating_hours") <= OPH_MAX)
after_set = get_row_nums(df_gold)
report_dropped(
    before_set, after_set,
    f"business rule: operating_hours > {OPH_MAX:,}"
)
print(f"  Rows remaining after business rule: {df_gold.count()}")

# Drop row_num before checking variance — it would always show as unique
df_gold = df_gold.drop("row_num")
# drop zero info columns
zero_info = []
for col_name in df_gold.columns:
    distinct_count = df_gold.select(col_name).distinct().count()
    if distinct_count <= 1:
        constant_val = df_gold.select(col_name).first()[col_name]
        zero_info.append(col_name)
        print(f"  Dropping '{col_name}' — only 1 unique value: '{constant_val}'")
df_gold = df_gold.drop(*zero_info)
(
  df_gold.write.format("delta").mode("overwrite").option("overwriteSchema","true")
  .saveAsTable(GOLD_TABLE)
)

print("gold - complete")
print(f"rows : {df_gold.count()}")
print(f"columns : {len(df_gold.columns)}")
print(f"columns : {df_gold.columns}")
df_gold.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Quality Report
# MAGIC
# MAGIC Summary of records lost at each layer and the reason why.
# MAGIC This report gives a complete picture of the pipeline results
# MAGIC and documents every data quality issue found in the source.

# COMMAND ----------

# all layers accurate counts
bronze_count = spark.read.table(BRONZE_TABLE).count()
silver_count = spark.read.table(SILVER_TABLE).count()
gold_count   = spark.read.table(GOLD_TABLE).count()

# build summary report
report = spark.createDataFrame([
   ("Bronze",bronze_count,0,"Raw ingestion - no filtering"
),
(
        "Silver",
        silver_count,
        bronze_count - silver_count,
        "Missing values, type errors, invalid values"
    ),
    (
        "Gold",
        gold_count,
        silver_count - gold_count,
        "Business rule: operating_hours > 120,000"
),  
   
],["Layer", "Row Count", "Records Dropped", "Reason"])

print("pipeline summary")
report.show(truncate=False)


df_raw = spark.read.table(BRONZE_TABLE).drop("ingested_at")
 
df_raw = df_raw.withColumn(
    "row_num",
    F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.monotonically_increasing_id()))
)
  
# 1. invalid issue_type
bad_issue_df = df_raw.filter(
    F.col("issue_type").isNotNull() &
    ~F.col("issue_type").isin(VALID_ISSUE_TYPES)
)
bad_issue_vals = [r["issue_type"] for r in bad_issue_df.select("issue_type").distinct().collect()]
bad_issue_rows = sorted([r["row_num"] for r in bad_issue_df.select("row_num").collect()])
 
# 2. invalid past_dmg
# In Bronze past_dmg is still a string, so we compare against ["0","1"]
bad_dmg_df = df_raw.filter(
    F.col("past_dmg").isNotNull() &
    ~F.col("past_dmg").isin(["0", "1"])
)
bad_dmg_vals = [r["past_dmg"] for r in bad_dmg_df.select("past_dmg").distinct().collect()]
bad_dmg_rows = sorted([r["row_num"] for r in bad_dmg_df.select("row_num").collect()])
 
# 3. rows with missing values (ignore op_set_2 which is intentionally fully empty)
cols_to_check = [c for c in df_raw.columns if c not in ("op_set_2", "row_num")]
null_cond = None
for c in cols_to_check:
    cond = F.col(c).isNull() | (F.trim(F.col(c)) == "")
    null_cond = cond if null_cond is None else null_cond | cond
missing_rows = [r["row_num"] for r in df_raw.filter(null_cond).select("row_num").collect()]
 
# 4. type errors — non-numeric values in numeric columns
numeric_cols_report = ["oph","pist_m","bmep","ng_imp","past_dmg",
    "resting_analysis_results","rpm_max","full_load_issues",
    "number_up","number_tc","op_set_1","op_set_3","high_breakdown_risk"]
type_errors = []
for c in numeric_cols_report:
    bad = df_raw.filter(
        F.col(c).isNotNull() &
        (F.trim(F.col(c)) != "") &
        F.isnull(F.expr(f"try_cast(`{c}` as DOUBLE)"))
    )
    for r in bad.select("row_num", c).collect():
        type_errors.append((r["row_num"], c, r[c]))
 
# 5. OPH business rule violations
oph_violations = df_raw.filter(
    F.expr("try_cast(`oph` as LONG)").isNotNull() &
    (F.expr("try_cast(`oph` as LONG)") > OPH_MAX)
)
oph_viol_rows = sorted(
    [(r["row_num"], r["oph"]) for r in oph_violations.select("row_num", "oph").collect()]
)
 
# 6. fully empty columns
# A column with zero non-null, non-empty values is fully empty
fully_empty_cols = [
    c for c in df_raw.columns
    if c != "row_num" and
    df_raw.filter(F.col(c).isNotNull() & (F.trim(F.col(c)) != "")).count() == 0
]
 
# 7. zero-variance columns
zero_var_cols = []
for c in df_raw.columns:
    if c in ("row_num",) + tuple(fully_empty_cols):
        continue
    vals = [r[c] for r in df_raw.filter(F.col(c).isNotNull()).select(c).distinct().collect()]
    if len(vals) == 1:
        zero_var_cols.append((c, vals[0]))
 
print("Data quality issues found in source data:")
print(f"1. Invalid issue_type   : values={bad_issue_vals}  rows={bad_issue_rows}")
print(f"2. Invalid past_dmg     : values={bad_dmg_vals}  rows={bad_dmg_rows}")
print(f"3. Missing value rows   : {len(missing_rows)} row(s) — row numbers {missing_rows}")
if type_errors:
    for row_num, col, val in type_errors:
        print(f"4. Type error: row {row_num} | column '{col}' = '{val}' (not numeric)")
else:
    print("4. Type errors: none found")
if oph_viol_rows:
    for row_num, val in oph_viol_rows:
        print(f"5. OPH rule violation   : row {row_num} | oph={val} (limit={OPH_MAX:,})")
else:
    print(f"5. OPH violations       : none found")
print(f"6. Fully empty columns  : {fully_empty_cols} — dropped in Silver before dropna()")
for col, val in zero_var_cols:
    print(f"7. Zero-variance column : '{col}' — constant='{val}' — dropped in Gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Quality Tests
# MAGIC
# MAGIC Automated assertions that verify the Gold layer is correct.
# MAGIC Each test checks one specific rule. If any rule is violated
# MAGIC the pipeline stops immediately and raises an error.
# MAGIC
# MAGIC This is called "failing loudly" — bad data never silently
# MAGIC reaches analysts. Every test must print PASS before the
# MAGIC pipeline is considered complete.

# COMMAND ----------

def assert_test(condition,test_name,detail=""):
  status = "✔ PASS" if condition else "X FAIL"
  msg = f"{status} | {test_name}"
  if detail:
    msg +=f" | {detail}"
  print(msg)
  if not condition:
    raise AssertionError(f"Quality test has failed: {test_name}")
df_g = spark.read.table(GOLD_TABLE)
total = df_g.count()

print("Running data quality tests on gold layer..")

# t1 no nulls in any column
for col_name in df_g.columns:
  nulls = df_g.filter(F.col(col_name).isNull()).count()
  assert_test(nulls == 0,f"No nulls in '{col_name}'")
# t2 business rule
violations = df_g.filter(F.col("operating_hours")>OPH_MAX).count()
assert_test(violations==0,f"operating_hours <= {OPH_MAX:,}")

# t3 valid issue types only 
bad = df_g.filter(~F.col("combustion_issue_type").isin(VALID_ISSUE_TYPES)).count()
assert_test(bad==0,"combustion_issue_type valid values")

# t4 past damage only 0 or 1 
bad_dmg = df_g.filter(~F.col("has_past_damage").isin(VALID_PAST_DMG)).count()
assert_test(bad_dmg == 0, "has_past_damage in {0, 1}")
# t5 resting result only 0, 1, or 2
bad_rar = df_g.filter(
    ~F.col("resting_analysis_result").isin(VALID_RESTING)
).count()
assert_test(bad_rar == 0, "resting_analysis_result in {0, 1, 2}")

# t6 gold is not empty
assert_test(total > 0, "Gold layer is not empty", f"{total} rows")

# t7  zero-info columns are gone
for col_name in ["operational_setting_1", "operational_setting_3", "op_set_2"]:
    assert_test(
        col_name not in df_g.columns,
        f"'{col_name}' absent from Gold"
    )

print(f"All tests passed. gold layer has {total} trustworthy rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Exploratory Statistics — Gold Layer
# MAGIC
# MAGIC These queries analyse the Gold layer to answer the core
# MAGIC business question: which factors influence engine breakdown risk?
# MAGIC
# MAGIC Each query looks at a different variable and its relationship
# MAGIC to high_breakdown_risk. These results can be used to build
# MAGIC a Databricks dashboard.

# COMMAND ----------

# Create a semantic view for analysts
# This abstracts the Gold table and provides a stable interface

spark.sql(f"""
    CREATE OR REPLACE VIEW {GOLD_VIEW} AS
    SELECT * FROM {GOLD_TABLE}
""")
print(f"View created: {GOLD_VIEW}")

# COMMAND ----------

print("6.1 — Breakdown Risk Distribution")

spark.sql(f"""
    SELECT high_breakdown_risk,
        COUNT(*) AS engine_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
    FROM {GOLD_TABLE}
    GROUP BY high_breakdown_risk
    ORDER BY high_breakdown_risk
""").show()

print("6.2 — Combustion Issue Type vs Breakdown Risk")
spark.sql(f"""
    SELECT
        combustion_issue_type,
        COUNT(*) AS total_engines,
        SUM(high_breakdown_risk) AS high_risk_count,
        ROUND(AVG(high_breakdown_risk) * 100, 1) AS high_risk_pct
    FROM {GOLD_TABLE}
    GROUP BY combustion_issue_type
    ORDER BY high_risk_pct DESC
""").show()

print("6.3 — Operating Hours by Breakdown Risk")
spark.sql(f"""
    SELECT
        high_breakdown_risk,
        ROUND(AVG(operating_hours), 0) AS avg_oph,
        MIN(operating_hours) AS min_oph,
        MAX(operating_hours) AS max_oph
    FROM {GOLD_TABLE}
    GROUP BY high_breakdown_risk
    ORDER BY high_breakdown_risk
""").show()

print("6.4 — Past Damage Impact on Breakdown Risk")
spark.sql(f"""
    SELECT
        has_past_damage,
        high_breakdown_risk,
        COUNT(*) AS count
    FROM {GOLD_TABLE}
    GROUP BY has_past_damage, high_breakdown_risk
    ORDER BY has_past_damage, high_breakdown_risk
""").show()

print("6.5 — Resting Analysis Result vs Breakdown Risk")
spark.sql(f"""
    SELECT
        resting_analysis_result,
        CASE resting_analysis_result
            WHEN 0 THEN 'Normal'
            WHEN 1 THEN 'Abnormal'
            WHEN 2 THEN 'Critical'
        END AS label,
        COUNT(*) AS count,
        ROUND(AVG(high_breakdown_risk) * 100, 1) AS pct_high_risk
    FROM {GOLD_TABLE}
    GROUP BY resting_analysis_result
    ORDER BY resting_analysis_result
""").show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Dashboard Visualizations
# MAGIC
# MAGIC **Purpose:** Create interactive charts for Databricks Dashboard
# MAGIC
# MAGIC **Instructions:**
# MAGIC 1. Run each code cell below
# MAGIC 2. For each output, click the chart icon (📊) above the output
# MAGIC 3. Configure chart type as described
# MAGIC 4. Click "Add to Dashboard" → Create new dashboard → Name it "Engine Failure Analysis Dashboard"
# MAGIC 5. Arrange charts and save

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chart 1: Breakdown Risk Distribution
# MAGIC **Chart Type:** Pie Chart
# MAGIC **Business Question:** What percentage of engines are high-risk vs low-risk?
# MAGIC **Note:** Percentages are calculated dynamically from the data

# COMMAND ----------

display(spark.sql(f"""
    WITH risk_stats AS (
    SELECT 
        high_breakdown_risk as risk_numeric,
        COUNT(*) AS engine_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS percentage
    FROM {GOLD_VIEW}
    GROUP BY high_breakdown_risk
)
SELECT 
    CASE risk_numeric
        WHEN 1 THEN CONCAT('High Risk Engines (', percentage, '%)')
        WHEN 0 THEN CONCAT('Low Risk Engines (', percentage, '%)')
    END AS risk_category,
    engine_count
FROM risk_stats
ORDER BY risk_numeric DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chart 2: Combustion Issue Type vs Breakdown Risk
# MAGIC **Chart Type:** Horizontal Bar Chart
# MAGIC **Business Question:** Which issue types lead to the highest breakdown risk?

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
    combustion_issue_type,
    ROUND(AVG(high_breakdown_risk) * 100, 1) AS risk_percentage,
    COUNT(*) AS engine_count
FROM {GOLD_VIEW}
GROUP BY combustion_issue_type
ORDER BY risk_percentage DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chart 3: Past Damage Impact on Breakdown Risk
# MAGIC **Chart Type:** Bar Chart
# MAGIC **Business Question:** Does past damage increase breakdown risk?

# COMMAND ----------


display(spark.sql(f"""
    WITH damage_stats AS (
    SELECT 
        has_past_damage,
        ROUND(AVG(high_breakdown_risk) * 100, 1) AS risk_percentage,
        COUNT(*) AS engine_count
    FROM {GOLD_VIEW}
    GROUP BY has_past_damage
)
SELECT 
    CASE has_past_damage
        WHEN 1 THEN CONCAT('Has Past Damage (', risk_percentage, '% Risk)')
        WHEN 0 THEN CONCAT('No Past Damage (', risk_percentage, '% Risk)')
    END AS damage_status,
    risk_percentage,
    engine_count
FROM damage_stats
ORDER BY has_past_damage DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chart 4: Resting Analysis Results — Strongest Risk Predictor
# MAGIC **Chart Type:** Bar Chart with Color Gradient
# MAGIC **Business Question:** How do resting analysis results predict breakdown risk?

# COMMAND ----------

display(spark.sql(f"""
    WITH resting_stats AS (
    SELECT 
        resting_analysis_result,
        ROUND(AVG(high_breakdown_risk) * 100, 1) AS risk_percentage,
        COUNT(*) AS engine_count
    FROM {GOLD_VIEW}
    GROUP BY resting_analysis_result
)
SELECT 
    CASE resting_analysis_result
        WHEN 0 THEN CONCAT('Normal (', risk_percentage, '% Risk)')
        WHEN 1 THEN CONCAT('Abnormal (', risk_percentage, '% Risk)')
        WHEN 2 THEN CONCAT('Critical (', risk_percentage, '% Risk)')
    END AS result_type,
    risk_percentage,
    engine_count
FROM resting_stats
ORDER BY resting_analysis_result
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chart 5: Operating Hours Distribution by Risk Level
# MAGIC **Chart Type:** Box Plot
# MAGIC **Business Question:** Do high-risk engines operate longer hours?
# MAGIC **Note:** This chart works best as a Box Plot to show distribution comparison

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
    CASE high_breakdown_risk
        WHEN 1 THEN 'High Risk'
        WHEN 0 THEN 'Low Risk'
    END AS risk_level,
    operating_hours
FROM {GOLD_VIEW}
ORDER BY operating_hours
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chart 6: Data Quality Issues Summary
# MAGIC **Chart Type:** Table
# MAGIC **Purpose:** Show what quality issues were discovered and fixed in the pipeline
# MAGIC **Value:** Demonstrates data quality rigor to stakeholders

# COMMAND ----------

rows = []
 
if bad_issue_vals:
    rows.append(("Invalid issue_type",
        f"Values {bad_issue_vals} in rows {bad_issue_rows}", "Silver", "Critical"))
 
if bad_dmg_vals:
    rows.append(("Invalid past_dmg",
        f"Values {bad_dmg_vals} in rows {bad_dmg_rows}", "Silver", "Critical"))
 
if missing_rows:
    rows.append(("Missing values",
        f"{len(missing_rows)} rows: {missing_rows}", "Silver", "Warning"))
 
for row_num, col, val in type_errors:
    rows.append(("Type error",
        f"Row {row_num}: '{col}' = '{val}'", "Silver", "Warning"))
 
for row_num, val in oph_viol_rows:
    rows.append(("OPH rule violation",
        f"Row {row_num}: oph={val} > {OPH_MAX:,}", "Gold", "Critical"))
 
for col in fully_empty_cols:
    rows.append(("Fully empty column",
        f"'{col}' — 100% null, dropped before dropna()", "Silver", "Info"))
 
for col, val in zero_var_cols:
    rows.append(("Zero-variance column",
        f"'{col}' — constant='{val}', dropped in Gold", "Gold", "Info"))
 
display(spark.createDataFrame(
    rows,
    ["Issue Category", "Details", "Removed In Layer", "Severity"]
))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chart 7: Pipeline Summary — Row Count Progression
# MAGIC **Chart Type:** Bar Chart or Table
# MAGIC **Business Question:** How many rows were removed at each layer?

# COMMAND ----------

display(spark.createDataFrame([
    ("Bronze (Raw)",          bronze_count, 0,                          "Raw ingestion — no filtering"),
    ("Silver (Cleaned)",      silver_count, bronze_count - silver_count, "Missing + type errors + invalid values"),
    ("Gold (Business Ready)", gold_count,   silver_count - gold_count,   "OPH > 120,000 business rule"),
], ["Layer", "Row Count", "Rows Dropped", "Reason"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Dashboard Summary
# MAGIC
# MAGIC The visualizations in Section 6 power the Databricks dashboard.
# MAGIC Each display() call above produces a chart that has been added
# MAGIC to the Engine Failure Analysis Dashboard.
# MAGIC
# MAGIC | Chart | Type | Key Insight |
# MAGIC |-------|------|-------------|
# MAGIC | 1. Risk Distribution | Pie Chart | Overall fleet risk split |
# MAGIC | 2. Issue Type vs Risk | Bar Chart | Which issue type is most dangerous |
# MAGIC | 3. Past Damage Impact | Bar Chart | Effect of past damage on risk |
# MAGIC | 4. Resting Results | Bar Chart | Critical = highest predictor |
# MAGIC | 5. Operating Hours | Box Plot | Hours distribution by risk level |
# MAGIC | 6. Data Quality Issues | Table | All issues found and fixed |
# MAGIC | 7. Pipeline Summary | Table | 319 → 314 → 313 row progression |