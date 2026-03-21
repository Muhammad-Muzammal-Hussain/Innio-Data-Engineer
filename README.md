# Engine Failure Analysis — INNIO Data Engineering Assignment

## Overview

This project implements a **Bronze → Silver → Gold medallion architecture** pipeline
to analyse engine failure data. The pipeline ingests raw engine sensor data,
applies data quality checks at each layer, and delivers a clean, trusted dataset
ready for business analysis.

---

## Architecture

```
source_data.csv (319 rows)
        │
        ▼
┌─────────────────────────────────────────┐
│  BRONZE — Raw ingestion                 │
│  All 319 rows · all 15 columns          │
│  No filtering · everything as string    │
└─────────────────────────────────────────┘
        │  Input quality tests applied
        ▼
┌─────────────────────────────────────────┐
│  SILVER — Cleaned and validated         │
│  314 rows · 14 columns                  │
│  Missing values removed                 │
│  Types cast · invalid values removed    │
│  Column names expanded                  │
└─────────────────────────────────────────┘
        │  Business tests applied
        ▼
┌─────────────────────────────────────────┐
│  GOLD — Business-ready semantic layer   │
│  313 rows · 11 columns                  │
│  Business rule applied (oph <= 120,000) │
│  Zero-information columns removed       │
└─────────────────────────────────────────┘
```

---

## Data Quality Issues Found

During analysis of source_data.csv, the following issues were identified:

| Issue | Column | Detail | Layer that removes it |
|-------|--------|--------|----------------------|
| Invalid value | issue_type | Values 'ddddd' and 'non-typical' not in allowed set | Silver |
| Invalid value | past_dmg | Value 2 found — only 0 or 1 allowed | Silver |
| Missing values | Multiple | 2 rows with empty fields across multiple columns | Silver |
| Business violation | oph | Value 1,000,000,000 — exceeds 120,000 limit | Gold |
| Zero-information column | op_set_2 | 100% empty across all rows | Silver |
| Zero-information column | op_set_1 | Constant value of 1 in every row | Gold |
| Zero-information column | op_set_3 | Constant value of 0 in every row | Gold |

---

## Layer Summary

| Layer | Rows | Columns | Notes |
|-------|------|---------|-------|
| Bronze | 319 | 16 | All source rows + ingested_at timestamp |
| Silver | 314 | 14 | After input quality tests |
| Gold | 313 | 11 | After business tests |

---

## Quality Tests

7 automated data quality tests run on the Gold layer:

- No null values in any column
- operating_hours <= 120,000 (business rule)
- combustion_issue_type contains only valid values
- has_past_damage contains only 0 or 1
- resting_analysis_result contains only 0, 1, or 2
- Gold layer is not empty
- Zero-information columns are absent from Gold

All 7 tests pass on the Gold layer.

---

## How to Run

### Requirements
- Azure Databricks workspace
- Python cluster with Databricks Runtime 13+

### Steps

1. Upload source data
   - In Databricks go to Catalog > Add data > Upload files to a Volume
   - Upload data/source_data.csv
   - Note the full volume path

2. Import the notebook
   - In Databricks Workspace click Import
   - Select notebooks/engine_failure_analysis.py

3. Update the source path
   - In Cell 0 update SOURCE_PATH to match your volume path
   - Update BRONZE_TABLE, SILVER_TABLE, GOLD_TABLE to match your catalog

4. Attach a cluster
   - Click Connect at the top of the notebook
   - Select or create an All-purpose cluster

5. Run all cells
   - Click Run All
   - Expected: Bronze 319 rows > Silver 314 rows > Gold 313 rows > 7 tests PASS

---

## Tools Used

| Tool | Purpose |
|------|---------|
| Azure Databricks | Notebook execution environment |
| PySpark | Data processing and transformation |
| Delta Lake | Table storage format |
| Unity Catalog | Table and data governance |
| Python | Pipeline logic and quality tests |

---

## Author

Muhammad Muzammal Hussain
GitHub: https://github.com/Muhammad-Muzammal-Hussain
  
