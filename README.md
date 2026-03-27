# Engine Failure Analysis — INNIO Data Engineering Assignment

## Overview

This project implements a **Bronze → Silver → Gold medallion architecture** pipeline to analyse engine failure data. The pipeline ingests raw engine sensor data, applies data quality checks at each layer, and delivers a clean, trusted dataset ready for business analysis and dashboarding.

---

## Architecture

```
source_data.csv  (319 rows, 15 columns)
        │
        ▼
┌─────────────────────────────────────────────┐
│  BRONZE — Raw ingestion                     │
│  319 rows · 16 columns (+ ingested_at)      │
│  No filtering · all columns as strings      │
│  Immutable audit trail of source data       │
└─────────────────────────────────────────────┘
        │  Input quality tests applied
        ▼  (5 rows removed)
┌─────────────────────────────────────────────┐
│  SILVER — Cleaned and validated             │
│  314 rows · 14 columns                      │
│  • Whitespace trimmed                       │
│  • Empty strings converted to null          │
│  • op_set_2 dropped (100% empty)            │
│  • Missing values removed                   │
│  • Types cast with try_cast (fault-safe)    │
│  • Invalid category values removed          │
│  • Column names expanded                    │
└─────────────────────────────────────────────┘
        │  Business tests applied
        ▼  (1 row removed)
┌─────────────────────────────────────────────┐
│  GOLD — Business-ready semantic layer        │
│  313 rows · 12 columns                      │
│  • Business rule: oph <= 120,000            │
│  • Zero-variance columns removed            │
│  • 7 automated quality tests pass           │
└─────────────────────────────────────────────┘
```

---

## Data Quality Issues Found

| Issue | Column | Detail | Removed In |
|-------|--------|--------|------------|
| Invalid value | `issue_type` | `'ddddd'` and `'non-typical'` not in allowed set | Silver |
| Invalid value | `past_dmg` | Value `2` found — only 0 or 1 allowed | Silver |
| Missing values | Multiple | 2 rows with empty fields (rows 169, 255) | Silver |
| Type error | `oph`, `rpm_max` | `'kkkkk'` and `'kartsi'` — not numeric (row 319) | Silver |
| Business violation | `oph` | Value `1,000,000,000` — exceeds 120,000 limit (row 125) | Gold |
| Zero-info column | `op_set_2` | 100% empty across all rows | Silver |
| Zero-info column | `op_set_1` | Constant value of `1` in every row | Gold |
| Zero-info column | `op_set_3` | Constant value of `0` in every row | Gold |

---

## Key Features

- **Dynamic row tracking** — every filter step prints the exact row numbers dropped and why
- **Fault-tolerant casting** — `try_cast` instead of `cast` prevents pipeline crashes on bad values
- **Idempotent pipeline** — safe to re-run multiple times; always produces the same result
- **7 automated quality tests** — Gold layer is verified before any analyst can query it
- **Fully dynamic quality report** — queries Bronze directly, no hardcoded row numbers
- **Databricks dashboard** — 7 charts covering risk distribution, issue type analysis, and pipeline health

---

## Layer Summary

| Layer | Rows | Columns | Notes |
|-------|------|---------|-------|
| Bronze | 319 | 16 | All source rows + `ingested_at` timestamp |
| Silver | 314 | 14 | After all input quality tests |
| Gold | 313 | 12 | After business tests + zero-variance removal |

---

## Quality Tests (all pass on Gold layer)

| # | Test |
|---|------|
| T1 | No null values in any column |
| T2 | `operating_hours <= 120,000` |
| T3 | `combustion_issue_type` contains only valid values |
| T4 | `has_past_damage` contains only 0 or 1 |
| T5 | `resting_analysis_result` contains only 0, 1, or 2 |
| T6 | Gold layer is not empty |
| T7 | Zero-information columns are absent from Gold |

---

## Project Structure

```
Innio-Data-Engineer/
├── data/
│   └── source_data.csv          ← raw source data (319 rows)
├── docs/
│   └── data_quality_report.md   ← detailed data quality findings
├── notebooks/
│   └── engine_failure_analysis.py  ← main pipeline notebook
└── README.md
```

---

## How to Run

### Requirements
- Azure Databricks workspace (free edition works)
- Python cluster with Databricks Runtime 13+

### Steps

**1. Upload source data**
- In Databricks go to **Catalog → Add data → Upload files to a Volume**
- Upload `data/source_data.csv`
- Note the full volume path (e.g. `/Volumes/innio_workspace/default/my_volume/source_data.csv`)

**2. Import the notebook**
- In Databricks Workspace click **Import**
- Select `notebooks/engine_failure_analysis.py`

**3. Update Cell 0 configuration**
```python
SOURCE_PATH  = "/Volumes/your_catalog/your_schema/your_volume/source_data.csv"
BRONZE_TABLE = "your_catalog.your_schema.bronze_engine_raw"
SILVER_TABLE = "your_catalog.your_schema.silver_engine_clean"
GOLD_TABLE   = "your_catalog.your_schema.gold_engine_semantic"
GOLD_VIEW    = "your_catalog.your_schema.vw_gold_engine"
```

**4. Attach a cluster**
- Click **Connect** at the top of the notebook
- Select or create an All-purpose cluster

**5. Run all cells**
- Click **Run All**
- Expected output:
  - Bronze: 319 rows
  - Silver: 314 rows (5 dropped with exact row numbers printed)
  - Gold: 313 rows (1 dropped with exact row number printed)
  - 7 quality tests: all ✔ PASS

---

## Tools Used

| Tool | Purpose |
|------|---------|
| Azure Databricks | Notebook execution and dashboard |
| PySpark | Distributed data processing |
| Delta Lake | ACID-compliant table storage |
| Unity Catalog | Table and data governance |
| Python | Pipeline logic and quality tests |

---

## Business Insights (from Gold layer)

| Finding | Value |
|---------|-------|
| High breakdown risk engines | 54.3% (170 of 313) |
| Highest risk combustion type | Atypical — 82.4% breakdown rate |
| Lowest risk combustion type | Typical — 26.4% breakdown rate |
| Resting result = Abnormal | 62.0% breakdown risk |
| Resting result = Normal | 47.0% breakdown risk |
| Past damage effect | Minimal — 50% vs 55.1% |
