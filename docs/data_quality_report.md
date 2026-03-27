# Data Quality Report — Engine Failure Analysis

**Project:** INNIO Data Engineering Intern Assignment  
**Author:** Muhammad Muzammal Hussain  
**Date:** March 2026  
**Source file:** `source_data.csv` (319 rows, 15 columns)

---

## Pipeline Overview

The pipeline implements a Bronze → Silver → Gold medallion architecture.  
Each layer applies progressively stricter quality rules.

```
source_data.csv  (319 rows)
       │
       ▼
   BRONZE  →  319 rows  (raw, no filtering)
       │
       ▼  [5 rows removed]
   SILVER  →  314 rows  (cleaned, typed, validated)
       │
       ▼  [1 row removed]
   GOLD    →  313 rows  (business-ready)
```

---

## Data Quality Issues Found

### Issue 1 — Invalid `issue_type` values
| Field | Detail |
|-------|--------|
| Column | `issue_type` |
| Invalid values | `'ddddd'`, `'non-typical'` |
| Affected rows | Row 315, Row 318 |
| Allowed values | `typical`, `atypical`, `non-related`, `non-symptomatic` |
| Removed in | Silver — Step 6 (value validity check) |
| Severity | Critical |

### Issue 2 — Invalid `past_dmg` value
| Field | Detail |
|-------|--------|
| Column | `past_dmg` |
| Invalid value | `'2'` |
| Affected rows | Row 319 |
| Allowed values | `0` (no past damage), `1` (has past damage) |
| Removed in | Silver — Step 5 (type cast makes it null via try_cast) and Step 6 |
| Severity | Critical |

### Issue 3 — Missing values
| Field | Detail |
|-------|--------|
| Affected rows | Row 169, Row 255 |
| Missing columns | `pist_m`, `issue_type`, `bmep`, `ng_imp`, `past_dmg`, `resting_analysis_results`, `rpm_max`, `full_load_issues`, `number_up`, `number_tc`, `op_set_1`, `op_set_3`, `high_breakdown_risk` |
| Removed in | Silver — Step 4 (completeness check via `dropna()`) |
| Severity | Warning |

### Issue 4 — Type errors (non-numeric values in numeric columns)
| Field | Detail |
|-------|--------|
| Column `oph` | Value `'kkkkk'` in Row 319 — not a valid integer |
| Column `rpm_max` | Value `'kartsi'` in Row 319 — not a valid integer |
| Removed in | Silver — Step 5 (`try_cast` returns null, then `dropna()` removes the row) |
| Severity | Warning |

### Issue 5 — Business rule violation
| Field | Detail |
|-------|--------|
| Column | `oph` (operating hours) |
| Invalid value | `1,000,000,000` |
| Affected row | Row 125 |
| Business rule | `operating_hours <= 120,000` |
| Removed in | Gold — business rule filter |
| Severity | Critical |

### Issue 6 — Fully empty column
| Field | Detail |
|-------|--------|
| Column | `op_set_2` |
| Detail | 100% null — 319 of 319 rows have no value |
| Action | Dropped in Silver **before** `dropna()` — if dropped after, all 319 rows would be eliminated because every row has a null in this column |
| Severity | Info |

### Issue 7 — Zero-variance columns
| Column | Constant value | Detail |
|--------|---------------|--------|
| `op_set_1` | `1` | Every single row has value 1. Cannot distinguish any engine. |
| `op_set_3` | `0` | Every single row has value 0. Cannot distinguish any engine. |

Both dropped automatically in Gold. A constant column has zero analytical value — it cannot explain why some engines fail and others do not.

---

## Layer Summary

| Layer | Rows | Columns | Records Dropped | Reason |
|-------|------|---------|-----------------|--------|
| Bronze | 319 | 16 | 0 | Raw ingestion — no filtering |
| Silver | 314 | 14 | 5 | Missing values + type errors + invalid values |
| Gold | 313 | 12 | 1 | Business rule: `operating_hours > 120,000` |

---

## Silver Layer — Step by Step Drop Tracking

The pipeline prints exact row numbers at every step:

```
Step 4 — completeness:
  Dropped 2 row(s) — missing values in one or more columns
  Row numbers: [169, 255]

Step 5 — type cast:
  Dropped 1 row(s) — type error — non-numeric value in numeric column
  Row numbers: [319]

Step 6 — value validity:
  Dropped 2 row(s) — value outside allowed set
  Row numbers: [315, 318]
```

---

## Gold Layer — Business Rule Drop Tracking

```
Gold — business rule:
  Dropped 1 row(s) — business rule: operating_hours > 120,000
  Row numbers: [125]
```

---

## Automated Quality Tests on Gold Layer

7 tests run automatically. All must pass before the pipeline is considered complete.

| # | Test | Result |
|---|------|--------|
| T1 | No nulls in any column | ✔ PASS |
| T2 | `operating_hours <= 120,000` | ✔ PASS |
| T3 | `combustion_issue_type` only valid values | ✔ PASS |
| T4 | `has_past_damage` only 0 or 1 | ✔ PASS |
| T5 | `resting_analysis_result` only 0, 1, or 2 | ✔ PASS |
| T6 | Gold layer is not empty | ✔ PASS (313 rows) |
| T7 | Zero-info columns absent from Gold | ✔ PASS |

---

## Gold Layer — Final Column List

| Column | Type | Description |
|--------|------|-------------|
| `operating_hours` | integer | Engine operating hours |
| `piston_material` | integer | Material type of pistons |
| `combustion_issue_type` | string | Type of combustion issue |
| `brake_mean_effective_pressure` | double | Average pressure on pistons |
| `natural_gas_impurities_nmol` | double | Gas impurity level in nmol |
| `has_past_damage` | integer | 0 = no past damage, 1 = has past damage |
| `resting_analysis_result` | integer | 0 = normal, 1 = abnormal, 2 = critical |
| `max_rotations_per_minute` | integer | Maximum RPM achieved |
| `has_full_load_issues` | integer | 0 = no, 1 = yes |
| `unplanned_events_count` | integer | Number of unplanned events |
| `turbocharger_count` | integer | Number of installed turbochargers |
| `high_breakdown_risk` | integer | 0 = low risk, 1 = high risk |

---

## Key Business Insights from Gold Layer

| Finding | Detail |
|---------|--------|
| Overall risk | 54.3% of engines are high breakdown risk |
| Highest risk issue type | Atypical combustion (82.4% breakdown rate) |
| Lowest risk issue type | Typical combustion (26.4% breakdown rate) |
| Past damage effect | Minimal — 50% risk with damage vs 55.1% without |
| Resting result = Abnormal | 62.0% breakdown risk |
| Resting result = Normal | 47.0% breakdown risk |
| Operating hours | Similar distribution across risk groups (avg ~54k hrs) |
