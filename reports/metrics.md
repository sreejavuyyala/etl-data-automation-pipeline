# Measured pipeline metrics

Generated 2026-08-17T02:46:45+00:00 from `etl.etl_run_log`, `etl.etl_validation_log` 
and `etl.etl_run_entity` on a live SQL Server 2022 instance holding the real
AdventureWorks2022 dataset. Regenerate with `python -m etl.metrics --write`.

## Runs

| Metric | Value |
| --- | ---: |
| Runs completed | 9 |
| — scheduled | 9 |
| — succeeded | 8 |
| — completed with warnings | 0 |
| — failed | 1 |
| Runs that required manual intervention | 1 |
| **Runs completed without human involvement** | **88.9%** |

## Rows

| Metric | Value |
| --- | ---: |
| Rows extracted from source | 155,077 |
| Rows loaded into the warehouse | 155,065 |
| Rows quarantined | 12 |
| **Rows loaded as a share of rows extracted** | **99.992%** |
| Orders in the warehouse | 31,899 |
| Order lines in the warehouse | 122,642 |

## Validation

| Metric | Value |
| --- | ---: |
| Checks executed | 324 |
| Checks passed | 321 |
| Checks failed | 3 |
| — of which Critical | 3 |
| **Check pass rate** | **99.074%** |

## Row-count reconciliation

| Metric | Value |
| --- | ---: |
| Reconciliation checks executed | 36 |
| Reconciliation checks passed | 36 |
| **Reconciliation pass rate** | **100.000%** |
| Total unexplained row-count variance | 0 |

## Alerting

| Metric | Value |
| --- | ---: |
| Alerts raised | 1 |
| Alerts delivered | 1 |

## Performance

| Metric | Value |
| --- | ---: |
| Median run duration | 0.45s |
| Slowest run | 73.42s |
| Total pipeline runtime | 77.42s |

## Which checks failed

| Check | Type | Severity | Occurrences |
| --- | --- | --- | ---: |
| `DATE_DueBeforeOrder` | DomainCheck | Critical | 1 |
| `DATE_ShipBeforeOrder` | DomainCheck | Critical | 1 |
| `NEG_Amounts` | DomainCheck | Critical | 1 |

## What was quarantined

| Entity | Rule | Rows |
| --- | --- | ---: |
| SalesOrderHeader | `DATE_DueBeforeOrder` | 4 |
| SalesOrderHeader | `DATE_ShipBeforeOrder` | 4 |
| SalesOrderHeader | `NEG_Amounts` | 4 |

## Run log

| Run | Trigger | Type | Status | Extracted | Loaded | Quarantined | Checks | Failed | Seconds |
| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | Scheduled | Full | Succeeded | 152,782 | 152,782 | 0 | 36 | 0 | 73.42 |
| 2 | Scheduled | Incremental | Succeeded | 74 | 74 | 0 | 36 | 0 | 0.48 |
| 3 | Scheduled | Incremental | Succeeded | 229 | 229 | 0 | 36 | 0 | 0.45 |
| 4 | Scheduled | Incremental | Succeeded | 829 | 829 | 0 | 36 | 0 | 0.79 |
| 5 | Scheduled | Incremental | Succeeded | 70 | 70 | 0 | 36 | 0 | 0.45 |
| 6 | Scheduled | Incremental | Succeeded | 232 | 232 | 0 | 36 | 0 | 0.39 |
| 7 | Scheduled | Incremental | Succeeded | 837 | 837 | 0 | 36 | 0 | 0.83 |
| 8 | Scheduled | Incremental | Failed | 12 | 0 | 12 | 36 | 3 | 0.36 |
| 9 | Scheduled | Incremental | Succeeded | 12 | 12 | 0 | 36 | 0 | 0.25 |

---

### What these numbers do not say

There is no measurement here of the manual export-and-load process this
pipeline replaces, because that process was never run and never timed. No
percentage reduction in manual effort can honestly be derived from this
data, and none is reported. What *is* measured is how many runs completed
with no human involvement, which is a different claim and the one the
evidence actually supports.

See `docs/metrics-methodology.md` for how each figure is defined and what
was simulated.
