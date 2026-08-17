# How the numbers were produced

Every figure in the README and in `reports/metrics.md` comes from
`python -m etl.metrics`, which reads three tables the pipeline wrote while it
was running: `etl.etl_run_log`, `etl.etl_run_entity` and
`etl.etl_validation_log`. Nothing is estimated or entered by hand, and there is
no code path that lets a number be supplied from outside the database.

This document says exactly what was real, what was simulated, and which claims
the evidence does **not** support.

---

## What was real

**The database.** SQL Server 2022 Developer Edition (16.0.4265.3, RTM-CU26),
running in Docker on macOS 26.5 / Apple M1, amd64 under Rosetta translation. A
genuine SQL Server engine, not an emulator or a compatible substitute.

**The data.** Microsoft's AdventureWorks2022 sample OLTP database, restored from
the official `AdventureWorks2022.bak` (~200 MB) published on the
[sql-server-samples releases page](https://github.com/Microsoft/sql-server-samples/releases/tag/adventureworks).
Baseline at restore:

| Table | Rows | ModifiedDate range |
| --- | ---: | --- |
| `Sales.SalesOrderHeader` | 31,465 | 2011-06-07 → 2014-07-07 |
| `Sales.SalesOrderDetail` | 121,317 | 2011-05-31 → 2014-06-30 |

**The pipeline.** Every stage ran for real: extraction across two separate
databases, bulk load into staging, the metadata-driven rule engine, `MERGE`
upserts into the warehouse, all validation checks, alert dispatch, and watermark
management. The initial full load moved all 152,782 rows.

**The failures.** The one failed run in the history was a real failure. Twelve
deliberately corrupted rows were quarantined, the run was marked `Failed`, an
alert was raised, and the watermark was held so the window would be re-read.

---

## What was simulated

AdventureWorks is a static sample database. Restore it, run a full load, and
every subsequent incremental run legitimately finds nothing to do — which proves
the watermark works and proves nothing else. Measuring the pipeline across a
series of scheduled runs needs a source that changes between them.

So `scripts/simulate_source_activity.py` plays the part of the AdventureWorks
Cycles order-entry system: it books new orders and amends existing ones, using
ordinary `INSERT` and `UPDATE` statements against the **source** database. It
never touches the target. The pipeline then has to discover those changes on its
own, through the `ModifiedDate` watermark, exactly as it would with a live OLTP
system.

Six simulated nights were run, in three shapes:

| Preset | New orders | Amended lines |
| --- | ---: | ---: |
| `quiet-night` | 12 | 8 |
| `normal-night` | 45 | 30 |
| `busy-night` | 160 | 90 |

**What this means for the numbers.** The row counts in the run log are real rows
that really moved through the pipeline. What is synthetic is the *arrival
pattern* — a real sales system would not produce exactly 160 orders on cue. Read
the per-run row counts as "the pipeline moved this much data correctly", not as
a forecast of production volume.

`scripts/inject_data_faults.py` similarly corrupts source rows to exercise the
validation layer. One detail is worth stating plainly: **AdventureWorks rejects
these faults on its own.** `Sales.SalesOrderHeader` carries CHECK constraints
(`CK_SalesOrderHeader_DueDate`, `_ShipDate`, `_Freight`) that make the
corruptions impossible upstream. The harness disables those constraints for the
injection and re-enables them `WITH CHECK` afterwards, simulating a source whose
guarantees are weaker than this one's — a legacy system, a third-party feed, a
replica with constraints disabled for a bulk load. That is the situation the
validation layer exists for, and it is not the situation AdventureWorks itself
presents.

---

## What each metric means

### Runs completed without human involvement — **88.9%** (8 of 9)

Share of completed runs where `required_manual_intervention = 0`. That flag is
set by `etl.usp_EndRun`, not by a person, and it is set to 1 exactly when the
run's final status is `Failed`. Warnings do not count: a `CompletedWithWarnings`
run loaded its data correctly and needs review on someone's own schedule, not an
interruption.

The single flagged run is the fault-injection run, where intervention was the
correct response — twelve corrupted rows really did need a human to look at
them.

**This is not a measure of effort saved.** See "What these numbers do not say".

### Rows loaded as a share of rows extracted — **99.992%** (155,065 of 155,077)

Sum of `rows_loaded ÷ rows_extracted` across every run. The 12-row shortfall is
the injected faults, quarantined in `etl.etl_rejected_row` rather than loaded.

This is a throughput figure, not an accuracy figure. It says the pipeline loaded
everything it was given except what it deliberately rejected. On a run with no
injected faults it is 100%.

### Check pass rate — **99.074%** (321 of 324)

Every row in `etl.etl_validation_log`, across every run. The three failures are
the three Critical rules that caught the injected faults — `DATE_DueBeforeOrder`,
`DATE_ShipBeforeOrder` and `NEG_Amounts`, one occurrence each.

A failing check is the system working. A pass rate of 100% across a history that
included corrupted data would mean the checks were not looking.

### Row-count reconciliation pass rate — **100%** (36 of 36)

`RECON_SourceToStaging` and `RECON_StagingToWarehouse`, per entity, per run.
Two independent comparisons:

- What the **source** reported it had in the window, against what actually
  landed in staging. The expected value is counted at the source, on the source
  connection, so this compares two separately observed numbers rather than one
  number against itself.
- What landed in staging, against what was loaded plus what was quarantined.
  Rejected rows are accounted for, not ignored — which is what makes the
  reconciliation exact rather than approximate.

Total unexplained row-count variance across the whole history: **0**.

---

## What these numbers do not say

**There is no measurement of the manual process this pipeline replaces.** No
manual export-and-load was ever performed or timed. Any claim of the form
"reduced manual effort by N%" would be an invention, and `etl/metrics.py`
deliberately produces no such figure.

What can honestly be said is narrower and better evidenced: across 9 runs, 8
completed with no human involvement, and the one that did not was a run where a
human was genuinely needed.

**The ~99% data accuracy figure needs care.** "99.992% of extracted rows were
loaded" and "the data is 99.992% accurate" are different claims. The first is
measured here. The second would require a reference dataset to compare the
warehouse against, and no such comparison was made. What *was* verified is that
reconciliation found zero unexplained variance and that no row violating a
Critical rule reached the warehouse — a statement about completeness and
validity, not about semantic correctness.

**Azure Data Factory was never deployed.** The artifacts in `adf/` and the
templates in `infra/` are written against the documented schemas and the 2024
API versions, and the SQL they call is the same SQL these measurements exercised
— but no pipeline run in these metrics was orchestrated by ADF. Every run was
driven by the local runner in `etl/`. See the deployment-status note in
`infra/main.bicep`.

**SSIS is not part of this project.** The original scope allowed for either an
SSIS package or an ADF-native implementation. This is ADF-native: there is no
`.dtsx`, no SSIS Integration Runtime, and no SSIS claim anywhere in this
repository.

---

## Reproducing the measurements

```bash
docker compose up -d
./scripts/restore_adventureworks.sh
./scripts/deploy_schema.sh
./scripts/reset_warehouse.sh --yes
python scripts/run_experiment.py --nights 6 --fault-count 4
python -m etl.metrics --write
```

`run_experiment.py` asserts its expectations rather than reporting whatever
happens: that every source row is accounted for, that injected faults are
quarantined, that no faulty row reaches the warehouse, that an alert is raised,
that the watermark is held on failure, and that the following run recovers
without anyone replaying anything. It exits non-zero if any of that fails to
hold.

The absolute row counts will differ from the published figures — the simulator
uses `NEWID()` ordering for product and customer selection, so the number of
lines per order varies between runs. The properties being asserted do not.
