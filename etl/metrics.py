"""Compute pipeline metrics from the run history.

Every number this produces is read out of ``etl.etl_run_log``,
``etl.etl_validation_log`` and ``etl.etl_run_entity`` -- tables the pipeline
wrote while running. Nothing here is estimated, projected, or typed in by hand,
and there is no code path that lets a number be supplied from outside.

    python -m etl.metrics             # print the report
    python -m etl.metrics --write     # also write reports/metrics.{md,json}
    python -m etl.metrics --json      # machine-readable, for CI

What the numbers mean -- and what they do not -- is set out in
docs/metrics-methodology.md. The short version:

  * Everything below describes runs against a real SQL Server 2022 instance
    holding the real AdventureWorks2022 dataset.
  * There is no measurement here of how long the equivalent manual process
    took, because no manual process was ever run. Any claim of the form
    "reduced manual effort by N%" would therefore be an invention, and this
    module deliberately does not produce one.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import REPO_ROOT, Settings
from .db import Database


def _pct(numerator: float, denominator: float) -> float:
    """Percentage, with an empty denominator reported as 0.0 rather than 100."""
    if not denominator:
        return 0.0
    return round(100.0 * numerator / denominator, 4)


@dataclass
class Metrics:
    """Everything measured, in one serialisable object."""

    generated_at_utc: str
    # -- runs ---------------------------------------------------------------
    total_runs: int = 0
    scheduled_runs: int = 0
    succeeded: int = 0
    completed_with_warnings: int = 0
    failed: int = 0
    runs_requiring_manual_intervention: int = 0
    autonomous_run_rate_pct: float = 0.0
    # -- rows ---------------------------------------------------------------
    rows_extracted: int = 0
    rows_loaded: int = 0
    rows_quarantined: int = 0
    row_load_rate_pct: float = 0.0
    # -- validation ---------------------------------------------------------
    checks_executed: int = 0
    checks_passed: int = 0
    checks_failed: int = 0
    check_pass_rate_pct: float = 0.0
    critical_checks_failed: int = 0
    # -- reconciliation -----------------------------------------------------
    reconciliation_checks: int = 0
    reconciliation_passed: int = 0
    reconciliation_pass_rate_pct: float = 0.0
    row_count_variance_total: int = 0
    # -- alerting -----------------------------------------------------------
    alerts_raised: int = 0
    alerts_delivered: int = 0
    # -- performance --------------------------------------------------------
    total_runtime_seconds: float = 0.0
    median_run_seconds: float = 0.0
    slowest_run_seconds: float = 0.0
    # -- warehouse ----------------------------------------------------------
    warehouse_orders: int = 0
    warehouse_order_lines: int = 0
    # -- detail -------------------------------------------------------------
    failed_check_breakdown: list[dict[str, Any]] = field(default_factory=list)
    quarantine_breakdown: list[dict[str, Any]] = field(default_factory=list)
    runs: list[dict[str, Any]] = field(default_factory=list)


def collect(db: Database) -> Metrics:
    """Read the run history and compute every metric."""
    metrics = Metrics(generated_at_utc=datetime.now(timezone.utc).isoformat(timespec="seconds"))

    # -- run-level ----------------------------------------------------------
    # Runs still marked 'Running' are excluded: they have no outcome yet, and
    # counting them would make an in-flight run look like a silent success.
    summary = db.fetch_one(
        """
        SELECT
            total_runs   = COUNT(*),
            scheduled    = SUM(CASE WHEN run_trigger = 'Scheduled' THEN 1 ELSE 0 END),
            succeeded    = SUM(CASE WHEN status = 'Succeeded' THEN 1 ELSE 0 END),
            warned       = SUM(CASE WHEN status = 'CompletedWithWarnings' THEN 1 ELSE 0 END),
            failed       = SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END),
            manual       = SUM(CASE WHEN required_manual_intervention = 1 THEN 1 ELSE 0 END),
            rows_ext     = SUM(rows_extracted),
            rows_load    = SUM(rows_loaded),
            rows_rej     = SUM(rows_rejected),
            total_secs   = SUM(duration_seconds),
            slowest      = MAX(duration_seconds)
        FROM etl.etl_run_log
        WHERE status <> 'Running'
        """
    )

    if summary:
        metrics.total_runs = int(summary["total_runs"] or 0)
        metrics.scheduled_runs = int(summary["scheduled"] or 0)
        metrics.succeeded = int(summary["succeeded"] or 0)
        metrics.completed_with_warnings = int(summary["warned"] or 0)
        metrics.failed = int(summary["failed"] or 0)
        metrics.runs_requiring_manual_intervention = int(summary["manual"] or 0)
        metrics.rows_extracted = int(summary["rows_ext"] or 0)
        metrics.rows_loaded = int(summary["rows_load"] or 0)
        metrics.rows_quarantined = int(summary["rows_rej"] or 0)
        metrics.total_runtime_seconds = round(float(summary["total_secs"] or 0), 3)
        metrics.slowest_run_seconds = round(float(summary["slowest"] or 0), 3)

    metrics.autonomous_run_rate_pct = _pct(
        metrics.total_runs - metrics.runs_requiring_manual_intervention, metrics.total_runs
    )
    metrics.row_load_rate_pct = _pct(metrics.rows_loaded, metrics.rows_extracted)

    median = db.scalar(
        """
        SELECT DISTINCT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_seconds)
                        OVER () AS median_seconds
        FROM etl.etl_run_log
        WHERE status <> 'Running' AND duration_seconds IS NOT NULL
        """
    )
    metrics.median_run_seconds = round(float(median), 3) if median is not None else 0.0

    # -- validation ---------------------------------------------------------
    checks = db.fetch_one(
        """
        SELECT
            executed = COUNT(*),
            passed   = SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END),
            failed   = SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END),
            critical = SUM(CASE WHEN status = 'FAIL' AND severity = 'Critical' THEN 1 ELSE 0 END)
        FROM etl.etl_validation_log
        """
    )
    if checks:
        metrics.checks_executed = int(checks["executed"] or 0)
        metrics.checks_passed = int(checks["passed"] or 0)
        metrics.checks_failed = int(checks["failed"] or 0)
        metrics.critical_checks_failed = int(checks["critical"] or 0)
    metrics.check_pass_rate_pct = _pct(metrics.checks_passed, metrics.checks_executed)

    # -- reconciliation -----------------------------------------------------
    recon = db.fetch_one(
        """
        SELECT
            total    = COUNT(*),
            passed   = SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END),
            variance = SUM(ABS(ISNULL(variance, 0)))
        FROM etl.etl_validation_log
        WHERE check_type = 'RowCountReconciliation'
        """
    )
    if recon:
        metrics.reconciliation_checks = int(recon["total"] or 0)
        metrics.reconciliation_passed = int(recon["passed"] or 0)
        metrics.row_count_variance_total = int(recon["variance"] or 0)
    metrics.reconciliation_pass_rate_pct = _pct(
        metrics.reconciliation_passed, metrics.reconciliation_checks
    )

    # -- alerting -----------------------------------------------------------
    alerts = db.fetch_one(
        "SELECT raised = COUNT(*), delivered = SUM(CASE WHEN delivered = 1 THEN 1 ELSE 0 END) "
        "FROM etl.etl_alert"
    )
    if alerts:
        metrics.alerts_raised = int(alerts["raised"] or 0)
        metrics.alerts_delivered = int(alerts["delivered"] or 0)

    # -- warehouse ----------------------------------------------------------
    metrics.warehouse_orders = int(db.scalar("SELECT COUNT_BIG(*) FROM dw.SalesOrderHeader") or 0)
    metrics.warehouse_order_lines = int(db.scalar("SELECT COUNT_BIG(*) FROM dw.SalesOrderDetail") or 0)

    # -- breakdowns ---------------------------------------------------------
    metrics.failed_check_breakdown = db.fetch_all(
        """
        SELECT check_name, check_type, severity, occurrences = COUNT(*)
        FROM   etl.etl_validation_log
        WHERE  status = 'FAIL'
        GROUP  BY check_name, check_type, severity
        ORDER  BY COUNT(*) DESC, check_name
        """
    )
    metrics.quarantine_breakdown = db.fetch_all(
        """
        SELECT entity_name, rejection_reason, rows_quarantined = COUNT(*)
        FROM   etl.etl_rejected_row
        GROUP  BY entity_name, rejection_reason
        ORDER  BY COUNT(*) DESC, rejection_reason
        """
    )
    metrics.runs = db.fetch_all(
        """
        SELECT run_id, run_trigger, load_type, status, rows_extracted, rows_loaded,
               rows_rejected, checks_run, checks_failed, required_manual_intervention,
               duration_seconds, start_time
        FROM   etl.etl_run_log
        WHERE  status <> 'Running'
        ORDER  BY run_id
        """
    )

    return metrics


def render_markdown(m: Metrics) -> str:
    """Render the report. Every figure is traceable to a table in the database."""
    lines: list[str] = []
    add = lines.append

    add("# Measured pipeline metrics")
    add("")
    add(f"Generated {m.generated_at_utc} from `etl.etl_run_log`, `etl.etl_validation_log` ")
    add("and `etl.etl_run_entity` on a live SQL Server 2022 instance holding the real")
    add("AdventureWorks2022 dataset. Regenerate with `python -m etl.metrics --write`.")
    add("")

    add("## Runs")
    add("")
    add("| Metric | Value |")
    add("| --- | ---: |")
    add(f"| Runs completed | {m.total_runs} |")
    add(f"| — scheduled | {m.scheduled_runs} |")
    add(f"| — succeeded | {m.succeeded} |")
    add(f"| — completed with warnings | {m.completed_with_warnings} |")
    add(f"| — failed | {m.failed} |")
    add(f"| Runs that required manual intervention | {m.runs_requiring_manual_intervention} |")
    add(f"| **Runs completed without human involvement** | **{m.autonomous_run_rate_pct:.1f}%** |")
    add("")

    add("## Rows")
    add("")
    add("| Metric | Value |")
    add("| --- | ---: |")
    add(f"| Rows extracted from source | {m.rows_extracted:,} |")
    add(f"| Rows loaded into the warehouse | {m.rows_loaded:,} |")
    add(f"| Rows quarantined | {m.rows_quarantined:,} |")
    add(f"| **Rows loaded as a share of rows extracted** | **{m.row_load_rate_pct:.3f}%** |")
    add(f"| Orders in the warehouse | {m.warehouse_orders:,} |")
    add(f"| Order lines in the warehouse | {m.warehouse_order_lines:,} |")
    add("")

    add("## Validation")
    add("")
    add("| Metric | Value |")
    add("| --- | ---: |")
    add(f"| Checks executed | {m.checks_executed:,} |")
    add(f"| Checks passed | {m.checks_passed:,} |")
    add(f"| Checks failed | {m.checks_failed:,} |")
    add(f"| — of which Critical | {m.critical_checks_failed:,} |")
    add(f"| **Check pass rate** | **{m.check_pass_rate_pct:.3f}%** |")
    add("")

    add("## Row-count reconciliation")
    add("")
    add("| Metric | Value |")
    add("| --- | ---: |")
    add(f"| Reconciliation checks executed | {m.reconciliation_checks:,} |")
    add(f"| Reconciliation checks passed | {m.reconciliation_passed:,} |")
    add(f"| **Reconciliation pass rate** | **{m.reconciliation_pass_rate_pct:.3f}%** |")
    add(f"| Total unexplained row-count variance | {m.row_count_variance_total:,} |")
    add("")

    add("## Alerting")
    add("")
    add("| Metric | Value |")
    add("| --- | ---: |")
    add(f"| Alerts raised | {m.alerts_raised} |")
    add(f"| Alerts delivered | {m.alerts_delivered} |")
    add("")

    add("## Performance")
    add("")
    add("| Metric | Value |")
    add("| --- | ---: |")
    add(f"| Median run duration | {m.median_run_seconds:.2f}s |")
    add(f"| Slowest run | {m.slowest_run_seconds:.2f}s |")
    add(f"| Total pipeline runtime | {m.total_runtime_seconds:.2f}s |")
    add("")

    if m.failed_check_breakdown:
        add("## Which checks failed")
        add("")
        add("| Check | Type | Severity | Occurrences |")
        add("| --- | --- | --- | ---: |")
        for row in m.failed_check_breakdown:
            add(
                f"| `{row['check_name']}` | {row['check_type']} | "
                f"{row['severity']} | {row['occurrences']} |"
            )
        add("")

    if m.quarantine_breakdown:
        add("## What was quarantined")
        add("")
        add("| Entity | Rule | Rows |")
        add("| --- | --- | ---: |")
        for row in m.quarantine_breakdown:
            add(f"| {row['entity_name']} | `{row['rejection_reason']}` | {row['rows_quarantined']} |")
        add("")

    if m.runs:
        add("## Run log")
        add("")
        add("| Run | Trigger | Type | Status | Extracted | Loaded | Quarantined | Checks | Failed | Seconds |")
        add("| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
        for row in m.runs:
            duration = row["duration_seconds"]
            add(
                f"| {row['run_id']} | {row['run_trigger']} | {row['load_type']} | "
                f"{row['status']} | {row['rows_extracted']:,} | {row['rows_loaded']:,} | "
                f"{row['rows_rejected']:,} | {row['checks_run']} | {row['checks_failed']} | "
                f"{float(duration):.2f} |"
            )
        add("")

    add("---")
    add("")
    add("### What these numbers do not say")
    add("")
    add("There is no measurement here of the manual export-and-load process this")
    add("pipeline replaces, because that process was never run and never timed. No")
    add("percentage reduction in manual effort can honestly be derived from this")
    add("data, and none is reported. What *is* measured is how many runs completed")
    add("with no human involvement, which is a different claim and the one the")
    add("evidence actually supports.")
    add("")
    add("See `docs/metrics-methodology.md` for how each figure is defined and what")
    add("was simulated.")

    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m etl.metrics", description="Compute pipeline metrics from the run history."
    )
    parser.add_argument("--write", action="store_true", help="Write reports/metrics.{md,json}.")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of Markdown.")
    args = parser.parse_args(argv)

    try:
        settings = Settings.from_env()
    except RuntimeError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    with Database(settings.target) as db:
        metrics = collect(db)

    if metrics.total_runs == 0:
        print("No completed runs found. Run the pipeline first:", file=sys.stderr)
        print("    python -m etl.run_pipeline --load-type Full", file=sys.stderr)
        return 1

    payload = asdict(metrics)

    if args.json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(render_markdown(metrics))

    if args.write:
        reports = REPO_ROOT / "reports"
        reports.mkdir(parents=True, exist_ok=True)
        (reports / "metrics.md").write_text(render_markdown(metrics), encoding="utf-8")
        (reports / "metrics.json").write_text(
            json.dumps(payload, indent=2, default=str), encoding="utf-8"
        )
        print(f"Wrote {reports / 'metrics.md'}", file=sys.stderr)
        print(f"Wrote {reports / 'metrics.json'}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
