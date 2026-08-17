#!/usr/bin/env python3
"""Drive the pipeline through a series of runs and assert what should happen.

This is what produced the numbers in the README. It is deliberately an
*experiment* rather than a demo: every stage states in advance what the
pipeline is supposed to do, and fails loudly if it does not.

    python scripts/run_experiment.py --nights 6
    python scripts/run_experiment.py --nights 6 --skip-faults

Stages
------
1. Baseline full load, if the warehouse is empty.
2. `--nights` incremental runs, each preceded by simulated order activity.
   Asserted: every row the source offers is either loaded or quarantined, and
   the warehouse stays reconciled.
3. A fault night: known-bad rows are injected, and the run must quarantine
   them, keep the warehouse clean, raise an alert, fail, and hold the
   watermark. Then the source is repaired and the next run must recover
   *without* the operator replaying anything -- because the held watermark
   means the same window is read again.

Every assertion reads from etl.etl_run_log / etl.etl_validation_log, i.e. from
what the pipeline recorded about itself, not from what this script believes.
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.config import Settings  # noqa: E402
from etl.db import Database  # noqa: E402
from etl.pipeline import RunResult, SalesDataPipeline  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))

import inject_data_faults  # noqa: E402
import simulate_source_activity as sim  # noqa: E402

log = logging.getLogger("experiment")


class ExperimentFailure(AssertionError):
    """An assertion about pipeline behaviour did not hold."""


def _check(condition: bool, message: str) -> None:
    if condition:
        log.info("    PASS  %s", message)
    else:
        log.error("    FAIL  %s", message)
        raise ExperimentFailure(message)


def _run(pipeline: SalesDataPipeline, load_type: str = "Incremental") -> RunResult:
    return pipeline.run(trigger="Scheduled", load_type=load_type)


def assert_reconciled(result: RunResult) -> None:
    """Every row the source offered must be accounted for."""
    for entity in result.entities.values():
        _check(
            entity.extracted_rows == entity.source_rows,
            f"{entity.name}: staged {entity.extracted_rows:,} == source {entity.source_rows:,}",
        )
        _check(
            entity.loaded_rows + entity.rejected_rows == entity.extracted_rows,
            f"{entity.name}: loaded {entity.loaded_rows:,} + quarantined "
            f"{entity.rejected_rows:,} == staged {entity.extracted_rows:,}",
        )


def stage_baseline(pipeline: SalesDataPipeline, db: Database) -> None:
    loaded = db.scalar("SELECT COUNT_BIG(*) FROM dw.SalesOrderHeader") or 0
    if loaded:
        log.info("Warehouse already holds %s order(s) -- skipping baseline load.", f"{loaded:,}")
        return

    log.info("Stage 1: baseline full load")
    result = _run(pipeline, load_type="Full")
    _check(result.succeeded, f"baseline load succeeded (status {result.status})")
    assert_reconciled(result)


def stage_nights(pipeline: SalesDataPipeline, settings: Settings, nights: int) -> None:
    log.info("Stage 2: %d night(s) of simulated activity", nights)

    presets = list(sim.PRESETS)
    for night in range(1, nights + 1):
        preset = presets[(night - 1) % len(presets)]
        log.info("  Night %d/%d (%s)", night, nights, preset)

        shape = sim.PRESETS[preset]
        with Database(settings.source) as source:
            sim.book_new_orders(source, shape["new_orders"])
            sim.amend_existing_orders(source, shape["amendments"])

        result = _run(pipeline)
        _check(result.succeeded, f"night {night} run succeeded (status {result.status})")
        _check(result.critical_failed == 0, f"night {night}: no critical check failed")
        assert_reconciled(result)


def stage_faults(pipeline: SalesDataPipeline, settings: Settings, count: int) -> None:
    log.info("Stage 3: fault injection")

    with Database(settings.source) as source:
        inject_data_faults.set_source_constraints(source, enabled=False)
        injected = inject_data_faults.inject(source, count)

    expected_bad = sum(injected.values())
    log.info("  Injected %d faulty row(s); running pipeline", expected_bad)

    result = _run(pipeline)

    _check(not result.succeeded, f"run failed as it should (status {result.status})")
    _check(
        result.total_rejected == expected_bad,
        f"quarantined {result.total_rejected} == injected {expected_bad}",
    )
    _check(result.critical_failed > 0, "at least one Critical check failed")
    _check(result.alerted, "an alert was raised and delivered")

    with Database(settings.target) as target:
        contamination = target.scalar(
            """
            SELECT COUNT_BIG(*) FROM dw.SalesOrderHeader
            WHERE DueDate < OrderDate OR ShipDate < OrderDate OR Freight < 0
            """
        )
        _check(contamination == 0, "no faulty row reached the warehouse")

        held = target.fetch_all("SELECT entity_name, watermark_value FROM etl.etl_watermark")
        run_start = target.scalar(
            "SELECT watermark_from FROM etl.etl_run_log WHERE run_id = ?", result.run_id
        )
        _check(
            all(row["watermark_value"] == run_start for row in held),
            "watermark held at the failed run's start, so the window will be re-read",
        )

    log.info("  Repairing the source and re-running")
    with Database(settings.source) as source:
        inject_data_faults.revert(source)
        inject_data_faults.set_source_constraints(source, enabled=True)

    recovery = _run(pipeline)
    _check(recovery.succeeded, f"recovery run succeeded (status {recovery.status})")
    _check(recovery.total_rejected == 0, "nothing quarantined once the source was repaired")
    _check(
        recovery.entities["SalesOrderHeader"].loaded_rows >= expected_bad,
        f"the {expected_bad} previously-rejected row(s) were re-read and loaded "
        f"({recovery.entities['SalesOrderHeader'].loaded_rows} header row(s) loaded)",
    )
    assert_reconciled(recovery)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a measured pipeline experiment.")
    parser.add_argument("--nights", type=int, default=6, help="Simulated nights to run.")
    parser.add_argument("--fault-count", type=int, default=4, help="Rows to corrupt per fault type.")
    parser.add_argument("--skip-faults", action="store_true", help="Skip the fault-injection stage.")
    parser.add_argument("--seed", type=int, default=20240501, help="RNG seed, for repeatability.")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s", datefmt="%H:%M:%S"
    )
    logging.getLogger("etl.pipeline").setLevel(logging.WARNING)
    random.seed(args.seed)

    settings = Settings.from_env()
    pipeline = SalesDataPipeline(settings)

    try:
        with Database(settings.target) as target:
            stage_baseline(pipeline, target)
        stage_nights(pipeline, settings, args.nights)
        if not args.skip_faults:
            stage_faults(pipeline, settings, args.fault_count)
    except ExperimentFailure as exc:
        log.error("Experiment FAILED: %s", exc)
        return 1

    log.info("")
    log.info("Experiment complete. Every assertion held.")
    log.info("Generate the metrics report with: python -m etl.metrics --write")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
