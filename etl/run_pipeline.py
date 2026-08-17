"""Command-line entry point.

    python -m etl.run_pipeline                      # incremental, manual
    python -m etl.run_pipeline --load-type Full     # re-read all history
    python -m etl.run_pipeline --trigger Scheduled  # what the scheduler passes

Exit codes are meaningful, because a scheduler needs them to be:

    0  the run succeeded, or completed with warnings only
    1  the run failed -- a Critical check failed, or the pipeline crashed
    2  the run could not be configured (bad or missing .env)
"""

from __future__ import annotations

import argparse
import logging
import sys

from .config import Settings
from .pipeline import RunResult, SalesDataPipeline


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    # pyodbc has nothing useful to say at DEBUG, and it says a lot of it.
    logging.getLogger("pyodbc").setLevel(logging.WARNING)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m etl.run_pipeline",
        description="Run the Sales ETL pipeline: AdventureWorks2022 -> SalesReportingDW.",
    )
    parser.add_argument(
        "--trigger",
        choices=["Scheduled", "Manual", "Backfill"],
        default="Manual",
        help="How this run was initiated. Recorded in etl.etl_run_log and used "
             "by the metrics report to separate scheduled runs from ad-hoc ones.",
    )
    parser.add_argument(
        "--load-type",
        choices=["Incremental", "Full"],
        default="Incremental",
        help="Incremental reads only rows changed since the stored watermark. "
             "Full ignores the watermark and re-reads all history.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Debug logging.")
    return parser.parse_args(argv)


def _print_summary(result: RunResult) -> None:
    line = "-" * 66
    print(f"\n{line}")
    print(f"  Run {result.run_id}  |  {result.trigger}  |  {result.load_type}")
    print(line)

    if result.watermark_from and result.watermark_to:
        print(f"  Window          {result.watermark_from}  ->  {result.watermark_to}")

    for entity in result.entities.values():
        print(
            f"  {entity.name:<18} "
            f"source {entity.source_rows:>8,}  "
            f"staged {entity.extracted_rows:>8,}  "
            f"loaded {entity.loaded_rows:>8,}  "
            f"rejected {entity.rejected_rows:>5,}"
        )

    print(line)
    print(f"  Validation      {result.checks_run} check(s), "
          f"{result.checks_failed} failed ({result.critical_failed} critical)")
    if result.failed_checks:
        print(f"  Failed checks   {result.failed_checks}")
    if result.error:
        print(f"  Error           {result.error}")
    if result.alerted:
        print("  Alert           raised and delivered")
    print(f"  Duration        {result.duration_seconds:.2f}s")
    print(f"  Status          {result.status}")
    print(f"{line}\n")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    _configure_logging(args.verbose)

    try:
        settings = Settings.from_env()
    except RuntimeError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    pipeline = SalesDataPipeline(settings)
    result = pipeline.run(trigger=args.trigger, load_type=args.load_type)
    _print_summary(result)

    return 0 if result.succeeded else 1


if __name__ == "__main__":
    raise SystemExit(main())
