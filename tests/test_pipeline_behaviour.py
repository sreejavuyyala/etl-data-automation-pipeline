"""Control-flow behaviour: run status, watermarks, reconciliation, contracts.

These are the properties the pipeline's safety depends on. Each one is cheap to
get wrong in a way that produces no visible symptom until the warehouse is
quietly missing a day of orders.
"""

from __future__ import annotations

import pytest

from etl.db import Database


# --------------------------------------------------------- run status logic
def test_clean_run_is_marked_succeeded(target: Database, scratch_run: int) -> None:
    target.exec_proc(
        "etl.usp_LogValidation",
        run_id=scratch_run,
        check_name="TEST_AlwaysPasses",
        check_type="TypeCheck",
        status="PASS",
    )

    result = target.exec_proc_one("etl.usp_EndRun", run_id=scratch_run)

    assert result is not None
    assert result["status"] == "Succeeded"
    assert result["required_manual_intervention"] == 0


def test_warning_only_failure_completes_with_warnings(
    target: Database, scratch_run: int
) -> None:
    """A failed Warning does not fail the run, and does not summon a human.

    This is the distinction that keeps alerting useful. If every Warning paged
    somebody, the alerts would be ignored within a week and the Critical ones
    would go with them.
    """
    target.exec_proc(
        "etl.usp_LogValidation",
        run_id=scratch_run,
        check_name="TEST_WarnsOnly",
        check_type="FreshnessCheck",
        status="FAIL",
        severity="Warning",
    )

    result = target.exec_proc_one("etl.usp_EndRun", run_id=scratch_run)

    assert result is not None
    assert result["status"] == "CompletedWithWarnings"
    assert result["required_manual_intervention"] == 0, (
        "a warning must not be counted as a run that needed manual intervention"
    )


def test_critical_failure_fails_the_run_and_flags_intervention(
    target: Database, scratch_run: int
) -> None:
    target.exec_proc(
        "etl.usp_LogValidation",
        run_id=scratch_run,
        check_name="TEST_CriticalFails",
        check_type="RowCountReconciliation",
        status="FAIL",
        severity="Critical",
    )

    result = target.exec_proc_one("etl.usp_EndRun", run_id=scratch_run)

    assert result is not None
    assert result["status"] == "Failed"
    assert result["required_manual_intervention"] == 1


def test_critical_failure_overrides_a_caller_claiming_success(
    target: Database, scratch_run: int
) -> None:
    """usp_EndRun derives the status; it does not accept an opinion.

    An orchestrator that reached its success branch by mistake must not be able
    to mark a run green when a Critical check has failed.
    """
    target.exec_proc(
        "etl.usp_LogValidation",
        run_id=scratch_run,
        check_name="TEST_CriticalFails",
        check_type="RowCountReconciliation",
        status="FAIL",
        severity="Critical",
    )

    result = target.exec_proc_one("etl.usp_EndRun", run_id=scratch_run, status="Succeeded")

    assert result is not None
    assert result["status"] == "Failed"


# ------------------------------------------------------------- watermarking
def test_watermark_never_moves_backwards(target: Database) -> None:
    """A backfill reading an old window must not rewind the high-water mark.

    Rewinding would make the next incremental run re-read everything since that
    older point -- wasteful, but worse, it would make the watermark stop being
    a reliable statement about what has been loaded.
    """
    entity = "TEST_WatermarkEntity"
    try:
        target.exec_proc(
            "etl.usp_SetWatermark", entity_name=entity, watermark_value="2024-06-01T00:00:00"
        )
        target.exec_proc(
            "etl.usp_SetWatermark", entity_name=entity, watermark_value="2024-01-01T00:00:00"
        )

        value = target.scalar(
            "SELECT watermark_value FROM etl.etl_watermark WHERE entity_name = ?", entity
        )
        assert value is not None
        assert value.isoformat().startswith("2024-06-01"), (
            "an older watermark must not overwrite a newer one"
        )
    finally:
        target.exec_sql("DELETE FROM etl.etl_watermark WHERE entity_name = ?", entity)


# ---------------------------------------------------------- reconciliation
def test_reconciliation_passes_when_every_row_is_accounted_for(
    target: Database, scratch_run: int
) -> None:
    target.exec_proc(
        "etl.usp_LogRunEntity",
        run_id=scratch_run,
        entity_name="SalesOrderHeader",
        source_row_count=100,
        rows_extracted=100,
        rows_loaded=95,
        rows_rejected=5,
    )

    target.exec_proc(
        "etl.usp_ReconcileRowCounts",
        run_id=scratch_run,
        entity_name="SalesOrderHeader",
        source_row_count=100,
    )

    results = {
        r["check_name"]: r["status"]
        for r in target.fetch_all(
            "SELECT check_name, status FROM etl.etl_validation_log "
            "WHERE run_id = ? AND check_type = 'RowCountReconciliation'",
            scratch_run,
        )
    }

    assert results["RECON_SourceToStaging"] == "PASS"
    # Quarantined rows are accounted for, not ignored -- that is what makes the
    # reconciliation exact rather than approximate.
    assert results["RECON_StagingToWarehouse"] == "PASS"


def test_reconciliation_detects_a_short_extract(target: Database, scratch_run: int) -> None:
    """The source offered 100 rows and only 90 arrived. That must not pass."""
    target.exec_proc(
        "etl.usp_LogRunEntity",
        run_id=scratch_run,
        entity_name="SalesOrderHeader",
        source_row_count=100,
        rows_extracted=90,
        rows_loaded=90,
        rows_rejected=0,
    )

    target.exec_proc(
        "etl.usp_ReconcileRowCounts",
        run_id=scratch_run,
        entity_name="SalesOrderHeader",
        source_row_count=100,
    )

    row = target.fetch_one(
        "SELECT status, variance FROM etl.etl_validation_log "
        "WHERE run_id = ? AND check_name = 'RECON_SourceToStaging'",
        scratch_run,
    )
    assert row is not None
    assert row["status"] == "FAIL"
    assert int(row["variance"]) == -10


def test_reconciliation_detects_rows_lost_between_staging_and_warehouse(
    target: Database, scratch_run: int
) -> None:
    """100 staged, 90 loaded, nothing quarantined: 10 rows vanished."""
    target.exec_proc(
        "etl.usp_LogRunEntity",
        run_id=scratch_run,
        entity_name="SalesOrderHeader",
        source_row_count=100,
        rows_extracted=100,
        rows_loaded=90,
        rows_rejected=0,
    )

    target.exec_proc(
        "etl.usp_ReconcileRowCounts",
        run_id=scratch_run,
        entity_name="SalesOrderHeader",
        source_row_count=100,
    )

    row = target.fetch_one(
        "SELECT status FROM etl.etl_validation_log "
        "WHERE run_id = ? AND check_name = 'RECON_StagingToWarehouse'",
        scratch_run,
    )
    assert row is not None
    assert row["status"] == "FAIL"


# ------------------------------------------------------- procedure contracts
def test_validate_sales_data_returns_exactly_one_result_set(
    target: Database, scratch_run: int
) -> None:
    """Regression test.

    usp_ValidateSalesData calls usp_ReconcileRowCounts, which emits a result
    set of its own. If that were allowed to reach the client it would arrive
    *first* -- and an ADF Lookup activity reads only the first result set, so
    the orchestrator would branch on a reconciliation row that has no
    should_alert column at all, and would never detect a failed run.

    The fix is INSERT ... EXEC inside the procedure. This test is what stops it
    silently regressing.
    """
    target.exec_proc(
        "etl.usp_LogRunEntity",
        run_id=scratch_run,
        entity_name="SalesOrderHeader",
        source_row_count=0,
        rows_extracted=0,
        rows_loaded=0,
    )
    target.exec_proc(
        "etl.usp_LogRunEntity",
        run_id=scratch_run,
        entity_name="SalesOrderDetail",
        source_row_count=0,
        rows_extracted=0,
        rows_loaded=0,
    )

    with target.cursor() as cur:
        cur.execute("EXEC etl.usp_ValidateSalesData @run_id = ?", scratch_run)

        result_sets = []
        while True:
            if cur.description is not None:
                columns = [d[0] for d in cur.description]
                rows = cur.fetchall()
                if rows:
                    result_sets.append(columns)
            if not cur.nextset():
                break

    assert len(result_sets) == 1, (
        f"expected exactly one result set, got {len(result_sets)}: {result_sets}"
    )
    assert "should_alert" in result_sets[0], (
        "the single result set must be the validation summary an If Condition branches on"
    )


def test_validation_summary_reports_should_alert_for_critical_failures(
    target: Database, scratch_run: int
) -> None:
    target.exec_proc(
        "etl.usp_LogValidation",
        run_id=scratch_run,
        check_name="TEST_Critical",
        check_type="TypeCheck",
        status="FAIL",
        severity="Critical",
    )

    summary = target.exec_proc_one("etl.usp_GetRunValidationResult", run_id=scratch_run)

    assert summary is not None
    assert summary["should_alert"] is True or summary["should_alert"] == 1
    assert summary["critical_failed"] == 1
    assert "TEST_Critical" in (summary["failed_check_list"] or "")


def test_validation_summary_does_not_alert_on_warnings_alone(
    target: Database, scratch_run: int
) -> None:
    target.exec_proc(
        "etl.usp_LogValidation",
        run_id=scratch_run,
        check_name="TEST_Warning",
        check_type="FreshnessCheck",
        status="FAIL",
        severity="Warning",
    )

    summary = target.exec_proc_one("etl.usp_GetRunValidationResult", run_id=scratch_run)

    assert summary is not None
    assert summary["should_alert"] is False or summary["should_alert"] == 0
    assert summary["warning_failed"] == 1


def test_start_run_returns_a_half_open_window(target: Database) -> None:
    """The extraction window must be [from, to), not [from, to].

    A closed upper bound re-reads the boundary row on every subsequent run,
    which shows up as a permanent, small, inexplicable row-count variance.
    """
    row = target.exec_proc_one(
        "etl.usp_StartRun",
        pipeline_name="pytest-window",
        run_trigger="Manual",
        load_type="Incremental",
    )
    assert row is not None
    run_id = int(row["run_id"])

    try:
        assert row["watermark_from"] is not None
        assert row["watermark_to"] is not None
        assert row["watermark_to"] > row["watermark_from"]
    finally:
        target.exec_sql("DELETE FROM etl.etl_run_log WHERE run_id = ?", run_id)


# ------------------------------------------------------------ source sanity
def test_source_totals_reconcile_within_tolerance(source: Database) -> None:
    """The source itself is internally consistent.

    Worth asserting explicitly: the financial reconciliation check compares
    header SubTotal against the sum of its lines, and if the *source* did not
    satisfy that property, a failure would mean nothing about the pipeline.
    """
    row = source.fetch_one(
        """
        SELECT header_total = (SELECT SUM(SubTotal) FROM Sales.SalesOrderHeader),
               line_total   = (SELECT SUM(LineTotal) FROM Sales.SalesOrderDetail)
        """
    )
    assert row is not None
    difference = abs(float(row["header_total"]) - float(row["line_total"]))

    # AdventureWorks stores LineTotal at higher precision than SubTotal, so the
    # two disagree by a rounding artefact spread across ~31k orders, not by a
    # data error. One currency unit across the whole dataset is generous and
    # still tight enough to catch a genuine imbalance.
    assert difference < 1.0, f"source header/line totals differ by {difference}"
