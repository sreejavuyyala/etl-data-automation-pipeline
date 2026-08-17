"""Shared fixtures.

These are integration tests: they run against the live SQL Server instance
configured in .env. There are no mocks, because the thing under test is
SQL -- a mocked database would only prove that the mock behaves like the mock.

If the database is unreachable the whole module is skipped rather than failed,
so `pytest` on a machine with no container running reports honestly instead of
looking broken.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.config import Settings  # noqa: E402
from etl.db import Database  # noqa: E402


@pytest.fixture(scope="session")
def settings() -> Settings:
    try:
        return Settings.from_env()
    except RuntimeError as exc:
        pytest.skip(f"ETL configuration unavailable: {exc}")


@pytest.fixture(scope="session")
def target(settings: Settings):
    """Connection to SalesReportingDW, or a skip if it is not up."""
    db = Database(settings.target)
    try:
        db.connect()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Target database unreachable ({exc}). Run: docker compose up -d")
    yield db
    db.close()


@pytest.fixture(scope="session")
def source(settings: Settings):
    """Connection to AdventureWorks2022, or a skip if it is not restored."""
    db = Database(settings.source)
    try:
        db.connect()
        db.scalar("SELECT TOP 1 SalesOrderID FROM Sales.SalesOrderHeader")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(
            f"Source database unavailable ({exc}). Run: ./scripts/restore_adventureworks.sh"
        )
    yield db
    db.close()


@pytest.fixture
def scratch_run(target: Database):
    """A throwaway run row, cleaned up afterwards.

    Tests that exercise staging rules need a run id to hang rows off, but must
    not leave anything behind in etl_run_log -- the metrics report reads that
    table, and a test fixture appearing in it as a real run would corrupt every
    figure in the README.
    """
    row = target.exec_proc_one(
        "etl.usp_StartRun",
        pipeline_name="pytest",
        run_trigger="Manual",
        load_type="Incremental",
    )
    assert row is not None
    run_id = int(row["run_id"])

    yield run_id

    # Children before parents: the foreign keys are real.
    for statement in (
        "DELETE FROM stg.SalesOrderHeader WHERE etl_run_id = ?",
        "DELETE FROM stg.SalesOrderDetail WHERE etl_run_id = ?",
        "DELETE FROM etl.etl_rejected_row WHERE run_id = ?",
        "DELETE FROM etl.etl_validation_log WHERE run_id = ?",
        "DELETE FROM etl.etl_alert WHERE run_id = ?",
        "DELETE FROM etl.etl_run_entity WHERE run_id = ?",
        "DELETE FROM etl.etl_run_log WHERE run_id = ?",
    ):
        target.exec_sql(statement, run_id)
