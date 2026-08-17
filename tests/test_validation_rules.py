"""The staging validation rules actually reject what they claim to reject.

These tests inject bad rows directly into stg, which is both convenient and
realistic: the source's own NOT NULL constraints mean a NULL can never
originate in AdventureWorks, so the only way one reaches the warehouse is by
being produced in transit -- a failed type conversion, a truncated column, a
malformed CSV in an intermediate hop. Staging is exactly where that shows up,
so staging is where it is tested.

scripts/inject_data_faults.py covers the complementary case: domain violations
that travel the full path from source through extract into staging.
"""

from __future__ import annotations

import pytest

from etl.db import Database

# One valid header, as a baseline to perturb. Anything not named here is
# nullable in stg and irrelevant to the rules under test.
VALID_HEADER = {
    "SalesOrderID": 999_000_001,
    "RevisionNumber": 1,
    "OrderDate": "2024-05-01T00:00:00",
    "DueDate": "2024-05-08T00:00:00",
    "ShipDate": "2024-05-03T00:00:00",
    "Status": 5,
    "OnlineOrderFlag": 1,
    "SalesOrderNumber": "SO999000001",
    "CustomerID": 29825,
    "BillToAddressID": 985,
    "ShipToAddressID": 985,
    "ShipMethodID": 5,
    "SubTotal": 100.0000,
    "TaxAmt": 8.0000,
    "Freight": 2.5000,
    "TotalDue": 110.5000,
    "ModifiedDate": "2024-05-01T00:00:00",
}

VALID_DETAIL = {
    "SalesOrderID": 999_000_001,
    "SalesOrderDetailID": 999_000_001,
    "OrderQty": 2,
    "ProductID": 776,
    "SpecialOfferID": 1,
    "UnitPrice": 50.0000,
    "UnitPriceDiscount": 0.0000,
    "LineTotal": 100.000000,
    "ModifiedDate": "2024-05-01T00:00:00",
}


def _insert(db: Database, table: str, row: dict, run_id: int) -> None:
    payload = {**row, "etl_run_id": run_id}
    columns = ", ".join(f"[{c}]" for c in payload)
    placeholders = ", ".join("?" for _ in payload)
    db.exec_sql(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", *payload.values())


def _run_rules(db: Database, run_id: int) -> dict[tuple[str, str], dict]:
    """Execute the staging rules and index the results by (entity, check).

    Keying on check_name alone would be wrong: rule names are scoped per
    entity, so NULL_SalesOrderID, NULL_Amounts and NULL_ModifiedDate each exist
    twice -- once for the header table and once for detail. Collapsing them
    into one key lets a PASS on one entity mask a FAIL on the other, which is
    precisely the bug that would make this suite report green while the detail
    rules did nothing.
    """
    db.exec_proc("etl.usp_RunStagingRules", run_id=run_id)
    rows = db.fetch_all(
        "SELECT entity_name, check_name, status, severity, actual_value "
        "FROM etl.etl_validation_log WHERE run_id = ?",
        run_id,
    )
    return {(r["entity_name"], r["check_name"]): r for r in rows}


def _staged_count(db: Database, table: str, run_id: int) -> int:
    return int(db.scalar(f"SELECT COUNT_BIG(*) FROM {table} WHERE etl_run_id = ?", run_id) or 0)


# ---------------------------------------------------------------- happy path
def test_valid_rows_pass_every_rule(target: Database, scratch_run: int) -> None:
    _insert(target, "stg.SalesOrderHeader", VALID_HEADER, scratch_run)
    _insert(target, "stg.SalesOrderDetail", VALID_DETAIL, scratch_run)

    results = _run_rules(target, scratch_run)

    failures = {key: r for key, r in results.items() if r["status"] == "FAIL"}
    assert not failures, f"valid rows should pass every rule, but these failed: {sorted(failures)}"

    # Nothing was quarantined, so both rows survive into the load stage.
    assert _staged_count(target, "stg.SalesOrderHeader", scratch_run) == 1
    assert _staged_count(target, "stg.SalesOrderDetail", scratch_run) == 1


# ------------------------------------------------------------- null checks
@pytest.mark.parametrize(
    ("column", "expected_rule"),
    [
        ("SalesOrderID", "NULL_SalesOrderID"),
        ("OrderDate", "NULL_OrderDate"),
        ("DueDate", "NULL_DueDate"),
        ("CustomerID", "NULL_CustomerID"),
        ("Status", "NULL_Status"),
        ("SalesOrderNumber", "NULL_SalesOrderNumber"),
        ("TotalDue", "NULL_Amounts"),
        ("ModifiedDate", "NULL_ModifiedDate"),
        ("BillToAddressID", "NULL_AddressIDs"),
        ("RevisionNumber", "NULL_RevisionNumber"),
    ],
)
def test_null_in_required_header_column_is_quarantined(
    target: Database, scratch_run: int, column: str, expected_rule: str
) -> None:
    bad = {**VALID_HEADER, column: None}
    _insert(target, "stg.SalesOrderHeader", bad, scratch_run)

    results = _run_rules(target, scratch_run)

    key = ("SalesOrderHeader", expected_rule)
    assert key in results, f"rule {expected_rule} did not run for SalesOrderHeader"
    assert results[key]["status"] == "FAIL", f"NULL {column} should have failed {expected_rule}"
    # Quarantined means removed from staging, so it can never reach dw.
    assert _staged_count(target, "stg.SalesOrderHeader", scratch_run) == 0

    quarantined = target.scalar(
        "SELECT COUNT_BIG(*) FROM etl.etl_rejected_row WHERE run_id = ? AND rejection_reason = ?",
        scratch_run,
        expected_rule,
    )
    assert quarantined == 1, "the offending row should be preserved in the reject log"


@pytest.mark.parametrize(
    ("column", "expected_rule"),
    [
        ("SalesOrderDetailID", "NULL_SalesOrderDetailID"),
        ("SalesOrderID", "NULL_SalesOrderID"),
        ("ProductID", "NULL_ProductID"),
        ("OrderQty", "NULL_Amounts"),
        ("LineTotal", "NULL_Amounts"),
        ("ModifiedDate", "NULL_ModifiedDate"),
    ],
)
def test_null_in_required_detail_column_is_quarantined(
    target: Database, scratch_run: int, column: str, expected_rule: str
) -> None:
    bad = {**VALID_DETAIL, column: None}
    _insert(target, "stg.SalesOrderDetail", bad, scratch_run)

    results = _run_rules(target, scratch_run)

    assert results[("SalesOrderDetail", expected_rule)]["status"] == "FAIL"
    assert _staged_count(target, "stg.SalesOrderDetail", scratch_run) == 0


# ----------------------------------------------------------- domain checks
@pytest.mark.parametrize(
    ("overrides", "expected_rule"),
    [
        ({"Freight": -1.0}, "NEG_Amounts"),
        ({"SubTotal": -0.01}, "NEG_Amounts"),
        ({"DueDate": "2024-04-01T00:00:00"}, "DATE_DueBeforeOrder"),
        ({"ShipDate": "2024-04-01T00:00:00"}, "DATE_ShipBeforeOrder"),
    ],
)
def test_header_domain_violations_are_quarantined(
    target: Database, scratch_run: int, overrides: dict, expected_rule: str
) -> None:
    _insert(target, "stg.SalesOrderHeader", {**VALID_HEADER, **overrides}, scratch_run)

    results = _run_rules(target, scratch_run)

    assert results[("SalesOrderHeader", expected_rule)]["status"] == "FAIL"
    assert _staged_count(target, "stg.SalesOrderHeader", scratch_run) == 0


@pytest.mark.parametrize(
    ("overrides", "expected_rule"),
    [
        ({"OrderQty": 0}, "QTY_NonPositive"),
        ({"OrderQty": -3}, "QTY_NonPositive"),
        ({"UnitPrice": -1.0}, "PRICE_Negative"),
        ({"UnitPriceDiscount": 1.5}, "DISCOUNT_OutOfRange"),
        ({"UnitPriceDiscount": -0.1}, "DISCOUNT_OutOfRange"),
    ],
)
def test_detail_domain_violations_are_quarantined(
    target: Database, scratch_run: int, overrides: dict, expected_rule: str
) -> None:
    _insert(target, "stg.SalesOrderDetail", {**VALID_DETAIL, **overrides}, scratch_run)

    results = _run_rules(target, scratch_run)

    assert results[("SalesOrderDetail", expected_rule)]["status"] == "FAIL"
    assert _staged_count(target, "stg.SalesOrderDetail", scratch_run) == 0


# ------------------------------------------------------- warn-only rules
def test_line_total_mismatch_warns_but_does_not_quarantine(
    target: Database, scratch_run: int
) -> None:
    """A recomputation mismatch is reported, not rejected.

    LineTotal is a computed column upstream, so a disagreement means the
    extract mangled a numeric type rather than that the order is invalid. The
    row is still loadable and the discrepancy is small; dropping it would lose
    real revenue from the warehouse to fix a rounding artefact.
    """
    bad = {**VALID_DETAIL, "LineTotal": 999.999999}
    _insert(target, "stg.SalesOrderDetail", bad, scratch_run)

    results = _run_rules(target, scratch_run)

    mismatch = results[("SalesOrderDetail", "LINETOTAL_Mismatch")]
    assert mismatch["status"] == "FAIL"
    assert mismatch["severity"] == "Warning"
    # Warning severity plus quarantine=0 means the row stays.
    assert _staged_count(target, "stg.SalesOrderDetail", scratch_run) == 1


def test_duplicate_business_key_warns_and_load_deduplicates(
    target: Database, scratch_run: int
) -> None:
    """Two rows for one order are reported, and the loader keeps the newest.

    MERGE raises error 8672 and aborts if its source has duplicate keys, so
    de-duplicating in the loader is what stops one malformed batch taking down
    the whole load. The duplicate is still surfaced rather than hidden.
    """
    _insert(target, "stg.SalesOrderHeader", VALID_HEADER, scratch_run)
    _insert(
        target,
        "stg.SalesOrderHeader",
        {**VALID_HEADER, "RevisionNumber": 2, "ModifiedDate": "2024-05-02T00:00:00"},
        scratch_run,
    )

    results = _run_rules(target, scratch_run)

    duplicate = results[("SalesOrderHeader", "DUP_SalesOrderID")]
    assert duplicate["status"] == "FAIL"
    assert duplicate["severity"] == "Warning"
    assert _staged_count(target, "stg.SalesOrderHeader", scratch_run) == 2

    # The loader collapses them to one row, keeping the newer revision.
    loaded = target.exec_proc_one("etl.usp_LoadSalesOrderHeader", run_id=scratch_run)
    assert loaded is not None
    assert loaded["rows_loaded"] == 1, "duplicate keys must collapse to a single warehouse row"

    revision = target.scalar(
        "SELECT RevisionNumber FROM dw.SalesOrderHeader WHERE SalesOrderID = ?",
        VALID_HEADER["SalesOrderID"],
    )
    assert revision == 2, "the newest revision should win"

    target.exec_sql(
        "DELETE FROM dw.SalesOrderHeader WHERE SalesOrderID = ?", VALID_HEADER["SalesOrderID"]
    )
