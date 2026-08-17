#!/usr/bin/env python3
"""Corrupt rows in the source so the validation layer can be observed catching them.

WHY THIS EXISTS
---------------
A data-quality layer that has never rejected anything is an untested claim. The
only way to know the checks work is to put bad data in front of them and watch
what happens -- so this injects specific, known faults into the *source*
database and lets the pipeline discover them through the normal extract.

WHAT IT PROVES
--------------
Run this, then run the pipeline, and three things should be true:

  1. The corrupted rows are quarantined in etl.etl_rejected_row, with the
     failing rule named.
  2. None of them reach dw. The warehouse stays clean.
  3. The run is marked Failed, an alert is raised, and the watermark is NOT
     advanced -- so the window is re-read once the source is fixed.

`scripts/run_experiment.py --scenario faults` does exactly that and checks all
three, rather than taking anyone's word for it.

DESIGN NOTES
------------
1. AdventureWorks will not accept these faults on its own.

   Sales.SalesOrderHeader carries CHECK constraints -- CK_SalesOrderHeader_
   DueDate, _ShipDate, _Freight -- that reject every corruption below. That is
   a good property of the source, and it is worth stating plainly: against a
   *well-constrained* OLTP system, these particular faults cannot originate
   upstream at all.

   They are still worth defending against, because the warehouse's real
   exposure is not to AdventureWorks specifically. It is to whatever the source
   becomes: a legacy system with weaker constraints, a third-party feed, a
   replica whose constraints were disabled for a bulk load, or an extract that
   mangles a type in transit. So the harness disables the relevant constraints
   for the duration of the injection and re-enables them WITH CHECK on revert
   -- simulating a source whose guarantees are weaker than this one's, which is
   the situation the validation layer actually exists for.

2. Every fault is header-only and leaves SubTotal untouched, so each isolates
   the rule it targets. Corrupting an order *line* would also change the
   header's SubTotal via the iduSalesOrderDetail trigger, and the resulting
   reconciliation failure -- correct, but cascading -- would make it impossible
   to tell which check caught the fault.

3. The faults are domain violations rather than NULLs, because the source
   columns are NOT NULL. Nulls arriving from a broken extract are covered by
   tests/test_validation_rules.py, which injects them at the staging layer,
   where they would really appear.

    python scripts/inject_data_faults.py --count 5
    python scripts/inject_data_faults.py --revert
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.config import Settings  # noqa: E402
from etl.db import Database  # noqa: E402

log = logging.getLogger("faults")

# The marker written into Comment so injected rows can be found again and
# reverted. Real corruption would not label itself; this is a test harness, and
# being able to undo it matters more than realism.
FAULT_MARKER = "__INJECTED_FAULT__"

FAULTS: dict[str, dict[str, str]] = {
    "due_before_order": {
        "rule": "DATE_DueBeforeOrder",
        "set": "DueDate = DATEADD(DAY, -5, OrderDate)",
        "description": "Order due five days before it was placed.",
    },
    "ship_before_order": {
        "rule": "DATE_ShipBeforeOrder",
        "set": "ShipDate = DATEADD(DAY, -2, OrderDate)",
        "description": "Order shipped two days before it was placed.",
    },
    "negative_freight": {
        "rule": "NEG_Amounts",
        "set": "Freight = -25.0000",
        "description": "Negative freight charge on the order header.",
    },
}


# The source constraints that would otherwise reject every fault below.
SOURCE_CONSTRAINTS = (
    "CK_SalesOrderHeader_DueDate",
    "CK_SalesOrderHeader_ShipDate",
    "CK_SalesOrderHeader_Freight",
)


def set_source_constraints(db: Database, enabled: bool) -> None:
    """Disable or re-enable the source CHECK constraints.

    Re-enabling uses WITH CHECK rather than the default NOCHECK, so SQL Server
    re-validates every existing row. If the revert missed anything, this raises
    rather than leaving the constraint marked untrusted -- the failure mode
    where a constraint exists, is enabled, and is quietly not being enforced.
    """
    for constraint in SOURCE_CONSTRAINTS:
        clause = f"WITH CHECK CHECK CONSTRAINT {constraint}" if enabled else f"NOCHECK CONSTRAINT {constraint}"
        db.exec_sql(f"ALTER TABLE Sales.SalesOrderHeader {clause};")
    log.info("Source CHECK constraints %s.", "re-enabled and re-validated" if enabled else "disabled")


def inject(db: Database, count_per_fault: int) -> dict[str, int]:
    """Apply each fault to `count_per_fault` distinct orders.

    ModifiedDate is stamped from GETUTCDATE() -- the server clock the pipeline
    also derives its watermark from. A client-supplied timestamp risks landing
    fractionally *before* the current watermark, in which case the corrupted
    rows are never extracted and the injection silently proves nothing.
    """
    injected: dict[str, int] = {}

    for name, fault in FAULTS.items():
        # Exclude orders already carrying a fault so the same row is not
        # corrupted twice and attributed to the wrong rule.
        affected = db.exec_sql(
            f"""
            WITH victims AS (
                SELECT TOP (?) h.SalesOrderID
                FROM   Sales.SalesOrderHeader AS h
                WHERE  h.ModifiedDate < DATEADD(MINUTE, -1, GETUTCDATE())
                  AND  (h.Comment IS NULL OR h.Comment NOT LIKE ?)
                  AND  h.ShipDate IS NOT NULL
                ORDER  BY NEWID()
            )
            UPDATE h
            SET    {fault["set"]},
                   h.Comment      = ?,
                   h.ModifiedDate = GETUTCDATE()
            FROM   Sales.SalesOrderHeader AS h
            INNER JOIN victims AS v ON v.SalesOrderID = h.SalesOrderID;
            """,
            count_per_fault,
            f"%{FAULT_MARKER}%",
            f"{FAULT_MARKER}:{name}",
        )
        injected[name] = affected
        log.info("  %-18s -> %d row(s) corrupted (expect rule %s)", name, affected, fault["rule"])

    return injected


def revert(db: Database) -> int:
    """Undo every injected fault, restoring plausible values.

    ModifiedDate is bumped again so the repaired rows are re-extracted by the
    next run. Leaving it alone would repair the source while the warehouse kept
    quarantining -- a state no real remediation would ever produce.
    """
    reverted = db.exec_sql(
        """
        UPDATE h
        SET    h.DueDate      = DATEADD(DAY, 7, h.OrderDate),
               h.ShipDate     = DATEADD(DAY, 5, h.OrderDate),
               h.Freight      = ROUND(h.SubTotal * 0.025, 4),
               h.Comment      = NULL,
               h.ModifiedDate = GETUTCDATE()
        FROM   Sales.SalesOrderHeader AS h
        WHERE  h.Comment LIKE ?;
        """,
        f"%{FAULT_MARKER}%",
    )
    log.info("Reverted %d injected fault(s).", reverted)
    return reverted


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inject or revert data-quality faults in the source.")
    parser.add_argument("--count", type=int, default=4, help="Rows to corrupt per fault type.")
    parser.add_argument("--revert", action="store_true", help="Undo previously injected faults.")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s", datefmt="%H:%M:%S"
    )

    settings = Settings.from_env()

    with Database(settings.source) as db:
        log.info("Source: %s", db.descriptor)
        if args.revert:
            revert(db)
            # Only now, with the data repaired, is it safe to re-validate.
            set_source_constraints(db, enabled=True)
        else:
            log.info("Injecting faults into %s...", settings.source.database)
            set_source_constraints(db, enabled=False)
            injected = inject(db, args.count)
            total = sum(injected.values())
            log.info("Injected %d faulty row(s) across %d fault type(s).", total, len(injected))
            log.warning(
                "Source CHECK constraints are DISABLED until you run: "
                "python scripts/inject_data_faults.py --revert"
            )
            log.info("Now run the pipeline -- it should quarantine every one of them.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
