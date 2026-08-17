#!/usr/bin/env python3
"""Generate realistic OLTP activity in the AdventureWorks source.

WHY THIS EXISTS
---------------
AdventureWorks is a static sample database. Restore it, run a full load, and
every subsequent incremental run legitimately finds nothing to do -- which
proves the watermark works, and proves nothing else. Measuring how the pipeline
behaves over a series of scheduled runs needs a source that actually changes
between them.

So this script does what the AdventureWorks Cycles order-entry system would
have been doing overnight: it books new orders and amends existing ones.

WHAT IT IS NOT
--------------
It is not a data generator for the warehouse, and it never touches the target.
Every row it writes goes into the *source* database through ordinary INSERT and
UPDATE statements, and the pipeline then has to discover those changes on its
own via the ModifiedDate watermark, exactly as it would with a real OLTP
system. The measurements in the README come from the pipeline's own logs, not
from anything this script asserts.

Any run that used simulated activity is recorded as such -- see
docs/metrics-methodology.md.

    python scripts/simulate_source_activity.py --new-orders 40 --amendments 25
    python scripts/simulate_source_activity.py --preset quiet-night
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

log = logging.getLogger("simulate")

# Rough shapes of a night's trading, so a multi-run experiment can vary the
# load without hand-tuning numbers on every invocation.
PRESETS: dict[str, dict[str, int]] = {
    "quiet-night": {"new_orders": 12, "amendments": 8},
    "normal-night": {"new_orders": 45, "amendments": 30},
    "busy-night": {"new_orders": 160, "amendments": 90},
}


def book_new_orders(db: Database, count: int) -> list[int]:
    """Book `count` new sales orders, each with 1-5 order lines.

    Header field values (customer, addresses, ship method, territory) are
    cloned from randomly chosen existing orders rather than invented, because
    they are foreign keys into a dozen other AdventureWorks tables and the
    point here is realistic *sales* traffic, not a synthetic customer base.

    Every timestamp comes from GETUTCDATE() -- SQL Server's clock -- rather
    than from Python. The pipeline derives its watermark from the same clock,
    so a row stamped here can never land *before* the current watermark and be
    stranded, which is exactly what happens if the client's clock runs even
    marginally behind the server's, or if a client-side timestamp is rounded
    down to fit the `datetime` column.
    """
    created: list[int] = []

    for _ in range(count):
        # SalesOrderID is an IDENTITY column, so the new id comes back via
        # OUTPUT rather than being chosen here.
        new_id = db.scalar(
            """
            INSERT INTO Sales.SalesOrderHeader
                (RevisionNumber, OrderDate, DueDate, ShipDate, Status, OnlineOrderFlag,
                 PurchaseOrderNumber, AccountNumber, CustomerID, SalesPersonID, TerritoryID,
                 BillToAddressID, ShipToAddressID, ShipMethodID,
                 SubTotal, TaxAmt, Freight, Comment, rowguid, ModifiedDate)
            OUTPUT INSERTED.SalesOrderID
            SELECT TOP 1
                1, GETUTCDATE(), DATEADD(DAY, 7, GETUTCDATE()), NULL, 1, t.OnlineOrderFlag,
                t.PurchaseOrderNumber, t.AccountNumber, t.CustomerID, t.SalesPersonID, t.TerritoryID,
                t.BillToAddressID, t.ShipToAddressID, t.ShipMethodID,
                0, 0, 0, 'Simulated order', NEWID(), GETUTCDATE()
            FROM Sales.SalesOrderHeader AS t
            ORDER BY NEWID();
            """
        )

        if new_id is None:
            raise RuntimeError("Failed to insert a simulated order header")

        new_id = int(new_id)
        created.append(new_id)

        # 1-5 lines per order, priced from the real product catalogue.
        # The iduSalesOrderDetail trigger recalculates the header's SubTotal
        # from these, which is what keeps the source internally consistent --
        # and is exactly what the RECON_HeaderSubTotalVsLines check verifies
        # survived the trip into the warehouse.
        line_count = random.randint(1, 5)
        db.exec_sql(
            """
            INSERT INTO Sales.SalesOrderDetail
                (SalesOrderID, CarrierTrackingNumber, OrderQty, ProductID, SpecialOfferID,
                 UnitPrice, UnitPriceDiscount, rowguid, ModifiedDate)
            SELECT TOP (?)
                ?, NULL,
                1 + ABS(CHECKSUM(NEWID())) % 8,
                p.ProductID, 1,
                p.ListPrice, 0, NEWID(), GETUTCDATE()
            FROM Production.Product AS p
            WHERE p.ListPrice > 0 AND p.FinishedGoodsFlag = 1
            ORDER BY NEWID();
            """,
            line_count,
            new_id,
        )

        # The trigger updated SubTotal but leaves TaxAmt and Freight alone, and
        # AdventureWorks derives both from SubTotal. Without this the new
        # orders would carry zero tax and zero freight, which is not what the
        # rest of the table looks like.
        db.exec_sql(
            """
            UPDATE Sales.SalesOrderHeader
            SET    TaxAmt       = ROUND(SubTotal * 0.08, 4),
                   Freight      = ROUND(SubTotal * 0.025, 4),
                   ModifiedDate = GETUTCDATE()
            WHERE  SalesOrderID = ?;
            """,
            new_id,
        )

    return created


def amend_existing_orders(db: Database, count: int) -> int:
    """Restate `count` existing orders by changing a line quantity.

    This is the case that exercises the MERGE's UPDATE branch. A pipeline that
    only ever inserts has not been shown to handle a restatement -- which, in a
    warehouse, is the change most likely to be silently wrong.
    """
    if count <= 0:
        return 0

    # The affected orders are captured with OUTPUT rather than re-selected by
    # timestamp afterwards. AdventureWorks stores ModifiedDate as `datetime`,
    # whose resolution is 1/300th of a second, so a value sent from the client
    # is silently rounded on the way in and no longer equals what was sent.
    # Matching headers with `WHERE d.ModifiedDate = @as_of` therefore updates
    # nothing -- leaving headers whose SubTotal the trigger has just changed
    # but whose ModifiedDate still points at 2011, invisible to an incremental
    # extract keyed on that column.
    #
    # OUTPUT sidesteps the question entirely by naming the rows that changed.
    result = db.fetch_one(
        """
        DECLARE @touched TABLE (SalesOrderID INT);

        WITH victims AS (
            SELECT TOP (?) d.SalesOrderDetailID
            FROM   Sales.SalesOrderDetail AS d
            WHERE  d.ModifiedDate < DATEADD(MINUTE, -1, GETUTCDATE())
            ORDER  BY NEWID()
        )
        UPDATE d
        SET    d.OrderQty     = CASE WHEN d.OrderQty > 1 THEN d.OrderQty - 1 ELSE d.OrderQty + 1 END,
               d.ModifiedDate = GETUTCDATE()
        OUTPUT INSERTED.SalesOrderID INTO @touched (SalesOrderID)
        FROM   Sales.SalesOrderDetail AS d
        INNER JOIN victims AS v ON v.SalesOrderDetailID = d.SalesOrderDetailID;

        DECLARE @lines INT = @@ROWCOUNT;

        /*  The iduSalesOrderDetail trigger has already recalculated SubTotal on
            these headers. It does not touch their ModifiedDate, so bumping it
            here is what makes the restatement visible to the pipeline at all.
            Skipping it would leave the source holding a header whose SubTotal
            disagrees with the warehouse copy -- which is exactly the silent
            corruption RECON_HeaderSubTotalVsLines exists to detect.          */
        UPDATE h
        SET    h.TaxAmt       = ROUND(h.SubTotal * 0.08, 4),
               h.Freight      = ROUND(h.SubTotal * 0.025, 4),
               h.ModifiedDate = GETUTCDATE()
        FROM   Sales.SalesOrderHeader AS h
        WHERE  EXISTS (SELECT 1 FROM @touched AS t WHERE t.SalesOrderID = h.SalesOrderID);

        SELECT lines_amended = @lines, headers_restated = @@ROWCOUNT;
        """,
        count,
    )

    if result is None:
        return 0

    log.info(
        "  amended %d line(s) across %d header(s)",
        result["lines_amended"],
        result["headers_restated"],
    )
    return int(result["lines_amended"])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Simulate a night of order activity in the AdventureWorks source."
    )
    parser.add_argument("--new-orders", type=int, default=None, help="Orders to book.")
    parser.add_argument("--amendments", type=int, default=None, help="Existing orders to restate.")
    parser.add_argument(
        "--preset",
        choices=sorted(PRESETS),
        default=None,
        help="Use a predefined activity level instead of explicit counts.",
    )
    parser.add_argument("--seed", type=int, default=None, help="Seed the RNG for a repeatable run.")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s", datefmt="%H:%M:%S"
    )

    if args.seed is not None:
        random.seed(args.seed)

    if args.preset:
        shape = PRESETS[args.preset]
        new_orders = args.new_orders if args.new_orders is not None else shape["new_orders"]
        amendments = args.amendments if args.amendments is not None else shape["amendments"]
    else:
        new_orders = args.new_orders if args.new_orders is not None else 25
        amendments = args.amendments if args.amendments is not None else 15

    settings = Settings.from_env()

    with Database(settings.source) as db:
        log.info("Source: %s", db.descriptor)
        log.info("Booking %d new order(s)...", new_orders)
        created = book_new_orders(db, new_orders)

        log.info("Amending %d existing order line(s)...", amendments)
        amended = amend_existing_orders(db, amendments)

        header_total = db.scalar("SELECT COUNT_BIG(*) FROM Sales.SalesOrderHeader")
        detail_total = db.scalar("SELECT COUNT_BIG(*) FROM Sales.SalesOrderDetail")

    log.info(
        "Booked %d new order(s) (ids %s..%s), amended %d line(s).",
        len(created),
        created[0] if created else "-",
        created[-1] if created else "-",
        amended,
    )
    log.info("Source now holds %s header(s) and %s line(s).", f"{header_total:,}", f"{detail_total:,}")
    log.info("Run the pipeline to pick these up: python -m etl.run_pipeline --trigger Scheduled")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
