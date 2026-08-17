"""What the pipeline moves.

One place that names the source tables, the columns taken from them, and the
procedure that loads each into the warehouse. Adding a third table to the
pipeline means adding an ``Entity`` here and a staging table in
``sql/02_target_schema.sql`` -- not editing the orchestration.

The column lists are deliberately explicit rather than ``SELECT *``. Three of
the source columns (``SalesOrderNumber``, ``TotalDue``, ``LineTotal``) are
computed columns in AdventureWorks, and several others (``rowguid``,
``CreditCardID``, ``CurrencyRateID``) carry no reporting value. Naming the
columns means a schema change upstream surfaces as an explicit error here
rather than as silently different data downstream.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Sequence


@dataclass(frozen=True)
class Entity:
    """One table's journey from source to warehouse."""

    name: str
    source_schema: str
    source_table: str
    staging_table: str
    warehouse_table: str
    columns: Sequence[str]
    load_proc: str
    watermark_column: str = "ModifiedDate"

    @property
    def source_object(self) -> str:
        return f"[{self.source_schema}].[{self.source_table}]"

    def extract_query(self) -> str:
        """Rows modified within the half-open window ``[from, to)``.

        The upper bound is exclusive so a row landing exactly on the boundary
        is read once, by the run that owns that window, rather than by both it
        and the next one.
        """
        column_list = ", ".join(f"[{c}]" for c in self.columns)
        return (
            f"SELECT {column_list} "
            f"FROM {self.source_object} "
            f"WHERE [{self.watermark_column}] >= ? AND [{self.watermark_column}] < ? "
            f"ORDER BY [{self.watermark_column}]"
        )

    def source_count_query(self) -> str:
        """How many rows the source claims for the same window.

        This is the number row-count reconciliation compares against, and it
        runs against the source connection -- the warehouse has no way to see
        across to the OLTP system, by design.
        """
        return (
            f"SELECT COUNT_BIG(*) FROM {self.source_object} "
            f"WHERE [{self.watermark_column}] >= ? AND [{self.watermark_column}] < ?"
        )

    def source_high_water_query(self) -> str:
        """The newest ModifiedDate anywhere in the source table.

        Not restricted to the extraction window: this is the reference point
        the freshness check compares the warehouse against, so it has to mean
        "how current is the source right now", independent of what this
        particular run happened to read.
        """
        return f"SELECT MAX([{self.watermark_column}]) FROM {self.source_object}"

    @property
    def staging_columns(self) -> list[str]:
        """Source columns plus the lineage column stamped on at load time."""
        return [*self.columns, "etl_run_id"]


SALES_ORDER_HEADER: Final = Entity(
    name="SalesOrderHeader",
    source_schema="Sales",
    source_table="SalesOrderHeader",
    staging_table="stg.SalesOrderHeader",
    warehouse_table="dw.SalesOrderHeader",
    load_proc="etl.usp_LoadSalesOrderHeader",
    columns=(
        "SalesOrderID",
        "RevisionNumber",
        "OrderDate",
        "DueDate",
        "ShipDate",
        "Status",
        "OnlineOrderFlag",
        "SalesOrderNumber",
        "PurchaseOrderNumber",
        "AccountNumber",
        "CustomerID",
        "SalesPersonID",
        "TerritoryID",
        "BillToAddressID",
        "ShipToAddressID",
        "ShipMethodID",
        "SubTotal",
        "TaxAmt",
        "Freight",
        "TotalDue",
        "Comment",
        "ModifiedDate",
    ),
)

SALES_ORDER_DETAIL: Final = Entity(
    name="SalesOrderDetail",
    source_schema="Sales",
    source_table="SalesOrderDetail",
    staging_table="stg.SalesOrderDetail",
    warehouse_table="dw.SalesOrderDetail",
    load_proc="etl.usp_LoadSalesOrderDetail",
    columns=(
        "SalesOrderID",
        "SalesOrderDetailID",
        "CarrierTrackingNumber",
        "OrderQty",
        "ProductID",
        "SpecialOfferID",
        "UnitPrice",
        "UnitPriceDiscount",
        "LineTotal",
        "ModifiedDate",
    ),
)

# Order matters: headers load before details so the foreign key from detail to
# header is satisfiable within a single run.
ENTITIES: Final[tuple[Entity, ...]] = (SALES_ORDER_HEADER, SALES_ORDER_DETAIL)
