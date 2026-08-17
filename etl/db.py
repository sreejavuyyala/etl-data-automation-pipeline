"""Thin database layer over pyodbc.

Deliberately thin. The pipeline's logic lives in stored procedures so that the
Azure Data Factory pipeline and this runner execute the same code; this module
exists only to call them and to move rows, which is the part ADF's Copy
activity would do.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Iterable, Iterator, Sequence

import pyodbc

from .config import SqlConnection

log = logging.getLogger(__name__)


class Database:
    """A connection to one SQL Server database."""

    def __init__(self, conn: SqlConnection, autocommit: bool = True) -> None:
        self._config = conn
        self._autocommit = autocommit
        self._connection: pyodbc.Connection | None = None

    # -- lifecycle ----------------------------------------------------------
    def connect(self) -> pyodbc.Connection:
        if self._connection is None:
            log.debug("Connecting to %s", self._config.safe_descriptor())
            self._connection = pyodbc.connect(
                self._config.connection_string(),
                autocommit=self._autocommit,
                timeout=self._config.login_timeout,
            )
        return self._connection

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __enter__(self) -> "Database":
        self.connect()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    @property
    def descriptor(self) -> str:
        return self._config.safe_descriptor()

    @contextmanager
    def cursor(self) -> Iterator[pyodbc.Cursor]:
        cur = self.connect().cursor()
        try:
            yield cur
        finally:
            cur.close()

    # -- reads --------------------------------------------------------------
    def scalar(self, sql: str, *params: Any) -> Any:
        with self.cursor() as cur:
            cur.execute(sql, *params)
            row = cur.fetchone()
            return None if row is None else row[0]

    def fetch_all(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        with self.cursor() as cur:
            cur.execute(sql, *params)
            return _rows_as_dicts(cur)

    def fetch_one(self, sql: str, *params: Any) -> dict[str, Any] | None:
        rows = self.fetch_all(sql, *params)
        return rows[0] if rows else None

    # -- stored procedures --------------------------------------------------
    def exec_proc(self, proc: str, **params: Any) -> list[dict[str, Any]]:
        """Call a stored procedure by name with keyword parameters.

        Returns the first result set that actually has columns. A procedure
        such as ``usp_ValidateSalesData`` calls several others internally, each
        of which may emit its own result set; ``nextset()`` walks past those to
        reach the one the caller asked for.
        """
        if not params:
            sql = f"EXEC {proc}"
            args: tuple[Any, ...] = ()
        else:
            placeholders = ", ".join(f"@{name} = ?" for name in params)
            sql = f"EXEC {proc} {placeholders}"
            args = tuple(params.values())

        with self.cursor() as cur:
            cur.execute(sql, args)
            while True:
                if cur.description is not None:
                    rows = _rows_as_dicts(cur)
                    if rows:
                        return rows
                if not cur.nextset():
                    return []

    def exec_proc_one(self, proc: str, **params: Any) -> dict[str, Any] | None:
        rows = self.exec_proc(proc, **params)
        return rows[0] if rows else None

    def exec_sql(self, sql: str, *params: Any) -> int:
        """Run a statement, returning the affected row count."""
        with self.cursor() as cur:
            cur.execute(sql, *params)
            return cur.rowcount

    # -- writes -------------------------------------------------------------
    def bulk_insert(
        self,
        table: str,
        columns: Sequence[str],
        rows: Iterable[Sequence[Any]],
        batch_size: int = 5000,
    ) -> int:
        """Insert rows in batches, returning the number written.

        ``fast_executemany`` is what makes this viable: without it pyodbc sends
        one round trip per row, which turns a 121,000-row load into a
        multi-minute exercise in network latency. With it, the driver binds the
        whole batch as an array and sends it in one go.
        """
        column_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        sql = f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"

        total = 0
        connection = self.connect()
        cur = connection.cursor()
        try:
            cur.fast_executemany = True
            batch: list[Sequence[Any]] = []
            for row in rows:
                batch.append(row)
                if len(batch) >= batch_size:
                    cur.executemany(sql, batch)
                    total += len(batch)
                    batch.clear()
            if batch:
                cur.executemany(sql, batch)
                total += len(batch)
            if not self._autocommit:
                connection.commit()
        finally:
            cur.close()

        return total

    def stream(
        self, sql: str, *params: Any, arraysize: int = 5000
    ) -> Iterator[Sequence[Any]]:
        """Yield rows from a query without materialising the whole result.

        The header extract is small enough to hold in memory, but the detail
        table is not guaranteed to be, and a pipeline that only works while the
        increment stays small is not much of a pipeline.
        """
        with self.cursor() as cur:
            cur.arraysize = arraysize
            cur.execute(sql, *params)
            while True:
                rows = cur.fetchmany(arraysize)
                if not rows:
                    break
                yield from rows


def _rows_as_dicts(cursor: pyodbc.Cursor) -> list[dict[str, Any]]:
    if cursor.description is None:
        return []
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]
