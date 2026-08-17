"""The pipeline itself.

This is the local counterpart of ``adf/pipeline/PL_LoadSalesData_Orchestrator``.
Both execute the same stages in the same order against the same stored
procedures; what differs is only who is holding the baton:

    stage                 here                     in Azure Data Factory
    -------------------   ----------------------   --------------------------
    open the run          exec_proc_one            Lookup activity
    clear staging         exec_proc                Stored Procedure activity
    count the source      source.scalar            Lookup activity (source LS)
    extract + land        stream + bulk_insert     Copy activity
    quarantine bad rows   exec_proc                Stored Procedure activity
    upsert into dw        exec_proc                Stored Procedure activity
    validate              exec_proc_one            Lookup activity
    branch on the result  if/else below            If Condition activity
    alert                 AlertDispatcher          Web activity -> Logic App
    close the run         exec_proc                Stored Procedure activity

Keeping the decisions in SQL rather than in either orchestrator is what makes
that table possible. Nothing here decides whether a check passed.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .alerting import Alert, AlertDispatcher
from .config import Settings
from .db import Database
from .entities import ENTITIES, Entity

log = logging.getLogger(__name__)


@dataclass
class EntityResult:
    """What happened to one table during a run."""

    name: str
    source_rows: int = 0
    source_high_water: datetime | None = None
    extracted_rows: int = 0
    loaded_rows: int = 0
    inserted_rows: int = 0
    updated_rows: int = 0
    rejected_rows: int = 0
    extract_seconds: float = 0.0
    load_seconds: float = 0.0


@dataclass
class RunResult:
    """The outcome of a single pipeline execution."""

    run_id: int
    status: str
    load_type: str
    trigger: str
    watermark_from: datetime | None = None
    watermark_to: datetime | None = None
    entities: dict[str, EntityResult] = field(default_factory=dict)
    checks_run: int = 0
    checks_failed: int = 0
    critical_failed: int = 0
    failed_checks: str = ""
    duration_seconds: float = 0.0
    alerted: bool = False
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"Succeeded", "CompletedWithWarnings"}

    @property
    def total_extracted(self) -> int:
        return sum(e.extracted_rows for e in self.entities.values())

    @property
    def total_loaded(self) -> int:
        return sum(e.loaded_rows for e in self.entities.values())

    @property
    def total_rejected(self) -> int:
        return sum(e.rejected_rows for e in self.entities.values())


class SalesDataPipeline:
    """Moves Sales data from AdventureWorks into SalesReportingDW."""

    PIPELINE_NAME = "PL_LoadSalesData"

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._source = Database(settings.source)
        self._target = Database(settings.target)
        self._alerts = AlertDispatcher(
            self._target,
            webhook_url=settings.alert_webhook_url,
            log_dir=settings.alert_log_dir,
        )

    # ------------------------------------------------------------------ run
    def run(self, trigger: str = "Manual", load_type: str = "Incremental") -> RunResult:
        started = time.monotonic()
        run_id: int | None = None
        result: RunResult | None = None

        try:
            with self._source, self._target:
                log.info("Source: %s", self._source.descriptor)
                log.info("Target: %s", self._target.descriptor)

                # -- 1. open the run, resolve the incremental window ---------
                run_row = self._target.exec_proc_one(
                    "etl.usp_StartRun",
                    pipeline_name=self.PIPELINE_NAME,
                    run_trigger=trigger,
                    load_type=load_type,
                )
                if run_row is None:
                    raise RuntimeError("etl.usp_StartRun returned no row")

                run_id = int(run_row["run_id"])
                wm_from = run_row["watermark_from"]
                wm_to = run_row["watermark_to"]

                result = RunResult(
                    run_id=run_id,
                    status="Running",
                    load_type=load_type,
                    trigger=trigger,
                    watermark_from=wm_from,
                    watermark_to=wm_to,
                )

                log.info(
                    "Run %d started (%s, %s). Window: %s -> %s",
                    run_id, trigger, load_type, wm_from, wm_to,
                )

                # -- 2. clear the landing zone -------------------------------
                self._target.exec_proc("etl.usp_TruncateStaging")

                # -- 3. extract each entity into staging ---------------------
                for entity in ENTITIES:
                    result.entities[entity.name] = self._extract(
                        entity, run_id, wm_from, wm_to
                    )

                # -- 4. quarantine rows that fail the pre-load rules ---------
                self._target.exec_proc("etl.usp_RunStagingRules", run_id=run_id)
                self._collect_rejected(result, run_id)

                # -- 5. upsert staging into the warehouse --------------------
                for entity in ENTITIES:
                    self._load(entity, run_id, result)

                # -- 6. validate ---------------------------------------------
                validation = self._target.exec_proc_one(
                    "etl.usp_ValidateSalesData", run_id=run_id
                )
                if validation is None:
                    raise RuntimeError("etl.usp_ValidateSalesData returned no row")

                result.checks_run = int(validation.get("checks_run") or 0)
                result.checks_failed = int(validation.get("checks_failed") or 0)
                result.critical_failed = int(validation.get("critical_failed") or 0)
                result.failed_checks = validation.get("failed_check_list") or ""
                should_alert = bool(validation.get("should_alert"))

                log.info(
                    "Validation: %d check(s), %d failed (%d critical)",
                    result.checks_run, result.checks_failed, result.critical_failed,
                )

                # -- 7. branch on the validation outcome ---------------------
                if should_alert:
                    result.status = self._fail_run(result, run_id)
                else:
                    result.status = self._finish_run(result, run_id, wm_to)

        except Exception as exc:  # noqa: BLE001 - converted into a failed run below
            log.exception("Pipeline run failed")
            result = self._handle_crash(result, run_id, exc, trigger, load_type)

        if result is not None:
            result.duration_seconds = round(time.monotonic() - started, 3)
        assert result is not None
        return result

    # -------------------------------------------------------------- stages
    def _extract(
        self, entity: Entity, run_id: int, wm_from: Any, wm_to: Any
    ) -> EntityResult:
        """Copy one entity's changed rows from source into staging."""
        outcome = EntityResult(name=entity.name)
        started = time.monotonic()

        # Ask the source how many rows it is offering, before reading any.
        # This is the number reconciliation is measured against, and taking it
        # from the source -- not from what we happened to receive -- is what
        # makes the check meaningful rather than tautological.
        outcome.source_rows = int(
            self._source.scalar(entity.source_count_query(), wm_from, wm_to) or 0
        )

        # Read the source's overall high-water mark while we are connected to
        # it. The freshness check compares the warehouse against this, and the
        # warehouse has no way to ask the source itself.
        outcome.source_high_water = self._source.scalar(entity.source_high_water_query())

        log.info(
            "[%s] source offers %s row(s) in window",
            entity.name, f"{outcome.source_rows:,}",
        )

        if outcome.source_rows:
            rows = (
                (*row, run_id)
                for row in self._source.stream(
                    entity.extract_query(),
                    wm_from,
                    wm_to,
                    arraysize=self._settings.batch_size,
                )
            )
            outcome.extracted_rows = self._target.bulk_insert(
                entity.staging_table,
                entity.staging_columns,
                rows,
                batch_size=self._settings.batch_size,
            )

        outcome.extract_seconds = round(time.monotonic() - started, 3)
        log.info(
            "[%s] extracted %s row(s) into %s in %.2fs",
            entity.name, f"{outcome.extracted_rows:,}",
            entity.staging_table, outcome.extract_seconds,
        )

        self._target.exec_proc(
            "etl.usp_LogRunEntity",
            run_id=run_id,
            entity_name=entity.name,
            source_row_count=outcome.source_rows,
            source_max_watermark=outcome.source_high_water,
            rows_extracted=outcome.extracted_rows,
            extract_seconds=outcome.extract_seconds,
        )
        return outcome

    def _load(self, entity: Entity, run_id: int, result: RunResult) -> None:
        """Upsert one entity from staging into the warehouse."""
        outcome = result.entities[entity.name]
        started = time.monotonic()

        load_row = self._target.exec_proc_one(entity.load_proc, run_id=run_id)

        outcome.load_seconds = round(time.monotonic() - started, 3)
        if load_row:
            outcome.loaded_rows = int(load_row.get("rows_loaded") or 0)
            outcome.inserted_rows = int(load_row.get("rows_inserted") or 0)
            outcome.updated_rows = int(load_row.get("rows_updated") or 0)

        log.info(
            "[%s] loaded %s row(s) (%s new, %s updated) in %.2fs",
            entity.name, f"{outcome.loaded_rows:,}",
            f"{outcome.inserted_rows:,}", f"{outcome.updated_rows:,}",
            outcome.load_seconds,
        )

        self._target.exec_proc(
            "etl.usp_LogRunEntity",
            run_id=run_id,
            entity_name=entity.name,
            load_seconds=outcome.load_seconds,
        )

    def _collect_rejected(self, result: RunResult, run_id: int) -> None:
        """Read back what the rule engine quarantined."""
        rows = self._target.fetch_all(
            "SELECT entity_name, rows_rejected FROM etl.etl_run_entity WHERE run_id = ?",
            run_id,
        )
        for row in rows:
            entity_result = result.entities.get(row["entity_name"])
            if entity_result is not None:
                entity_result.rejected_rows = int(row["rows_rejected"] or 0)

        if result.total_rejected:
            log.warning(
                "%s row(s) quarantined to etl.etl_rejected_row",
                f"{result.total_rejected:,}",
            )

    def _finish_run(self, result: RunResult, run_id: int, wm_to: Any) -> str:
        """Close a run that passed validation and advance the watermarks."""
        end_row = self._target.exec_proc_one("etl.usp_EndRun", run_id=run_id)
        status = (end_row or {}).get("status", "Succeeded")

        # Only now, after validation passed. A failed run deliberately leaves
        # the watermark where it was so the next run re-reads the same window
        # instead of stepping over data that never arrived.
        for entity in ENTITIES:
            self._target.exec_proc(
                "etl.usp_SetWatermark",
                entity_name=entity.name,
                watermark_value=wm_to,
                run_id=run_id,
            )

        log.info("Run %d finished: %s. Watermarks advanced to %s", run_id, status, wm_to)
        return str(status)

    def _fail_run(self, result: RunResult, run_id: int) -> str:
        """Close a run that failed validation, and raise the alert."""
        subject = f"ETL run {run_id} FAILED validation ({result.critical_failed} critical)"
        result.alerted = self._alerts.dispatch(
            Alert(
                alert_type="ValidationFailure",
                severity="Critical",
                subject=subject,
                run_id=run_id,
                body={
                    "checksRun": result.checks_run,
                    "checksFailed": result.checks_failed,
                    "criticalFailed": result.critical_failed,
                    "failedChecks": result.failed_checks,
                    "rowsExtracted": result.total_extracted,
                    "rowsLoaded": result.total_loaded,
                    "rowsQuarantined": result.total_rejected,
                    "watermarkHeld": True,
                },
            )
        )

        end_row = self._target.exec_proc_one(
            "etl.usp_EndRun",
            run_id=run_id,
            status="Failed",
            error_message=f"Validation failed: {result.failed_checks}"[:4000],
        )
        status = (end_row or {}).get("status", "Failed")

        log.error(
            "Run %d FAILED validation. Watermark held. Failed checks: %s",
            run_id, result.failed_checks,
        )
        return str(status)

    def _handle_crash(
        self,
        result: RunResult | None,
        run_id: int | None,
        exc: Exception,
        trigger: str,
        load_type: str,
    ) -> RunResult:
        """Turn an unhandled exception into a recorded, alerted, failed run.

        A crash that leaves ``etl_run_log`` showing 'Running' forever is worse
        than the crash: the metrics silently stop counting it, and nothing
        notices the pipeline stopped. So the run gets closed either way.
        """
        message = f"{type(exc).__name__}: {exc}"

        if result is None:
            result = RunResult(
                run_id=run_id or -1,
                status="Failed",
                load_type=load_type,
                trigger=trigger,
            )
        result.error = message
        result.status = "Failed"

        if run_id is None:
            # Failed before the run row existed -- nothing to close, but the
            # alert still needs to go out.
            log.error("Run failed before it could be registered: %s", message)
            try:
                self._alerts.dispatch(
                    Alert(
                        alert_type="PipelineFailure",
                        severity="Critical",
                        subject="ETL pipeline failed before the run could start",
                        body={"error": message},
                    )
                )
            except Exception:  # noqa: BLE001
                log.exception("Could not dispatch startup-failure alert")
            return result

        try:
            with self._target:
                result.alerted = self._alerts.dispatch(
                    Alert(
                        alert_type="PipelineFailure",
                        severity="Critical",
                        subject=f"ETL run {run_id} FAILED: {type(exc).__name__}",
                        run_id=run_id,
                        body={
                            "error": message,
                            "rowsExtracted": result.total_extracted,
                            "rowsLoaded": result.total_loaded,
                            "watermarkHeld": True,
                        },
                    )
                )
                self._target.exec_proc(
                    "etl.usp_EndRun",
                    run_id=run_id,
                    status="Failed",
                    error_message=message[:4000],
                )
        except Exception:  # noqa: BLE001
            log.exception("Could not close run %s after failure", run_id)

        return result
