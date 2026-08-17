"""Failure notification.

Two things happen when the pipeline decides a run needs attention, and they are
kept separate on purpose:

  1. The alert is *recorded* in ``etl.etl_alert``, inside the database, in the
     same transaction scope as everything else the run wrote. This is what the
     "how many runs needed a human?" metric counts, and it survives the
     notification channel being unreachable.

  2. The alert is *delivered* -- to the Logic App webhook in Azure, or to a
     file when no webhook is configured. Delivery is best-effort: a pipeline
     that crashes because its alerting endpoint is down has turned its
     monitoring into an outage.

In the deployed topology step 2 is an ADF Web activity POSTing to the Logic App
in ``adf/alerts/``. Here it is the same JSON payload sent by the same method,
so a webhook that works for one works for the other.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


@dataclass
class Alert:
    """A notification the pipeline wants a human to see."""

    alert_type: str
    severity: str
    subject: str
    run_id: int | None = None
    body: dict[str, Any] = field(default_factory=dict)

    def payload(self) -> dict[str, Any]:
        """The JSON body posted to the webhook.

        Shaped to be readable in a Teams message card or an email template
        without transformation -- the Logic App in adf/alerts/ binds directly
        to these field names.
        """
        return {
            "alertType": self.alert_type,
            "severity": self.severity,
            "subject": self.subject,
            "runId": self.run_id,
            "pipeline": "LoadSalesData",
            "raisedAtUtc": datetime.now(timezone.utc).isoformat(),
            "details": self.body,
        }


class AlertDispatcher:
    """Records alerts in the database and attempts delivery."""

    def __init__(
        self,
        target_db: Any,
        webhook_url: str = "",
        log_dir: Path | None = None,
        timeout_seconds: int = 15,
    ) -> None:
        self._db = target_db
        self._webhook_url = webhook_url
        self._log_dir = log_dir
        self._timeout = timeout_seconds

    @property
    def channel(self) -> str:
        return "LogicApp" if self._webhook_url else "File"

    def dispatch(self, alert: Alert) -> bool:
        """Record and deliver. Returns whether delivery succeeded.

        Never raises: an alert that cannot be delivered must not become a
        second failure on top of the one it was reporting.
        """
        payload = alert.payload()
        body_json = json.dumps(payload, indent=2, default=str)

        alert_id: int | None = None
        try:
            row = self._db.exec_proc_one(
                "etl.usp_RaiseAlert",
                run_id=alert.run_id,
                alert_type=alert.alert_type,
                severity=alert.severity,
                subject=alert.subject,
                body=body_json,
                channel=self.channel,
            )
            alert_id = row.get("alert_id") if row else None
        except Exception:
            # Losing the durable record is worse than losing the notification,
            # so it is logged loudly -- but it still must not abort the run.
            log.exception("Failed to record alert in etl.etl_alert")

        delivered, detail = self._deliver(alert, body_json)

        if alert_id is not None:
            try:
                # Same procedure the ADF RaiseAlert_Pipeline calls, so the two
                # orchestrators cannot drift on how delivery is recorded.
                self._db.exec_proc(
                    "etl.usp_MarkAlertDelivered",
                    run_id=alert.run_id,
                    alert_id=alert_id,
                    delivered=1 if delivered else 0,
                    delivery_detail=detail[:1000],
                )
            except Exception:
                log.exception("Failed to update alert delivery status")

        return delivered

    def _deliver(self, alert: Alert, body_json: str) -> tuple[bool, str]:
        if self._webhook_url:
            return self._post_webhook(body_json)
        return self._write_file(alert, body_json)

    def _post_webhook(self, body_json: str) -> tuple[bool, str]:
        request = urllib.request.Request(
            self._webhook_url,
            data=body_json.encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                detail = f"HTTP {response.status} from alert webhook"
                log.info("Alert delivered: %s", detail)
                return True, detail
        except urllib.error.HTTPError as exc:
            detail = f"HTTP {exc.code} from alert webhook: {exc.reason}"
        except urllib.error.URLError as exc:
            detail = f"Alert webhook unreachable: {exc.reason}"
        except Exception as exc:  # noqa: BLE001 - delivery must never propagate
            detail = f"Alert webhook failed: {exc}"

        log.error("%s", detail)
        return False, detail

    def _write_file(self, alert: Alert, body_json: str) -> tuple[bool, str]:
        """Fallback sink when no webhook is configured.

        Not a no-op: a run performed with no notification endpoint still leaves
        the same evidence on disk that a delivered alert would have carried, so
        offline runs remain auditable.
        """
        if self._log_dir is None:
            log.warning("Alert raised with no webhook and no log directory: %s", alert.subject)
            return False, "No delivery channel configured"

        try:
            self._log_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            run_part = f"run{alert.run_id}" if alert.run_id is not None else "norun"
            path = self._log_dir / f"{stamp}_{run_part}_{alert.alert_type}.json"
            path.write_text(body_json, encoding="utf-8")
            detail = f"Written to {path.relative_to(self._log_dir.parent.parent)}"
            log.warning("ALERT (no webhook configured) -> %s", path)
            return True, detail
        except Exception as exc:  # noqa: BLE001
            detail = f"Failed to write alert file: {exc}"
            log.error("%s", detail)
            return False, detail
