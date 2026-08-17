"""Configuration for the ETL runner.

Everything is read from the environment, with ``.env`` loaded as a fallback for
local development. Nothing is hard-coded and no default carries a real
credential -- the deployed pipeline gets the same values from Azure Key Vault
via the data factory's managed identity, and this module is the local-shell
equivalent of that lookup.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent


def load_dotenv(path: Path | None = None) -> None:
    """Load ``KEY=VALUE`` pairs from a .env file into ``os.environ``.

    Written by hand rather than pulled from python-dotenv to keep the runtime
    dependency list to the database driver alone. Existing environment
    variables always win, so an explicit ``export`` or a CI secret overrides
    the file rather than the other way round.
    """
    env_path = path or REPO_ROOT / ".env"
    if not env_path.is_file():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        # Strip one layer of matching quotes -- values such as the ODBC driver
        # name contain spaces and are quoted for the shell scripts that source
        # this same file.
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ.setdefault(key, value)


@dataclass(frozen=True)
class SqlConnection:
    """Connection details for one SQL Server / Azure SQL database."""

    server: str
    database: str
    user: str
    password: str = field(repr=False)
    driver: str = "ODBC Driver 18 for SQL Server"
    trust_server_certificate: bool = True
    login_timeout: int = 30

    def connection_string(self) -> str:
        """Build an ODBC connection string.

        ``Encrypt=yes`` is unconditional -- ODBC Driver 18 defaults to it, and
        turning it off to silence a certificate error would trade a warning for
        an unencrypted connection. Certificate *validation* is what
        ``TrustServerCertificate`` relaxes, and that is set from configuration
        so it can be enabled for the local container's self-signed certificate
        and left off against Azure SQL, whose certificate is genuinely valid.
        """
        return ";".join(
            [
                f"DRIVER={{{self.driver}}}",
                f"SERVER={self.server}",
                f"DATABASE={self.database}",
                f"UID={self.user}",
                f"PWD={self.password}",
                "Encrypt=yes",
                f"TrustServerCertificate={'yes' if self.trust_server_certificate else 'no'}",
                f"Connection Timeout={self.login_timeout}",
            ]
        )

    def safe_descriptor(self) -> str:
        """Human-readable identity with no credential in it. Safe to log."""
        return f"{self.user}@{self.server}/{self.database}"


@dataclass(frozen=True)
class Settings:
    """Everything the runner needs, resolved once at startup."""

    source: SqlConnection
    target: SqlConnection
    batch_size: int = 5000
    alert_webhook_url: str = field(default="", repr=False)
    alert_log_dir: Path = REPO_ROOT / "reports" / "alerts"

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()

        driver = os.environ.get("ETL_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
        trust = os.environ.get("ETL_TRUST_SERVER_CERTIFICATE", "yes").lower() in {
            "yes",
            "true",
            "1",
        }

        def _required(name: str) -> str:
            value = os.environ.get(name, "")
            if not value:
                raise RuntimeError(
                    f"{name} is not set. Copy .env.example to .env and fill it in, "
                    f"or export {name} in your shell."
                )
            return value

        source = SqlConnection(
            server=os.environ.get("ETL_SOURCE_SERVER", "localhost,1433"),
            database=os.environ.get("ETL_SOURCE_DATABASE", "AdventureWorks2022"),
            user=_required("ETL_SOURCE_USER"),
            password=_required("ETL_SOURCE_PASSWORD"),
            driver=driver,
            trust_server_certificate=trust,
        )
        target = SqlConnection(
            server=os.environ.get("ETL_TARGET_SERVER", "localhost,1433"),
            database=os.environ.get("ETL_TARGET_DATABASE", "SalesReportingDW"),
            user=_required("ETL_TARGET_USER"),
            password=_required("ETL_TARGET_PASSWORD"),
            driver=driver,
            trust_server_certificate=trust,
        )

        alert_dir = os.environ.get("ETL_ALERT_LOG_DIR", "reports/alerts")
        alert_path = Path(alert_dir)
        if not alert_path.is_absolute():
            alert_path = REPO_ROOT / alert_path

        return cls(
            source=source,
            target=target,
            batch_size=int(os.environ.get("ETL_BATCH_SIZE", "5000")),
            alert_webhook_url=os.environ.get("ETL_ALERT_WEBHOOK_URL", "").strip(),
            alert_log_dir=alert_path,
        )
