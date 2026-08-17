# Runbook

Operational procedures: setting the pipeline up, and what to do when it breaks.

---

## Local setup

### Prerequisites

| Requirement | Notes |
| --- | --- |
| Docker | Docker Desktop, Colima, or OrbStack. The SQL Server container needs ~3 GB. |
| Python 3.11+ | For the runner and tests. |
| ODBC Driver 18 for SQL Server | See below — this is the step most likely to bite. |
| ~3 GB free disk | 2.3 GB image, 200 MB backup, ~500 MB restored database. |

**Installing the ODBC driver on macOS.** The Homebrew formula is gated behind a
Microsoft EULA, and the environment variable it checks is `HOMEBREW_ACCEPT_EULA`
— not `ACCEPT_EULA`, which is what the container image uses. Setting the wrong
one leaves `brew install` waiting on a prompt you cannot see, apparently hung:

```bash
brew tap microsoft/mssql-release
brew trust microsoft/mssql-release
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18
python -c "import pyodbc; print(pyodbc.drivers())"
# -> ['ODBC Driver 18 for SQL Server']
```

On Debian/Ubuntu, follow Microsoft's
[ODBC install guide](https://learn.microsoft.com/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server).

**Apple Silicon.** `mcr.microsoft.com/mssql/server` has no arm64 build, so the
container runs amd64 under Rosetta. It works and it is a genuine SQL Server
engine, but the initial full load takes noticeably longer than it would on
x86 hardware. Enable Docker Desktop → Settings → General → *Use Rosetta for
x86_64/amd64 emulation*.

### Standing it up

```bash
cp .env.example .env
$EDITOR .env                        # set MSSQL_SA_PASSWORD and the ETL_*_PASSWORD values

docker compose up -d                # SQL Server 2022, healthcheck-gated
./scripts/restore_adventureworks.sh # downloads ~200 MB, restores, prints the baseline
./scripts/deploy_schema.sh          # creates SalesReportingDW and every object

python -m etl.run_pipeline --load-type Full   # first load: all 152,782 rows
```

Then, for incremental runs:

```bash
python -m etl.run_pipeline --trigger Scheduled
```

### Verifying

```bash
pytest tests/ -q                    # 41 integration tests against the live database
python -m etl.metrics               # the measured report
```

---

## Scheduling it locally

The pipeline exits non-zero on failure, so any scheduler can drive it:

```cron
0 2 * * *  cd /path/to/etl-data-automation-pipeline && \
           /path/to/.venv/bin/python -m etl.run_pipeline --trigger Scheduled \
           >> logs/etl.log 2>&1
```

`--trigger Scheduled` is not cosmetic — the metrics report separates scheduled
runs from ad-hoc ones.

---

## Azure deployment

> **Not yet executed.** These steps are written against the documented resource
> schemas and the az CLI 2.58–2.60 surface, but have not been run against a live
> subscription. Use `-WhatIf` first. See `infra/main.bicep`.

```powershell
az login
./scripts/Deploy-Azure.ps1 -ResourceGroup rg-etl-salesdw -SqlAdminLogin etladmin -WhatIf
./scripts/Deploy-Azure.ps1 -ResourceGroup rg-etl-salesdw -SqlAdminLogin etladmin
```

Then, in order:

1. **Restore AdventureWorks into Azure SQL.** A `.bak` cannot be restored to
   Azure SQL Database — it only accepts `.bacpac`. Either import the
   [AdventureWorks bacpac](https://learn.microsoft.com/sql/samples/adventureworks-install-configure)
   through the portal, or restore the `.bak` to a local instance and use the
   *Deploy Database to Microsoft Azure SQL Database* wizard in SSMS.

2. **Deploy the Logic App**, then store its callback URL in Key Vault. The URL
   embeds a SAS signature, so it is a credential:

   ```bash
   az deployment group create -g rg-etl-salesdw \
     --template-file adf/alerts/LogicApp_EtlFailureNotification.json \
     --parameters notificationEmail=you@example.com

   CALLBACK=$(az rest --method post \
     --uri "<logicAppResourceId>/triggers/When_an_ETL_alert_is_received/listCallbackUrl?api-version=2019-05-01" \
     --query value -o tsv)

   az keyvault secret set --vault-name <kv> --name etl-alert-logicapp-url --value "$CALLBACK"
   ```

3. **Deploy the Azure Monitor rules** with the action group id the Logic App
   template outputs.

4. **Run the pipeline manually once** and confirm it succeeds.

5. **Start the trigger.** It ships `Stopped` deliberately — a scheduled pipeline
   should never begin firing as a side effect of a deployment:

   ```bash
   az datafactory trigger start -g rg-etl-salesdw \
     --factory-name adf-etl-salesdw --name TR_LoadSalesData_Daily_0200
   ```

---

## When a run fails

### 1. Find out what happened

```sql
SELECT TOP 10 * FROM etl.vw_RunSummary ORDER BY run_id DESC;
```

Then, for the failing run:

```sql
DECLARE @run_id BIGINT = <run id>;

SELECT check_name, entity_name, expected_value, actual_value, severity, message
FROM   etl.etl_validation_log
WHERE  run_id = @run_id AND status = 'FAIL'
ORDER  BY CASE severity WHEN 'Critical' THEN 0 ELSE 1 END;

SELECT business_key, rejection_reason, rejection_detail, row_payload
FROM   etl.etl_rejected_row
WHERE  run_id = @run_id;
```

### 2. Understand the failure mode

| Failed check | What it means | What to do |
| --- | --- | --- |
| `RECON_SourceToStaging` | Fewer rows arrived than the source offered. A truncated or partially-failed copy. | Check for a timeout or a dropped connection. Re-run — the watermark was held, so the same window is re-read. |
| `RECON_StagingToWarehouse` | Rows disappeared between staging and the warehouse without being quarantined. | Serious: the load path lost data. Check for a constraint violation swallowed by the MERGE. |
| `NULL_*`, `NEG_*`, `DATE_*`, `QTY_*` | Source rows violate a business rule. The rows are in quarantine. | Inspect `etl_rejected_row`. Fix at source, or amend the rule if the rule is wrong. |
| `ORPHAN_DetailWithoutHeader` | An order line references a missing order. | Should be impossible — the FK prevents it. Check whether a constraint was disabled. |
| `RECON_HeaderSubTotalVsLines` | An order's total disagrees with the sum of its lines. | Usually a partially-loaded order. Identify it with the query below and re-run. |
| `FRESH_PipelineLag` (Warning) | The warehouse is behind the source. | Not itself a failure. If it persists across runs, the pipeline is falling behind. |
| `DUP_*` (Warning) | Two rows for one key in a single batch. | The loader keeps the newest. Investigate why the source produced two. |

Orders whose totals disagree with their lines:

```sql
SELECT h.SalesOrderID, h.SubTotal, agg.line_sum, h.SubTotal - agg.line_sum AS difference
FROM   dw.SalesOrderHeader AS h
CROSS APPLY (SELECT line_sum = SUM(d.LineTotal)
             FROM   dw.SalesOrderDetail AS d
             WHERE  d.SalesOrderID = h.SalesOrderID) AS agg
WHERE  agg.line_sum IS NOT NULL AND ABS(h.SubTotal - agg.line_sum) > 0.01;
```

### 3. Recover

**In most cases, do nothing but fix the source and let the schedule run.**

A failed run does not advance the watermark, so the next run re-reads exactly
the same window. There is no replay to trigger and no backfill to schedule —
this is the designed behaviour, and it has been verified end to end.

To recover immediately rather than waiting for 02:00:

```bash
python -m etl.run_pipeline --trigger Manual
```

### 4. If the watermark is genuinely wrong

Only when a bug advanced it past data that was never loaded. Moving it backwards
causes the next run to re-read everything since that point; the `MERGE` is
idempotent, so re-reading is safe, just slower.

```sql
-- Inspect first.
SELECT * FROM etl.etl_watermark;

EXEC etl.usp_SetWatermark @entity_name = 'SalesOrderHeader',
                          @watermark_value = '2024-05-01T00:00:00';
```

Note that `usp_SetWatermark` refuses to move a watermark backwards — it takes
the greater of the current and supplied values, to stop a backfill run rewinding
it. To genuinely rewind, update `etl.etl_watermark` directly, deliberately.

---

## Common problems

**`docker compose up` starts, then the container exits.**
Almost always the SA password. SQL Server requires 8+ characters with three of:
uppercase, lowercase, digit, symbol — and reports the failure obscurely.

```bash
docker logs etl-sqlserver | tail -30
```

**`Login failed for user 'sa'`** — `.env` and the running container disagree.
The password is baked in at container creation, so changing `.env` afterwards
does not change it:

```bash
docker compose down -v && docker compose up -d   # -v drops the volume; you will re-restore
```

**`Can't open lib 'ODBC Driver 18 for SQL Server'`** — the driver is not
installed or not registered. See the prerequisites above.

**`SSL Provider: certificate verify failed`** — the container presents a
self-signed certificate. `ETL_TRUST_SERVER_CERTIFICATE=yes` is correct locally.
Against Azure SQL set it to `no`: that certificate is valid, and trusting any
certificate there would defeat TLS entirely.

**The restore fails with a path error.** The `.bak` was taken on Windows and
carries `C:\...` paths. `sql/00_restore_adventureworks.sql` generates the `MOVE`
clauses from `RESTORE FILELISTONLY`, so this should not happen — if it does, the
backup file is probably truncated. Delete `data/backup/*.bak` and re-run.

**Incremental runs find 0 rows when the source has changed.** The changed rows
carry a `ModifiedDate` older than the current watermark, so they fall outside
every future window. This is why `scripts/simulate_source_activity.py` stamps
rows using SQL Server's own clock (`GETUTCDATE()`) rather than the client's — a
client-supplied timestamp can land fractionally *before* the watermark and be
stranded permanently. Confirm with:

```sql
SELECT entity_name, watermark_value FROM etl.etl_watermark;
SELECT MAX(ModifiedDate) FROM Sales.SalesOrderHeader;  -- on the source
```

---

## Routine maintenance

**Quarantine review.** Rejected rows accumulate and are never purged
automatically — losing the evidence of a data-quality problem is worse than the
disk cost.

```sql
SELECT entity_name, rejection_reason, COUNT(*) AS rows_held, MAX(rejected_at) AS most_recent
FROM   etl.etl_rejected_row
GROUP  BY entity_name, rejection_reason
ORDER  BY COUNT(*) DESC;
```

**Tuning a check.** Thresholds live in `etl.etl_config`, not in code:

```sql
SELECT * FROM etl.etl_config;

UPDATE etl.etl_config SET config_value = '48' WHERE config_key = 'freshness.max_lag_hours';
```

Redeploying the schema will not overwrite a tuned value — the seed `MERGE`
updates descriptions but leaves values alone.

**Adding a data-quality rule.** An `INSERT`, not a code change:

```sql
INSERT INTO etl.etl_validation_rule
    (entity_name, staging_table, check_name, check_type, column_name,
     predicate_sql, business_key_expr, severity, quarantine, description)
VALUES
    ('SalesOrderHeader', 'SalesOrderHeader', 'NEG_TaxAmt', 'DomainCheck', 'TaxAmt',
     N's.TaxAmt < 0', N'CONCAT(''SalesOrderID='', s.SalesOrderID)', 'Critical', 1,
     N'Negative tax on an order header.');
```

`predicate_sql` identifies a **bad** row. It is executed as written, so treat it
with the same care as a view definition — it is deployment-time content, never
user input.
