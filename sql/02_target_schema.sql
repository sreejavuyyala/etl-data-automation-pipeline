/*==============================================================================
  02_target_schema.sql
  ------------------------------------------------------------------------------
  Staging / reporting schema for the Sales ETL pipeline.

  Three schemas, three jobs:

    stg  -- landing zone. Wide-open nullable columns so a bad row from the
            source *lands* rather than blowing up the extract. Validation
            happens here, after the data is safely on disk.
    dw   -- the clean, constrained reporting tables. Nothing arrives here that
            has not passed validation.
    etl  -- pipeline control and observability: run log, validation log,
            rejected rows, watermarks, alerts.

  Idempotent: safe to run repeatedly against an existing database.

  Target platform: SQL Server 2022 / Azure SQL Database
==============================================================================*/

SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

/*==============================================================================
  SCHEMAS
==============================================================================*/
IF SCHEMA_ID('stg') IS NULL EXEC ('CREATE SCHEMA stg AUTHORIZATION dbo');
GO
IF SCHEMA_ID('dw')  IS NULL EXEC ('CREATE SCHEMA dw  AUTHORIZATION dbo');
GO
IF SCHEMA_ID('etl') IS NULL EXEC ('CREATE SCHEMA etl AUTHORIZATION dbo');
GO


/*==============================================================================
  ETL CONTROL TABLES
==============================================================================*/

/*------------------------------------------------------------------------------
  etl.etl_run_log -- one row per pipeline execution.

  This is the table the resume metrics are computed from. Every run writes
  exactly one row: Running on start, then Succeeded / Failed / CompletedWith-
  Warnings on finish. `required_manual_intervention` is the flag that drives
  the "manual touches" metric -- it is set by usp_EndRun, not by a human.
------------------------------------------------------------------------------*/
IF OBJECT_ID('etl.etl_run_log', 'U') IS NULL
BEGIN
    CREATE TABLE etl.etl_run_log
    (
        run_id                       BIGINT         IDENTITY(1,1) NOT NULL,
        pipeline_name                SYSNAME        NOT NULL,
        run_trigger                  VARCHAR(20)    NOT NULL,   -- Scheduled | Manual | Backfill
        load_type                    VARCHAR(20)    NOT NULL,   -- Incremental | Full
        watermark_from               DATETIME2(3)   NULL,
        watermark_to                 DATETIME2(3)   NULL,
        start_time                   DATETIME2(3)   NOT NULL CONSTRAINT DF_etl_run_log_start DEFAULT (SYSUTCDATETIME()),
        end_time                     DATETIME2(3)   NULL,
        duration_seconds             AS (CASE WHEN end_time IS NULL THEN NULL
                                              ELSE CAST(DATEDIFF(MILLISECOND, start_time, end_time) / 1000.0 AS DECIMAL(12,3))
                                         END),
        rows_extracted               BIGINT         NOT NULL CONSTRAINT DF_etl_run_log_ext  DEFAULT (0),
        rows_loaded                  BIGINT         NOT NULL CONSTRAINT DF_etl_run_log_load DEFAULT (0),
        rows_rejected                BIGINT         NOT NULL CONSTRAINT DF_etl_run_log_rej  DEFAULT (0),
        checks_run                   INT            NOT NULL CONSTRAINT DF_etl_run_log_ck   DEFAULT (0),
        checks_failed                INT            NOT NULL CONSTRAINT DF_etl_run_log_ckf  DEFAULT (0),
        status                       VARCHAR(24)    NOT NULL CONSTRAINT DF_etl_run_log_st   DEFAULT ('Running'),
        required_manual_intervention BIT            NOT NULL CONSTRAINT DF_etl_run_log_mi   DEFAULT (0),
        error_message                NVARCHAR(4000) NULL,
        created_at                   DATETIME2(3)   NOT NULL CONSTRAINT DF_etl_run_log_ca   DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT PK_etl_run_log PRIMARY KEY CLUSTERED (run_id),
        CONSTRAINT CK_etl_run_log_status CHECK (status IN
            ('Running', 'Succeeded', 'CompletedWithWarnings', 'Failed')),
        CONSTRAINT CK_etl_run_log_trigger CHECK (run_trigger IN
            ('Scheduled', 'Manual', 'Backfill')),
        CONSTRAINT CK_etl_run_log_loadtype CHECK (load_type IN
            ('Incremental', 'Full'))
    );

    CREATE INDEX IX_etl_run_log_start  ON etl.etl_run_log (start_time DESC) INCLUDE (status);
    CREATE INDEX IX_etl_run_log_status ON etl.etl_run_log (status, start_time DESC);
END
GO

/*------------------------------------------------------------------------------
  etl.etl_run_entity -- per-table detail within a run.

  etl_run_log answers "did the 02:00 load work?"; this answers "which table
  moved how many rows?" without forcing one row per table in the parent log.
------------------------------------------------------------------------------*/
IF OBJECT_ID('etl.etl_run_entity', 'U') IS NULL
BEGIN
    CREATE TABLE etl.etl_run_entity
    (
        run_entity_id    BIGINT       IDENTITY(1,1) NOT NULL,
        run_id           BIGINT       NOT NULL,
        entity_name      SYSNAME      NOT NULL,     -- e.g. SalesOrderHeader
        source_row_count BIGINT       NULL,         -- counted at the source, in-window
        -- The newest ModifiedDate anywhere in the source table, not just in
        -- this window. This is the reference the freshness check measures the
        -- warehouse against -- see usp_ValidatePostLoad.
        source_max_watermark DATETIME2(3) NULL,
        rows_extracted   BIGINT       NOT NULL CONSTRAINT DF_etl_run_entity_ext DEFAULT (0),
        rows_loaded      BIGINT       NOT NULL CONSTRAINT DF_etl_run_entity_ld  DEFAULT (0),
        rows_inserted    BIGINT       NOT NULL CONSTRAINT DF_etl_run_entity_i   DEFAULT (0),
        rows_updated     BIGINT       NOT NULL CONSTRAINT DF_etl_run_entity_u   DEFAULT (0),
        rows_rejected    BIGINT       NOT NULL CONSTRAINT DF_etl_run_entity_r   DEFAULT (0),
        extract_seconds  DECIMAL(12,3) NULL,
        load_seconds     DECIMAL(12,3) NULL,
        logged_at        DATETIME2(3) NOT NULL CONSTRAINT DF_etl_run_entity_at  DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT PK_etl_run_entity PRIMARY KEY CLUSTERED (run_entity_id),
        CONSTRAINT FK_etl_run_entity_run FOREIGN KEY (run_id)
            REFERENCES etl.etl_run_log (run_id),
        CONSTRAINT UQ_etl_run_entity UNIQUE (run_id, entity_name)
    );
END
GO

/*------------------------------------------------------------------------------
  Additive column migrations.

  The CREATE TABLE above only runs on a fresh database, so a column added later
  needs its own guarded ALTER to reach databases that already exist. Keeping
  these next to the table -- rather than in a numbered migration folder -- suits
  a project with one deployment target; it would not scale to many.
------------------------------------------------------------------------------*/
IF COL_LENGTH('etl.etl_run_entity', 'source_max_watermark') IS NULL
    ALTER TABLE etl.etl_run_entity ADD source_max_watermark DATETIME2(3) NULL;
GO

/*------------------------------------------------------------------------------
  etl.etl_validation_log -- one row per check, per run.

  expected_value / actual_value are NVARCHAR so the same table can hold a row
  count ("31465"), a decimal tolerance comparison, and a textual assertion
  without three sets of columns. `severity` decides whether a failure fails the
  whole run (Critical) or just annotates it (Warning).
------------------------------------------------------------------------------*/
IF OBJECT_ID('etl.etl_validation_log', 'U') IS NULL
BEGIN
    CREATE TABLE etl.etl_validation_log
    (
        validation_id  BIGINT         IDENTITY(1,1) NOT NULL,
        run_id         BIGINT         NOT NULL,
        check_name     VARCHAR(120)   NOT NULL,
        check_type     VARCHAR(40)    NOT NULL,   -- RowCountReconciliation | NullCheck | TypeCheck | ...
        entity_name    SYSNAME        NULL,
        column_name    SYSNAME        NULL,
        expected_value NVARCHAR(200)  NULL,
        actual_value   NVARCHAR(200)  NULL,
        variance       DECIMAL(18,6)  NULL,       -- actual - expected, where numeric
        status         VARCHAR(10)    NOT NULL,   -- PASS | FAIL | WARN
        severity       VARCHAR(10)    NOT NULL CONSTRAINT DF_etl_val_sev DEFAULT ('Critical'),
        message        NVARCHAR(1000) NULL,
        checked_at     DATETIME2(3)   NOT NULL CONSTRAINT DF_etl_val_at DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT PK_etl_validation_log PRIMARY KEY CLUSTERED (validation_id),
        CONSTRAINT FK_etl_validation_log_run FOREIGN KEY (run_id)
            REFERENCES etl.etl_run_log (run_id),
        CONSTRAINT CK_etl_validation_log_status   CHECK (status   IN ('PASS', 'FAIL', 'WARN')),
        CONSTRAINT CK_etl_validation_log_severity CHECK (severity IN ('Critical', 'Warning', 'Info'))
    );

    CREATE INDEX IX_etl_validation_log_run    ON etl.etl_validation_log (run_id, status);
    CREATE INDEX IX_etl_validation_log_status ON etl.etl_validation_log (status, checked_at DESC);
END
GO

/*------------------------------------------------------------------------------
  etl.etl_rejected_row -- quarantine.

  A row that fails a Critical validation does not silently vanish and does not
  poison dw. It lands here as JSON with the reason attached, so it can be
  inspected and replayed. This is the difference between "the load failed" and
  "the load succeeded and here are the 3 rows a human should look at".
------------------------------------------------------------------------------*/
IF OBJECT_ID('etl.etl_rejected_row', 'U') IS NULL
BEGIN
    CREATE TABLE etl.etl_rejected_row
    (
        rejected_id       BIGINT         IDENTITY(1,1) NOT NULL,
        run_id            BIGINT         NOT NULL,
        entity_name       SYSNAME        NOT NULL,
        business_key      NVARCHAR(100)  NULL,      -- e.g. "SalesOrderID=43659"
        rejection_reason  VARCHAR(120)   NOT NULL,
        rejection_detail  NVARCHAR(1000) NULL,
        row_payload       NVARCHAR(MAX)  NULL,      -- the offending row, as JSON
        rejected_at       DATETIME2(3)   NOT NULL CONSTRAINT DF_etl_rej_at DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT PK_etl_rejected_row PRIMARY KEY CLUSTERED (rejected_id),
        CONSTRAINT FK_etl_rejected_row_run FOREIGN KEY (run_id)
            REFERENCES etl.etl_run_log (run_id)
    );

    CREATE INDEX IX_etl_rejected_row_run ON etl.etl_rejected_row (run_id, entity_name);
END
GO

/*------------------------------------------------------------------------------
  etl.etl_watermark -- incremental high-water mark per entity.

  This is what makes the pipeline incremental instead of a nightly full reload:
  each run extracts only rows whose ModifiedDate is greater than the stored
  watermark. The watermark advances only after the run is validated, so a
  failed run re-reads the same window next time rather than skipping it.
------------------------------------------------------------------------------*/
IF OBJECT_ID('etl.etl_watermark', 'U') IS NULL
BEGIN
    CREATE TABLE etl.etl_watermark
    (
        entity_name        SYSNAME      NOT NULL,
        watermark_value    DATETIME2(3) NOT NULL,
        last_run_id        BIGINT       NULL,
        updated_at         DATETIME2(3) NOT NULL CONSTRAINT DF_etl_wm_at DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT PK_etl_watermark PRIMARY KEY CLUSTERED (entity_name)
    );
END
GO

/*------------------------------------------------------------------------------
  etl.etl_alert -- every alert the pipeline raised, and whether it was delivered.

  Recording the alert in the database (not only in the notification channel)
  means the "how many runs needed a human?" metric survives even if the Logic
  App / SMTP endpoint was down.
------------------------------------------------------------------------------*/
IF OBJECT_ID('etl.etl_alert', 'U') IS NULL
BEGIN
    CREATE TABLE etl.etl_alert
    (
        alert_id        BIGINT         IDENTITY(1,1) NOT NULL,
        run_id          BIGINT         NULL,
        alert_type      VARCHAR(40)    NOT NULL,   -- PipelineFailure | ValidationFailure | Freshness
        severity        VARCHAR(10)    NOT NULL,
        subject         NVARCHAR(300)  NOT NULL,
        body            NVARCHAR(MAX)  NULL,
        channel         VARCHAR(40)    NULL,       -- LogicApp | Webhook | File | Console
        delivered       BIT            NOT NULL CONSTRAINT DF_etl_alert_d DEFAULT (0),
        delivery_detail NVARCHAR(1000) NULL,
        raised_at       DATETIME2(3)   NOT NULL CONSTRAINT DF_etl_alert_at DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT PK_etl_alert PRIMARY KEY CLUSTERED (alert_id),
        CONSTRAINT FK_etl_alert_run FOREIGN KEY (run_id)
            REFERENCES etl.etl_run_log (run_id)
    );

    CREATE INDEX IX_etl_alert_raised ON etl.etl_alert (raised_at DESC);
END
GO

/*------------------------------------------------------------------------------
  etl.etl_config -- knobs the pipeline reads at runtime.

  Tolerances and thresholds live here rather than being hard-coded in the
  procedures, so tuning a check does not require a schema deployment.
------------------------------------------------------------------------------*/
IF OBJECT_ID('etl.etl_config', 'U') IS NULL
BEGIN
    CREATE TABLE etl.etl_config
    (
        config_key   VARCHAR(80)    NOT NULL,
        config_value NVARCHAR(400)  NOT NULL,
        description  NVARCHAR(400)  NULL,
        updated_at   DATETIME2(3)   NOT NULL CONSTRAINT DF_etl_cfg_at DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_etl_config PRIMARY KEY CLUSTERED (config_key)
    );
END
GO

MERGE etl.etl_config AS tgt
USING (VALUES
    ('rowcount.tolerance_rows',      '0',
     'Permitted absolute difference between source and target row counts. Zero = exact reconciliation.'),
    ('financial.tolerance_amount',   '0.01',
     'Permitted absolute difference (currency) when reconciling SalesOrderHeader.SubTotal against SUM(SalesOrderDetail.LineTotal).'),
    ('freshness.max_lag_hours',      '24',
     'Warn if the warehouse high-water mark falls this many hours behind the SOURCE high-water mark. Measures pipeline lag, not the absolute age of the data.'),
    ('initial.watermark',            '1900-01-01T00:00:00',
     'Watermark used the first time an entity is loaded, i.e. the initial full extract.')
) AS src (config_key, config_value, description)
ON tgt.config_key = src.config_key
/*  Descriptions are documentation and are kept in step with this file. Values
    are not overwritten: once deployed, a threshold belongs to whoever tuned it,
    and a redeployment silently resetting it would be a nasty surprise.       */
WHEN MATCHED AND ISNULL(tgt.description, N'') <> src.description THEN
    UPDATE SET description = src.description
WHEN NOT MATCHED BY TARGET THEN
    INSERT (config_key, config_value, description)
    VALUES (src.config_key, src.config_value, src.description);
GO


/*==============================================================================
  STAGING TABLES

  Deliberately permissive. Every column is NULLable and the numeric/date types
  are the widest reasonable choice, because the entire point of the landing
  zone is that a malformed source row lands here and gets *reported* by the
  validation step rather than aborting the extract.
==============================================================================*/

IF OBJECT_ID('stg.SalesOrderHeader', 'U') IS NULL
BEGIN
    CREATE TABLE stg.SalesOrderHeader
    (
        SalesOrderID           INT             NULL,
        RevisionNumber         TINYINT         NULL,
        OrderDate              DATETIME2(3)    NULL,
        DueDate                DATETIME2(3)    NULL,
        ShipDate               DATETIME2(3)    NULL,
        Status                 TINYINT         NULL,
        OnlineOrderFlag        BIT             NULL,
        SalesOrderNumber       NVARCHAR(25)    NULL,
        PurchaseOrderNumber    NVARCHAR(25)    NULL,
        AccountNumber          NVARCHAR(15)    NULL,
        CustomerID             INT             NULL,
        SalesPersonID          INT             NULL,
        TerritoryID            INT             NULL,
        BillToAddressID        INT             NULL,
        ShipToAddressID        INT             NULL,
        ShipMethodID           INT             NULL,
        SubTotal               DECIMAL(19,4)   NULL,
        TaxAmt                 DECIMAL(19,4)   NULL,
        Freight                DECIMAL(19,4)   NULL,
        TotalDue               DECIMAL(19,4)   NULL,
        Comment                NVARCHAR(128)   NULL,
        ModifiedDate           DATETIME2(3)    NULL,
        -- ETL lineage
        etl_run_id             BIGINT          NULL,
        etl_loaded_at          DATETIME2(3)    NOT NULL CONSTRAINT DF_stg_soh_at DEFAULT (SYSUTCDATETIME())
    );

    CREATE INDEX IX_stg_SalesOrderHeader_run ON stg.SalesOrderHeader (etl_run_id);
    CREATE INDEX IX_stg_SalesOrderHeader_id  ON stg.SalesOrderHeader (SalesOrderID);
END
GO

IF OBJECT_ID('stg.SalesOrderDetail', 'U') IS NULL
BEGIN
    CREATE TABLE stg.SalesOrderDetail
    (
        SalesOrderID           INT             NULL,
        SalesOrderDetailID     INT             NULL,
        CarrierTrackingNumber  NVARCHAR(25)    NULL,
        OrderQty               SMALLINT        NULL,
        ProductID              INT             NULL,
        SpecialOfferID         INT             NULL,
        UnitPrice              DECIMAL(19,4)   NULL,
        UnitPriceDiscount      DECIMAL(19,4)   NULL,
        LineTotal              DECIMAL(38,6)   NULL,
        ModifiedDate           DATETIME2(3)    NULL,
        -- ETL lineage
        etl_run_id             BIGINT          NULL,
        etl_loaded_at          DATETIME2(3)    NOT NULL CONSTRAINT DF_stg_sod_at DEFAULT (SYSUTCDATETIME())
    );

    CREATE INDEX IX_stg_SalesOrderDetail_run ON stg.SalesOrderDetail (etl_run_id);
    CREATE INDEX IX_stg_SalesOrderDetail_id  ON stg.SalesOrderDetail (SalesOrderDetailID);
END
GO


/*==============================================================================
  REPORTING TABLES

  Constrained. A row only reaches dw after passing validation, so the NOT NULL
  and CHECK constraints here are a second line of defence rather than the
  primary one -- if one of them ever fires, a validation check is missing.
==============================================================================*/

IF OBJECT_ID('dw.SalesOrderHeader', 'U') IS NULL
BEGIN
    CREATE TABLE dw.SalesOrderHeader
    (
        SalesOrderID           INT             NOT NULL,
        RevisionNumber         TINYINT         NOT NULL,
        OrderDate              DATETIME2(3)    NOT NULL,
        DueDate                DATETIME2(3)    NOT NULL,
        ShipDate               DATETIME2(3)    NULL,
        Status                 TINYINT         NOT NULL,
        OnlineOrderFlag        BIT             NOT NULL,
        SalesOrderNumber       NVARCHAR(25)    NOT NULL,
        PurchaseOrderNumber    NVARCHAR(25)    NULL,
        AccountNumber          NVARCHAR(15)    NULL,
        CustomerID             INT             NOT NULL,
        SalesPersonID          INT             NULL,
        TerritoryID            INT             NULL,
        BillToAddressID        INT             NOT NULL,
        ShipToAddressID        INT             NOT NULL,
        ShipMethodID           INT             NOT NULL,
        SubTotal               DECIMAL(19,4)   NOT NULL,
        TaxAmt                 DECIMAL(19,4)   NOT NULL,
        Freight                DECIMAL(19,4)   NOT NULL,
        TotalDue               DECIMAL(19,4)   NOT NULL,
        Comment                NVARCHAR(128)   NULL,
        ModifiedDate           DATETIME2(3)    NOT NULL,
        -- ETL lineage
        etl_run_id             BIGINT          NOT NULL,
        etl_inserted_at        DATETIME2(3)    NOT NULL CONSTRAINT DF_dw_soh_ins DEFAULT (SYSUTCDATETIME()),
        etl_updated_at         DATETIME2(3)    NOT NULL CONSTRAINT DF_dw_soh_upd DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT PK_dw_SalesOrderHeader PRIMARY KEY CLUSTERED (SalesOrderID),
        CONSTRAINT CK_dw_soh_amounts   CHECK (SubTotal >= 0 AND TaxAmt >= 0 AND Freight >= 0 AND TotalDue >= 0),
        CONSTRAINT CK_dw_soh_due_date  CHECK (DueDate >= OrderDate),
        CONSTRAINT CK_dw_soh_ship_date CHECK (ShipDate IS NULL OR ShipDate >= OrderDate)
    );

    CREATE INDEX IX_dw_SalesOrderHeader_OrderDate  ON dw.SalesOrderHeader (OrderDate)    INCLUDE (TotalDue, CustomerID);
    CREATE INDEX IX_dw_SalesOrderHeader_Customer   ON dw.SalesOrderHeader (CustomerID)   INCLUDE (OrderDate, TotalDue);
    CREATE INDEX IX_dw_SalesOrderHeader_Modified   ON dw.SalesOrderHeader (ModifiedDate);
END
GO

IF OBJECT_ID('dw.SalesOrderDetail', 'U') IS NULL
BEGIN
    CREATE TABLE dw.SalesOrderDetail
    (
        SalesOrderDetailID     INT             NOT NULL,
        SalesOrderID           INT             NOT NULL,
        CarrierTrackingNumber  NVARCHAR(25)    NULL,
        OrderQty               SMALLINT        NOT NULL,
        ProductID              INT             NOT NULL,
        SpecialOfferID         INT             NOT NULL,
        UnitPrice              DECIMAL(19,4)   NOT NULL,
        UnitPriceDiscount      DECIMAL(19,4)   NOT NULL,
        LineTotal              DECIMAL(38,6)   NOT NULL,
        ModifiedDate           DATETIME2(3)    NOT NULL,
        -- ETL lineage
        etl_run_id             BIGINT          NOT NULL,
        etl_inserted_at        DATETIME2(3)    NOT NULL CONSTRAINT DF_dw_sod_ins DEFAULT (SYSUTCDATETIME()),
        etl_updated_at         DATETIME2(3)    NOT NULL CONSTRAINT DF_dw_sod_upd DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT PK_dw_SalesOrderDetail PRIMARY KEY CLUSTERED (SalesOrderDetailID),
        CONSTRAINT CK_dw_sod_qty      CHECK (OrderQty > 0),
        CONSTRAINT CK_dw_sod_price    CHECK (UnitPrice >= 0),
        CONSTRAINT CK_dw_sod_discount CHECK (UnitPriceDiscount >= 0 AND UnitPriceDiscount <= 1)
    );

    CREATE INDEX IX_dw_SalesOrderDetail_Order   ON dw.SalesOrderDetail (SalesOrderID) INCLUDE (LineTotal, OrderQty);
    CREATE INDEX IX_dw_SalesOrderDetail_Product ON dw.SalesOrderDetail (ProductID)    INCLUDE (OrderQty, LineTotal);
END
GO

/*------------------------------------------------------------------------------
  Foreign key detail -> header.

  NOT trusted-by-default: the loader writes headers before details within a
  run, but an incremental window can legitimately contain a detail row whose
  header was loaded in an *earlier* run. The FK is therefore enforced, and the
  orphan check in validation_checks.sql catches the case where the source
  itself is inconsistent, before the load is attempted.
------------------------------------------------------------------------------*/
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_dw_SalesOrderDetail_Header')
BEGIN
    ALTER TABLE dw.SalesOrderDetail
        ADD CONSTRAINT FK_dw_SalesOrderDetail_Header
        FOREIGN KEY (SalesOrderID) REFERENCES dw.SalesOrderHeader (SalesOrderID);
END
GO


/*==============================================================================
  REPORTING VIEWS
  The reason the warehouse exists: the questions the manual export used to
  answer, now answerable with a query.
==============================================================================*/

CREATE OR ALTER VIEW dw.vw_DailySales
AS
/*  Daily order and revenue totals -- the headline reporting surface. */
SELECT
    CAST(h.OrderDate AS DATE)          AS OrderDate,
    COUNT_BIG(DISTINCT h.SalesOrderID) AS OrderCount,
    SUM(h.SubTotal)                    AS SubTotal,
    SUM(h.TaxAmt)                      AS TaxAmt,
    SUM(h.Freight)                     AS Freight,
    SUM(h.TotalDue)                    AS TotalDue,
    CAST(AVG(h.TotalDue) AS DECIMAL(19,4)) AS AvgOrderValue
FROM dw.SalesOrderHeader AS h
GROUP BY CAST(h.OrderDate AS DATE);
GO

CREATE OR ALTER VIEW dw.vw_ProductSales
AS
/*  Units and revenue by product, for the "what sold" question. */
SELECT
    d.ProductID,
    COUNT_BIG(*)            AS LineCount,
    SUM(CAST(d.OrderQty AS BIGINT)) AS UnitsSold,
    SUM(d.LineTotal)        AS Revenue,
    MIN(h.OrderDate)        AS FirstOrderDate,
    MAX(h.OrderDate)        AS LastOrderDate
FROM dw.SalesOrderDetail AS d
INNER JOIN dw.SalesOrderHeader AS h
        ON h.SalesOrderID = d.SalesOrderID
GROUP BY d.ProductID;
GO

CREATE OR ALTER VIEW etl.vw_RunSummary
AS
/*  Operator's view: what happened on each run, newest first. */
SELECT
    r.run_id,
    r.pipeline_name,
    r.run_trigger,
    r.load_type,
    r.start_time,
    r.end_time,
    r.duration_seconds,
    r.rows_extracted,
    r.rows_loaded,
    r.rows_rejected,
    r.checks_run,
    r.checks_failed,
    r.status,
    r.required_manual_intervention,
    r.error_message,
    (SELECT COUNT(*) FROM etl.etl_alert AS a WHERE a.run_id = r.run_id) AS alerts_raised
FROM etl.etl_run_log AS r;
GO

PRINT 'Target schema deployed: stg / dw / etl.';
GO
