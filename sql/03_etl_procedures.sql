/*==============================================================================
  03_etl_procedures.sql
  ------------------------------------------------------------------------------
  Pipeline control and load logic.

  Everything the orchestrator does is a call into one of these procedures. That
  is deliberate: Azure Data Factory and the local runner in etl/ execute the
  *same* logic, because the logic lives here in the database rather than in the
  orchestrator. Swapping ADF for SSIS, Airflow, or a cron job would not change
  a single line of what follows.

  Contents
    etl.usp_StartRun              open a run, resolve the incremental window
    etl.usp_LogRunEntity          record per-table extract/load counts
    etl.usp_EndRun                close a run, roll up counts, set final status
    etl.usp_LogValidation         write one validation result
    etl.usp_SetWatermark          advance the high-water mark after success
    etl.usp_TruncateStaging       clear the landing zone for a run
    etl.usp_LoadSalesOrderHeader  MERGE stg -> dw
    etl.usp_LoadSalesOrderDetail  MERGE stg -> dw
    etl.usp_RaiseAlert            record an alert (delivery is the caller's job)

  Target platform: SQL Server 2022 / Azure SQL Database
==============================================================================*/

SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO


/*==============================================================================
  etl.usp_StartRun

  Opens a run and resolves the incremental window in one round trip, so the
  orchestrator needs a single Lookup activity rather than three.

  Returns a single row: run_id, watermark_from, watermark_to.

  The window is [watermark_from, watermark_to). Half-open on purpose -- a
  closed upper bound would re-read the boundary row on every subsequent run.
  watermark_to is pinned to the run's start time rather than "now" so that rows
  modified *during* the run belong to the next window and cannot be silently
  skipped.
==============================================================================*/
CREATE OR ALTER PROCEDURE etl.usp_StartRun
    @pipeline_name SYSNAME,
    @run_trigger   VARCHAR(20)  = 'Manual',
    @load_type     VARCHAR(20)  = 'Incremental',
    @entity_list   NVARCHAR(400) = N'SalesOrderHeader,SalesOrderDetail'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @run_id         BIGINT,
            @watermark_from DATETIME2(3),
            @watermark_to   DATETIME2(3),
            @start_time     DATETIME2(3) = SYSUTCDATETIME(),
            @initial        DATETIME2(3);

    SELECT @initial = TRY_CONVERT(DATETIME2(3), config_value)
    FROM   etl.etl_config
    WHERE  config_key = 'initial.watermark';

    SET @initial = ISNULL(@initial, '1900-01-01T00:00:00');

    /*  A Full load ignores the stored watermark entirely and re-reads history.
        An Incremental load starts from the *lowest* watermark across the
        entities in scope, so header and detail stay in step even if one of
        them was advanced independently by a backfill.                        */
    IF @load_type = 'Full'
        SET @watermark_from = @initial;
    ELSE
        SELECT @watermark_from = ISNULL(MIN(w.watermark_value), @initial)
        FROM   etl.etl_watermark AS w
        WHERE  EXISTS (SELECT 1
                       FROM   STRING_SPLIT(@entity_list, ',') AS s
                       WHERE  LTRIM(RTRIM(s.value)) = w.entity_name);

    SET @watermark_to = @start_time;

    INSERT INTO etl.etl_run_log
        (pipeline_name, run_trigger, load_type, watermark_from, watermark_to, start_time, status)
    VALUES
        (@pipeline_name, @run_trigger, @load_type, @watermark_from, @watermark_to, @start_time, 'Running');

    SET @run_id = SCOPE_IDENTITY();

    SELECT
        run_id         = @run_id,
        watermark_from = @watermark_from,
        watermark_to   = @watermark_to,
        load_type      = @load_type;
END
GO


/*==============================================================================
  etl.usp_LogRunEntity

  Upsert of the per-table counters. Called once per entity per run, after that
  entity's extract and load have finished.
==============================================================================*/
CREATE OR ALTER PROCEDURE etl.usp_LogRunEntity
    @run_id           BIGINT,
    @entity_name      SYSNAME,
    @source_row_count BIGINT        = NULL,
    @source_max_watermark DATETIME2(3) = NULL,
    @rows_extracted   BIGINT        = NULL,
    @rows_loaded      BIGINT        = NULL,
    @rows_inserted    BIGINT        = NULL,
    @rows_updated     BIGINT        = NULL,
    @rows_rejected    BIGINT        = NULL,
    @extract_seconds  DECIMAL(12,3) = NULL,
    @load_seconds     DECIMAL(12,3) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    /*  COALESCE against the existing value, not against 0: a later call that
        only supplies load counts must not wipe the extract counts written
        earlier in the run.                                                   */
    MERGE etl.etl_run_entity AS tgt
    USING (SELECT @run_id AS run_id, @entity_name AS entity_name) AS src
        ON tgt.run_id = src.run_id AND tgt.entity_name = src.entity_name
    WHEN MATCHED THEN UPDATE SET
        source_row_count = COALESCE(@source_row_count, tgt.source_row_count),
        source_max_watermark = COALESCE(@source_max_watermark, tgt.source_max_watermark),
        rows_extracted   = COALESCE(@rows_extracted,   tgt.rows_extracted),
        rows_loaded      = COALESCE(@rows_loaded,      tgt.rows_loaded),
        rows_inserted    = COALESCE(@rows_inserted,    tgt.rows_inserted),
        rows_updated     = COALESCE(@rows_updated,     tgt.rows_updated),
        rows_rejected    = COALESCE(@rows_rejected,    tgt.rows_rejected),
        extract_seconds  = COALESCE(@extract_seconds,  tgt.extract_seconds),
        load_seconds     = COALESCE(@load_seconds,     tgt.load_seconds),
        logged_at        = SYSUTCDATETIME()
    WHEN NOT MATCHED BY TARGET THEN INSERT
        (run_id, entity_name, source_row_count, source_max_watermark, rows_extracted, rows_loaded,
         rows_inserted, rows_updated, rows_rejected, extract_seconds, load_seconds)
    VALUES
        (@run_id, @entity_name, @source_row_count, @source_max_watermark,
         ISNULL(@rows_extracted, 0), ISNULL(@rows_loaded, 0),
         ISNULL(@rows_inserted, 0), ISNULL(@rows_updated, 0), ISNULL(@rows_rejected, 0),
         @extract_seconds, @load_seconds);
END
GO


/*==============================================================================
  etl.usp_LogValidation

  Writes one validation result. Deliberately dumb -- the *decision* about
  whether a check passed belongs to the check itself, in validation_checks.sql.
==============================================================================*/
CREATE OR ALTER PROCEDURE etl.usp_LogValidation
    @run_id         BIGINT,
    @check_name     VARCHAR(120),
    @check_type     VARCHAR(40),
    @status         VARCHAR(10),
    @entity_name    SYSNAME        = NULL,
    @column_name    SYSNAME        = NULL,
    @expected_value NVARCHAR(200)  = NULL,
    @actual_value   NVARCHAR(200)  = NULL,
    @variance       DECIMAL(18,6)  = NULL,
    @severity       VARCHAR(10)    = 'Critical',
    @message        NVARCHAR(1000) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO etl.etl_validation_log
        (run_id, check_name, check_type, entity_name, column_name,
         expected_value, actual_value, variance, status, severity, message)
    VALUES
        (@run_id, @check_name, @check_type, @entity_name, @column_name,
         @expected_value, @actual_value, @variance, @status, @severity, @message);
END
GO


/*==============================================================================
  etl.usp_RaiseAlert

  Records an alert against the run. Delivery -- Logic App, Teams webhook,
  email -- is the orchestrator's responsibility; this is the durable record
  that survives the notification channel being down, and it is what the
  "runs that needed a human" metric is counted from.
==============================================================================*/
CREATE OR ALTER PROCEDURE etl.usp_RaiseAlert
    @run_id     BIGINT,
    @alert_type VARCHAR(40),
    @severity   VARCHAR(10),
    @subject    NVARCHAR(300),
    @body       NVARCHAR(MAX) = NULL,
    @channel    VARCHAR(40)   = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO etl.etl_alert (run_id, alert_type, severity, subject, body, channel)
    VALUES (@run_id, @alert_type, @severity, @subject, @body, @channel);

    SELECT alert_id = SCOPE_IDENTITY();
END
GO


/*==============================================================================
  etl.usp_MarkAlertDelivered

  Closes the loop on an alert: the notification went out.

  Targets the most recent alert for the run, because both orchestrators raise
  exactly one alert per failure. Taking an alert_id instead would be more
  precise, but ADF's Stored Procedure activity cannot capture an output value
  from the earlier usp_RaiseAlert call to pass along -- that would require a
  Lookup, and a second round trip, to express something the run id already
  determines unambiguously.
==============================================================================*/
CREATE OR ALTER PROCEDURE etl.usp_MarkAlertDelivered
    @run_id          BIGINT,
    @alert_id        BIGINT         = NULL,
    @delivered       BIT            = 1,
    @delivery_detail NVARCHAR(1000) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @alert_id IS NULL
        SELECT TOP (1) @alert_id = alert_id
        FROM   etl.etl_alert
        WHERE  run_id = @run_id
        ORDER  BY alert_id DESC;

    IF @alert_id IS NULL
    BEGIN
        RAISERROR('No alert found for run %I64d to mark as delivered.', 16, 1, @run_id);
        RETURN;
    END

    UPDATE etl.etl_alert
    SET    delivered       = @delivered,
           delivery_detail = @delivery_detail
    WHERE  alert_id = @alert_id;

    SELECT alert_id = @alert_id, delivered = @delivered;
END
GO


/*==============================================================================
  etl.usp_EndRun

  Closes the run: rolls per-entity counters up into the parent row, counts the
  validation results, and derives the final status.

  Status is derived, never passed in as an opinion:

    Failed                 -- the orchestrator reported an error, or a
                              Critical check failed. Data is not trustworthy.
    CompletedWithWarnings  -- only Warning-severity checks failed. Data loaded;
                              somebody should look, but nothing is broken.
    Succeeded              -- everything passed.

  required_manual_intervention is set for exactly the first case. That is the
  flag docs/metrics-methodology.md counts, and the reason it is computed here
  rather than typed in by hand.
==============================================================================*/
CREATE OR ALTER PROCEDURE etl.usp_EndRun
    @run_id        BIGINT,
    @status        VARCHAR(24)    = NULL,   -- pass 'Failed' to force failure; otherwise derived
    @error_message NVARCHAR(4000) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @rows_extracted   BIGINT,
            @rows_loaded      BIGINT,
            @rows_rejected    BIGINT,
            @checks_run       INT,
            @checks_failed    INT,
            @critical_failed  INT,
            @final_status     VARCHAR(24),
            @manual           BIT;

    SELECT @rows_extracted = ISNULL(SUM(rows_extracted), 0),
           @rows_loaded    = ISNULL(SUM(rows_loaded), 0),
           @rows_rejected  = ISNULL(SUM(rows_rejected), 0)
    FROM   etl.etl_run_entity
    WHERE  run_id = @run_id;

    SELECT @checks_run      = COUNT(*),
           @checks_failed   = SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END),
           @critical_failed = SUM(CASE WHEN status = 'FAIL' AND severity = 'Critical' THEN 1 ELSE 0 END)
    FROM   etl.etl_validation_log
    WHERE  run_id = @run_id;

    SET @checks_run      = ISNULL(@checks_run, 0);
    SET @checks_failed   = ISNULL(@checks_failed, 0);
    SET @critical_failed = ISNULL(@critical_failed, 0);

    IF @status = 'Failed' OR @critical_failed > 0
        SET @final_status = 'Failed';
    ELSE IF @checks_failed > 0
        SET @final_status = 'CompletedWithWarnings';
    ELSE
        SET @final_status = 'Succeeded';

    /*  A run needs a human iff it failed. Warnings are logged and reviewed on
        the operator's own schedule; they do not block the next load.         */
    SET @manual = CASE WHEN @final_status = 'Failed' THEN 1 ELSE 0 END;

    UPDATE etl.etl_run_log
    SET    end_time                     = SYSUTCDATETIME(),
           rows_extracted               = @rows_extracted,
           rows_loaded                  = @rows_loaded,
           rows_rejected                = @rows_rejected,
           checks_run                   = @checks_run,
           checks_failed                = @checks_failed,
           status                       = @final_status,
           required_manual_intervention = @manual,
           error_message                = @error_message
    WHERE  run_id = @run_id;

    SELECT run_id         = @run_id,
           status         = @final_status,
           checks_run     = @checks_run,
           checks_failed  = @checks_failed,
           rows_loaded    = @rows_loaded,
           rows_rejected  = @rows_rejected,
           required_manual_intervention = @manual;
END
GO


/*==============================================================================
  etl.usp_SetWatermark

  Advances the high-water mark. Called only after a run has been validated --
  a failed run leaves the watermark where it was, so the next run re-reads the
  same window instead of stepping over it.

  MAX() against the existing value guards against a Backfill run (which reads
  an old window) dragging the watermark backwards.
==============================================================================*/
CREATE OR ALTER PROCEDURE etl.usp_SetWatermark
    @entity_name     SYSNAME,
    @watermark_value DATETIME2(3),
    @run_id          BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    MERGE etl.etl_watermark AS tgt
    USING (SELECT @entity_name AS entity_name) AS src
        ON tgt.entity_name = src.entity_name
    WHEN MATCHED THEN UPDATE SET
        watermark_value = CASE WHEN @watermark_value > tgt.watermark_value
                               THEN @watermark_value ELSE tgt.watermark_value END,
        last_run_id     = @run_id,
        updated_at      = SYSUTCDATETIME()
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (entity_name, watermark_value, last_run_id)
        VALUES (@entity_name, @watermark_value, @run_id);

    SELECT entity_name, watermark_value
    FROM   etl.etl_watermark
    WHERE  entity_name = @entity_name;
END
GO


/*==============================================================================
  etl.usp_TruncateStaging

  Clears the landing zone. Called at the start of every run.

  TRUNCATE rather than DELETE: staging holds no history worth preserving --
  anything interesting has already been copied into etl.etl_rejected_row --
  and TRUNCATE avoids logging every row.
==============================================================================*/
CREATE OR ALTER PROCEDURE etl.usp_TruncateStaging
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    TRUNCATE TABLE stg.SalesOrderHeader;
    TRUNCATE TABLE stg.SalesOrderDetail;
END
GO


/*==============================================================================
  etl.usp_LoadSalesOrderHeader

  stg -> dw upsert for the header table.

  Two things worth noting:

  1. The source de-dup. AdventureWorks has SalesOrderID as a primary key, so
     duplicates should be impossible -- but MERGE raises error 8672 and aborts
     the entire load if the source ever produces two rows for one key. Ranking
     by ModifiedDate and taking the newest makes the load resilient to a source
     that misbehaves, and the duplicate is separately *reported* by the
     DUP_SalesOrderID check rather than being swept under the rug.

  2. Row counting via OUTPUT $action. Splitting inserts from updates lets
     etl_run_entity distinguish "1,000 new orders" from "1,000 orders restated",
     which is the difference between a normal day and something to look at.
==============================================================================*/
CREATE OR ALTER PROCEDURE etl.usp_LoadSalesOrderHeader
    @run_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @actions TABLE (action_taken NVARCHAR(10));
    DECLARE @inserted BIGINT, @updated BIGINT;

    BEGIN TRY
        BEGIN TRANSACTION;

        WITH src AS
        (
            SELECT *,
                   rn = ROW_NUMBER() OVER (PARTITION BY SalesOrderID
                                           ORDER BY ModifiedDate DESC, RevisionNumber DESC)
            FROM   stg.SalesOrderHeader
            WHERE  etl_run_id = @run_id
              AND  SalesOrderID IS NOT NULL
        )
        MERGE dw.SalesOrderHeader AS tgt
        USING (SELECT * FROM src WHERE rn = 1) AS s
            ON tgt.SalesOrderID = s.SalesOrderID
        WHEN MATCHED AND (s.ModifiedDate >= tgt.ModifiedDate) THEN UPDATE SET
            RevisionNumber      = s.RevisionNumber,
            OrderDate           = s.OrderDate,
            DueDate             = s.DueDate,
            ShipDate            = s.ShipDate,
            Status              = s.Status,
            OnlineOrderFlag     = s.OnlineOrderFlag,
            SalesOrderNumber    = s.SalesOrderNumber,
            PurchaseOrderNumber = s.PurchaseOrderNumber,
            AccountNumber       = s.AccountNumber,
            CustomerID          = s.CustomerID,
            SalesPersonID       = s.SalesPersonID,
            TerritoryID         = s.TerritoryID,
            BillToAddressID     = s.BillToAddressID,
            ShipToAddressID     = s.ShipToAddressID,
            ShipMethodID        = s.ShipMethodID,
            SubTotal            = s.SubTotal,
            TaxAmt              = s.TaxAmt,
            Freight             = s.Freight,
            TotalDue            = s.TotalDue,
            Comment             = s.Comment,
            ModifiedDate        = s.ModifiedDate,
            etl_run_id          = @run_id,
            etl_updated_at      = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN INSERT
            (SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate, Status,
             OnlineOrderFlag, SalesOrderNumber, PurchaseOrderNumber, AccountNumber,
             CustomerID, SalesPersonID, TerritoryID, BillToAddressID, ShipToAddressID,
             ShipMethodID, SubTotal, TaxAmt, Freight, TotalDue, Comment, ModifiedDate,
             etl_run_id)
        VALUES
            (s.SalesOrderID, s.RevisionNumber, s.OrderDate, s.DueDate, s.ShipDate, s.Status,
             s.OnlineOrderFlag, s.SalesOrderNumber, s.PurchaseOrderNumber, s.AccountNumber,
             s.CustomerID, s.SalesPersonID, s.TerritoryID, s.BillToAddressID, s.ShipToAddressID,
             s.ShipMethodID, s.SubTotal, s.TaxAmt, s.Freight, s.TotalDue, s.Comment, s.ModifiedDate,
             @run_id)
        OUTPUT $action INTO @actions;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH

    SELECT @inserted = SUM(CASE WHEN action_taken = 'INSERT' THEN 1 ELSE 0 END),
           @updated  = SUM(CASE WHEN action_taken = 'UPDATE' THEN 1 ELSE 0 END)
    FROM   @actions;

    SET @inserted = ISNULL(@inserted, 0);
    SET @updated  = ISNULL(@updated, 0);

    /*  Self-logging, for the same reason usp_RunStagingRules does it: the
        counts are a property of the load, not of whoever called it. This also
        keeps the ADF graph to a plain Stored Procedure activity instead of a
        Lookup whose output has to be threaded into a follow-up activity.     */
    DECLARE @loaded BIGINT = @inserted + @updated;
    EXEC etl.usp_LogRunEntity
         @run_id        = @run_id,
         @entity_name   = 'SalesOrderHeader',
         @rows_loaded   = @loaded,
         @rows_inserted = @inserted,
         @rows_updated  = @updated;

    SELECT entity_name   = 'SalesOrderHeader',
           rows_inserted = @inserted,
           rows_updated  = @updated,
           rows_loaded   = @loaded;
END
GO


/*==============================================================================
  etl.usp_LoadSalesOrderDetail

  Same shape as the header loader, with one extra guard: detail rows whose
  parent header is present in neither dw nor this batch are held back rather
  than allowed to violate the foreign key. They are quarantined by the
  ORPHAN_SalesOrderID check in validation_checks.sql, which runs first.
==============================================================================*/
CREATE OR ALTER PROCEDURE etl.usp_LoadSalesOrderDetail
    @run_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @actions TABLE (action_taken NVARCHAR(10));
    DECLARE @inserted BIGINT, @updated BIGINT;

    BEGIN TRY
        BEGIN TRANSACTION;

        WITH src AS
        (
            SELECT d.*,
                   rn = ROW_NUMBER() OVER (PARTITION BY d.SalesOrderDetailID
                                           ORDER BY d.ModifiedDate DESC)
            FROM   stg.SalesOrderDetail AS d
            WHERE  d.etl_run_id = @run_id
              AND  d.SalesOrderDetailID IS NOT NULL
              AND  EXISTS (SELECT 1 FROM dw.SalesOrderHeader AS h
                           WHERE h.SalesOrderID = d.SalesOrderID)
        )
        MERGE dw.SalesOrderDetail AS tgt
        USING (SELECT * FROM src WHERE rn = 1) AS s
            ON tgt.SalesOrderDetailID = s.SalesOrderDetailID
        WHEN MATCHED AND (s.ModifiedDate >= tgt.ModifiedDate) THEN UPDATE SET
            SalesOrderID          = s.SalesOrderID,
            CarrierTrackingNumber = s.CarrierTrackingNumber,
            OrderQty              = s.OrderQty,
            ProductID             = s.ProductID,
            SpecialOfferID        = s.SpecialOfferID,
            UnitPrice             = s.UnitPrice,
            UnitPriceDiscount     = s.UnitPriceDiscount,
            LineTotal             = s.LineTotal,
            ModifiedDate          = s.ModifiedDate,
            etl_run_id            = @run_id,
            etl_updated_at        = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN INSERT
            (SalesOrderDetailID, SalesOrderID, CarrierTrackingNumber, OrderQty,
             ProductID, SpecialOfferID, UnitPrice, UnitPriceDiscount, LineTotal,
             ModifiedDate, etl_run_id)
        VALUES
            (s.SalesOrderDetailID, s.SalesOrderID, s.CarrierTrackingNumber, s.OrderQty,
             s.ProductID, s.SpecialOfferID, s.UnitPrice, s.UnitPriceDiscount, s.LineTotal,
             s.ModifiedDate, @run_id)
        OUTPUT $action INTO @actions;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH

    SELECT @inserted = SUM(CASE WHEN action_taken = 'INSERT' THEN 1 ELSE 0 END),
           @updated  = SUM(CASE WHEN action_taken = 'UPDATE' THEN 1 ELSE 0 END)
    FROM   @actions;

    SET @inserted = ISNULL(@inserted, 0);
    SET @updated  = ISNULL(@updated, 0);

    DECLARE @loaded BIGINT = @inserted + @updated;
    EXEC etl.usp_LogRunEntity
         @run_id        = @run_id,
         @entity_name   = 'SalesOrderDetail',
         @rows_loaded   = @loaded,
         @rows_inserted = @inserted,
         @rows_updated  = @updated;

    SELECT entity_name   = 'SalesOrderDetail',
           rows_inserted = @inserted,
           rows_updated  = @updated,
           rows_loaded   = @loaded;
END
GO

PRINT 'ETL control and load procedures deployed.';
GO
