/*==============================================================================
  04_validation_checks.sql
  ------------------------------------------------------------------------------
  Data quality layer.

  Two kinds of check live here, and the distinction matters:

    Pre-load  (staging rules) -- run against stg, before anything reaches dw.
                                 A row that fails a Critical rule is copied to
                                 etl.etl_rejected_row and deleted from staging,
                                 so it is quarantined rather than either
                                 silently loaded or crashing the run.

    Post-load (assertions)    -- run against dw, after the MERGE. These answer
                                 "is the warehouse internally consistent?" --
                                 row-count reconciliation, orphaned detail
                                 lines, header totals that disagree with the
                                 sum of their lines, staleness.

  The pre-load rules are metadata-driven: they live as rows in
  etl.etl_validation_rule and are executed by a generic engine. Adding a check
  is an INSERT, not a code change and not a redeployment.

  A note on the dynamic SQL below: predicate_sql is executed as written. Rules
  are deployment-time content authored by whoever owns the schema -- the same
  trust level as a view definition -- and are never accepted from user input.
  Identifiers coming from the rule table are passed through QUOTENAME, and
  @run_id is always a bound parameter, never string-concatenated.

  Contents
    etl.etl_validation_rule       rule definitions (seeded below)
    etl.usp_RunStagingRules       generic pre-load rule engine
    etl.usp_ReconcileRowCounts    source vs staged vs loaded
    etl.usp_ValidatePostLoad      warehouse consistency assertions
    etl.usp_ValidateSalesData     single entry point for the orchestrator
    etl.usp_GetRunValidationResult  pass/fail summary for a conditional branch

  Target platform: SQL Server 2022 / Azure SQL Database
==============================================================================*/

SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO


/*==============================================================================
  RULE DEFINITIONS
==============================================================================*/
IF OBJECT_ID('etl.etl_validation_rule', 'U') IS NULL
BEGIN
    CREATE TABLE etl.etl_validation_rule
    (
        rule_id           INT            IDENTITY(1,1) NOT NULL,
        entity_name       SYSNAME        NOT NULL,
        staging_schema    SYSNAME        NOT NULL CONSTRAINT DF_vr_sch DEFAULT ('stg'),
        staging_table     SYSNAME        NOT NULL,
        check_name        VARCHAR(120)   NOT NULL,
        check_type        VARCHAR(40)    NOT NULL,   -- NullCheck | TypeCheck | DomainCheck | DuplicateCheck | ReferentialCheck
        column_name       SYSNAME        NULL,
        predicate_sql     NVARCHAR(1000) NOT NULL,   -- TRUE identifies a BAD row
        business_key_expr NVARCHAR(400)  NOT NULL,   -- how to name the offending row in the reject log
        severity          VARCHAR(10)    NOT NULL CONSTRAINT DF_vr_sev DEFAULT ('Critical'),
        quarantine        BIT            NOT NULL CONSTRAINT DF_vr_q   DEFAULT (1),
        is_enabled        BIT            NOT NULL CONSTRAINT DF_vr_en  DEFAULT (1),
        description       NVARCHAR(400)  NULL,

        CONSTRAINT PK_etl_validation_rule PRIMARY KEY CLUSTERED (rule_id),
        CONSTRAINT UQ_etl_validation_rule UNIQUE (entity_name, check_name),
        CONSTRAINT CK_etl_validation_rule_sev CHECK (severity IN ('Critical', 'Warning', 'Info'))
    );
END
GO

/*------------------------------------------------------------------------------
  Seed rules.

  Null checks cover every column that is NOT NULL in dw -- if a rule here is
  missing, the constraint in dw fires instead and takes the whole load down,
  which is exactly the failure mode the landing zone exists to prevent.

  Type and domain checks encode what the *business* considers valid, which is
  narrower than what the column type permits: a SMALLINT OrderQty can hold 0
  and -5, but an order line for zero units is a data error, not an order.
------------------------------------------------------------------------------*/
MERGE etl.etl_validation_rule AS tgt
USING (VALUES
    /* ---------------------------- SalesOrderHeader --------------------------- */
    ('SalesOrderHeader', 'SalesOrderHeader', 'NULL_SalesOrderID',      'NullCheck',   'SalesOrderID',
     N's.SalesOrderID IS NULL', N'''SalesOrderID=<null>''', 'Critical', 1,
     N'Primary key of the order. A row without it cannot be identified or upserted.'),
    ('SalesOrderHeader', 'SalesOrderHeader', 'NULL_OrderDate',         'NullCheck',   'OrderDate',
     N's.OrderDate IS NULL', N'CONCAT(''SalesOrderID='', s.SalesOrderID)', 'Critical', 1,
     N'Every downstream report is grouped by order date.'),
    ('SalesOrderHeader', 'SalesOrderHeader', 'NULL_DueDate',           'NullCheck',   'DueDate',
     N's.DueDate IS NULL', N'CONCAT(''SalesOrderID='', s.SalesOrderID)', 'Critical', 1,
     N'Required by dw.SalesOrderHeader and by the due-date CHECK constraint.'),
    ('SalesOrderHeader', 'SalesOrderHeader', 'NULL_CustomerID',        'NullCheck',   'CustomerID',
     N's.CustomerID IS NULL', N'CONCAT(''SalesOrderID='', s.SalesOrderID)', 'Critical', 1,
     N'An order with no customer cannot be attributed.'),
    ('SalesOrderHeader', 'SalesOrderHeader', 'NULL_Status',            'NullCheck',   'Status',
     N's.Status IS NULL', N'CONCAT(''SalesOrderID='', s.SalesOrderID)', 'Critical', 1,
     N'Order status drives the fulfilment reports.'),
    ('SalesOrderHeader', 'SalesOrderHeader', 'NULL_SalesOrderNumber',  'NullCheck',   'SalesOrderNumber',
     N's.SalesOrderNumber IS NULL', N'CONCAT(''SalesOrderID='', s.SalesOrderID)', 'Critical', 1,
     N'Human-facing order reference.'),
    ('SalesOrderHeader', 'SalesOrderHeader', 'NULL_Amounts',           'NullCheck',   'TotalDue',
     N's.SubTotal IS NULL OR s.TaxAmt IS NULL OR s.Freight IS NULL OR s.TotalDue IS NULL',
     N'CONCAT(''SalesOrderID='', s.SalesOrderID)', 'Critical', 1,
     N'Any NULL money column would silently zero out a revenue total.'),
    ('SalesOrderHeader', 'SalesOrderHeader', 'NULL_ModifiedDate',      'NullCheck',   'ModifiedDate',
     N's.ModifiedDate IS NULL', N'CONCAT(''SalesOrderID='', s.SalesOrderID)', 'Critical', 1,
     N'ModifiedDate is the incremental watermark column -- a NULL breaks change tracking.'),
    ('SalesOrderHeader', 'SalesOrderHeader', 'NULL_AddressIDs',        'NullCheck',   'BillToAddressID',
     N's.BillToAddressID IS NULL OR s.ShipToAddressID IS NULL OR s.ShipMethodID IS NULL',
     N'CONCAT(''SalesOrderID='', s.SalesOrderID)', 'Critical', 1,
     N'Required by dw.SalesOrderHeader.'),
    ('SalesOrderHeader', 'SalesOrderHeader', 'NULL_RevisionNumber',    'NullCheck',   'RevisionNumber',
     N's.RevisionNumber IS NULL OR s.OnlineOrderFlag IS NULL',
     N'CONCAT(''SalesOrderID='', s.SalesOrderID)', 'Critical', 1,
     N'Required by dw.SalesOrderHeader.'),
    ('SalesOrderHeader', 'SalesOrderHeader', 'NEG_Amounts',            'DomainCheck', 'TotalDue',
     N's.SubTotal < 0 OR s.TaxAmt < 0 OR s.Freight < 0 OR s.TotalDue < 0',
     N'CONCAT(''SalesOrderID='', s.SalesOrderID)', 'Critical', 1,
     N'Negative money on an order header indicates a corrupted extract, not a refund -- refunds are separate orders in AdventureWorks.'),
    ('SalesOrderHeader', 'SalesOrderHeader', 'DATE_DueBeforeOrder',    'DomainCheck', 'DueDate',
     N's.DueDate < s.OrderDate', N'CONCAT(''SalesOrderID='', s.SalesOrderID)', 'Critical', 1,
     N'An order due before it was placed. Violates CK_dw_soh_due_date.'),
    ('SalesOrderHeader', 'SalesOrderHeader', 'DATE_ShipBeforeOrder',   'DomainCheck', 'ShipDate',
     N's.ShipDate IS NOT NULL AND s.ShipDate < s.OrderDate',
     N'CONCAT(''SalesOrderID='', s.SalesOrderID)', 'Critical', 1,
     N'Shipped before ordered. Violates CK_dw_soh_ship_date.'),
    ('SalesOrderHeader', 'SalesOrderHeader', 'DATE_OrderInFuture',     'DomainCheck', 'OrderDate',
     N's.OrderDate > DATEADD(DAY, 1, SYSUTCDATETIME())',
     N'CONCAT(''SalesOrderID='', s.SalesOrderID)', 'Warning', 0,
     N'Future-dated order. One day of slack absorbs source/target clock skew. Warn only -- it is loadable, just odd.'),
    ('SalesOrderHeader', 'SalesOrderHeader', 'DUP_SalesOrderID',       'DuplicateCheck', 'SalesOrderID',
     N'EXISTS (SELECT 1 FROM stg.SalesOrderHeader AS d WHERE d.etl_run_id = s.etl_run_id AND d.SalesOrderID = s.SalesOrderID GROUP BY d.SalesOrderID HAVING COUNT(*) > 1)',
     N'CONCAT(''SalesOrderID='', s.SalesOrderID)', 'Warning', 0,
     N'Two rows for one order in a single batch. The loader de-duplicates on newest ModifiedDate, so this warns rather than blocks -- but it means the source is not behaving.'),

    /* ---------------------------- SalesOrderDetail --------------------------- */
    ('SalesOrderDetail', 'SalesOrderDetail', 'NULL_SalesOrderDetailID', 'NullCheck',  'SalesOrderDetailID',
     N's.SalesOrderDetailID IS NULL', N'''SalesOrderDetailID=<null>''', 'Critical', 1,
     N'Primary key of the order line.'),
    ('SalesOrderDetail', 'SalesOrderDetail', 'NULL_SalesOrderID',       'NullCheck',  'SalesOrderID',
     N's.SalesOrderID IS NULL', N'CONCAT(''SalesOrderDetailID='', s.SalesOrderDetailID)', 'Critical', 1,
     N'Foreign key to the header. Without it the line cannot be attached to an order.'),
    ('SalesOrderDetail', 'SalesOrderDetail', 'NULL_ProductID',          'NullCheck',  'ProductID',
     N's.ProductID IS NULL OR s.SpecialOfferID IS NULL',
     N'CONCAT(''SalesOrderDetailID='', s.SalesOrderDetailID)', 'Critical', 1,
     N'Required by dw.SalesOrderDetail.'),
    ('SalesOrderDetail', 'SalesOrderDetail', 'NULL_Amounts',            'NullCheck',  'LineTotal',
     N's.OrderQty IS NULL OR s.UnitPrice IS NULL OR s.UnitPriceDiscount IS NULL OR s.LineTotal IS NULL',
     N'CONCAT(''SalesOrderDetailID='', s.SalesOrderDetailID)', 'Critical', 1,
     N'A NULL quantity or price would understate revenue without any error.'),
    ('SalesOrderDetail', 'SalesOrderDetail', 'NULL_ModifiedDate',       'NullCheck',  'ModifiedDate',
     N's.ModifiedDate IS NULL', N'CONCAT(''SalesOrderDetailID='', s.SalesOrderDetailID)', 'Critical', 1,
     N'Watermark column for incremental extraction.'),
    ('SalesOrderDetail', 'SalesOrderDetail', 'QTY_NonPositive',         'DomainCheck', 'OrderQty',
     N's.OrderQty <= 0', N'CONCAT(''SalesOrderDetailID='', s.SalesOrderDetailID)', 'Critical', 1,
     N'An order line for zero or negative units. Violates CK_dw_sod_qty.'),
    ('SalesOrderDetail', 'SalesOrderDetail', 'PRICE_Negative',          'DomainCheck', 'UnitPrice',
     N's.UnitPrice < 0', N'CONCAT(''SalesOrderDetailID='', s.SalesOrderDetailID)', 'Critical', 1,
     N'Negative unit price. Violates CK_dw_sod_price.'),
    ('SalesOrderDetail', 'SalesOrderDetail', 'DISCOUNT_OutOfRange',     'DomainCheck', 'UnitPriceDiscount',
     N's.UnitPriceDiscount < 0 OR s.UnitPriceDiscount > 1',
     N'CONCAT(''SalesOrderDetailID='', s.SalesOrderDetailID)', 'Critical', 1,
     N'Discount is a fraction in [0,1]. Anything else is a unit error. Violates CK_dw_sod_discount.'),
    ('SalesOrderDetail', 'SalesOrderDetail', 'LINETOTAL_Mismatch',      'TypeCheck',  'LineTotal',
     N'ABS(s.LineTotal - (s.UnitPrice * (1 - s.UnitPriceDiscount) * s.OrderQty)) > 0.01',
     N'CONCAT(''SalesOrderDetailID='', s.SalesOrderDetailID)', 'Warning', 0,
     N'LineTotal should equal UnitPrice * (1 - discount) * qty. AdventureWorks computes this column, so a mismatch means the extract mangled a numeric type -- warn, do not block.'),
    ('SalesOrderDetail', 'SalesOrderDetail', 'DUP_SalesOrderDetailID',  'DuplicateCheck', 'SalesOrderDetailID',
     N'EXISTS (SELECT 1 FROM stg.SalesOrderDetail AS d WHERE d.etl_run_id = s.etl_run_id AND d.SalesOrderDetailID = s.SalesOrderDetailID GROUP BY d.SalesOrderDetailID HAVING COUNT(*) > 1)',
     N'CONCAT(''SalesOrderDetailID='', s.SalesOrderDetailID)', 'Warning', 0,
     N'Duplicate line in one batch. De-duplicated by the loader; reported here.')
) AS src (entity_name, staging_table, check_name, check_type, column_name,
          predicate_sql, business_key_expr, severity, quarantine, description)
ON  tgt.entity_name = src.entity_name
AND tgt.check_name  = src.check_name
WHEN MATCHED THEN UPDATE SET
    check_type        = src.check_type,
    column_name       = src.column_name,
    predicate_sql     = src.predicate_sql,
    business_key_expr = src.business_key_expr,
    severity          = src.severity,
    quarantine        = src.quarantine,
    description       = src.description
WHEN NOT MATCHED BY TARGET THEN INSERT
    (entity_name, staging_table, check_name, check_type, column_name,
     predicate_sql, business_key_expr, severity, quarantine, description)
VALUES
    (src.entity_name, src.staging_table, src.check_name, src.check_type, src.column_name,
     src.predicate_sql, src.business_key_expr, src.severity, src.quarantine, src.description);
GO


/*==============================================================================
  etl.usp_RunStagingRules

  The pre-load rule engine. For every enabled rule matching @entity_name:

    1. Count the rows in this run's staging batch that satisfy the predicate.
    2. If any, and the rule quarantines, copy them to etl.etl_rejected_row with
       the full row serialised as JSON, then delete them from staging.
    3. Log a PASS or FAIL row to etl.etl_validation_log.

  Returns the total number of rows quarantined, per entity.
==============================================================================*/
CREATE OR ALTER PROCEDURE etl.usp_RunStagingRules
    @run_id      BIGINT,
    @entity_name SYSNAME = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @rule_id     INT,
            @entity      SYSNAME,
            @schema      SYSNAME,
            @table       SYSNAME,
            @check_name  VARCHAR(120),
            @check_type  VARCHAR(40),
            @column_name SYSNAME,
            @predicate   NVARCHAR(1000),
            @bk_expr     NVARCHAR(400),
            @severity    VARCHAR(10),
            @quarantine  BIT,
            @description NVARCHAR(400),
            @sql         NVARCHAR(MAX),
            @bad_rows    BIGINT,
            @status      VARCHAR(10),
            @message     NVARCHAR(1000);

    DECLARE @rejected TABLE (entity_name SYSNAME, rows_rejected BIGINT);

    DECLARE rule_cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT rule_id, entity_name, staging_schema, staging_table, check_name, check_type,
               column_name, predicate_sql, business_key_expr, severity, quarantine, description
        FROM   etl.etl_validation_rule
        WHERE  is_enabled = 1
          AND  (@entity_name IS NULL OR entity_name = @entity_name)
        ORDER  BY entity_name, rule_id;

    OPEN rule_cur;
    FETCH NEXT FROM rule_cur INTO @rule_id, @entity, @schema, @table, @check_name, @check_type,
                                  @column_name, @predicate, @bk_expr, @severity, @quarantine, @description;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @bad_rows = 0;

        /* -- 1. how many rows in this batch violate the rule? ------------------ */
        SET @sql = N'SELECT @bad = COUNT_BIG(*) FROM '
                 + QUOTENAME(@schema) + N'.' + QUOTENAME(@table) + N' AS s '
                 + N'WHERE s.etl_run_id = @rid AND (' + @predicate + N');';

        EXEC sp_executesql @sql,
             N'@rid BIGINT, @bad BIGINT OUTPUT',
             @rid = @run_id, @bad = @bad_rows OUTPUT;

        /* -- 2. quarantine and remove, if the rule says so --------------------- */
        IF @bad_rows > 0 AND @quarantine = 1
        BEGIN
            SET @sql = N'
                INSERT INTO etl.etl_rejected_row
                    (run_id, entity_name, business_key, rejection_reason, rejection_detail, row_payload)
                SELECT @rid, @ent, ' + @bk_expr + N', @ck, @dsc,
                       (SELECT s.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
                FROM   ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@table) + N' AS s
                WHERE  s.etl_run_id = @rid AND (' + @predicate + N');

                DELETE s
                FROM   ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@table) + N' AS s
                WHERE  s.etl_run_id = @rid AND (' + @predicate + N');';

            EXEC sp_executesql @sql,
                 N'@rid BIGINT, @ent SYSNAME, @ck VARCHAR(120), @dsc NVARCHAR(400)',
                 @rid = @run_id, @ent = @entity, @ck = @check_name, @dsc = @description;

            INSERT INTO @rejected (entity_name, rows_rejected) VALUES (@entity, @bad_rows);
        END

        /* -- 3. record the outcome -------------------------------------------- */
        SET @status = CASE WHEN @bad_rows = 0 THEN 'PASS' ELSE 'FAIL' END;
        SET @message = CASE
            WHEN @bad_rows = 0 THEN NULL
            WHEN @quarantine = 1 THEN CONCAT(@bad_rows, N' row(s) failed and were quarantined to etl.etl_rejected_row.')
            ELSE CONCAT(@bad_rows, N' row(s) failed. Not quarantined -- rule is reporting only.')
        END;

        EXEC etl.usp_LogValidation
             @run_id         = @run_id,
             @check_name     = @check_name,
             @check_type     = @check_type,
             @status         = @status,
             @entity_name    = @entity,
             @column_name    = @column_name,
             @expected_value = N'0',
             @actual_value   = @bad_rows,
             @variance       = @bad_rows,
             @severity       = @severity,
             @message        = @message;

        FETCH NEXT FROM rule_cur INTO @rule_id, @entity, @schema, @table, @check_name, @check_type,
                                      @column_name, @predicate, @bk_expr, @severity, @quarantine, @description;
    END

    CLOSE rule_cur;
    DEALLOCATE rule_cur;

    /*--------------------------------------------------------------------------
      Post the rejected counts to etl.etl_run_entity ourselves.

      The alternative -- returning the counts and making the orchestrator loop
      over them to call usp_LogRunEntity -- would put a ForEach in the pipeline
      graph purely for bookkeeping, and would mean the counts were only correct
      when the caller remembered to do it. A procedure that quarantines rows
      owns the job of recording how many it quarantined.
    --------------------------------------------------------------------------*/
    DECLARE @ent_name SYSNAME, @rej_count BIGINT;

    DECLARE rej_cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT entity_name, SUM(rows_rejected) FROM @rejected GROUP BY entity_name;

    OPEN rej_cur;
    FETCH NEXT FROM rej_cur INTO @ent_name, @rej_count;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        EXEC etl.usp_LogRunEntity
             @run_id = @run_id, @entity_name = @ent_name, @rows_rejected = @rej_count;
        FETCH NEXT FROM rej_cur INTO @ent_name, @rej_count;
    END
    CLOSE rej_cur;
    DEALLOCATE rej_cur;

    SELECT entity_name, rows_rejected = SUM(rows_rejected)
    FROM   @rejected
    GROUP  BY entity_name;
END
GO


/*==============================================================================
  etl.usp_ReconcileRowCounts

  The headline check: did everything the source offered actually arrive?

  @source_row_count is a parameter rather than a cross-database query on
  purpose. Azure Data Factory obtains it with a Lookup activity against the
  *source* linked service and passes it in; on Azure SQL Database a cross-
  database join would not be possible at all. Keeping the count as an input
  means this procedure works identically whether the source is a local
  instance, a linked server, or a different Azure SQL logical server.

  Three counts are compared:

    source    -- rows the source reported in the incremental window
    extracted -- rows that actually landed in staging
    loaded    -- rows the MERGE wrote to dw

  source vs extracted catches a truncated or partially-failed copy.
  extracted vs (loaded + rejected) catches rows lost between staging and the
  warehouse. Rejected rows are accounted for, not ignored -- that is what
  makes the reconciliation exact rather than approximate.
==============================================================================*/
CREATE OR ALTER PROCEDURE etl.usp_ReconcileRowCounts
    @run_id           BIGINT,
    @entity_name      SYSNAME,
    @source_row_count BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @extracted BIGINT,
            @loaded    BIGINT,
            @rejected  BIGINT,
            @tolerance BIGINT,
            @variance  BIGINT,
            @status    VARCHAR(10),
            @message   NVARCHAR(1000);

    SELECT @tolerance = TRY_CONVERT(BIGINT, config_value)
    FROM   etl.etl_config WHERE config_key = 'rowcount.tolerance_rows';
    SET @tolerance = ISNULL(@tolerance, 0);

    SELECT @extracted = ISNULL(rows_extracted, 0),
           @loaded    = ISNULL(rows_loaded, 0),
           @rejected  = ISNULL(rows_rejected, 0)
    FROM   etl.etl_run_entity
    WHERE  run_id = @run_id AND entity_name = @entity_name;

    SET @extracted = ISNULL(@extracted, 0);
    SET @loaded    = ISNULL(@loaded, 0);
    SET @rejected  = ISNULL(@rejected, 0);

    /* -- source vs extracted ------------------------------------------------- */
    SET @variance = @extracted - @source_row_count;
    SET @status   = CASE WHEN ABS(@variance) <= @tolerance THEN 'PASS' ELSE 'FAIL' END;
    SET @message  = CASE WHEN @status = 'PASS'
                         THEN CONCAT(N'Extracted ', @extracted, N' of ', @source_row_count, N' source rows in window.')
                         ELSE CONCAT(N'Extract shortfall: source offered ', @source_row_count,
                                     N' row(s), staging received ', @extracted, N'. Difference ', @variance, N'.')
                    END;

    EXEC etl.usp_LogValidation
         @run_id = @run_id, @check_name = 'RECON_SourceToStaging',
         @check_type = 'RowCountReconciliation', @status = @status,
         @entity_name = @entity_name,
         @expected_value = @source_row_count, @actual_value = @extracted,
         @variance = @variance, @severity = 'Critical', @message = @message;

    /* -- extracted vs loaded + rejected -------------------------------------- */
    DECLARE @accounted BIGINT = @loaded + @rejected;
    SET @variance = @accounted - @extracted;
    SET @status   = CASE WHEN @variance = 0 THEN 'PASS' ELSE 'FAIL' END;
    SET @message  = CASE WHEN @status = 'PASS'
                         THEN CONCAT(N'All ', @extracted, N' staged row(s) accounted for: ',
                                     @loaded, N' loaded, ', @rejected, N' quarantined.')
                         ELSE CONCAT(N'Unaccounted rows between staging and warehouse. Staged ', @extracted,
                                     N', loaded ', @loaded, N', quarantined ', @rejected,
                                     N'. Difference ', @variance, N'.')
                    END;

    EXEC etl.usp_LogValidation
         @run_id = @run_id, @check_name = 'RECON_StagingToWarehouse',
         @check_type = 'RowCountReconciliation', @status = @status,
         @entity_name = @entity_name,
         @expected_value = @extracted, @actual_value = @accounted,
         @variance = @variance, @severity = 'Critical', @message = @message;

    SELECT entity_name   = @entity_name,
           source_rows   = @source_row_count,
           staged_rows   = @extracted,
           loaded_rows   = @loaded,
           rejected_rows = @rejected,
           status        = @status;
END
GO


/*==============================================================================
  etl.usp_ValidatePostLoad

  Warehouse consistency assertions, run after the MERGE. These are the checks
  that cannot be expressed as a per-row predicate on staging, because they
  involve joins across tables or aggregates across a whole order.
==============================================================================*/
CREATE OR ALTER PROCEDURE etl.usp_ValidatePostLoad
    @run_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @count     BIGINT,
            @status    VARCHAR(10),
            @message   NVARCHAR(1000),
            @tol_money DECIMAL(18,6),
            @max_lag   INT,
            @lag_hours DECIMAL(18,6);

    SELECT @tol_money = TRY_CONVERT(DECIMAL(18,6), config_value)
    FROM   etl.etl_config WHERE config_key = 'financial.tolerance_amount';
    SET @tol_money = ISNULL(@tol_money, 0.01);

    SELECT @max_lag = TRY_CONVERT(INT, config_value)
    FROM   etl.etl_config WHERE config_key = 'freshness.max_lag_hours';
    SET @max_lag = ISNULL(@max_lag, 48);

    /*--------------------------------------------------------------------------
      1. Orphaned detail lines.

      The foreign key makes this impossible to *create*, so a non-zero result
      means the constraint was disabled or the load path bypassed it. Cheap to
      check, and a silent failure if it ever happens.
    --------------------------------------------------------------------------*/
    SELECT @count = COUNT_BIG(*)
    FROM   dw.SalesOrderDetail AS d
    WHERE  NOT EXISTS (SELECT 1 FROM dw.SalesOrderHeader AS h
                       WHERE h.SalesOrderID = d.SalesOrderID);

    SET @status  = CASE WHEN @count = 0 THEN 'PASS' ELSE 'FAIL' END;
    SET @message = CASE WHEN @count = 0 THEN N'Every order line resolves to a header.'
                        ELSE CONCAT(@count, N' order line(s) reference a missing header.') END;

    EXEC etl.usp_LogValidation
         @run_id = @run_id, @check_name = 'ORPHAN_DetailWithoutHeader',
         @check_type = 'ReferentialCheck', @status = @status,
         @entity_name = 'SalesOrderDetail', @column_name = 'SalesOrderID',
         @expected_value = N'0', @actual_value = @count, @variance = @count,
         @severity = 'Critical', @message = @message;

    /*--------------------------------------------------------------------------
      2. Headers with no lines.

      Legitimate in an incremental world -- a header can arrive in one window
      and its lines in the next -- so this is a Warning. It becomes interesting
      only if the number keeps climbing across runs.
    --------------------------------------------------------------------------*/
    SELECT @count = COUNT_BIG(*)
    FROM   dw.SalesOrderHeader AS h
    WHERE  NOT EXISTS (SELECT 1 FROM dw.SalesOrderDetail AS d
                       WHERE d.SalesOrderID = h.SalesOrderID);

    SET @status  = CASE WHEN @count = 0 THEN 'PASS' ELSE 'FAIL' END;
    SET @message = CASE WHEN @count = 0 THEN N'Every order has at least one line.'
                        ELSE CONCAT(@count, N' order(s) have no lines. Expected transiently during incremental loads; investigate if it persists across runs.') END;

    EXEC etl.usp_LogValidation
         @run_id = @run_id, @check_name = 'ORPHAN_HeaderWithoutDetail',
         @check_type = 'ReferentialCheck', @status = @status,
         @entity_name = 'SalesOrderHeader', @column_name = 'SalesOrderID',
         @expected_value = N'0', @actual_value = @count, @variance = @count,
         @severity = 'Warning', @message = @message;

    /*--------------------------------------------------------------------------
      3. Financial reconciliation.

      SalesOrderHeader.SubTotal must equal the sum of its lines' LineTotal.
      This is the check that would catch a partially-loaded order -- the header
      says $5,000, the lines add to $3,200, and every revenue report is wrong
      by $1,800 with nothing else to indicate it.

      Restricted to orders whose lines are fully present, so the transient
      header-without-detail case above does not produce a false alarm.
    --------------------------------------------------------------------------*/
    SELECT @count = COUNT_BIG(*)
    FROM   dw.SalesOrderHeader AS h
    CROSS APPLY (SELECT line_sum = SUM(d.LineTotal)
                 FROM   dw.SalesOrderDetail AS d
                 WHERE  d.SalesOrderID = h.SalesOrderID) AS agg
    WHERE  agg.line_sum IS NOT NULL
      AND  ABS(h.SubTotal - agg.line_sum) > @tol_money;

    SET @status  = CASE WHEN @count = 0 THEN 'PASS' ELSE 'FAIL' END;
    SET @message = CASE WHEN @count = 0
                        THEN CONCAT(N'Header SubTotal agrees with the sum of its lines within ', @tol_money, N'.')
                        ELSE CONCAT(@count, N' order(s) where SubTotal disagrees with SUM(LineTotal) by more than ', @tol_money, N'.') END;

    EXEC etl.usp_LogValidation
         @run_id = @run_id, @check_name = 'RECON_HeaderSubTotalVsLines',
         @check_type = 'FinancialReconciliation', @status = @status,
         @entity_name = 'SalesOrderHeader', @column_name = 'SubTotal',
         @expected_value = N'0', @actual_value = @count, @variance = @count,
         @severity = 'Critical', @message = @message;

    /*--------------------------------------------------------------------------
      4. Duplicate business keys.

      The primary keys make this impossible too. Same reasoning as the orphan
      check: it is nearly free, and it is the assertion that catches somebody
      having dropped a constraint to "fix" a load.
    --------------------------------------------------------------------------*/
    SELECT @count = COUNT_BIG(*) FROM (
        SELECT SalesOrderID FROM dw.SalesOrderHeader
        GROUP BY SalesOrderID HAVING COUNT(*) > 1
    ) AS dup;

    SET @status  = CASE WHEN @count = 0 THEN 'PASS' ELSE 'FAIL' END;
    SET @message = CASE WHEN @count = 0 THEN N'SalesOrderID is unique in the warehouse.'
                        ELSE CONCAT(@count, N' duplicate SalesOrderID value(s) in dw.SalesOrderHeader.') END;

    EXEC etl.usp_LogValidation
         @run_id = @run_id, @check_name = 'DUP_WarehouseOrderID',
         @check_type = 'DuplicateCheck', @status = @status,
         @entity_name = 'SalesOrderHeader', @column_name = 'SalesOrderID',
         @expected_value = N'0', @actual_value = @count, @variance = @count,
         @severity = 'Critical', @message = @message;

    /*--------------------------------------------------------------------------
      5. Type integrity in the warehouse.

      The CHECK constraints on dw should make all of these unreachable. Running
      them anyway costs one scan and turns "the constraints are still doing
      their job" from an assumption into an observation.
    --------------------------------------------------------------------------*/
    SELECT @count = COUNT_BIG(*)
    FROM   dw.SalesOrderDetail
    WHERE  OrderQty <= 0
       OR  UnitPrice < 0
       OR  UnitPriceDiscount < 0
       OR  UnitPriceDiscount > 1;

    SET @status  = CASE WHEN @count = 0 THEN 'PASS' ELSE 'FAIL' END;
    SET @message = CASE WHEN @count = 0 THEN N'All warehouse order lines are within their valid domains.'
                        ELSE CONCAT(@count, N' warehouse order line(s) outside the valid domain.') END;

    EXEC etl.usp_LogValidation
         @run_id = @run_id, @check_name = 'TYPE_WarehouseDetailDomains',
         @check_type = 'TypeCheck', @status = @status,
         @entity_name = 'SalesOrderDetail',
         @expected_value = N'0', @actual_value = @count, @variance = @count,
         @severity = 'Critical', @message = @message;

    /*--------------------------------------------------------------------------
      6. Freshness -- measured as pipeline lag, not as data age.

      Every other check can pass on a warehouse that quietly stopped receiving
      new data three weeks ago. This is the one that notices.

      The question it asks is "has the warehouse fallen behind the source?",
      not "is the data recent?". Those are different, and only the first is a
      statement about the pipeline. Comparing the warehouse's newest
      ModifiedDate against the wall clock would mean this check fails forever
      on AdventureWorks -- whose newest order is dated 2014 -- while telling us
      nothing about whether the pipeline is working. Comparing it against the
      *source's* newest ModifiedDate answers the question that matters, and
      answers it identically on a static sample database and a live OLTP
      system.

      The source's high-water mark is read from etl_run_entity, where the
      extract stage recorded it. The target database cannot query the source
      directly -- the same constraint that makes row-count reconciliation take
      its expected value as an input.
    --------------------------------------------------------------------------*/
    DECLARE @entity   SYSNAME,
            @src_max  DATETIME2(3),
            @dw_max   DATETIME2(3);

    DECLARE fresh_cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT entity_name, source_max_watermark
        FROM   etl.etl_run_entity
        WHERE  run_id = @run_id AND source_max_watermark IS NOT NULL;

    OPEN fresh_cur;
    FETCH NEXT FROM fresh_cur INTO @entity, @src_max;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @dw_max = CASE @entity
                          WHEN 'SalesOrderHeader' THEN (SELECT MAX(ModifiedDate) FROM dw.SalesOrderHeader)
                          WHEN 'SalesOrderDetail' THEN (SELECT MAX(ModifiedDate) FROM dw.SalesOrderDetail)
                      END;

        IF @dw_max IS NULL
        BEGIN
            EXEC etl.usp_LogValidation
                 @run_id = @run_id, @check_name = 'FRESH_PipelineLag',
                 @check_type = 'FreshnessCheck', @status = 'WARN',
                 @entity_name = @entity, @column_name = 'ModifiedDate',
                 @severity = 'Warning',
                 @message = N'Warehouse table is empty -- no lag measurement possible.';
        END
        ELSE
        BEGIN
            SET @lag_hours = DATEDIFF(MINUTE, @dw_max, @src_max) / 60.0;

            /*  Negative lag means the warehouse holds something newer than the
                source's high-water mark -- possible mid-run if the source is
                live. Clamp to zero: being ahead is not being behind.          */
            IF @lag_hours < 0 SET @lag_hours = 0;

            SET @status  = CASE WHEN @lag_hours <= @max_lag THEN 'PASS' ELSE 'FAIL' END;
            SET @message = CONCAT(N'Warehouse is ',
                                  CAST(ROUND(@lag_hours, 1) AS DECIMAL(18,1)),
                                  N'h behind the source high-water mark (source ',
                                  CONVERT(NVARCHAR(30), @src_max, 126),
                                  N', warehouse ', CONVERT(NVARCHAR(30), @dw_max, 126),
                                  N'; threshold ', @max_lag, N'h).');

            EXEC etl.usp_LogValidation
                 @run_id = @run_id, @check_name = 'FRESH_PipelineLag',
                 @check_type = 'FreshnessCheck', @status = @status,
                 @entity_name = @entity, @column_name = 'ModifiedDate',
                 @expected_value = @max_lag, @actual_value = @lag_hours,
                 @variance = @lag_hours, @severity = 'Warning', @message = @message;
        END

        FETCH NEXT FROM fresh_cur INTO @entity, @src_max;
    END

    CLOSE fresh_cur;
    DEALLOCATE fresh_cur;
END
GO


/*==============================================================================
  etl.usp_GetRunValidationResult

  Flattens a run's validation results into the single row an ADF If Condition
  branches on. `should_alert` is the expression the pipeline tests.
==============================================================================*/
CREATE OR ALTER PROCEDURE etl.usp_GetRunValidationResult
    @run_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        run_id           = @run_id,
        checks_run       = COUNT(*),
        checks_passed    = SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END),
        checks_failed    = SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END),
        critical_failed  = SUM(CASE WHEN status = 'FAIL' AND severity = 'Critical' THEN 1 ELSE 0 END),
        warning_failed   = SUM(CASE WHEN status = 'FAIL' AND severity = 'Warning'  THEN 1 ELSE 0 END),
        should_alert     = CASE WHEN SUM(CASE WHEN status = 'FAIL' AND severity = 'Critical' THEN 1 ELSE 0 END) > 0
                                THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END,
        failed_check_list = STUFF((
            SELECT N', ' + v.check_name
            FROM   etl.etl_validation_log AS v
            WHERE  v.run_id = @run_id AND v.status = 'FAIL'
            ORDER  BY CASE v.severity WHEN 'Critical' THEN 0 ELSE 1 END, v.check_name
            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, N'')
    FROM etl.etl_validation_log
    WHERE run_id = @run_id;
END
GO

/*==============================================================================
  etl.usp_ValidateSalesData

  Single entry point for the orchestrator's validation stage. ADF calls this
  one procedure rather than four, so the pipeline graph stays readable.

  It must return exactly ONE result set -- the summary from
  usp_GetRunValidationResult. usp_ReconcileRowCounts emits a result set of its
  own, and if that were allowed to reach the client it would arrive *first*.
  An ADF Lookup activity reads only the first result set, so the pipeline would
  branch on the reconciliation row instead of the validation summary and never
  see should_alert at all. Capturing the inner result sets into a table
  variable with INSERT ... EXEC is what keeps the contract to one result set.

  Source counts may be passed explicitly, but default to the values the extract
  stage already recorded in etl.etl_run_entity.source_row_count. That default
  is what lets the orchestrator call this with nothing but a run id: the
  pipeline counted the source rows once, during extraction, and does not need
  to carry the number through the graph as a parameter or query the source a
  second time (by which point the answer could have changed).
==============================================================================*/
CREATE OR ALTER PROCEDURE etl.usp_ValidateSalesData
    @run_id                  BIGINT,
    @source_header_row_count BIGINT = NULL,
    @source_detail_row_count BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @source_header_row_count IS NULL
        SELECT @source_header_row_count = source_row_count
        FROM   etl.etl_run_entity
        WHERE  run_id = @run_id AND entity_name = 'SalesOrderHeader';

    IF @source_detail_row_count IS NULL
        SELECT @source_detail_row_count = source_row_count
        FROM   etl.etl_run_entity
        WHERE  run_id = @run_id AND entity_name = 'SalesOrderDetail';

    DECLARE @recon TABLE
    (
        entity_name   SYSNAME,
        source_rows   BIGINT,
        staged_rows   BIGINT,
        loaded_rows   BIGINT,
        rejected_rows BIGINT,
        status        VARCHAR(10)
    );

    IF @source_header_row_count IS NOT NULL
        INSERT INTO @recon
        EXEC etl.usp_ReconcileRowCounts @run_id, 'SalesOrderHeader', @source_header_row_count;

    IF @source_detail_row_count IS NOT NULL
        INSERT INTO @recon
        EXEC etl.usp_ReconcileRowCounts @run_id, 'SalesOrderDetail', @source_detail_row_count;

    EXEC etl.usp_ValidatePostLoad @run_id;

    EXEC etl.usp_GetRunValidationResult @run_id;
END
GO


PRINT 'Validation rules and check procedures deployed.';
GO
