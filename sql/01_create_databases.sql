/*==============================================================================
  01_create_databases.sql
  ------------------------------------------------------------------------------
  Creates the staging / reporting database that the ETL pipeline loads into.

  The source database (AdventureWorks2022) is restored separately -- see
  scripts/restore_adventureworks.sh. This script only creates the *target*.

  Idempotent: safe to run repeatedly.

  Target platform: SQL Server 2022 / Azure SQL Database
==============================================================================*/

SET NOCOUNT ON;
GO

/*------------------------------------------------------------------------------
  On Azure SQL Database you cannot CREATE DATABASE from inside another database
  context, and the whole server is a single logical DB. If you are deploying to
  Azure SQL, create the database via the portal / az CLI / infra/main.bicep and
  skip straight to sql/02_target_schema.sql.
------------------------------------------------------------------------------*/
IF SERVERPROPERTY('EngineEdition') = 5
BEGIN
    PRINT 'Azure SQL Database detected -- skipping CREATE DATABASE.';
    PRINT 'Provision the database via infra/main.bicep, then run 02_target_schema.sql.';
END
ELSE
BEGIN
    IF DB_ID('SalesReportingDW') IS NULL
    BEGIN
        PRINT 'Creating database SalesReportingDW...';
        EXEC ('CREATE DATABASE SalesReportingDW');
    END
    ELSE
    BEGIN
        PRINT 'Database SalesReportingDW already exists -- nothing to do.';
    END
END
GO

/*------------------------------------------------------------------------------
  Recovery model: SIMPLE is correct for a reporting/staging target that is
  rebuilt from an upstream OLTP system. It keeps the log from growing during the
  bulk loads and we have no need for point-in-time restore -- the source of
  truth is AdventureWorks2022, and a full reload is always possible.
------------------------------------------------------------------------------*/
IF SERVERPROPERTY('EngineEdition') <> 5 AND DB_ID('SalesReportingDW') IS NOT NULL
BEGIN
    EXEC ('ALTER DATABASE SalesReportingDW SET RECOVERY SIMPLE');
    PRINT 'SalesReportingDW recovery model set to SIMPLE.';
END
GO
