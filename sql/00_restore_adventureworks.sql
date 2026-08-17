/*==============================================================================
  00_restore_adventureworks.sql
  ------------------------------------------------------------------------------
  Restores the AdventureWorks2022 sample OLTP database from a .bak file.

  Called with :setvar BackupPath and :setvar DatabaseName by
  scripts/restore_adventureworks.sh.

  The MOVE clauses are generated rather than hard-coded. The backup was taken
  on Windows, so every file inside it carries a path like
  'C:\...\AdventureWorks2022.mdf' which cannot exist on a Linux container --
  the restore fails outright without a MOVE for each file. Reading
  RESTORE FILELISTONLY and building the statement from it means this works for
  AdventureWorks2022, AdventureWorksLT2022, or any other .bak, without anyone
  having to look up the logical file names first.
==============================================================================*/

SET NOCOUNT ON;
GO

:on error exit

DECLARE @backup_path NVARCHAR(400) = N'$(BackupPath)';
DECLARE @db_name     SYSNAME       = N'$(DatabaseName)';
DECLARE @data_dir    NVARCHAR(400) = N'/var/opt/mssql/data/';
DECLARE @sql         NVARCHAR(MAX);

IF DB_ID(@db_name) IS NOT NULL
BEGIN
    PRINT CONCAT('Database ', @db_name, ' already exists -- skipping restore.');
    PRINT 'Drop it first if you want a clean baseline:';
    PRINT CONCAT('    ALTER DATABASE [', @db_name, '] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [', @db_name, '];');
    RETURN;
END

/*------------------------------------------------------------------------------
  Read the file manifest out of the backup.
------------------------------------------------------------------------------*/
IF OBJECT_ID('tempdb..#filelist') IS NOT NULL DROP TABLE #filelist;

CREATE TABLE #filelist
(
    LogicalName          NVARCHAR(128),
    PhysicalName         NVARCHAR(260),
    [Type]               CHAR(1),
    FileGroupName        NVARCHAR(128),
    Size                 NUMERIC(20,0),
    MaxSize              NUMERIC(20,0),
    FileID               BIGINT,
    CreateLSN            NUMERIC(25,0),
    DropLSN              NUMERIC(25,0),
    UniqueID             UNIQUEIDENTIFIER,
    ReadOnlyLSN          NUMERIC(25,0),
    ReadWriteLSN         NUMERIC(25,0),
    BackupSizeInBytes    BIGINT,
    SourceBlockSize      INT,
    FileGroupID          INT,
    LogGroupGUID         UNIQUEIDENTIFIER,
    DifferentialBaseLSN  NUMERIC(25,0),
    DifferentialBaseGUID UNIQUEIDENTIFIER,
    IsReadOnly           BIT,
    IsPresent            BIT,
    TDEThumbprint        VARBINARY(32),
    SnapshotURL          NVARCHAR(360)
);

SET @sql = N'RESTORE FILELISTONLY FROM DISK = @p;';
INSERT INTO #filelist
EXEC sp_executesql @sql, N'@p NVARCHAR(400)', @p = @backup_path;

IF NOT EXISTS (SELECT 1 FROM #filelist)
BEGIN
    RAISERROR('Backup file contained no readable file list: %s', 16, 1, @backup_path);
    RETURN;
END

/*------------------------------------------------------------------------------
  Build one MOVE clause per file, redirecting each to the container's data
  directory while keeping the original file extension.
------------------------------------------------------------------------------*/
DECLARE @moves NVARCHAR(MAX) = N'';

SELECT @moves = @moves + N'    MOVE ' + QUOTENAME(LogicalName, '''')
              + N' TO ' + QUOTENAME(@data_dir + @db_name + N'_' + CAST(FileID AS NVARCHAR(10))
                                    + CASE [Type] WHEN 'L' THEN N'.ldf' ELSE N'.mdf' END, '''')
              + N',' + CHAR(13) + CHAR(10)
FROM   #filelist
ORDER  BY FileID;

SET @sql = N'RESTORE DATABASE ' + QUOTENAME(@db_name) + CHAR(13) + CHAR(10)
         + N'FROM DISK = ' + QUOTENAME(@backup_path, '''') + CHAR(13) + CHAR(10)
         + N'WITH' + CHAR(13) + CHAR(10)
         + @moves
         + N'    REPLACE, RECOVERY, STATS = 10;';

PRINT 'Executing:';
PRINT @sql;

EXEC sp_executesql @sql;
GO

/*------------------------------------------------------------------------------
  SIMPLE recovery: this is a read-only source for a demo pipeline. Nobody is
  going to take log backups of it, and FULL recovery without them grows the log
  until the container's disk fills.
------------------------------------------------------------------------------*/
DECLARE @db_name SYSNAME = N'$(DatabaseName)';
DECLARE @sql NVARCHAR(MAX) = N'ALTER DATABASE ' + QUOTENAME(@db_name) + N' SET RECOVERY SIMPLE;';
EXEC sp_executesql @sql;

PRINT CONCAT('Restored ', @db_name, ' and set recovery model to SIMPLE.');
GO
