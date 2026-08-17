#!/usr/bin/env bash
# =============================================================================
#  Downloads and restores the AdventureWorks2022 sample OLTP database.
#
#  This is the source system the pipeline reads from. Microsoft publishes the
#  backup on the sql-server-samples releases page linked from
#  https://learn.microsoft.com/en-us/sql/samples/adventureworks-install-configure
#
#      ./scripts/restore_adventureworks.sh
#
#  Roughly 200MB to download and about a minute to restore. Skips both steps if
#  the database is already present -- pass --force to rebuild from scratch.
# =============================================================================

source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

BAK_URL="https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorks2022.bak"
BAK_DIR="$REPO_ROOT/data/backup"
BAK_FILE="$BAK_DIR/AdventureWorks2022.bak"
SOURCE_DB="AdventureWorks2022"
FORCE=0

[[ "${1:-}" == "--force" ]] && FORCE=1

load_env
require_container
wait_for_sql

# --- Already restored? ------------------------------------------------------
existing=$(sql_scalar master "SELECT COUNT(*) FROM sys.databases WHERE name = '${SOURCE_DB}';")
if [[ "$existing" == "1" && $FORCE -eq 0 ]]; then
  ok "${SOURCE_DB} is already restored. Pass --force to rebuild it."
  exit 0
fi

if [[ "$existing" == "1" && $FORCE -eq 1 ]]; then
  warn "Dropping existing ${SOURCE_DB} (--force)..."
  sql master "ALTER DATABASE [${SOURCE_DB}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [${SOURCE_DB}];"
fi

# --- Fetch the backup -------------------------------------------------------
mkdir -p "$BAK_DIR"
if [[ -f "$BAK_FILE" ]]; then
  ok "Backup already downloaded: $(du -h "$BAK_FILE" | cut -f1)"
else
  info "Downloading AdventureWorks2022.bak (~200MB)..."
  curl -fSL --retry 3 --progress-bar -o "$BAK_FILE" "$BAK_URL" \
    || die "Download failed. Fetch it manually from ${BAK_URL} and place it at ${BAK_FILE}"
  ok "Downloaded $(du -h "$BAK_FILE" | cut -f1)"
fi

# docker-compose mounts ./data/backup at /var/opt/mssql/backup, so the file is
# already visible inside the container -- no docker cp needed.
docker exec "$CONTAINER_NAME" test -f "/var/opt/mssql/backup/AdventureWorks2022.bak" \
  || die "Backup is not visible inside the container. Is the ./data/backup volume mounted? Try: docker compose up -d --force-recreate"

# --- Restore ----------------------------------------------------------------
info "Restoring ${SOURCE_DB}..."
sql_file master "$REPO_ROOT/sql/00_restore_adventureworks.sql" \
  -v BackupPath="/var/opt/mssql/backup/AdventureWorks2022.bak" DatabaseName="${SOURCE_DB}"

# --- Baseline ---------------------------------------------------------------
# These counts are the source of truth every later reconciliation is measured
# against, so they are printed at restore time rather than assumed.
info "Source baseline:"
sql "$SOURCE_DB" "
SET NOCOUNT ON;
SELECT
    [Table]       = 'Sales.SalesOrderHeader',
    [Rows]        = COUNT_BIG(*),
    [MinModified] = MIN(ModifiedDate),
    [MaxModified] = MAX(ModifiedDate)
FROM Sales.SalesOrderHeader
UNION ALL
SELECT
    'Sales.SalesOrderDetail', COUNT_BIG(*), MIN(ModifiedDate), MAX(ModifiedDate)
FROM Sales.SalesOrderDetail;"

printf '\n%sSource restored.%s Next: ./scripts/deploy_schema.sh\n' "$C_BOLD" "$C_RESET"
