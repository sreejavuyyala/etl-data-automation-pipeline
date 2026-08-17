#!/usr/bin/env bash
# =============================================================================
#  Empties the warehouse and the entire run history.
#
#      ./scripts/reset_warehouse.sh
#
#  Use this to start a measurement run from a known-clean baseline. It deletes
#  every row in dw and every row in the etl control tables, so the run ids in
#  any previously generated report will no longer resolve.
#
#  It does NOT touch the source database. Use
#  ./scripts/restore_adventureworks.sh --force for that.
# =============================================================================

source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

load_env
require_container
wait_for_sql

TARGET_DB="${ETL_TARGET_DATABASE:-SalesReportingDW}"

if [[ "${1:-}" != "--yes" ]]; then
  runs=$(sql_scalar "$TARGET_DB" "SELECT COUNT(*) FROM etl.etl_run_log;")
  orders=$(sql_scalar "$TARGET_DB" "SELECT COUNT(*) FROM dw.SalesOrderHeader;")
  printf 'This will delete %s warehouse order(s) and %s run log entr(ies) from %s.\n' \
    "$orders" "$runs" "$TARGET_DB"
  read -r -p 'Continue? [y/N] ' reply
  [[ "$reply" =~ ^[Yy]$ ]] || die "Aborted."
fi

info "Resetting ${TARGET_DB}..."

# Order matters: children before parents, because the foreign keys are real.
sql "$TARGET_DB" "
SET NOCOUNT ON;
DELETE FROM dw.SalesOrderDetail;
DELETE FROM dw.SalesOrderHeader;
TRUNCATE TABLE stg.SalesOrderDetail;
TRUNCATE TABLE stg.SalesOrderHeader;
DELETE FROM etl.etl_validation_log;
DELETE FROM etl.etl_rejected_row;
DELETE FROM etl.etl_alert;
DELETE FROM etl.etl_run_entity;
DELETE FROM etl.etl_run_log;
DELETE FROM etl.etl_watermark;
DBCC CHECKIDENT('etl.etl_run_log', RESEED, 0) WITH NO_INFOMSGS;
SELECT [Warehouse orders] = (SELECT COUNT(*) FROM dw.SalesOrderHeader),
       [Runs logged]      = (SELECT COUNT(*) FROM etl.etl_run_log);
"

# Alert files from previous runs would otherwise be mistaken for evidence from
# the new one.
if [[ -d "$REPO_ROOT/reports/alerts" ]]; then
  rm -f "$REPO_ROOT"/reports/alerts/*.json 2>/dev/null || true
  ok "Cleared reports/alerts/"
fi

printf '\n%sWarehouse reset.%s Next: python scripts/run_experiment.py\n' "$C_BOLD" "$C_RESET"
