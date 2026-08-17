#!/usr/bin/env bash
# =============================================================================
#  Deploys the target schema and all pipeline procedures to SalesReportingDW.
#
#  Every script it runs is idempotent, so this is also the upgrade path: change
#  a procedure, re-run this, done.
#
#      ./scripts/deploy_schema.sh
# =============================================================================

source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

load_env
require_container
wait_for_sql

TARGET_DB="${ETL_TARGET_DATABASE:-SalesReportingDW}"

info "Creating database ${TARGET_DB} (if absent)..."
sql_file master "$REPO_ROOT/sql/01_create_databases.sql"

for script in 02_target_schema 03_etl_procedures 04_validation_checks; do
  info "Applying sql/${script}.sql ..."
  sql_file "$TARGET_DB" "$REPO_ROOT/sql/${script}.sql"
done

# --- Prove the deployment actually took -------------------------------------
info "Verifying deployed objects..."

tables=$(sql_scalar "$TARGET_DB" \
  "SELECT COUNT(*) FROM sys.tables WHERE SCHEMA_NAME(schema_id) IN ('stg','dw','etl');")
procs=$(sql_scalar "$TARGET_DB" \
  "SELECT COUNT(*) FROM sys.procedures WHERE SCHEMA_NAME(schema_id) = 'etl';")
rules=$(sql_scalar "$TARGET_DB" \
  "SELECT COUNT(*) FROM etl.etl_validation_rule WHERE is_enabled = 1;")

ok "Tables (stg/dw/etl): ${tables}"
ok "Procedures (etl):    ${procs}"
ok "Enabled DQ rules:    ${rules}"

[[ "$tables" -ge 10 ]] || die "Expected at least 10 tables, found ${tables}."
[[ "$procs"  -ge 12 ]] || die "Expected at least 12 procedures, found ${procs}."
[[ "$rules"  -ge 20 ]] || die "Expected at least 20 enabled rules, found ${rules}."

printf '\n%sSchema deployment complete.%s Next: ./scripts/run_pipeline.sh\n' "$C_BOLD" "$C_RESET"
