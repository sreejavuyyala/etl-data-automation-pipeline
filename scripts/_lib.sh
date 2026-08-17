#!/usr/bin/env bash
# =============================================================================
#  Shared helpers for the setup scripts.
#
#  Sourced, never executed directly.
# =============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTAINER_NAME="${CONTAINER_NAME:-etl-sqlserver}"
SQLCMD="/opt/mssql-tools18/bin/sqlcmd"

# --- Console ----------------------------------------------------------------
if [[ -t 1 ]]; then
  C_RESET=$'\033[0m'; C_BOLD=$'\033[1m'; C_DIM=$'\033[2m'
  C_RED=$'\033[31m'; C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'; C_BLUE=$'\033[34m'
else
  C_RESET=''; C_BOLD=''; C_DIM=''; C_RED=''; C_GREEN=''; C_YELLOW=''; C_BLUE=''
fi

info()  { printf '%s==>%s %s\n' "$C_BLUE$C_BOLD" "$C_RESET" "$*"; }
ok()    { printf '%s  ok%s %s\n' "$C_GREEN" "$C_RESET" "$*"; }
warn()  { printf '%swarn%s %s\n' "$C_YELLOW" "$C_RESET" "$*" >&2; }
die()   { printf '%sfail%s %s\n' "$C_RED" "$C_RESET" "$*" >&2; exit 1; }

# --- Environment ------------------------------------------------------------
load_env() {
  local env_file="$REPO_ROOT/.env"
  [[ -f "$env_file" ]] || die ".env not found. Run: cp .env.example .env, then edit it."
  # Export every KEY=VALUE line, ignoring comments and blanks. `set -a` makes
  # the assignments exported without repeating `export` on each line.
  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
  : "${MSSQL_SA_PASSWORD:?MSSQL_SA_PASSWORD is not set in .env}"
}

require_container() {
  docker inspect "$CONTAINER_NAME" >/dev/null 2>&1 \
    || die "Container '$CONTAINER_NAME' does not exist. Run: docker compose up -d"
  [[ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER_NAME")" == "true" ]] \
    || die "Container '$CONTAINER_NAME' is not running. Run: docker compose up -d"
}

wait_for_sql() {
  local timeout="${1:-300}" waited=0
  info "Waiting for SQL Server to accept connections (timeout ${timeout}s)..."
  until docker exec "$CONTAINER_NAME" "$SQLCMD" \
          -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -b -Q "SELECT 1" >/dev/null 2>&1; do
    [[ $waited -ge $timeout ]] && die "SQL Server did not become ready within ${timeout}s. Check: docker logs $CONTAINER_NAME"
    sleep 5; waited=$((waited + 5))
  done
  ok "SQL Server is accepting connections."
}

# sql <database> <query>            -- run an ad-hoc query
sql() {
  local db="$1"; shift
  docker exec -i "$CONTAINER_NAME" "$SQLCMD" \
    -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -b -d "$db" -Q "$*"
}

# sql_scalar <database> <query>     -- run a query, return one bare value
sql_scalar() {
  local db="$1"; shift
  docker exec -i "$CONTAINER_NAME" "$SQLCMD" \
    -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -b -d "$db" -h -1 -W -Q "SET NOCOUNT ON; $*" \
    | tr -d '\r' | head -1
}

# sql_file <database> <path> [-v NAME=VALUE ...]  -- run a .sql file
sql_file() {
  local db="$1" file="$2"; shift 2
  [[ -f "$file" ]] || die "SQL file not found: $file"
  docker exec -i "$CONTAINER_NAME" "$SQLCMD" \
    -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -b -d "$db" "$@" -i /dev/stdin < "$file"
}
