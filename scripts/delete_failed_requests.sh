#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Delete failed requests from CPA Usage Keeper SQLite safely.

Usage:
  scripts/delete_failed_requests.sh --db /path/to/app.db [options]

Options:
  --before <timestamp>  Delete only failed requests with timestamp < value.
                        Example: 2026-09-01 00:00:00
  --dry-run             Show how many rows would be deleted and exit.
  --skip-backup         Do not create a SQLite backup before mutation.
  -h, --help            Show this help.

What this script does:
  1) Enables foreign keys and starts an IMMEDIATE transaction.
  2) Deletes child rows that reference usage_events when FK is not CASCADE/SET NULL.
  3) Deletes failed rows from usage_events (and usage_events_archive if present).
  4) Recomputes usage_identities counters from remaining usage_events.
  5) Clears derived aggregate tables and resets aggregation checkpoints to 0.

Notes:
  - Stop the app before running to avoid race conditions.
  - Keep the backup file until you verify dashboard metrics.
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required command not found: $1" >&2
    exit 1
  fi
}

sql_quote() {
  local value="$1"
  value=${value//\'/\'\'}
  printf "'%s'" "$value"
}

table_exists() {
  local table_name="$1"
  local exists
  exists="$(sqlite3 "$DB_PATH" "SELECT 1 FROM sqlite_master WHERE type='table' AND name=$(sql_quote "$table_name") LIMIT 1;")"
  [[ "$exists" == "1" ]]
}

require_cmd sqlite3
require_cmd mktemp

DB_PATH=""
BEFORE=""
DRY_RUN=false
SKIP_BACKUP=false

while (($# > 0)); do
  case "$1" in
    --db)
      DB_PATH="${2:-}"
      shift 2
      ;;
    --before)
      BEFORE="${2:-}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --skip-backup)
      SKIP_BACKUP=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$DB_PATH" ]]; then
  echo "error: --db is required" >&2
  usage
  exit 1
fi

if [[ ! -f "$DB_PATH" ]]; then
  echo "error: database file not found: $DB_PATH" >&2
  exit 1
fi

if ! table_exists "usage_events"; then
  echo "error: usage_events table not found in $DB_PATH" >&2
  exit 1
fi

WHERE_MAIN="failed = 1"
if [[ -n "$BEFORE" ]]; then
  WHERE_MAIN+=" AND timestamp < $(sql_quote "$BEFORE")"
fi

where_archive="$WHERE_MAIN"

failed_main_count="$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM usage_events WHERE $WHERE_MAIN;")"
failed_archive_count="0"
if table_exists "usage_events_archive"; then
  failed_archive_count="$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM usage_events_archive WHERE $where_archive;")"
fi

printf "Database: %s\n" "$DB_PATH"
printf "Failed rows to delete from usage_events: %s\n" "$failed_main_count"
printf "Failed rows to delete from usage_events_archive: %s\n" "$failed_archive_count"

if [[ "$DRY_RUN" == "true" ]]; then
  echo "Dry run complete. No data changed."
  exit 0
fi

if [[ "$failed_main_count" == "0" && "$failed_archive_count" == "0" ]]; then
  echo "Nothing to delete. Exiting."
  exit 0
fi

if [[ "$SKIP_BACKUP" != "true" ]]; then
  backup_path="${DB_PATH}.backup-before-delete-failed-$(date +%Y%m%d-%H%M%S).db"
  sqlite3 "$DB_PATH" ".timeout 5000" ".backup '$backup_path'"
  printf "Backup created: %s\n" "$backup_path"
fi

fk_cleanup_sql=""
while IFS=$'\t' read -r child_table child_column on_delete; do
  [[ -z "$child_table" || -z "$child_column" ]] && continue
  case "$on_delete" in
    CASCADE|SET\ NULL)
      ;;
    *)
      fk_cleanup_sql+="DELETE FROM \"${child_table}\" WHERE \"${child_column}\" IN (SELECT id FROM _failed_usage_event_ids);\n"
      ;;
  esac
done < <(
  sqlite3 -tabs "$DB_PATH" "
    SELECT m.name, fk.\"from\", UPPER(COALESCE(fk.on_delete, ''))
    FROM sqlite_master AS m
    JOIN pragma_foreign_key_list(m.name) AS fk
    WHERE m.type = 'table'
      AND m.name NOT LIKE 'sqlite_%'
      AND fk.\"table\" = 'usage_events';"
)

aggregate_reset_sql=""
for table_name in usage_overview_hourly_stats usage_overview_daily_stats usage_activity_stats usage_latency_stats local_ranking_period_stats; do
  if table_exists "$table_name"; then
    aggregate_reset_sql+="DELETE FROM \"${table_name}\";\n"
  fi
done

checkpoint_reset_sql=""
if table_exists "usage_aggregation_checkpoints"; then
  checkpoint_reset_sql+="UPDATE usage_aggregation_checkpoints\n"
  checkpoint_reset_sql+="SET last_aggregated_usage_event_id = 0,\n"
  checkpoint_reset_sql+="    stats_updated_at = NULL,\n"
  checkpoint_reset_sql+="    updated_at = CURRENT_TIMESTAMP\n"
  checkpoint_reset_sql+="WHERE name IN ('overview', 'activity', 'latency');\n"
fi

recompute_identity_sql=""
if table_exists "usage_identities"; then
  recompute_identity_sql+="UPDATE usage_identities\n"
  recompute_identity_sql+="SET total_requests = 0,\n"
  recompute_identity_sql+="    success_count = 0,\n"
  recompute_identity_sql+="    failure_count = 0,\n"
  recompute_identity_sql+="    input_tokens = 0,\n"
  recompute_identity_sql+="    output_tokens = 0,\n"
  recompute_identity_sql+="    reasoning_tokens = 0,\n"
  recompute_identity_sql+="    cached_tokens = 0,\n"
  recompute_identity_sql+="    cache_read_tokens = 0,\n"
  recompute_identity_sql+="    total_tokens = 0,\n"
  recompute_identity_sql+="    first_used_at = NULL,\n"
  recompute_identity_sql+="    last_used_at = NULL,\n"
  recompute_identity_sql+="    stats_updated_at = NULL,\n"
  recompute_identity_sql+="    last_aggregated_usage_event_id = 0;\n"

  recompute_identity_sql+="WITH oauth AS (\n"
  recompute_identity_sql+="  SELECT auth_index AS identity,\n"
  recompute_identity_sql+="         COUNT(*) AS total_requests,\n"
  recompute_identity_sql+="         SUM(CASE WHEN failed THEN 0 ELSE 1 END) AS success_count,\n"
  recompute_identity_sql+="         SUM(CASE WHEN failed THEN 1 ELSE 0 END) AS failure_count,\n"
  recompute_identity_sql+="         COALESCE(SUM(input_tokens), 0) AS input_tokens,\n"
  recompute_identity_sql+="         COALESCE(SUM(output_tokens), 0) AS output_tokens,\n"
  recompute_identity_sql+="         COALESCE(SUM(reasoning_tokens), 0) AS reasoning_tokens,\n"
  recompute_identity_sql+="         COALESCE(SUM(cached_tokens), 0) AS cached_tokens,\n"
  recompute_identity_sql+="         COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,\n"
  recompute_identity_sql+="         COALESCE(SUM(total_tokens), 0) AS total_tokens,\n"
  recompute_identity_sql+="         MIN(timestamp) AS first_used_at,\n"
  recompute_identity_sql+="         MAX(timestamp) AS last_used_at,\n"
  recompute_identity_sql+="         MAX(id) AS max_usage_event_id\n"
  recompute_identity_sql+="  FROM usage_events\n"
  recompute_identity_sql+="  WHERE auth_type = 'oauth'\n"
  recompute_identity_sql+="  GROUP BY auth_index\n"
  recompute_identity_sql+=")\n"
  recompute_identity_sql+="UPDATE usage_identities\n"
  recompute_identity_sql+="SET total_requests = oauth.total_requests,\n"
  recompute_identity_sql+="    success_count = oauth.success_count,\n"
  recompute_identity_sql+="    failure_count = oauth.failure_count,\n"
  recompute_identity_sql+="    input_tokens = oauth.input_tokens,\n"
  recompute_identity_sql+="    output_tokens = oauth.output_tokens,\n"
  recompute_identity_sql+="    reasoning_tokens = oauth.reasoning_tokens,\n"
  recompute_identity_sql+="    cached_tokens = oauth.cached_tokens,\n"
  recompute_identity_sql+="    cache_read_tokens = oauth.cache_read_tokens,\n"
  recompute_identity_sql+="    total_tokens = oauth.total_tokens,\n"
  recompute_identity_sql+="    first_used_at = oauth.first_used_at,\n"
  recompute_identity_sql+="    last_used_at = oauth.last_used_at,\n"
  recompute_identity_sql+="    stats_updated_at = oauth.last_used_at,\n"
  recompute_identity_sql+="    last_aggregated_usage_event_id = oauth.max_usage_event_id\n"
  recompute_identity_sql+="FROM oauth\n"
  recompute_identity_sql+="WHERE usage_identities.auth_type = 1\n"
  recompute_identity_sql+="  AND usage_identities.identity = oauth.identity;\n"

  recompute_identity_sql+="WITH apikey AS (\n"
  recompute_identity_sql+="  SELECT auth_index AS identity,\n"
  recompute_identity_sql+="         COUNT(*) AS total_requests,\n"
  recompute_identity_sql+="         SUM(CASE WHEN failed THEN 0 ELSE 1 END) AS success_count,\n"
  recompute_identity_sql+="         SUM(CASE WHEN failed THEN 1 ELSE 0 END) AS failure_count,\n"
  recompute_identity_sql+="         COALESCE(SUM(input_tokens), 0) AS input_tokens,\n"
  recompute_identity_sql+="         COALESCE(SUM(output_tokens), 0) AS output_tokens,\n"
  recompute_identity_sql+="         COALESCE(SUM(reasoning_tokens), 0) AS reasoning_tokens,\n"
  recompute_identity_sql+="         COALESCE(SUM(cached_tokens), 0) AS cached_tokens,\n"
  recompute_identity_sql+="         COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,\n"
  recompute_identity_sql+="         COALESCE(SUM(total_tokens), 0) AS total_tokens,\n"
  recompute_identity_sql+="         MIN(timestamp) AS first_used_at,\n"
  recompute_identity_sql+="         MAX(timestamp) AS last_used_at,\n"
  recompute_identity_sql+="         MAX(id) AS max_usage_event_id\n"
  recompute_identity_sql+="  FROM usage_events\n"
  recompute_identity_sql+="  WHERE auth_type = 'apikey'\n"
  recompute_identity_sql+="  GROUP BY auth_index\n"
  recompute_identity_sql+=")\n"
  recompute_identity_sql+="UPDATE usage_identities\n"
  recompute_identity_sql+="SET total_requests = apikey.total_requests,\n"
  recompute_identity_sql+="    success_count = apikey.success_count,\n"
  recompute_identity_sql+="    failure_count = apikey.failure_count,\n"
  recompute_identity_sql+="    input_tokens = apikey.input_tokens,\n"
  recompute_identity_sql+="    output_tokens = apikey.output_tokens,\n"
  recompute_identity_sql+="    reasoning_tokens = apikey.reasoning_tokens,\n"
  recompute_identity_sql+="    cached_tokens = apikey.cached_tokens,\n"
  recompute_identity_sql+="    cache_read_tokens = apikey.cache_read_tokens,\n"
  recompute_identity_sql+="    total_tokens = apikey.total_tokens,\n"
  recompute_identity_sql+="    first_used_at = apikey.first_used_at,\n"
  recompute_identity_sql+="    last_used_at = apikey.last_used_at,\n"
  recompute_identity_sql+="    stats_updated_at = apikey.last_used_at,\n"
  recompute_identity_sql+="    last_aggregated_usage_event_id = apikey.max_usage_event_id\n"
  recompute_identity_sql+="FROM apikey\n"
  recompute_identity_sql+="WHERE usage_identities.auth_type = 2\n"
  recompute_identity_sql+="  AND usage_identities.identity = apikey.identity;\n"
fi

archive_delete_sql=""
if table_exists "usage_events_archive"; then
  archive_delete_sql+="DELETE FROM usage_events_archive WHERE ${where_archive};\n"
fi

sql_file="$(mktemp "${TMPDIR:-/tmp}/delete-failed-requests.XXXXXX.sql")"
trap 'rm -f "$sql_file"' EXIT

{
  printf '%s\n' "PRAGMA foreign_keys = ON;"
  printf '%s\n' "PRAGMA busy_timeout = 5000;"
  printf '%s\n' "BEGIN IMMEDIATE;"
  printf '%s\n' "CREATE TEMP TABLE _failed_usage_event_ids(id INTEGER PRIMARY KEY);"
  printf '%s\n' "INSERT INTO _failed_usage_event_ids(id)"
  printf '%s\n' "SELECT id"
  printf '%s\n' "FROM usage_events"
  printf '%s\n' "WHERE ${WHERE_MAIN};"
  printf '%b' "$fk_cleanup_sql"
  printf '%s\n' "DELETE FROM usage_events WHERE id IN (SELECT id FROM _failed_usage_event_ids);"
  printf '%s\n' "DROP TABLE _failed_usage_event_ids;"
  printf '%b' "$archive_delete_sql"
  printf '%b' "$recompute_identity_sql"
  printf '%b' "$aggregate_reset_sql"
  printf '%b' "$checkpoint_reset_sql"
  printf '%s\n' "COMMIT;"
} >"$sql_file"

sqlite3 "$DB_PATH" <"$sql_file"

remaining_failed_main="$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM usage_events WHERE failed = 1;")"
remaining_failed_archive="0"
if table_exists "usage_events_archive"; then
  remaining_failed_archive="$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM usage_events_archive WHERE failed = 1;")"
fi

printf "Done. Remaining failed rows in usage_events: %s\n" "$remaining_failed_main"
printf "Done. Remaining failed rows in usage_events_archive: %s\n" "$remaining_failed_archive"

if table_exists "usage_identities"; then
  events_total="$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM usage_events WHERE auth_type IN ('oauth','apikey');")"
  identities_total="$(sqlite3 "$DB_PATH" "SELECT COALESCE(SUM(total_requests), 0) FROM usage_identities WHERE auth_type IN (1,2);")"
  printf "Sanity check: usage_events(oauth/apikey)=%s, sum(usage_identities.total_requests)=%s\n" "$events_total" "$identities_total"
fi

echo "Next step: restart app so background aggregators rebuild overview/activity/latency/ranking tables from checkpoint 0."
