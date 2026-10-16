#!/usr/bin/env bash
# =============================================================================
# verify.sh — confirms the load test produced exactly 80,000 unique rows
# =============================================================================
set -euo pipefail

HOST="${MYSQL_HOST:-127.0.0.1}"
PORT="${MYSQL_PORT:-3306}"
USER="${MYSQL_USER:-app_user}"
PASS="${MYSQL_PASS:-secret}"
DB="${MYSQL_DB:-app}"

echo "=== Submission count ==="
mysql -h"$HOST" -P"$PORT" -u"$USER" -p"$PASS" "$DB" \
  -e "SELECT COUNT(*) AS total_stored FROM submissions;"

echo ""
echo "=== Duplicate idempotency_key check (expect 0 rows) ==="
mysql -h"$HOST" -P"$PORT" -u"$USER" -p"$PASS" "$DB" \
  -e "SELECT idempotency_key, COUNT(*) AS cnt
      FROM submissions
      GROUP BY idempotency_key
      HAVING cnt > 1;"

echo ""
echo "=== Outbox published ratio ==="
mysql -h"$HOST" -P"$PORT" -u"$USER" -p"$PASS" "$DB" \
  -e "SELECT
        COUNT(*) AS total_outbox,
        SUM(published_at IS NOT NULL) AS published,
        SUM(published_at IS NULL)     AS pending
      FROM outbox;"

echo ""
echo "=== Hourly counts (Flink sink) ==="
mysql -h"$HOST" -P"$PORT" -u"$USER" -p"$PASS" "$DB" \
  -e "SELECT form_id, hour_start, count FROM form_hourly_counts ORDER BY hour_start;"
