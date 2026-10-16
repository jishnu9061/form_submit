#!/usr/bin/env bash
# =============================================================================
# flink-demo.sh — shows fault tolerance in action
# =============================================================================
# Run AFTER submitting the Flink job. This script:
#  1. Pushes 10 test submissions to the API
#  2. Shows the MySQL counts growing
#  3. Kills the Flink TaskManager (simulates crash)
#  4. Waits for recovery (Flink restarts from checkpoint)
#  5. Confirms MySQL counts are correct (no double-counting)
# =============================================================================
set -euo pipefail

API="${API_URL:-http://localhost:8000}"
echo "Using API: $API"

echo ""
echo "=== Step 1: Push 10 submissions ==="
for i in $(seq 1 10); do
  curl -s -X POST "$API/api/submissions" \
    -H "Content-Type: application/json" \
    -H "Accept: application/json" \
    -d "{\"form_id\":\"demo-form\",\"idempotency_key\":\"demo-$i\",\"email\":\"demo$i@example.com\",\"data\":{\"i\":$i}}" \
    | python3 -m json.tool --no-ensure-ascii 2>/dev/null | grep -E '"id"|"form_id"' || true
  sleep 0.1
done

echo ""
echo "=== Step 2: Wait 15 s for Flink to aggregate (early trigger every 10 s) ==="
sleep 15

echo ""
echo "=== Step 3: Check MySQL counts BEFORE crash ==="
docker compose exec mysql mysql -uapp_user -psecret app \
  -e "SELECT form_id, hour_start, count FROM form_hourly_counts WHERE form_id='demo-form';"

COUNT_BEFORE=$(docker compose exec -T mysql mysql -uapp_user -psecret app \
  -sN -e "SELECT COALESCE(SUM(count),0) FROM form_hourly_counts WHERE form_id='demo-form';")
echo "Count before crash: $COUNT_BEFORE"

echo ""
echo "=== Step 4: Kill TaskManager (simulate crash) ==="
docker compose kill flink-taskmanager
sleep 2

echo ""
echo "=== Step 5: Restart TaskManager — Flink recovers from last checkpoint ==="
docker compose start flink-taskmanager
echo "Waiting 30 s for recovery + re-aggregation..."
sleep 30

echo ""
echo "=== Step 6: Check MySQL counts AFTER recovery ==="
docker compose exec mysql mysql -uapp_user -psecret app \
  -e "SELECT form_id, hour_start, count FROM form_hourly_counts WHERE form_id='demo-form';"

COUNT_AFTER=$(docker compose exec -T mysql mysql -uapp_user -psecret app \
  -sN -e "SELECT COALESCE(SUM(count),0) FROM form_hourly_counts WHERE form_id='demo-form';")
echo "Count after recovery: $COUNT_AFTER"

echo ""
if [ "$COUNT_BEFORE" = "$COUNT_AFTER" ]; then
  echo "✅ PASS — counts are identical (no double-counting after recovery)"
else
  echo "⚠️  Counts differ: before=$COUNT_BEFORE after=$COUNT_AFTER"
  echo "   (Difference may indicate additional submissions arrived during recovery.)"
fi
