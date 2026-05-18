#!/usr/bin/env bash
set -euo pipefail

BASE="${BASE:-http://localhost:8080}"
TENANTS=("alpha" "bravo" "charlie" "delta" "echo")

echo "==> live pods"
curl -fsS "$BASE/pods" | jq .
echo

echo "==> creating ${#TENANTS[@]} tenants"
for t in "${TENANTS[@]}"; do
  curl -fsS -X POST "$BASE/tenants/$t" | jq -c .
done
echo

echo "==> writing one key per tenant"
for t in "${TENANTS[@]}"; do
  curl -fsS -X PUT "$BASE/tenants/$t/keys/greeting" \
    -H 'content-type: application/json' \
    -d "{\"value\":\"hello from $t\"}"
done
echo "done"
echo

echo "==> reading back, observing which pod served each tenant"
for t in "${TENANTS[@]}"; do
  result=$(curl -fsS "$BASE/tenants/$t/keys/greeting")
  echo "$t -> $result"
done
echo

echo "==> final pod load distribution"
curl -fsS "$BASE/pods" | jq .
