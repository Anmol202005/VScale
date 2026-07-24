#!/usr/bin/env bash
set -uo pipefail

# ============================================================
# Config - adjust paths/flags to match your actual setup
# ============================================================
PG_USER="tomato"
PG_HOST="localhost"
PG_PORT="5432"
SHARD0_DB="vscale_shard0"
SHARD1_DB="vscale_shard1"

ETCD_ENDPOINTS="localhost:2379"
ETCD_PREFIX="/vscale/tablets/"
VSCHEMA_PATH="./vschema.json"

VTGATE_PORT="50052"
TABLET1_GRPC_PORT="50051"
TABLET2_GRPC_PORT="50053"

VTGATE_ADDR="localhost:${VTGATE_PORT}"
TABLET1_ADDR="localhost:${TABLET1_GRPC_PORT}"
TABLET2_ADDR="localhost:${TABLET2_GRPC_PORT}"

LOG_DIR="./test_logs"
mkdir -p "$LOG_DIR"

PIDS=()

# ============================================================
# Helpers
# ============================================================
green() { echo -e "\033[32m$1\033[0m"; }
red()   { echo -e "\033[31m$1\033[0m"; }
info()  { echo -e "\033[36m$1\033[0m"; }

cleanup() {
  echo ""
  info "=== Tearing down background processes ==="
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null
      wait "$pid" 2>/dev/null
    fi
  done
  green "Teardown complete."
}
trap cleanup EXIT INT TERM

wait_for_port() {
  local addr="$1"
  local name="$2"
  local host="${addr%%:*}"
  local port="${addr##*:}"
  for i in $(seq 1 30); do
    if nc -z "$host" "$port" 2>/dev/null; then
      green "  $name is up ($addr)"
      return 0
    fi
    sleep 0.5
  done
  red "  $name did NOT come up on $addr in time"
  return 1
}

gcall() {
  local addr="$1"
  local method="$2"
  local data="$3"
  grpcurl -plaintext -d "$data" "$addr" "tablet.TabletService/$method" 2>&1
}

# Extract transactionId from grpcurl JSON output
extract_txid() {
  local json="$1"
  echo "$json" | jq -r '.transactionId // 0'
}

PASS=0
FAIL=0
check() {
  local desc="$1"
  local output="$2"
  local expect_substr="$3"
  if echo "$output" | grep -q "$expect_substr"; then
    green "  PASS: $desc"
    PASS=$((PASS+1))
  else
    red "  FAIL: $desc"
    echo "    expected to contain: $expect_substr"
    echo "$output" | sed 's/^/    got: /'
    FAIL=$((FAIL+1))
  fi
}

# ============================================================
# 1. Postgres databases - create if missing, reset tables
# ============================================================
info "=== 1. Setting up Postgres databases ==="

createdb -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" "$SHARD0_DB" 2>/dev/null || true
createdb -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" "$SHARD1_DB" 2>/dev/null || true

psql -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" -d "$SHARD0_DB" -c \
  "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name TEXT); DELETE FROM users;" > "$LOG_DIR/pg_shard0_setup.log" 2>&1
psql -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" -d "$SHARD1_DB" -c \
  "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name TEXT); DELETE FROM users;" > "$LOG_DIR/pg_shard1_setup.log" 2>&1

green "  Databases ready: $SHARD0_DB, $SHARD1_DB"

# ============================================================
# 2. etcd
# ============================================================
info "=== 2. Starting etcd ==="
if nc -z localhost 2379 2>/dev/null; then
  info "  etcd already running, reusing it"
else
  etcd > "$LOG_DIR/etcd.log" 2>&1 &
  PIDS+=($!)
  wait_for_port "localhost:2379" "etcd" || exit 1
fi

# clear any stale tablet registrations from previous runs
etcdctl del "$ETCD_PREFIX" --prefix > /dev/null 2>&1 || true

# ============================================================
# 3. Tablets
# ============================================================
info "=== 3. Starting tablet 1 (shard-0, keys [0,5000)) ==="
DATABASE_URL="postgres://${PG_USER}@${PG_HOST}:${PG_PORT}/${SHARD0_DB}" \
ETCD_ENDPOINTS="$ETCD_ENDPOINTS" ETCD_PREFIX="$ETCD_PREFIX" \
go run ./cmd/vscale/tablet \
  -cell=cell1 -uid=1 -keyspace=vscale -shard=shard-0 \
  -tablet_type=PRIMARY -hostname=localhost -grpc_port="$TABLET1_GRPC_PORT" \
  -key_range_start=0 -key_range_end=5000 \
  > "$LOG_DIR/tablet1.log" 2>&1 &
PIDS+=($!)

info "=== 3. Starting tablet 2 (shard-1, keys [5000,10000)) ==="
DATABASE_URL="postgres://${PG_USER}@${PG_HOST}:${PG_PORT}/${SHARD1_DB}" \
ETCD_ENDPOINTS="$ETCD_ENDPOINTS" ETCD_PREFIX="$ETCD_PREFIX" \
go run ./cmd/vscale/tablet \
  -cell=cell1 -uid=2 -keyspace=vscale -shard=shard-1 \
  -tablet_type=PRIMARY -hostname=localhost -grpc_port="$TABLET2_GRPC_PORT" \
  -key_range_start=5000 -key_range_end=10000 \
  > "$LOG_DIR/tablet2.log" 2>&1 &
PIDS+=($!)

wait_for_port "$TABLET1_ADDR" "tablet1" || { cat "$LOG_DIR/tablet1.log"; exit 1; }
wait_for_port "$TABLET2_ADDR" "tablet2" || { cat "$LOG_DIR/tablet2.log"; exit 1; }

# give tablets a moment to finish etcd registration after port opens
sleep 1

# ============================================================
# 4. vtgate
# ============================================================
info "=== 4. Starting vtgate ==="
VTGATE_PORT="$VTGATE_PORT" ETCD_ENDPOINTS="$ETCD_ENDPOINTS" \
ETCD_PREFIX="$ETCD_PREFIX" VSCHEMA_PATH="$VSCHEMA_PATH" \
go run ./cmd/vscale/gate > "$LOG_DIR/vtgate.log" 2>&1 &
PIDS+=($!)

wait_for_port "$VTGATE_ADDR" "vtgate" || { cat "$LOG_DIR/vtgate.log"; exit 1; }

# give vtgate's etcd watch a moment to pick up both tablets
sleep 2

# ============================================================
# 5. Run tests
# ============================================================
echo ""
info "=== 5. Running tests ==="

echo ""
info "-- Sanity checks --"
out=$(gcall "$TABLET1_ADDR" Health '{}')
check "tablet1 healthy" "$out" "\"healthy\": true"
out=$(gcall "$TABLET2_ADDR" Health '{}')
check "tablet2 healthy" "$out" "\"healthy\": true"
out=$(gcall "$VTGATE_ADDR" Health '{}')
check "vtgate healthy" "$out" "\"healthy\": true"

echo ""
info "-- Seed data directly per-shard --"
out=$(gcall "$TABLET1_ADDR" Execute '{"sql":"INSERT INTO users (id, name) VALUES (42, '"'"'Alice'"'"')"}')
check "seed Alice into tablet1" "$out" "rowsAffected"
out=$(gcall "$TABLET2_ADDR" Execute '{"sql":"INSERT INTO users (id, name) VALUES (7234, '"'"'Bob'"'"')"}')
check "seed Bob into tablet2" "$out" "rowsAffected"

echo ""
info "-- Single-shard routing via vtgate --"
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"SELECT * FROM users WHERE id = 42"}')
check "id=42 returns Alice" "$out" "Alice"
if echo "$out" | grep -q "Bob"; then
  red "  FAIL: id=42 should NOT return Bob (routing leaked to wrong shard)"
  FAIL=$((FAIL+1))
else
  green "  PASS: id=42 correctly excludes Bob"
  PASS=$((PASS+1))
fi

out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"SELECT * FROM users WHERE id = 7234"}')
check "id=7234 returns Bob" "$out" "Bob"

echo ""
info "-- Scatter fallback: no WHERE --"
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"SELECT * FROM users"}')
check "scatter returns Alice" "$out" "Alice"
check "scatter returns Bob" "$out" "Bob"

echo ""
info "-- Scatter fallback: WHERE on non-shard-key column --"
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"SELECT * FROM users WHERE name = '"'"'Alice'"'"'"}')
check "non-shard-key WHERE still finds Alice via scatter" "$out" "Alice"

echo ""
info "-- Single-shard transaction via vtgate --"
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"BEGIN"}')
check "BEGIN succeeds" "$out" "transactionId"
TXID=$(extract_txid "$out")
if [ -z "$TXID" ] || [ "$TXID" = "0" ]; then
  red "  FAIL: could not extract transactionId from BEGIN response"
  FAIL=$((FAIL+1))
  exit 1
fi
green "  txid=$TXID"

out=$(gcall "$VTGATE_ADDR" Execute "{\"sql\":\"INSERT INTO users (id, name) VALUES (300, 'Charlie')\", \"transactionId\": $TXID}")
check "INSERT in transaction" "$out" "rowsAffected"

out=$(gcall "$VTGATE_ADDR" Execute "{\"sql\":\"COMMIT\", \"transactionId\": $TXID}")
check "COMMIT succeeds" "$out" "COMMIT"

# Verify Charlie landed in shard-0 (id=300 in [0,5000))
out=$(gcall "$TABLET1_ADDR" Execute '{"sql":"SELECT * FROM users WHERE id = 300"}')
check "Charlie in tablet1" "$out" "Charlie"
out=$(gcall "$TABLET2_ADDR" Execute '{"sql":"SELECT * FROM users WHERE id = 300"}')
if echo "$out" | grep -q "Charlie"; then
  red "  FAIL: Charlie should NOT be in tablet2"
  FAIL=$((FAIL+1))
else
  green "  PASS: Charlie correctly absent from tablet2"
  PASS=$((PASS+1))
fi

echo ""
info "-- Single-shard ROLLBACK via vtgate --"
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"BEGIN"}')
TXID=$(extract_txid "$out")
if [ -z "$TXID" ] || [ "$TXID" = "0" ]; then
  red "  FAIL: could not extract txid for rollback test"
  FAIL=$((FAIL+1))
  exit 1
fi

out=$(gcall "$VTGATE_ADDR" Execute "{\"sql\":\"INSERT INTO users (id, name) VALUES (301, 'Dave')\", \"transactionId\": $TXID}")
check "INSERT for rollback" "$out" "rowsAffected"

out=$(gcall "$VTGATE_ADDR" Execute "{\"sql\":\"ROLLBACK\", \"transactionId\": $TXID}")
check "ROLLBACK succeeds" "$out" "ROLLBACK"

out=$(gcall "$TABLET1_ADDR" Execute '{"sql":"SELECT * FROM users WHERE id = 301"}')
if echo "$out" | grep -q "Dave"; then
  red "  FAIL: Dave should NOT exist after rollback"
  FAIL=$((FAIL+1))
else
  green "  PASS: Dave correctly absent after rollback"
  PASS=$((PASS+1))
fi

echo ""
info "-- Multi-shard transaction via vtgate --"
# Clear tables first
psql -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" -d "$SHARD0_DB" -c "DELETE FROM users;" >/dev/null 2>&1
psql -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" -d "$SHARD1_DB" -c "DELETE FROM users;" >/dev/null 2>&1

out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"BEGIN"}')
check "BEGIN for multi-shard" "$out" "transactionId"
TXID=$(extract_txid "$out")
if [ -z "$TXID" ] || [ "$TXID" = "0" ]; then
  red "  FAIL: could not extract txid for multi-shard test"
  FAIL=$((FAIL+1))
  exit 1
fi

out=$(gcall "$VTGATE_ADDR" Execute "{\"sql\":\"INSERT INTO users (id, name) VALUES (100, 'Eve')\", \"transactionId\": $TXID}")
check "INSERT Eve into shard-0" "$out" "rowsAffected"

out=$(gcall "$VTGATE_ADDR" Execute "{\"sql\":\"INSERT INTO users (id, name) VALUES (6000, 'Frank')\", \"transactionId\": $TXID}")
check "INSERT Frank into shard-1" "$out" "rowsAffected"

out=$(gcall "$VTGATE_ADDR" Execute "{\"sql\":\"COMMIT\", \"transactionId\": $TXID}")
check "COMMIT multi-shard" "$out" "COMMIT"

out=$(gcall "$TABLET1_ADDR" Execute '{"sql":"SELECT * FROM users WHERE id = 100"}')
check "Eve in tablet1" "$out" "Eve"
out=$(gcall "$TABLET2_ADDR" Execute '{"sql":"SELECT * FROM users WHERE id = 6000"}')
check "Frank in tablet2" "$out" "Frank"

echo ""
info "-- Insert-routing check: insert via vtgate (autocommit) --"
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"INSERT INTO users (id, name) VALUES (200, '\''Grace'\'')"}')
check "insert id=200 via vtgate succeeds" "$out" "rowsAffected"
out=$(gcall "$TABLET1_ADDR" Execute '{"sql":"SELECT * FROM users WHERE id = 200"}')
check "Grace landed in tablet1" "$out" "Grace"
out=$(gcall "$TABLET2_ADDR" Execute '{"sql":"SELECT * FROM users WHERE id = 200"}')
if echo "$out" | grep -q "Grace"; then
  red "  FAIL: Grace should NOT be in tablet2"
  FAIL=$((FAIL+1))
else
  green "  PASS: Grace correctly absent from tablet2"
  PASS=$((PASS+1))
fi

# ============================================================
# 6. Summary
# ============================================================
echo ""
echo "=================================="
green "PASSED: $PASS"
if [ "$FAIL" -gt 0 ]; then red "FAILED: $FAIL"; else green "FAILED: $FAIL"; fi
echo "=================================="
echo "Logs available in: $LOG_DIR/"

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
exit 0
