#!/usr/bin/env bash
set -o pipefail

# ============================================================
# Scatter Query Aggregation Test Suite
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
T1_PORT="50051"
T2_PORT="50053"

VTGATE_ADDR="localhost:${VTGATE_PORT}"
T1_ADDR="localhost:${T1_PORT}"
T2_ADDR="localhost:${T2_PORT}"

LOG_DIR="./test_logs"
mkdir -p "$LOG_DIR"

PIDS=()

green() { echo -e "\033[32m$1\033[0m"; }
red()   { echo -e "\033[31m$1\033[0m"; }
info()  { echo -e "\033[36m$1\033[0m"; }

cleanup() {
  echo ""
  info "=== Teardown ==="
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
    fi
  done
  green "Done."
}
trap cleanup EXIT INT TERM

wait_for_port() {
  local addr="$1" name="$2"
  local host="${addr%%:*}" port="${addr##*:}"
  for i in $(seq 1 30); do
    if nc -z "$host" "$port" 2>/dev/null; then green "  $name is up"; return 0; fi
    sleep 0.5
  done
  red "  $name FAILED on $addr"; return 1
}

gcall() {
  local addr="$1" method="$2" data="$3"
  grpcurl -plaintext -d "$data" "$addr" "tablet.TabletService/$method" 2>&1
}

PASS=0; FAIL=0
check() {
  local desc="$1" output="$2" expect="$3"
  if echo "$output" | grep -q "$expect"; then
    green "  PASS: $desc"; PASS=$((PASS+1))
  else
    red "  FAIL: $desc"; echo "    expected: $expect"; echo "$output" | sed 's/^/    got: /'
    FAIL=$((FAIL+1))
  fi
}
check_json() {
  local desc="$1" json="$2" jqfilter="$3" expect="$4"
  local got
  got=$(echo "$json" | jq -r "$jqfilter" 2>/dev/null)
  if [ "$got" = "$expect" ]; then
    green "  PASS: $desc"; PASS=$((PASS+1))
  else
    red "  FAIL: $desc"; echo "    expected: $expect"; echo "    got: $got"
    FAIL=$((FAIL+1))
  fi
}

# ============================================================
# 1. Databases
# ============================================================
info "=== 1. Setting up databases ==="
createdb -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" "$SHARD0_DB" 2>/dev/null || true
createdb -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" "$SHARD1_DB" 2>/dev/null || true

psql -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" -d "$SHARD0_DB" -c \
  "DROP TABLE IF EXISTS users;
   CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT);
   INSERT INTO users (id, name, score) VALUES (1, 'Alice', 100);
   INSERT INTO users (id, name, score) VALUES (2, 'Bob', 80);
   INSERT INTO users (id, name, score) VALUES (3, 'Charlie', 95);
   INSERT INTO users (id, name, score) VALUES (4, 'Diana', 80);
   SELECT COUNT(*) FROM users;" > "$LOG_DIR/pg_s0.log" 2>&1

psql -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" -d "$SHARD1_DB" -c \
  "DROP TABLE IF EXISTS users;
   CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT);
   INSERT INTO users (id, name, score) VALUES (5001, 'Eve', 90);
   INSERT INTO users (id, name, score) VALUES (5002, 'Frank', 85);
   INSERT INTO users (id, name, score) VALUES (5003, 'Grace', 100);
   INSERT INTO users (id, name, score) VALUES (5004, 'Hank', 70);
   SELECT COUNT(*) FROM users;" > "$LOG_DIR/pg_s1.log" 2>&1

green "  Databases ready"

# ============================================================
# 2. etcd + tablets + vtgate
# ============================================================
info "=== 2. Starting services ==="
if nc -z localhost 2379 2>/dev/null; then info "  etcd running"; else
  etcd > "$LOG_DIR/etcd.log" 2>&1 &
  PIDS+=($!)
  wait_for_port "localhost:2379" "etcd" || exit 1
fi
etcdctl del "$ETCD_PREFIX" --prefix >/dev/null 2>&1 || true

DATABASE_URL="postgres://${PG_USER}@${PG_HOST}:${PG_PORT}/${SHARD0_DB}" \
ETCD_ENDPOINTS="$ETCD_ENDPOINTS" ETCD_PREFIX="$ETCD_PREFIX" \
go run ./cmd/vscale/tablet \
  -cell=cell1 -uid=1 -keyspace=vscale -shard=shard-0 \
  -tablet_type=PRIMARY -hostname=localhost -grpc_port="$T1_PORT" \
  -key_range_start=0 -key_range_end=5000 \
  > "$LOG_DIR/t1.log" 2>&1 &
PIDS+=($!)

DATABASE_URL="postgres://${PG_USER}@${PG_HOST}:${PG_PORT}/${SHARD1_DB}" \
ETCD_ENDPOINTS="$ETCD_ENDPOINTS" ETCD_PREFIX="$ETCD_PREFIX" \
go run ./cmd/vscale/tablet \
  -cell=cell1 -uid=2 -keyspace=vscale -shard=shard-1 \
  -tablet_type=PRIMARY -hostname=localhost -grpc_port="$T2_PORT" \
  -key_range_start=5000 -key_range_end=10000 \
  > "$LOG_DIR/t2.log" 2>&1 &
PIDS+=($!)

wait_for_port "$T1_ADDR" "t1" || { cat "$LOG_DIR/t1.log"; exit 1; }
wait_for_port "$T2_ADDR" "t2" || { cat "$LOG_DIR/t2.log"; exit 1; }
sleep 1

VTGATE_PORT="$VTGATE_PORT" ETCD_ENDPOINTS="$ETCD_ENDPOINTS" \
ETCD_PREFIX="$ETCD_PREFIX" VSCHEMA_PATH="$VSCHEMA_PATH" \
go run ./cmd/vscale/gate > "$LOG_DIR/vtgate.log" 2>&1 &
PIDS+=($!)
wait_for_port "$VTGATE_ADDR" "vtgate" || { cat "$LOG_DIR/vtgate.log"; exit 1; }
sleep 2

# ============================================================
# 3. Tests
# ============================================================
echo ""
info "=== 3. Running scatter aggregation tests ==="

out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"SELECT * FROM users"}')
check "scatter returns Alice"  "$out" "Alice"
check "scatter returns Eve"    "$out" "Eve"

echo ""
info "-- ORDER BY --"
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"SELECT * FROM users ORDER BY id"}')
check_json "ORDER BY id ASC - first row id=1" "$out" '.results[0].rows[0].values[0]' "1"
check_json "ORDER BY id ASC - last row id=5004" "$out" '.results[0].rows[-1].values[0]' "5004"

out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"SELECT * FROM users ORDER BY id DESC"}')
check_json "ORDER BY id DESC - first row id=5004" "$out" '.results[0].rows[0].values[0]' "5004"
check_json "ORDER BY id DESC - last row id=1" "$out" '.results[0].rows[-1].values[0]' "1"

echo ""
info "-- LIMIT + OFFSET --"
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"SELECT * FROM users ORDER BY id LIMIT 3 OFFSET 2"}')
check_json "LIMIT 3 OFFSET 2 returns 3 rows" "$out" '.results[0].rows | length' "3"
check_json "LIMIT 3 OFFSET 2 first id=3" "$out" '.results[0].rows[0].values[0]' "3"
check_json "LIMIT 3 OFFSET 2 second id=4" "$out" '.results[0].rows[1].values[0]' "4"
check_json "LIMIT 3 OFFSET 2 third id=5001" "$out" '.results[0].rows[2].values[0]' "5001"

# Also verify OFFSET alone works
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"SELECT * FROM users ORDER BY id OFFSET 2"}')
check_json "OFFSET 2 returns 6 rows" "$out" '.results[0].rows | length' "6"
check_json "OFFSET 2 first id=3" "$out" '.results[0].rows[0].values[0]' "3"

echo ""
info "-- DISTINCT --"
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"SELECT DISTINCT score FROM users"}')
check_json "DISTINCT score count" "$out" '.results[0].rows | length' "6"

echo ""
info "-- COUNT(*) --"
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"SELECT COUNT(*) FROM users"}')
check_json "COUNT(*) = 8" "$out" '.results[0].rows[0].values[0]' "8"

echo ""
info "-- SUM --"
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"SELECT SUM(score) FROM users"}')
check_json "SUM(score) = 700" "$out" '.results[0].rows[0].values[0]' "700"

echo ""
info "-- MAX --"
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"SELECT MAX(score) FROM users"}')
check_json "MAX(score) = 100" "$out" '.results[0].rows[0].values[0]' "100"

echo ""
info "-- MIN --"
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"SELECT MIN(score) FROM users"}')
check_json "MIN(score) = 70" "$out" '.results[0].rows[0].values[0]' "70"

echo ""
info "-- Regression: plain scatter still works --"
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"SELECT * FROM users"}')
# Plain scatter returns one QueryResult per shard → count across all results
check_json "plain scatter row count = 8" "$out" '([.results[].rows[]] | length)' "8"

echo ""
echo "=================================="
green "PASSED: $PASS"
if [ "$FAIL" -gt 0 ]; then red "FAILED: $FAIL"; else green "FAILED: $FAIL"; fi
echo "=================================="
echo "Logs: $LOG_DIR/"

if [ "$FAIL" -gt 0 ]; then exit 1; fi
exit 0
