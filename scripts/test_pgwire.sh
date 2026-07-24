#!/usr/bin/env bash
set -o pipefail

PG_USER="tomato"
PG_HOST="localhost"
PG_PORT="5432"
SHARD0_DB="vscale_shard0"
SHARD1_DB="vscale_shard1"

ETCD_ENDPOINTS="localhost:2379"
ETCD_PREFIX="/vscale/tablets/"
VSCHEMA_PATH="./vschema.json"

VTGATE_PORT="50052"
PGWIRE_PORT="5433"
T1_PORT="50051"
T2_PORT="50053"

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
    if nc -z "$host" "$port" 2>/dev/null; then green "  $name is up ($addr)"; return 0; fi
    sleep 0.5
  done
  red "  $name FAILED on $addr"; return 1
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
check_not() {
  local desc="$1" output="$2" expect="$3"
  if echo "$output" | grep -q "$expect"; then
    red "  FAIL: $desc (should NOT contain '$expect')"; FAIL=$((FAIL+1))
  else
    green "  PASS: $desc"; PASS=$((PASS+1))
  fi
}

psql_vscale() {
  psql "host=localhost port=$PGWIRE_PORT sslmode=disable user=vscaleuser dbname=vscale" -tAX "$@" 2>&1
}

info "=== 1. Setting up databases ==="
createdb -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" "$SHARD0_DB" 2>/dev/null || true
createdb -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" "$SHARD1_DB" 2>/dev/null || true

psql -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" -d "$SHARD0_DB" -c \
  "DROP TABLE IF EXISTS users;
   CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT);
   INSERT INTO users (id, name, score) VALUES (1, 'Alice', 100);
   INSERT INTO users (id, name, score) VALUES (2, 'Bob', 80);
   INSERT INTO users (id, name, score) VALUES (3, 'Charlie', 95);
   INSERT INTO users (id, name, score) VALUES (4, 'Diana', 80);" > "$LOG_DIR/pg_s0.log" 2>&1

psql -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" -d "$SHARD1_DB" -c \
  "DROP TABLE IF EXISTS users;
   CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT);
   INSERT INTO users (id, name, score) VALUES (5001, 'Eve', 90);
   INSERT INTO users (id, name, score) VALUES (5002, 'Frank', 85);
   INSERT INTO users (id, name, score) VALUES (5003, 'Grace', 100);
   INSERT INTO users (id, name, score) VALUES (5004, 'Hank', 70);" > "$LOG_DIR/pg_s1.log" 2>&1

green "  Databases ready"

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

VTGATE_PORT="$VTGATE_PORT" PGWIRE_PORT="$PGWIRE_PORT" \
ETCD_ENDPOINTS="$ETCD_ENDPOINTS" ETCD_PREFIX="$ETCD_PREFIX" VSCHEMA_PATH="$VSCHEMA_PATH" \
go run ./cmd/vscale/gate > "$LOG_DIR/vtgate.log" 2>&1 &
PIDS+=($!)
wait_for_port "localhost:$PGWIRE_PORT" "pgwire" || { cat "$LOG_DIR/vtgate.log"; exit 1; }
sleep 2

info "=== 3. Testing pgwire ==="

out=$(psql_vscale -c "SELECT * FROM users WHERE id = 1")
check "point-read id=1 returns Alice" "$out" "Alice"
check_not "point-read id=1 no Bob" "$out" "Bob"

out=$(psql_vscale -c "SELECT * FROM users WHERE id = 5001")
check "point-read id=5001 returns Eve" "$out" "Eve"

out=$(psql_vscale -c "SELECT COUNT(*) FROM users")
check "COUNT(*) = 8" "$out" "8"

out=$(psql_vscale -c "SELECT * FROM users ORDER BY id")
check "ORDER BY id first=Alice" "$out" "Alice"
check "ORDER BY id has Hank" "$out" "Hank"

out=$(psql_vscale -c "SELECT * FROM users ORDER BY id LIMIT 3")
line_count=$(echo "$out" | grep -c "^" || true)
if [ "$line_count" -eq "3" ]; then
  green "  PASS: LIMIT 3 returns 3 rows"
  PASS=$((PASS+1))
else
  red "  FAIL: LIMIT 3 returns $line_count rows"
  FAIL=$((FAIL+1))
fi

out=$(echo "BEGIN; INSERT INTO users (id, name, score) VALUES (100, 'TestTxn', 50); SELECT name FROM users WHERE id = 100; COMMIT;" | psql_vscale)
check "txn BEGIN/INSERT/SELECT/COMMIT sees TestTxn" "$out" "TestTxn"

out=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$SHARD0_DB" -tAX -c "SELECT name FROM users WHERE id = 100")
check "committed row in shard-0" "$out" "TestTxn"

out=$(echo "BEGIN; INSERT INTO users (id, name, score) VALUES (101, 'RollbackMe', 60); ROLLBACK;" | psql_vscale)

out=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$SHARD0_DB" -tAX -c "SELECT COUNT(*) FROM users WHERE id = 101")
check "rollback row absent count=0" "$out" "0"

echo ""
echo "=================================="
green "PASSED: $PASS"
if [ "$FAIL" -gt 0 ]; then red "FAILED: $FAIL"; else green "FAILED: $FAIL"; fi
echo "=================================="

if [ "$FAIL" -gt 0 ]; then exit 1; fi
exit 0
