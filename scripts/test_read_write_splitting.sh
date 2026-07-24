#!/usr/bin/env bash
set -uo pipefail

# Test suite for Read-Write Splitting.
# ---------------------------------------------------------
# We use THREE tablets for shard-0 to prove routing:
#
#   Tablet 1 (PRIMARY)  → vscale_shard0         (real primary DB)
#   Tablet 2 (PRIMARY)  → vscale_shard1         (real primary DB)
#   Tablet 3 (REPLICA)  → vscale_shard0_repl    (synthetic replica DB)
#
# We seed DIFFERENT data into the replica DB than the primary DB.
# If read-write splitting works, autocommit SELECTs on shard-0 keys
# will hit the REPLICA tablet and return the replica-only data.
# Writes and transactional reads will always hit the PRIMARY tablet.
# ---------------------------------------------------------

PG_USER="tomato"
PG_HOST="localhost"
PG_PORT="5432"
SHARD0_DB="vscale_shard0"
SHARD0_REPL_DB="vscale_shard0_repl"
SHARD1_DB="vscale_shard1"

ETCD_ENDPOINTS="localhost:2379"
ETCD_PREFIX="/vscale/tablets/"
VSCHEMA_PATH="./vschema.json"

VTGATE_PORT="50052"
T1_PORT="50051"  # tablet-1 PRIMARY shard-0
T2_PORT="50053"  # tablet-2 PRIMARY shard-1
T3_PORT="50054"  # tablet-3 REPLICA  shard-0

VTGATE_ADDR="localhost:${VTGATE_PORT}"
T1_ADDR="localhost:${T1_PORT}"
T2_ADDR="localhost:${T2_PORT}"
T3_ADDR="localhost:${T3_PORT}"

LOG_DIR="./test_logs"
mkdir -p "$LOG_DIR"

PIDS=()

green() { echo -e "\033[32m$1\033[0m"; }
red()   { echo -e "\033[31m$1\033[0m"; }
info()  { echo -e "\033[36m$1\033[0m"; }

cleanup() {
  echo ""
  info "=== Tearing down background processes ==="
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
    fi
  done
  green "Teardown complete."
}
trap cleanup EXIT INT TERM

wait_for_port() {
  local addr="$1" name="$2"
  local host="${addr%%:*}" port="${addr##*:}"
  for i in $(seq 1 30); do
    if nc -z "$host" "$port" 2>/dev/null; then
      green "  $name is up ($addr)"; return 0
    fi
    sleep 0.5
  done
  red "  $name did NOT come up on $addr"; return 1
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
    red "  FAIL: $desc"
    echo "    expected: $expect"
    echo "$output" | sed 's/^/    got: /'
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

extract_txid() {
  echo "$1" | jq -r '.transactionId // 0'
}

# ============================================================
# 1. Postgres databases
# ============================================================
info "=== 1. Setting up databases ==="
createdb -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" "$SHARD0_DB" 2>/dev/null || true
createdb -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" "$SHARD0_REPL_DB" 2>/dev/null || true
createdb -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" "$SHARD1_DB" 2>/dev/null || true

# Primary shard-0: users table, one row id=1 name='PrimaryOwner'
psql -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" -d "$SHARD0_DB" -c \
  "DROP TABLE IF EXISTS users; CREATE TABLE users (id INT PRIMARY KEY, name TEXT); INSERT INTO users (id, name) VALUES (1, 'PrimaryOwner');" > "$LOG_DIR/pg_s0_setup.log" 2>&1

# Replica shard-0: users table, same schema, DIFFERENT data
# id=1 name='ReplicaOwner' (proves reads hit replica)
# id=1 is deliberately different so we can distinguish
psql -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" -d "$SHARD0_REPL_DB" -c \
  "DROP TABLE IF EXISTS users; CREATE TABLE users (id INT PRIMARY KEY, name TEXT); INSERT INTO users (id, name) VALUES (1, 'ReplicaOwner');" > "$LOG_DIR/pg_s0r_setup.log" 2>&1

# Primary shard-1: users table
psql -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" -d "$SHARD1_DB" -c \
  "DROP TABLE IF EXISTS users; CREATE TABLE users (id INT PRIMARY KEY, name TEXT); INSERT INTO users (id, name) VALUES (9000, 'Shard1Data');" > "$LOG_DIR/pg_s1_setup.log" 2>&1

green "  Databases ready"

# ============================================================
# 2. etcd
# ============================================================
info "=== 2. Starting etcd ==="
if nc -z localhost 2379 2>/dev/null; then
  info "  etcd already running"
else
  etcd > "$LOG_DIR/etcd.log" 2>&1 &
  PIDS+=($!); wait_for_port "localhost:2379" "etcd" || exit 1
fi
etcdctl del "$ETCD_PREFIX" --prefix > /dev/null 2>&1 || true

# ============================================================
# 3. Start Tablets
# ============================================================
info "=== 3. Starting tablets ==="

# Tablet 1 -- PRIMARY shard-0
DATABASE_URL="postgres://${PG_USER}@${PG_HOST}:${PG_PORT}/${SHARD0_DB}" \
ETCD_ENDPOINTS="$ETCD_ENDPOINTS" ETCD_PREFIX="$ETCD_PREFIX" \
go run ./cmd/vscale/tablet \
  -cell=cell1 -uid=1 -keyspace=vscale -shard=shard-0 \
  -tablet_type=PRIMARY -hostname=localhost -grpc_port="$T1_PORT" \
  -key_range_start=0 -key_range_end=5000 \
  > "$LOG_DIR/t1_primary_s0.log" 2>&1 &
PIDS+=($!)

# Tablet 2 -- PRIMARY shard-1
DATABASE_URL="postgres://${PG_USER}@${PG_HOST}:${PG_PORT}/${SHARD1_DB}" \
ETCD_ENDPOINTS="$ETCD_ENDPOINTS" ETCD_PREFIX="$ETCD_PREFIX" \
go run ./cmd/vscale/tablet \
  -cell=cell1 -uid=2 -keyspace=vscale -shard=shard-1 \
  -tablet_type=PRIMARY -hostname=localhost -grpc_port="$T2_PORT" \
  -key_range_start=5000 -key_range_end=10000 \
  > "$LOG_DIR/t2_primary_s1.log" 2>&1 &
PIDS+=($!)

# Tablet 3 -- REPLICA shard-0 (points to different DB for routing proof)
DATABASE_URL="postgres://${PG_USER}@${PG_HOST}:${PG_PORT}/${SHARD0_REPL_DB}" \
ETCD_ENDPOINTS="$ETCD_ENDPOINTS" ETCD_PREFIX="$ETCD_PREFIX" \
go run ./cmd/vscale/tablet \
  -cell=cell1 -uid=3 -keyspace=vscale -shard=shard-0 \
  -tablet_type=REPLICA -hostname=localhost -grpc_port="$T3_PORT" \
  -key_range_start=0 -key_range_end=5000 \
  > "$LOG_DIR/t3_replica_s0.log" 2>&1 &
PIDS+=($!)

wait_for_port "$T1_ADDR" "t1-primary-s0" || { cat "$LOG_DIR/t1_primary_s0.log"; exit 1; }
wait_for_port "$T2_ADDR" "t2-primary-s1" || { cat "$LOG_DIR/t2_primary_s1.log"; exit 1; }
wait_for_port "$T3_ADDR" "t3-replica-s0" || { cat "$LOG_DIR/t3_replica_s0.log"; exit 1; }

sleep 1

# ============================================================
# 4. Start vtgate
# ============================================================
info "=== 4. Starting vtgate ==="
VTGATE_PORT="$VTGATE_PORT" ETCD_ENDPOINTS="$ETCD_ENDPOINTS" \
ETCD_PREFIX="$ETCD_PREFIX" VSCHEMA_PATH="$VSCHEMA_PATH" \
go run ./cmd/vscale/gate > "$LOG_DIR/vtgate.log" 2>&1 &
PIDS+=($!)
wait_for_port "$VTGATE_ADDR" "vtgate" || { cat "$LOG_DIR/vtgate.log"; exit 1; }
sleep 2

# ============================================================
# 5. Tests
# ============================================================
echo ""
info "=== 5. Running tests ==="

echo ""
info "-- Sanity --"
out=$(gcall "$T1_ADDR" Health '{}')
check "t1 (PRIMARY s0) healthy" "$out" "\"healthy\": true"
out=$(gcall "$T2_ADDR" Health '{}')
check "t2 (PRIMARY s1) healthy" "$out" "\"healthy\": true"
out=$(gcall "$T3_ADDR" Health '{}')
check "t3 (REPLICA s0) healthy" "$out" "\"healthy\": true"
out=$(gcall "$VTGATE_ADDR" Health '{}')
check "vtgate healthy" "$out" "\"healthy\": true"

# --------------------------------------------------------
# Read-Write Splitting Core Tests
# --------------------------------------------------------
echo ""
info "-- R/W Splitting: Autocommit read hits REPLICA --"
# id=1 lives in shard-0. The REPLICA DB has 'ReplicaOwner' for id=1.
# If routing is correct, autocommit SELECT should return 'ReplicaOwner'.
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"SELECT * FROM users WHERE id = 1"}')
check "read returns ReplicaOwner (from REPLICA)" "$out" "ReplicaOwner"
check_not "read does NOT return PrimaryOwner (wrong tablet)" "$out" "PrimaryOwner"

echo ""
info "-- R/W Splitting: Autocommit write hits PRIMARY --"
# INSERT id=2 should land on PRIMARY shard-0 DB.
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"INSERT INTO users (id, name) VALUES (2, '\''WriteTarget'\'')"}')
check "insert succeeds" "$out" "rowsAffected"

# Verify id=2 is in PRIMARY DB
out=$(gcall "$T1_ADDR" Execute '{"sql":"SELECT * FROM users WHERE id = 2"}')
check "write landed on PRIMARY (t1)" "$out" "WriteTarget"

# Verify id=2 is NOT in REPLICA DB (proving write didn't go there)
out=$(gcall "$T3_ADDR" Execute '{"sql":"SELECT * FROM users WHERE id = 2"}')
check_not "write did NOT go to REPLICA (t3)" "$out" "WriteTarget"

echo ""
info "-- R/W Splitting: Transaction read hits PRIMARY --"
# Inside a transaction, even reads must go to PRIMARY for consistency.
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"BEGIN"}')
TXID=$(extract_txid "$out")
if [ "$TXID" = "0" ]; then red "  FAIL: no txid"; exit 1; fi
green "  txid=$TXID"

# SELECT id=1 in transaction → should hit PRIMARY, return 'PrimaryOwner'.
out=$(gcall "$VTGATE_ADDR" Execute "{\"sql\":\"SELECT * FROM users WHERE id = 1\", \"transactionId\": $TXID}")
check "txn read returns PrimaryOwner (from PRIMARY)" "$out" "PrimaryOwner"
check_not "txn read does NOT return ReplicaOwner" "$out" "ReplicaOwner"

# Also verify the previously written row is visible in transaction
out=$(gcall "$VTGATE_ADDR" Execute "{\"sql\":\"SELECT * FROM users WHERE id = 2\", \"transactionId\": $TXID}")
check "txn read sees uncommitted write" "$out" "WriteTarget"

out=$(gcall "$VTGATE_ADDR" Execute "{\"sql\":\"COMMIT\", \"transactionId\": $TXID}")
check "commit txn" "$out" "COMMIT"

echo ""
info "-- R/W Splitting: Shard-1 unaffected (no replica configured) --"
# shard-1 has no replica. Reads should fall back to PRIMARY t2.
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"SELECT * FROM users WHERE id = 9000"}')
check "shard-1 read falls back to primary" "$out" "Shard1Data"

# Write to shard-1 still works
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"INSERT INTO users (id, name) VALUES (9001, '\''Shard1Write'\'')"}')
check "shard-1 write" "$out" "rowsAffected"

# Verify on t2
out=$(gcall "$T2_ADDR" Execute '{"sql":"SELECT * FROM users WHERE id = 9001"}')
check "shard-1 write on t2" "$out" "Shard1Write"

echo ""
info "-- R/W Splitting: Scatter read hits REPLICA when available --"
# A scatter SELECT on shard-0 should include the REPLICA row.
out=$(gcall "$VTGATE_ADDR" Execute '{"sql":"SELECT * FROM users"}')
check "scatter sees shard-0 replica data" "$out" "ReplicaOwner"
check "scatter sees shard-1 data" "$out" "Shard1Data"
# It should NOT see the primary-only rows because scatter read goes to replica for s0
check_not "scatter does NOT see primary-only data" "$out" "PrimaryOwner"

# --------------------------------------------------------
# Summary
# --------------------------------------------------------
echo ""
echo "=================================="
green "PASSED: $PASS"
if [ "$FAIL" -gt 0 ]; then red "FAILED: $FAIL"; else green "FAILED: $FAIL"; fi
echo "=================================="
echo "Logs available in: $LOG_DIR/"

if [ "$FAIL" -gt 0 ]; then exit 1; fi
exit 0
