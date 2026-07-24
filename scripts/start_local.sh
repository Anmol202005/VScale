#!/usr/bin/env bash
#
# scripts/start_local.sh — Start a minimal local VScale cluster on localhost.
#
# Usage:
#   ./scripts/start_local.sh         # start from prebuilt binaries (build first)
#   ./scripts/start_local.sh --build # build binaries before starting
#
# This sets up:
#   - etcd on localhost:2379 (if not already running)
#   - PostgreSQL databases: vscale_shard0, vscale_shard1
#   - Tablet 1: PRIMARY for shard-0 [0,5000)
#   - Tablet 2: PRIMARY for shard-1 [5000,10000)
#   - Gate: grpc on :50052, pgwire on :5433, admin panel on :8080
#
# Prerequisites: etcdctl, psql/createdb, go, nc (netcat)
#
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$PROJECT_DIR/bin"
LOG_DIR="$PROJECT_DIR/test_logs"
mkdir -p "$BUILD_DIR" "$LOG_DIR"

# ----------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------
PG_USER="${PG_USER:-$(whoami)}"
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"

ETCD_ENDPOINTS="${ETCD_ENDPOINTS:-localhost:2379}"
ETCD_PREFIX="${ETCD_PREFIX:-/vscale/tablets/}"
VSCHEMA_PATH="$PROJECT_DIR/vschema.json"

VTGATE_PORT="50052"
PGWIRE_PORT="5433"
ADMIN_PORT="8080"
T1_PORT="50051"
T2_PORT="50053"

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
green()  { echo -e "\033[32m$1\033[0m"; }
red()    { echo -e "\033[31m$1\033[0m"; }
yellow() { echo -e "\033[33m$1\033[0m"; }
info()   { echo -e "\033[36m$1\033[0m"; }

PIDS=()
stop_all() {
  echo ""
  info "=== Stopping services ==="
  for pid in "${PIDS[@]-}"; do
    if [ -z "$pid" ]; then continue; fi
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null
      wait "$pid" 2>/dev/null
    fi
  done
  green "Stopped."
}
trap stop_all EXIT INT TERM

wait_for_port() {
  local host="${1%%:*}" port="${1##*:}" name="$2" max="${3:-30}"
  for i in $(seq 1 "$max"); do
    if nc -z "$host" "$port" 2>/dev/null; then
      green "  $name ready on $host:$port"; return 0
    fi
    sleep 0.5
  done
  red "  $name did NOT become ready on $host:$port"
  return 1
}

# ----------------------------------------------------------------------
# Check prerequisites
# ----------------------------------------------------------------------
check_cmd() {
  command -v "$1" >/dev/null 2>&1 || { red "Missing: $1 ($2)"; exit 1; }
}

info "=== Checking prerequisites ==="
check_cmd nc      "netcat (for port checks)"
check_cmd etcd    "etcd server"
check_cmd etcdctl "etcdctl"
check_cmd psql    "PostgreSQL client (psql)"
check_cmd createdb "PostgreSQL client (createdb)"
check_cmd go      "Go toolchain"

check_pg() {
  if ! psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d postgres -c "SELECT 1" >/dev/null 2>&1; then
    red "Could not connect to PostgreSQL at $PG_HOST:$PG_PORT as $PG_USER"
    red "Set PG_USER / PG_HOST / PG_PORT or start PostgreSQL."
    exit 1
  fi
}
check_pg
green "  All checks passed"

# ----------------------------------------------------------------------
# Build binaries (optional)
# ----------------------------------------------------------------------
if [ "${1:-}" = "--build" ] || [ ! -f "$BUILD_DIR/vscale-gate" ] || [ ! -f "$BUILD_DIR/vscale-tablet" ]; then
  info "=== Building binaries ==="
  cd "$PROJECT_DIR"
  go build -o "$BUILD_DIR/vscale-gate" ./cmd/vscale/gate
  go build -o "$BUILD_DIR/vscale-tablet" ./cmd/vscale/tablet
  green "  Binaries in $BUILD_DIR/"
fi

# ----------------------------------------------------------------------
# Setup PostgreSQL databases
# ----------------------------------------------------------------------
info "=== Creating PostgreSQL databases ==="
DB0="vscale_shard0"
DB1="vscale_shard1"

for db in "$DB0" "$DB1"; do
  createdb -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" "$db" 2>/dev/null || true
  psql -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" -d "$db" -c \
    "DROP TABLE IF EXISTS users;
     CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT);
     INSERT INTO users (id, name, score) VALUES
       (1, 'Alice', 100), (2, 'Bob', 80), (3, 'Charlie', 95), (4, 'Diana', 80)" \
    >"$LOG_DIR/pg_${db}.log" 2>&1
done

# Seed shard-1 with higher IDs
psql -U "$PG_USER" -h "$PG_HOST" -p "$PG_PORT" -d "$DB1" -c \
  "DELETE FROM users;
   INSERT INTO users (id, name, score) VALUES
     (5001, 'Eve', 90), (5002, 'Frank', 85), (5003, 'Grace', 100), (5004, 'Hank', 70)" \
  >"$LOG_DIR/pg_${DB1}.log" 2>&1

green "  Databases '$DB0' and '$DB1' ready"

# ----------------------------------------------------------------------
# etcd
# ----------------------------------------------------------------------
info "=== etcd ==="
if nc -z localhost 2379 2>/dev/null; then
  yellow "  etcd already running on :2379 — reusing it"
else
  etcd > "$LOG_DIR/etcd.log" 2>&1 &
  PIDS+=($!)
  wait_for_port "localhost:2379" "etcd" || exit 1
fi
etcdctl del "$ETCD_PREFIX" --prefix >/dev/null 2>&1 || true
green "  etcd ready"

# ----------------------------------------------------------------------
# Tablet 1 — shard-0, PRIMARY
# ----------------------------------------------------------------------
info "=== Starting Tablet 1 (shard-0 PRIMARY, range [0,5000)) ==="
DATABASE_URL="postgres://${PG_USER}@${PG_HOST}:${PG_PORT}/${DB0}" \
ETCD_ENDPOINTS="$ETCD_ENDPOINTS" ETCD_PREFIX="$ETCD_PREFIX" \
"$BUILD_DIR/vscale-tablet" \
  -cell=cell1 -uid=1 -keyspace=vscale -shard=shard-0 \
  -tablet_type=PRIMARY -hostname=localhost -grpc_port="$T1_PORT" \
  -key_range_start=0 -key_range_end=5000 \
  > "$LOG_DIR/tablet1.log" 2>&1 &
PIDS+=($!)
wait_for_port "localhost:$T1_PORT" "tablet1" || { cat "$LOG_DIR/tablet1.log"; exit 1; }

# ----------------------------------------------------------------------
# Tablet 2 — shard-1, PRIMARY
# ----------------------------------------------------------------------
info "=== Starting Tablet 2 (shard-1 PRIMARY, range [5000,10000)) ==="
DATABASE_URL="postgres://${PG_USER}@${PG_HOST}:${PG_PORT}/${DB1}" \
ETCD_ENDPOINTS="$ETCD_ENDPOINTS" ETCD_PREFIX="$ETCD_PREFIX" \
"$BUILD_DIR/vscale-tablet" \
  -cell=cell1 -uid=2 -keyspace=vscale -shard=shard-1 \
  -tablet_type=PRIMARY -hostname=localhost -grpc_port="$T2_PORT" \
  -key_range_start=5000 -key_range_end=10000 \
  > "$LOG_DIR/tablet2.log" 2>&1 &
PIDS+=($!)
wait_for_port "localhost:$T2_PORT" "tablet2" || { cat "$LOG_DIR/tablet2.log"; exit 1; }

sleep 1

# ----------------------------------------------------------------------
# Gate
# ----------------------------------------------------------------------
info "=== Starting Gate ==="
VTGATE_PORT="$VTGATE_PORT" \
PGWIRE_PORT="$PGWIRE_PORT" \
ADMIN_PORT="$ADMIN_PORT" \
ETCD_ENDPOINTS="$ETCD_ENDPOINTS" \
ETCD_PREFIX="$ETCD_PREFIX" \
VSCHEMA_PATH="$VSCHEMA_PATH" \
"$BUILD_DIR/vscale-gate" \
  > "$LOG_DIR/vtgate.log" 2>&1 &
PIDS+=($!)
wait_for_port "localhost:$VTGATE_PORT" "gate gRPC" || { cat "$LOG_DIR/vtgate.log"; exit 1; }
wait_for_port "localhost:$PGWIRE_PORT" "gate pgwire" || { cat "$LOG_DIR/vtgate.log"; exit 1; }

# quick wait for topology sync through etcd watch
sleep 2

green "  Gate ready"

# ----------------------------------------------------------------------
# Quick sanity check
# ----------------------------------------------------------------------
info "=== Sanity checks ==="

if command -v grpcurl >/dev/null 2>&1; then
  out=$(grpcurl -plaintext -d '{}' "localhost:$VTGATE_PORT" tablet.TabletService/Health 2>&1)
  if echo "$out" | grep -q '"healthy": true'; then
    green "  gate health: ok"
  else
    yellow "  gate health check inconclusive (grpcurl output: $out)"
  fi
else
  yellow "  grpcurl not installed — skipping gRPC health check"
fi

if echo "SELECT COUNT(*) FROM users" | psql "host=localhost port=$PGWIRE_PORT sslmode=disable user=vscaleuser dbname=vscale" -tAX 2>&1 | grep -q '^8$'; then
  green "  pgwire sanity: 8 rows across shards — ok"
else
  yellow "  pgwire sanity check inconclusive (try again in a moment)"
fi

# ----------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------
echo ""
echo "========================================================"
green "VScale local cluster is running!"
echo "========================================================"
echo ""
echo "  Admin Panel:   http://localhost:$ADMIN_PORT"
echo "  gRPC Gate:     localhost:$VTGATE_PORT"
echo "  PgWire Gate:   localhost:$PGWIRE_PORT"
echo "  Tablet 1:      localhost:$T1_PORT  (shard-0 PRIMARY)"
echo "  Tablet 2:      localhost:$T2_PORT  (shard-1 PRIMARY)"
echo "  etcd:          $ETCD_ENDPOINTS"
echo ""
echo "  Try:"
echo "    psql \"host=localhost port=$PGWIRE_PORT sslmode=disable user=vscaleuser dbname=vscale\" -c 'SELECT * FROM users WHERE id = 1;'"
echo "    grpcurl -plaintext -d '{\"sql\":\"SELECT * FROM users WHERE id = 1\"}' localhost:$VTGATE_PORT tablet.TabletService/Execute"
echo "    curl -s http://localhost:$ADMIN_PORT/api/health | jq ."
echo ""
echo "  Press Ctrl+C to stop everything."
echo ""

# Keep script alive so trap can clean up on Ctrl+C
while true; do sleep 1; done
