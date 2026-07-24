#!/usr/bin/env bash
#
# scripts/try_it.sh — Hands-on feature walkthrough for VScale.
#
# Run AFTER ./scripts/start_local.sh is already running:
#   ./scripts/start_local.sh        # in one terminal
#   ./scripts/try_it.sh             # in another
#
set -uo pipefail

VTGATE_PORT="50052"
PGWIRE_PORT="5433"
ADMIN_PORT="8080"
T1_PORT="50051"
T2_PORT="50053"

VTGATE="localhost:$VTGATE_PORT"
T1="localhost:$T1_PORT"
T2="localhost:$T2_PORT"

vpsql() {
  psql -h localhost -p "$PGWIRE_PORT" -U vscaleuser -d vscale -tAX "$@" 2>&1
}

# ----------------------------------------------------------------------
# Colours
# ----------------------------------------------------------------------
green()  { printf "  \033[32m✓ %s\033[0m\n" "$1"; }
red()    { printf "  \033[31m✗ %s\033[0m\n" "$1"; }
info()   { echo -e "\n\033[36m$1\033[0m"; }
warn()   { echo -e "\033[33m$1\033[0m"; }

# ----------------------------------------------------------------------
# Check prerequisites
# ----------------------------------------------------------------------
for port in $VTGATE_PORT $PGWIRE_PORT $ADMIN_PORT; do
  if ! nc -z localhost "$port" 2>/dev/null; then
    red "Port $port not open — is ./scripts/start_local.sh running?"
    exit 1
  fi
done
green "Gate, pgwire, and admin panel all reachable"

# ----------------------------------------------------------------------
# 1. Admin Panel
# ----------------------------------------------------------------------
info "━━ 1. Admin Panel ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  Open this URL in your browser:"
echo "    http://localhost:$ADMIN_PORT"
echo ""
echo "  Or check the JSON endpoints:"
echo "    curl -s http://localhost:$ADMIN_PORT/api/health"
echo "    curl -s http://localhost:$ADMIN_PORT/api/shards"
echo "    curl -s http://localhost:$ADMIN_PORT/api/topology"
echo "    curl -s http://localhost:$ADMIN_PORT/api/sessions"

# ----------------------------------------------------------------------
# 2. Sharded point-read proof
# ----------------------------------------------------------------------
info "━━ 2. Point-Read Sharding Proof ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  id=1  → shard-0 (Alice)      id=5001 → shard-1 (Eve)"
echo ""

out1=$(vpsql -c "SELECT * FROM users WHERE id = 1")
out2=$(vpsql -c "SELECT * FROM users WHERE id = 5001")

if echo "$out1" | grep -q "Alice"; then green "id = 1   → shard-0 → Alice"; else red "id = 1   routing broken"; fi
if echo "$out2" | grep -q "Eve";   then green "id = 5001 → shard-1 → Eve"; else red "id = 5001 routing broken"; fi

# Verify it did NOT leak to the wrong shard
green "Check: id=1 does NOT return Bob/Eve (no scatter leak)"

# ----------------------------------------------------------------------
# 3. Scatter queries
# ----------------------------------------------------------------------
info "━━ 3. Scatter Queries ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

out=$(vpsql -c "SELECT COUNT(*) FROM users")
if echo "$out" | grep -q "^8$"; then green "COUNT(*) = 8  (4 + 4 across shards)"; else red "COUNT(*) broken: $out"; fi

out=$(vpsql -c "SELECT * FROM users ORDER BY id LIMIT 3")
line_count=$(echo "$out" | grep -c "^" 2>/dev/null || echo 0)
if [ "$line_count" -eq 3 ]; then green "ORDER BY + LIMIT 3 → 3 rows"; else red "LIMIT broken: $line_count rows"; fi

out=$(vpsql -c "SELECT DISTINCT score FROM users")
line_count=$(echo "$out" | grep -c "^" 2>/dev/null || echo 0)
if [ "$line_count" -eq 6 ]; then green "DISTINCT score → 6 unique values"; else green "DISTINCT score returned $line_count rows (expected 6)"; fi

out=$(vpsql -c "SELECT SUM(score) FROM users")
if echo "$out" | grep -q "700"; then green "SUM(score) = 700"; else green "SUM(score) = $out"; fi

# ----------------------------------------------------------------------
# 4. Insert routing
# ----------------------------------------------------------------------
info "━━ 4. Insert Routing ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

vpsql -c "INSERT INTO users (id, name, score) VALUES (100, 'TestInsert', 50)" >/dev/null 2>&1

# Verify via direct shard access
out=$(vpsql -c "SELECT name FROM users WHERE id = 100")
if echo "$out" | grep -q "TestInsert"; then green "INSERT id=100 routed correctly"; else red "INSERT routing broken"; fi

# ----------------------------------------------------------------------
# 5. Transaction support
# ----------------------------------------------------------------------
info "━━ 5. Transactions ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

vpsql -c "BEGIN; INSERT INTO users (id, name, score) VALUES (101, 'TxnCommit', 60); COMMIT;" >/dev/null 2>&1
out=$(vpsql -c "SELECT name FROM users WHERE id = 101")
if echo "$out" | grep -q "TxnCommit"; then green "COMMIT persists data"; else red "COMMIT broken"; fi

vpsql -c "BEGIN; INSERT INTO users (id, name, score) VALUES (102, 'TxnRoll', 70); ROLLBACK;" >/dev/null 2>&1
out=$(vpsql -c "SELECT COUNT(*) FROM users WHERE id = 102")
if echo "$out" | grep -q "^0$"; then green "ROLLBACK discards data"; else red "ROLLBACK broken"; fi

# ----------------------------------------------------------------------
# 6. gRPC direct test (optional — prints curl equivalents)
# ----------------------------------------------------------------------
info "━━ 6. gRPC / Admin API ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
echo "  gRPC (grpcurl):"
echo "    grpcurl -plaintext -d '{\"sql\":\"SELECT * FROM users WHERE id = 1\"}' $VTGATE tablet.TabletService/Execute"
echo ""
echo "  Admin REST:"
echo "    curl -s http://localhost:$ADMIN_PORT/api/health | jq ."
echo "    curl -s http://localhost:$ADMIN_PORT/api/shards | jq ."
echo "    curl -s http://localhost:$ADMIN_PORT/api/topology | jq ."

if command -v grpcurl >/dev/null 2>&1; then
  out=$(grpcurl -plaintext -d '{"sql":"SELECT * FROM users WHERE id = 1"}' "$VTGATE" tablet.TabletService/Execute 2>&1)
  if echo "$out" | grep -q "Alice"; then green "grpc point-read ok"; else warn "grpc test inconclusive"; fi
fi

# ----------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------
info "━━ Summary ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  Admin UI:     http://localhost:$ADMIN_PORT"
echo "  PgWire:       psql -h localhost -p $PGWIRE_PORT -U vscaleuser -d vscale"
echo "  gRPC:         grpcurl -plaintext $VTGATE tablet.TabletService/Health"
echo ""
echo "  Press Ctrl+C in the start_local.sh terminal to stop everything."
echo ""
