# VScale

A lightweight, PostgreSQL-wire-compatible sharding proxy. VScale sits between your application and your database shards, routing queries automatically while presenting a single connection endpoint.

## What It Does

- **Transparent Sharding** — Routes SQL to the right backend shard based on table and key-range configuration.
- **PostgreSQL Wire Protocol** — Connect with any Postgres driver or client. No application changes needed.
- **Automatic Read/Write Splitting** — Sends writes to primaries, distributes reads across replicas.
- **Distributed Transactions** — Coordinates multi-shard transactions with a two-phase commit protocol.
- **Scatter Queries** — Automatically broadcasts unshardable queries across all shards with optional aggregation.
- **Real-Time Topology** — Watches etcd for live tablet additions, removals, or failovers.
- **Admin Panel** — Built-in web dashboard on the gate for live observability of shards, sessions, and topology.

## Architecture

```
┌─────────────┐      ┌─────────┐      ┌─────────────────┐
│ Application │──────▶│  Gate   │──────▶│ Shard (Primary) │
│   (psql/libpq)      │(vtgate) │      └─────────────────┘
└─────────────┘      └─────────┘      ┌─────────────────┐
          pgwire/tcp   etcd watch      │ Shard (Replica) │
                       discovery       └─────────────────┘
```

- **Gate** — The entry point. Accepts PostgreSQL wire connections and gRPC, routes queries, and hosts the admin panel.
- **Tablet** — A backend PostgreSQL process managed as part of a shard. Can be PRIMARY or REPLICA.
- **Shard** — A logical grouping of a primary tablet and its replicas, responsible for a contiguous key range.
- **VSchema** — A JSON file declaring which tables are sharded and by which column.
- **etcd** — The source of truth for tablet topology.

## Quick Start

```bash
# Terminal 1 — start the full local cluster
cd scripts && ./start_local.sh

# Terminal 2 — run the feature walkthrough
./try_it.sh
```

`start_local.sh` boots everything: etcd, PostgreSQL databases, two shards with a PRIMARY each, and the gate.

`try_it.sh` verifies sharded point-reads, scatter queries, aggregations, insert routing, transactions, and the admin panel.

After that, open your browser at the port printed by the script (default `8080`).

### Admin Panel

The gate exposes a minimal dark-themed web UI that auto-refreshes every 5 seconds:

| Section    | What You See                                           |
|------------|--------------------------------------------------------|
| Overview   | Shards, tablets, sessions, goroutines, memory          |
| Shards     | Per-shard key ranges, primary and replica addresses    |
| Topology   | All registered tablets with type, cell, and range      |
| Sessions   | Active sessions and their participating shards         |
| VSchema    | Keyspace and sharded table definitions                 |
| Gateway    | Internal component status                              |

### Manual commands

```bash
# PostgreSQL wire
psql -h localhost -p 5433 -U vscaleuser -d vscale -c "SELECT COUNT(*) FROM users"

# gRPC
grpcurl -plaintext -d '{"sql":"SELECT * FROM users WHERE id = 1"}' localhost:50052 tablet.TabletService/Execute

# Admin REST
curl -s http://localhost:8080/api/health | jq .
curl -s http://localhost:8080/api/shards | jq .
curl -s http://localhost:8080/api/topology | jq .
```

## Admin Panel

The gate exposes a minimal web UI at its admin port (`ADMIN_PORT`, default `8080`) for live cluster insight. It auto-refreshes every 5 seconds and shows:

| Section    | What You See                                           |
|------------|--------------------------------------------------------|
| Overview   | Shards, tablets, sessions, goroutines, memory          |
| Shards     | Per-shard key ranges, primary and replica addresses    |
| Topology   | All registered tablets with type, cell, and range      |
| Sessions   | Active sessions and their participating shards         |
| VSchema    | Keyspace and sharded table definitions                 |
| Gateway    | Internal component status                              |

## Configuration

| Variable        | Purpose                              | Default              |
|-----------------|--------------------------------------|----------------------|
| `VTGATE_PORT`   | gRPC listen port for the gate        | required             |
| `PGWIRE_PORT`   | PostgreSQL wire protocol port        | `5433`               |
| `ADMIN_PORT`    | Admin web panel HTTP port            | `8080`               |
| `ETCD_ENDPOINTS`| Comma-separated etcd hosts           | required             |
| `ETCD_PREFIX`   | etcd key prefix for tablet records   | `/vscale/tablets/`   |
| `VSCHEMA_PATH`  | Path to vschema JSON                 | `./vschema.json`     |

## Design Notes

- The gate keeps no persistent state of its own; all configuration comes from etcd and the vschema file.
- Query routing is statement-level. The parser inspects each incoming query to determine target shards.
- Cross-shard transactions use a lightweight coordinator that tracks session-to-shard mappings.
- Scatter queries strip `ORDER BY`, `LIMIT`, and aggregation functions before fan-out, then merge results on the gate.
