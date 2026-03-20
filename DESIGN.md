# ClusterLite Design

## Problem

SQLite NIF references are local to a BEAM instance. You can't pass them across node boundaries in distributed Erlang. If your SQLite files live on a storage node but your Phoenix app runs elsewhere, you need a way to proxy operations.

## Solution

A thin RPC bridge: a GenServer on the remote node owns the real Exqlite connections, and a DBConnection adapter on the local node proxies operations to it via `:rpc.call/4` using MFA tuples only.

## Architecture

```
Local Node                                  Remote Node
┌─────────────────────────┐                ┌──────────────────────────┐
│ Ecto.Repo               │                │ ClusterLite.Remote       │
│   ↓                     │                │   .DbServer (GenServer)  │
│ Ecto.Adapters.ClusterLite│               │     manages:             │
│   ↓                     │                │     conn_id=1 → pool PID │
│ ClusterLite.Connection  │── :rpc.call ──→│     conn_id=2 → pool PID │
│  (DBConnection impl)    │   (MFA only)   │     conn_id=N → pool PID │
│                         │                │       ↓                  │
│                         │                │     Exqlite.Connection   │
│                         │                │       (DBConnection)     │
└─────────────────────────┘                └──────────────────────────┘
```

## Module Inventory (630 lines total)

| Module | Lines | Role |
|--------|-------|------|
| `ClusterLite.Connection` | 176 | DBConnection impl, proxies to remote via RPC |
| `ClusterLite.Remote.DbServer` | 108 | GenServer managing conn_id → Exqlite pool map |
| `Ecto.Adapters.ClusterLite` | 95 | Ecto adapter, delegates types to ecto_sqlite3 |
| `Ecto.Adapters.ClusterLite.Connection` | 70 | SQL.Connection, delegates everything except child_spec |
| `ClusterLite.DynamicRepos` | 59 | DynamicSupervisor for multi-tenant repos |
| `ClusterLite` | 41 | Top-level API (query, prepare, execute, transaction) |
| `ClusterLite.Remote.RpcApi` | 31 | 5 MFA-safe functions for :rpc.call |
| `ClusterLite.Application` | 28 | OTP app, starts remote/local supervisors |
| `ClusterLite.Remote.Supervisor` | 22 | Supervises DynamicSupervisor for DbServers |

## Key Design Decisions

**Wrap exqlite, don't reimplement it.** The DbServer wraps a real `DBConnection` pool backed by `Exqlite.Connection`. Exqlite handles pragma application, statement lifecycle, NIF management, and `num_rows` via `changes()`. We just serialize the results across the wire.

**Reuse ecto_sqlite3's structs and adapter.** We use `Exqlite.Query`, `Exqlite.Result`, and `Exqlite.Error` directly — no custom structs. The Ecto SQL.Connection module delegates all SQL generation, DDL, `to_constraints`, and `explain_query` to `Ecto.Adapters.SQLite3.Connection`. The adapter delegates `loaders/dumpers/autogenerate` to `Ecto.Adapters.SQLite3`. The only override is `child_spec/1`, which swaps in our DBConnection backend.

**conn_id for multiple connections.** Each local `ClusterLite.Connection` gets a `conn_id` on the remote DbServer, mapping to its own dedicated pool_size:1 Exqlite pool. This enables concurrent WAL reads while keeping transaction state isolated per connection. The DbServer serializes the `open_conn`/`close_conn`/`query` calls but each query runs against its own independent pool.

**All RPC via MFA tuples.** The RPC API is 5 functions: `start_db`, `stop_db`, `open_conn`, `close_conn`, `query`. No closures or NIF references cross the wire. Arguments and return values are atoms, integers, strings, and lists.

**Command detection for num_rows.** Exqlite uses `Sqlite3.changes()` for write commands but needs the `command` option set. DbServer detects the command from the SQL prefix (SELECT/INSERT/UPDATE/DELETE) and passes it through so exqlite returns the correct affected row count.

## Evolution

### v1: Manual NIF management (1265 lines)
The initial implementation had `DbServer` directly calling `Exqlite.Sqlite3` NIF functions — `open`, `prepare`, `bind`, `step`, `release`, `close`. It managed a `%{stmt_id => nif_ref}` map for prepared statements, cursor state for streaming, manual pragma application, and its own transaction tracking. Custom `ClusterLite.Query`, `ClusterLite.Error`, and `ClusterLite.Result` structs. The Ecto adapter reimplemented loaders/dumpers/to_constraints from ecto_sqlite3.

### v2: Wrap Exqlite DBConnection (875 lines, -390)
Replaced all manual NIF management with a single `DBConnection.start_link(Exqlite.Connection, opts)` inside the DbServer. Exqlite handles pragma application, statement lifecycle, and result building. The RPC API went from 15+ functions to 4. Eliminated ~360 lines of duplicated logic.

### v3: Reuse exqlite structs (595 lines, -280)
Deleted `ClusterLite.Query`, `ClusterLite.Error`, `ClusterLite.Result` — use `Exqlite.Query/Result/Error` directly. Delegated loaders/dumpers/autogenerate/to_constraints/explain_query to ecto_sqlite3. The SQL.Connection module became almost entirely `defdelegate` calls.

### v4: conn_id for multiple connections (630 lines, +35)
DbServer now manages a `%{conn_id => pool_pid}` map instead of owning a single pool. Each local connection gets its own dedicated remote pool, enabling concurrent reads in WAL mode while keeping transaction state isolated.
