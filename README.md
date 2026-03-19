# ClusterLite

SQLite over RPC for distributed Elixir/Phoenix applications.

ClusterLite is an Ecto adapter that lets Phoenix apps use SQLite databases hosted on remote nodes in a cluster. SQLite NIF references are local to a BEAM instance and can't cross node boundaries — ClusterLite solves this by running a GenServer on the remote node that owns the NIF references, with a local DBConnection adapter that proxies operations via `:rpc.call/4`.

## Architecture

```
Local Node (Phoenix app)                    Remote Node (SQLite files)
┌─────────────────────────┐                ┌──────────────────────────┐
│ Ecto.Repo (dynamic)     │                │                          │
│   ↓                     │                │ ClusterLite.Remote       │
│ Ecto.Adapters.ClusterLite│               │   .Supervisor            │
│   ↓                     │                │     ↓                    │
│ ClusterLite.Connection  │── :rpc.call ──→│ ClusterLite.Remote       │
│  (DBConnection)         │   (MFA only)   │   .RpcApi                │
│                         │                │     ↓                    │
│ ClusterLite.DynamicRepos│                │ ClusterLite.Remote       │
│  (manages many repos)   │                │   .DbServer (GenServer)  │
│                         │                │     ↓                    │
│                         │                │ Exqlite.Sqlite3 (NIFs)   │
└─────────────────────────┘                └──────────────────────────┘
```

## How it works

- **Remote side**: A `DbServer` GenServer opens the SQLite database via exqlite and owns all NIF references. Prepared statements get integer IDs so nothing non-serializable crosses the wire.
- **Local side**: A `DBConnection` adapter translates Ecto operations into RPC calls to the remote `DbServer`. One connection = one GenServer = correct for SQLite's single-writer model.
- **SQL generation**: Delegates entirely to `ecto_sqlite3`'s Connection module, reusing its 2000+ lines of tested SQLite dialect code.
- **Dynamic repos**: Create many SQLite databases on the fly, each with its own Ecto repo, potentially on different nodes.

## Usage

Define a repo:

```elixir
defmodule MyApp.Repo do
  use Ecto.Repo, otp_app: :my_app, adapter: Ecto.Adapters.ClusterLite
end
```

Configure it:

```elixir
config :my_app, MyApp.Repo,
  database: "/data/my_app.db",
  cluster_lite_node: :"storage@10.0.0.1",
  journal_mode: :wal,
  pool_size: 1
```

Use dynamic repos for multi-tenant databases:

```elixir
{:ok, repo_pid} = ClusterLite.DynamicRepos.start_repo(
  MyApp.Repo, :"storage@10.0.0.2", "/data/tenant_42.db"
)
MyApp.Repo.put_dynamic_repo(repo_pid)
MyApp.Repo.all(MyApp.User)
```

Works locally too — when `cluster_lite_node` is omitted or set to `node()`, RPC calls go to the same node, so everything works transparently in dev/test.

## Design decisions

- **Single GenServer per database** — SQLite is single-writer; serializing through one GenServer matches its semantics
- **Integer stmt_ids instead of NIF refs** — prepared statements get integer IDs on the remote side; only serializable data crosses node boundaries
- **pool_size: 1 default** — one DBConnection maps to one remote GenServer, correct for SQLite
- **All RPC via MFA tuples** — safe for distributed Erlang, no closures cross the wire
- **No auto-reconnect** — on remote node disconnect, the connection errors; caller must restart the repo
