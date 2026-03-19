# ClusterLite

SQLite over RPC for distributed Elixir/Phoenix applications.

ClusterLite is an Ecto adapter that lets Phoenix apps use SQLite databases hosted on remote nodes in a cluster. SQLite NIF references are local to a BEAM instance and can't cross node boundaries — ClusterLite solves this by running a GenServer on the remote node that wraps an Exqlite DBConnection pool, with a local DBConnection adapter that proxies operations via `:rpc.call/4`.

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
│                         │                │ DBConnection/Exqlite     │
└─────────────────────────┘                └──────────────────────────┘
```

## How it works

- **Remote side**: A `DbServer` GenServer wraps an Exqlite DBConnection pool (pool_size: 1). All queries go through this GenServer, which serializes access and keeps NIF references contained. The entire RPC API is 4 functions: `start_db`, `stop_db`, `ping`, `query`.
- **Local side**: A `DBConnection` adapter translates Ecto operations into RPC `query` calls. One local connection = one remote GenServer = correct for SQLite's single-writer model.
- **SQL generation**: Delegates entirely to `ecto_sqlite3`'s Connection module, reusing its tested SQLite dialect code.
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

- **Wraps Exqlite, doesn't reimplement it** — the remote DbServer delegates to a real Exqlite DBConnection pool for pragma application, statement lifecycle, and result building
- **Single GenServer per database** — SQLite is single-writer; serializing through one GenServer matches its semantics
- **pool_size: 1 default** — one DBConnection locally maps to one remote GenServer, correct for SQLite
- **All RPC via MFA tuples** — safe for distributed Erlang, no closures cross the wire
- **No auto-reconnect** — on remote node disconnect, the connection errors; caller must restart the repo
