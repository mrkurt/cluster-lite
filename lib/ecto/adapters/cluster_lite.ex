defmodule Ecto.Adapters.ClusterLite do
  @moduledoc """
  Ecto adapter for ClusterLite (SQLite over RPC).

  Reuses ecto_sqlite3 for SQL generation, type handling, and DDL.
  Only swaps the DBConnection backend to `ClusterLite.Connection`,
  which proxies queries to a remote node via `:rpc.call/4`.

  ## Configuration

      config :my_app, MyApp.Repo,
        database: "/data/my_app.db",
        cluster_lite_node: :"storage@10.0.0.1",
        journal_mode: :wal,
        pool_size: 1
  """

  use Ecto.Adapters.SQL,
    driver: :cluster_lite

  @sqlite3 Ecto.Adapters.SQLite3

  @impl Ecto.Adapter
  def ensure_all_started(_config, _type), do: {:ok, [:cluster_lite]}

  # Storage — needs RPC to the remote node
  def storage_up(opts) do
    database = Keyword.fetch!(opts, :database)
    node = Keyword.get(opts, :cluster_lite_node, node())

    case rpc(node, File, :exists?, [database]) do
      true ->
        {:error, :already_up}

      false ->
        rpc(node, File, :mkdir_p!, [Path.dirname(database)])

        case rpc(node, ClusterLite.Remote.RpcApi, :start_db, [database, opts]) do
          {:ok, pid} ->
            rpc(node, ClusterLite.Remote.RpcApi, :stop_db, [pid])
            :ok

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  def storage_down(opts) do
    database = Keyword.fetch!(opts, :database)
    node = Keyword.get(opts, :cluster_lite_node, node())

    case rpc(node, File, :exists?, [database]) do
      true ->
        for ext <- ["", "-wal", "-shm"], do: rpc(node, File, :rm, [database <> ext])
        :ok

      false ->
        {:error, :already_down}
    end
  end

  def storage_status(opts) do
    database = Keyword.fetch!(opts, :database)
    node = Keyword.get(opts, :cluster_lite_node, node())
    if rpc(node, File, :exists?, [database]), do: :up, else: :down
  end

  # Type handling — delegate to ecto_sqlite3
  @impl Ecto.Adapter
  defdelegate loaders(primitive, type), to: @sqlite3

  @impl Ecto.Adapter
  defdelegate dumpers(primitive, type), to: @sqlite3

  @impl Ecto.Adapter.Schema
  defdelegate autogenerate(type), to: @sqlite3

  @impl Ecto.Adapter.Migration
  defdelegate supports_ddl_transaction?, to: @sqlite3

  @impl Ecto.Adapter.Migration
  def lock_for_migrations(_meta, _options, fun), do: fun.()

  defp rpc(node, mod, fun, args) do
    if node == node() do
      apply(mod, fun, args)
    else
      case :rpc.call(node, mod, fun, args) do
        {:badrpc, reason} -> {:error, reason}
        result -> result
      end
    end
  end
end
