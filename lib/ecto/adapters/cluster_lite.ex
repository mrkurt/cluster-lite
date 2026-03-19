defmodule Ecto.Adapters.ClusterLite do
  @moduledoc """
  Ecto adapter for ClusterLite (SQLite over RPC).

  Uses `Ecto.Adapters.SQL` with ClusterLite as the driver, delegating
  SQL generation to ecto_sqlite3's Connection module and type handling
  to ecto_sqlite3's Codec.

  ## Configuration

      config :my_app, MyApp.Repo,
        database: "/data/my_app.db",
        cluster_lite_node: :"storage@10.0.0.1",
        journal_mode: :wal,
        pool_size: 1
  """

  use Ecto.Adapters.SQL,
    driver: :cluster_lite

  alias Ecto.Adapters.SQLite3.Codec

  @impl Ecto.Adapter
  def ensure_all_started(_config, _type) do
    {:ok, [:cluster_lite]}
  end

  # -------------------------------------------------------------------
  # Storage callbacks (not part of a behaviour, called by Mix tasks)
  # -------------------------------------------------------------------

  def storage_up(opts) do
    database = Keyword.fetch!(opts, :database)
    node = Keyword.get(opts, :cluster_lite_node, node())

    case rpc(node, File, :exists?, [database]) do
      true ->
        {:error, :already_up}

      false ->
        dir = Path.dirname(database)
        rpc(node, File, :mkdir_p!, [dir])

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
        rpc(node, File, :rm, [database])
        rpc(node, File, :rm, ["#{database}-wal"])
        rpc(node, File, :rm, ["#{database}-shm"])
        :ok

      false ->
        {:error, :already_down}
    end
  end

  def storage_status(opts) do
    database = Keyword.fetch!(opts, :database)
    node = Keyword.get(opts, :cluster_lite_node, node())

    case rpc(node, File, :exists?, [database]) do
      true -> :up
      false -> :down
    end
  end

  # -------------------------------------------------------------------
  # Type handling (reuse ecto_sqlite3 Codec)
  # -------------------------------------------------------------------

  @impl Ecto.Adapter
  def loaders(:boolean, type), do: [&Codec.bool_decode/1, type]
  def loaders(:naive_datetime_usec, type), do: [&Codec.naive_datetime_decode/1, type]
  def loaders(:time, type), do: [&Codec.time_decode/1, type]
  def loaders(:utc_datetime_usec, type), do: [&Codec.utc_datetime_decode/1, type]
  def loaders(:utc_datetime, type), do: [&Codec.utc_datetime_decode/1, type]
  def loaders(:naive_datetime, type), do: [&Codec.naive_datetime_decode/1, type]
  def loaders(:date, type), do: [&Codec.date_decode/1, type]
  def loaders({:map, _}, type), do: [&Codec.json_decode/1, &Ecto.Type.embedded_load(type, &1, :json)]
  def loaders({:array, _}, type), do: [&Codec.json_decode/1, type]
  def loaders(:map, type), do: [&Codec.json_decode/1, type]
  def loaders(:float, type), do: [&Codec.float_decode/1, type]
  def loaders(:decimal, type), do: [&Codec.decimal_decode/1, type]
  def loaders(:binary_id, type), do: [Ecto.UUID, type]
  def loaders(:uuid, type), do: [Ecto.UUID, type]
  def loaders(_primitive, type), do: [type]

  @dt_type :iso8601

  @impl Ecto.Adapter
  def dumpers(:binary, type), do: [type, &Codec.blob_encode/1]
  def dumpers(:boolean, type), do: [type, &Codec.bool_encode/1]
  def dumpers(:decimal, type), do: [type, &Codec.decimal_encode/1]
  def dumpers(:time, type), do: [type, &Codec.time_encode/1]
  def dumpers(:utc_datetime, type), do: [type, &Codec.utc_datetime_encode(&1, @dt_type)]
  def dumpers(:utc_datetime_usec, type), do: [type, &Codec.utc_datetime_encode(&1, @dt_type)]
  def dumpers(:naive_datetime, type), do: [type, &Codec.naive_datetime_encode(&1, @dt_type)]
  def dumpers(:naive_datetime_usec, type), do: [type, &Codec.naive_datetime_encode(&1, @dt_type)]
  def dumpers({:array, _}, type), do: [type, &Codec.json_encode/1]
  def dumpers({:map, _}, type), do: [&Ecto.Type.embedded_dump(type, &1, :json), &Codec.json_encode/1]
  def dumpers(:map, type), do: [type, &Codec.json_encode/1]
  def dumpers(_primitive, type), do: [type]

  # -------------------------------------------------------------------
  # Schema
  # -------------------------------------------------------------------

  @impl Ecto.Adapter.Schema
  def autogenerate(:id), do: nil
  def autogenerate(:embed_id), do: Ecto.UUID.generate()
  def autogenerate(:binary_id), do: Ecto.UUID.generate()

  @impl Ecto.Adapter.Migration
  def supports_ddl_transaction?, do: true

  @impl Ecto.Adapter.Migration
  def lock_for_migrations(_meta, _options, fun) do
    fun.()
  end

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

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
