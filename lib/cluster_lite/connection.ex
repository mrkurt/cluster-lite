defmodule ClusterLite.Connection do
  @moduledoc """
  DBConnection implementation that proxies operations to a remote
  ClusterLite.Remote.DbServer via `:rpc.call/4`.

  Accepts `Exqlite.Query` structs and returns `Exqlite.Result` structs,
  so ecto_sqlite3's SQL.Connection module can drive it directly.
  """

  @behaviour DBConnection

  alias ClusterLite.Remote.RpcApi

  defstruct [
    :node,
    :pid,
    :path,
    :monitor_ref,
    :config,
    transaction_status: :idle
  ]

  @rpc_timeout 15_000

  @impl true
  def connect(opts) do
    node = Keyword.get(opts, :cluster_lite_node, node())
    path = Keyword.fetch!(opts, :database)

    case rpc_call(node, RpcApi, :start_db, [path, opts]) do
      {:ok, pid} ->
        ref = Process.monitor(pid)

        state = %__MODULE__{
          node: node,
          pid: pid,
          path: path,
          monitor_ref: ref,
          config: opts,
          transaction_status: :idle
        }

        {:ok, state}

      {:error, reason} ->
        {:error, %Exqlite.Error{message: "failed to start remote db: #{inspect(reason)}"}}
    end
  end

  @impl true
  def disconnect(_err, state) do
    Process.demonitor(state.monitor_ref, [:flush])
    rpc_call(state.node, RpcApi, :stop_db, [state.pid])
    :ok
  end

  @impl true
  def checkout(state), do: {:ok, state}

  @impl true
  def ping(state) do
    case rpc_call(state.node, RpcApi, :ping, [state.pid]) do
      :ok -> {:ok, state}
      {:error, reason} -> {:disconnect, %Exqlite.Error{message: inspect(reason)}, state}
    end
  end

  @impl true
  def handle_prepare(%{statement: sql} = query, _opts, state) do
    sql = to_sql_string(sql)
    {:ok, %{query | statement: sql}, state}
  end

  @impl true
  def handle_execute(%{statement: sql} = query, params, _opts, state) do
    case rpc_call(state.node, RpcApi, :query, [state.pid, sql, params]) do
      {:ok, {columns, rows, num_rows}} ->
        result = %Exqlite.Result{
          columns: columns,
          rows: rows,
          num_rows: num_rows
        }

        {:ok, query, result, state}

      {:error, reason} ->
        {:error, %Exqlite.Error{message: to_string(reason), statement: sql}, state}
    end
  end

  @impl true
  def handle_close(_query, _opts, state), do: {:ok, nil, state}

  @impl true
  def handle_begin(_opts, state) do
    mode =
      case Keyword.get(state.config, :transaction_mode, :deferred) do
        :deferred -> "BEGIN DEFERRED"
        :immediate -> "BEGIN IMMEDIATE"
        :exclusive -> "BEGIN EXCLUSIVE"
        _ -> "BEGIN"
      end

    case rpc_call(state.node, RpcApi, :query, [state.pid, mode, []]) do
      {:ok, _} -> {:ok, nil, %{state | transaction_status: :transaction}}
      {:error, reason} -> {:error, %Exqlite.Error{message: to_string(reason)}, state}
    end
  end

  @impl true
  def handle_commit(_opts, state) do
    case rpc_call(state.node, RpcApi, :query, [state.pid, "COMMIT", []]) do
      {:ok, _} -> {:ok, nil, %{state | transaction_status: :idle}}
      {:error, reason} -> {:error, %Exqlite.Error{message: to_string(reason)}, state}
    end
  end

  @impl true
  def handle_rollback(_opts, state) do
    case rpc_call(state.node, RpcApi, :query, [state.pid, "ROLLBACK", []]) do
      {:ok, _} -> {:ok, nil, %{state | transaction_status: :idle}}
      {:error, reason} -> {:error, %Exqlite.Error{message: to_string(reason)}, state}
    end
  end

  @impl true
  def handle_status(_opts, state), do: {state.transaction_status, state}

  @impl true
  def handle_declare(%{statement: sql} = query, params, _opts, state) do
    {:ok, query, {sql, params}, state}
  end

  @impl true
  def handle_fetch(_query, {sql, params}, _opts, state) do
    case rpc_call(state.node, RpcApi, :query, [state.pid, sql, params]) do
      {:ok, {columns, rows, num_rows}} ->
        {:halt, %Exqlite.Result{columns: columns, rows: rows, num_rows: num_rows}, state}

      {:error, reason} ->
        {:error, %Exqlite.Error{message: to_string(reason)}, state}
    end
  end

  @impl true
  def handle_deallocate(_query, _cursor, _opts, state), do: {:ok, nil, state}

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{monitor_ref: ref} = state) do
    {:disconnect, %Exqlite.Error{message: "remote DbServer down: #{inspect(reason)}"}, state}
  end

  def handle_info(_msg, state), do: {:ok, state}

  defp rpc_call(node, mod, fun, args) do
    if node == node() do
      apply(mod, fun, args)
    else
      case :rpc.call(node, mod, fun, args, @rpc_timeout) do
        {:badrpc, reason} -> {:error, "RPC failed: #{inspect(reason)}"}
        result -> result
      end
    end
  end

  defp to_sql_string(sql) when is_binary(sql), do: sql
  defp to_sql_string(sql) when is_list(sql), do: IO.iodata_to_binary(sql)
end
