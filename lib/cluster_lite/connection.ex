defmodule ClusterLite.Connection do
  @moduledoc """
  DBConnection implementation that proxies operations to a remote
  ClusterLite.Remote.DbServer via `:rpc.call/4`.

  Each local connection gets its own `conn_id` on the remote DbServer,
  mapping to a dedicated pool_size:1 Exqlite DBConnection. This allows
  multiple concurrent connections (concurrent WAL reads) while keeping
  transaction state isolated per connection.
  """

  @behaviour DBConnection

  alias ClusterLite.Remote.RpcApi

  defstruct [
    :node,
    :pid,
    :conn_id,
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

    with {:ok, pid} <- get_or_start_db(node, path, opts),
         {:ok, conn_id} <- rpc_call(node, RpcApi, :open_conn, [pid]) do
      ref = Process.monitor(pid)

      state = %__MODULE__{
        node: node,
        pid: pid,
        conn_id: conn_id,
        path: path,
        monitor_ref: ref,
        config: opts,
        transaction_status: :idle
      }

      {:ok, state}
    else
      {:error, reason} ->
        {:error, %Exqlite.Error{message: "failed to connect: #{inspect(reason)}"}}
    end
  end

  @impl true
  def disconnect(_err, state) do
    Process.demonitor(state.monitor_ref, [:flush])
    rpc_call(state.node, RpcApi, :close_conn, [state.pid, state.conn_id])
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
    case rpc_call(state.node, RpcApi, :query, [state.pid, state.conn_id, sql, params]) do
      {:ok, {columns, rows, num_rows}} ->
        result = %Exqlite.Result{columns: columns, rows: rows, num_rows: num_rows}
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

    case rpc_call(state.node, RpcApi, :query, [state.pid, state.conn_id, mode, []]) do
      {:ok, _} -> {:ok, nil, %{state | transaction_status: :transaction}}
      {:error, reason} -> {:error, %Exqlite.Error{message: to_string(reason)}, state}
    end
  end

  @impl true
  def handle_commit(_opts, state) do
    case rpc_call(state.node, RpcApi, :query, [state.pid, state.conn_id, "COMMIT", []]) do
      {:ok, _} -> {:ok, nil, %{state | transaction_status: :idle}}
      {:error, reason} -> {:error, %Exqlite.Error{message: to_string(reason)}, state}
    end
  end

  @impl true
  def handle_rollback(_opts, state) do
    case rpc_call(state.node, RpcApi, :query, [state.pid, state.conn_id, "ROLLBACK", []]) do
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
    case rpc_call(state.node, RpcApi, :query, [state.pid, state.conn_id, sql, params]) do
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

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  defp get_or_start_db(node, path, opts) do
    # TODO: Registry-based lookup to share DbServer across connections
    # to the same path. For now, each connection starts its own DbServer.
    rpc_call(node, RpcApi, :start_db, [path, opts])
  end

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
