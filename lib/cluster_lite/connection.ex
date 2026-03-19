defmodule ClusterLite.Connection do
  @moduledoc """
  DBConnection implementation that proxies operations to a remote
  ClusterLite.Remote.DbServer via `:rpc.call/4`.
  """

  @behaviour DBConnection

  alias ClusterLite.{Error, Query, Result}
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

  # -------------------------------------------------------------------
  # DBConnection callbacks
  # -------------------------------------------------------------------

  @impl true
  def connect(opts) do
    node = Keyword.get(opts, :cluster_lite_node, node())
    path = Keyword.fetch!(opts, :database)
    config = opts

    case rpc_call(node, RpcApi, :start_db, [path, config]) do
      {:ok, pid} ->
        ref = Process.monitor(pid)

        state = %__MODULE__{
          node: node,
          pid: pid,
          path: path,
          monitor_ref: ref,
          config: config,
          transaction_status: :idle
        }

        {:ok, state}

      {:error, reason} ->
        {:error, %Error{message: "failed to start remote db: #{inspect(reason)}"}}
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
      {:error, reason} -> {:disconnect, %Error{message: inspect(reason)}, state}
    end
  end

  @impl true
  def handle_prepare(%Query{statement: sql} = query, _opts, state) do
    sql = to_sql_string(sql)
    query = %{query | statement: sql}

    case rpc_call(state.node, RpcApi, :prepare, [state.pid, sql]) do
      {:ok, stmt_id} ->
        command = Query.command_from_sql(sql)
        {:ok, %{query | ref: stmt_id, command: command}, state}

      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: sql}, state}
    end
  end

  @impl true
  def handle_execute(%Query{ref: nil, statement: sql} = query, params, _opts, state) do
    sql = to_sql_string(sql)
    command = Query.command_from_sql(sql)

    case rpc_call(state.node, RpcApi, :prepare_and_execute, [state.pid, sql, params]) do
      {:ok, stmt_id, columns, rows, changes} ->
        result = %Result{
          command: command,
          columns: columns,
          rows: normalize_rows(columns, rows),
          num_rows: num_rows(command, rows, changes)
        }

        {:ok, %{query | ref: stmt_id, command: command}, result, state}

      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: sql}, state}
    end
  end

  def handle_execute(%Query{ref: stmt_id, statement: sql} = query, params, _opts, state) do
    command = query.command || Query.command_from_sql(sql)

    case rpc_call(state.node, RpcApi, :execute, [state.pid, stmt_id, params]) do
      {:ok, columns, rows, changes} ->
        result = %Result{
          command: command,
          columns: columns,
          rows: normalize_rows(columns, rows),
          num_rows: num_rows(command, rows, changes)
        }

        {:ok, query, result, state}

      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: sql}, state}
    end
  end

  @impl true
  def handle_close(%Query{ref: nil}, _opts, state) do
    {:ok, nil, state}
  end

  def handle_close(%Query{ref: stmt_id}, _opts, state) do
    rpc_call(state.node, RpcApi, :close_statement, [state.pid, stmt_id])
    {:ok, nil, state}
  end

  @impl true
  def handle_begin(_opts, state) do
    mode = Keyword.get(state.config, :transaction_mode, :deferred)

    case rpc_call(state.node, RpcApi, :begin_transaction, [state.pid, mode]) do
      :ok ->
        {:ok, nil, %{state | transaction_status: :transaction}}

      {:error, reason} ->
        {:error, %Error{message: to_string(reason)}, state}
    end
  end

  @impl true
  def handle_commit(_opts, state) do
    case rpc_call(state.node, RpcApi, :commit, [state.pid]) do
      :ok ->
        {:ok, nil, %{state | transaction_status: :idle}}

      {:error, reason} ->
        {:error, %Error{message: to_string(reason)}, state}
    end
  end

  @impl true
  def handle_rollback(_opts, state) do
    case rpc_call(state.node, RpcApi, :rollback, [state.pid]) do
      :ok ->
        {:ok, nil, %{state | transaction_status: :idle}}

      {:error, reason} ->
        {:error, %Error{message: to_string(reason)}, state}
    end
  end

  @impl true
  def handle_status(_opts, state) do
    {state.transaction_status, state}
  end

  @impl true
  def handle_declare(%Query{statement: sql} = query, params, opts, state) do
    max_rows = Keyword.get(opts, :max_rows, 500)

    case rpc_call(state.node, RpcApi, :declare_cursor, [state.pid, sql, params, max_rows]) do
      {:ok, cursor_id} ->
        {:ok, query, cursor_id, state}

      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: sql}, state}
    end
  end

  @impl true
  def handle_fetch(_query, cursor_id, opts, state) do
    max_rows = Keyword.get(opts, :max_rows)

    case rpc_call(state.node, RpcApi, :fetch_cursor, [state.pid, cursor_id, max_rows]) do
      {:ok, columns, rows, :halt} ->
        result = %Result{columns: columns, rows: rows, num_rows: length(rows)}
        {:halt, result, state}

      {:ok, columns, rows, :cont} ->
        result = %Result{columns: columns, rows: rows, num_rows: length(rows)}
        {:cont, result, state}

      {:error, reason} ->
        {:error, %Error{message: to_string(reason)}, state}
    end
  end

  @impl true
  def handle_deallocate(_query, cursor_id, _opts, state) do
    rpc_call(state.node, RpcApi, :deallocate_cursor, [state.pid, cursor_id])
    {:ok, nil, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{monitor_ref: ref} = state) do
    {:disconnect, %Error{message: "remote DbServer down: #{inspect(reason)}"}, state}
  end

  def handle_info(_msg, state) do
    {:ok, state}
  end

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  defp rpc_call(node, mod, fun, args) do
    if node == node() do
      apply(mod, fun, args)
    else
      case :rpc.call(node, mod, fun, args, @rpc_timeout) do
        {:badrpc, reason} ->
          {:error, "RPC failed: #{inspect(reason)}"}

        result ->
          result
      end
    end
  end

  defp to_sql_string(sql) when is_binary(sql), do: sql
  defp to_sql_string(sql) when is_list(sql), do: IO.iodata_to_binary(sql)

  defp num_rows(command, _rows, changes)
       when command in [:insert, :update, :delete],
       do: changes

  defp num_rows(_command, rows, _changes), do: length(rows)

  defp normalize_rows([], []), do: nil
  defp normalize_rows(_columns, rows), do: rows
end
