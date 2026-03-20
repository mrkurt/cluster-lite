defmodule ClusterLite.Remote.DbServer do
  @moduledoc """
  GenServer that manages multiple Exqlite DBConnection pools for a
  single database path on the remote node.

  Each local ClusterLite.Connection gets its own `conn_id` mapping to
  a dedicated pool_size:1 DBConnection. This allows concurrent reads
  in WAL mode while keeping each connection's transaction state isolated.
  """

  use GenServer

  defstruct [:path, :db_opts, conns: %{}, next_id: 1]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    config = Keyword.get(opts, :config, [])

    db_opts = [
      database: path,
      journal_mode: Keyword.get(config, :journal_mode, :wal),
      busy_timeout: Keyword.get(config, :busy_timeout, 2000),
      cache_size: Keyword.get(config, :cache_size, -64000),
      foreign_keys: if(Keyword.get(config, :foreign_keys, true), do: :on, else: :off),
      pool_size: 1
    ]

    {:ok, %__MODULE__{path: path, db_opts: db_opts}}
  end

  @impl true
  def terminate(_reason, state) do
    for {_id, conn} <- state.conns do
      GenServer.stop(conn)
    end
  catch
    :exit, _ -> :ok
  end

  @impl true
  def handle_call(:open_conn, _from, state) do
    case DBConnection.start_link(Exqlite.Connection, state.db_opts) do
      {:ok, conn} ->
        id = state.next_id
        conns = Map.put(state.conns, id, conn)
        {:reply, {:ok, id}, %{state | conns: conns, next_id: id + 1}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:close_conn, conn_id}, _from, state) do
    case Map.pop(state.conns, conn_id) do
      {nil, _} ->
        {:reply, :ok, state}

      {conn, conns} ->
        GenServer.stop(conn)
        {:reply, :ok, %{state | conns: conns}}
    end
  catch
    :exit, _ -> {:reply, :ok, %{state | conns: Map.delete(state.conns, conn_id)}}
  end

  def handle_call(:ping, _from, state) do
    {:reply, :ok, state}
  end

  def handle_call({:query, conn_id, sql, params}, _from, state) do
    case Map.fetch(state.conns, conn_id) do
      {:ok, conn} ->
        command = command_from_sql(sql)
        query = %Exqlite.Query{statement: sql, command: command}

        case DBConnection.prepare_execute(conn, query, params, command: command) do
          {:ok, _query, %Exqlite.Result{} = result} ->
            {:reply, {:ok, {result.columns, result.rows, result.num_rows}}, state}

          {:error, err} ->
            {:reply, {:error, format_error(err)}, state}
        end

      :error ->
        {:reply, {:error, "unknown conn_id: #{conn_id}"}, state}
    end
  end

  defp command_from_sql(sql) do
    case sql |> String.trim_leading() |> String.split(~r/\s/, parts: 2) |> hd() |> String.downcase() do
      "select" -> :select
      "insert" -> :insert
      "update" -> :update
      "delete" -> :delete
      "with" -> :select
      _ -> :execute
    end
  end

  defp format_error(%Exqlite.Error{message: msg}), do: msg
  defp format_error(%DBConnection.ConnectionError{message: msg}), do: msg
  defp format_error(other), do: inspect(other)
end
