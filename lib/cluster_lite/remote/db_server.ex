defmodule ClusterLite.Remote.DbServer do
  @moduledoc """
  GenServer that owns an Exqlite DBConnection pool on the remote node.

  All NIF references stay inside the pool process. This GenServer
  serializes access and ensures transaction consistency (all calls
  from the same process so DBConnection routing works correctly).
  """

  use GenServer

  defstruct [:conn, :path]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    config = Keyword.get(opts, :config, [])

    db_opts =
      [
        database: path,
        journal_mode: Keyword.get(config, :journal_mode, :wal),
        busy_timeout: Keyword.get(config, :busy_timeout, 2000),
        cache_size: Keyword.get(config, :cache_size, -64000),
        foreign_keys: if(Keyword.get(config, :foreign_keys, true), do: :on, else: :off),
        pool_size: 1
      ]

    case DBConnection.start_link(Exqlite.Connection, db_opts) do
      {:ok, conn} ->
        {:ok, %__MODULE__{conn: conn, path: path}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def terminate(_reason, %__MODULE__{conn: conn}) do
    GenServer.stop(conn)
  catch
    :exit, _ -> :ok
  end

  @impl true
  def handle_call(:ping, _from, state) do
    {:reply, :ok, state}
  end

  def handle_call({:query, sql, params}, _from, state) do
    command = command_from_sql(sql)
    query = %Exqlite.Query{statement: sql, command: command}

    case DBConnection.prepare_execute(state.conn, query, params, command: command) do
      {:ok, _query, %Exqlite.Result{} = result} ->
        {:reply, {:ok, serialize_result(result)}, state}

      {:error, err} ->
        {:reply, {:error, format_error(err)}, state}
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

  defp serialize_result(%Exqlite.Result{} = r) do
    {r.columns, r.rows, r.num_rows}
  end

  defp format_error(%Exqlite.Error{message: msg}), do: msg
  defp format_error(%DBConnection.ConnectionError{message: msg}), do: msg
  defp format_error(other), do: inspect(other)
end
