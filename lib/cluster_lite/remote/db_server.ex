defmodule ClusterLite.Remote.DbServer do
  @moduledoc """
  GenServer that owns the SQLite NIF connection on the remote node.

  All NIF references stay inside this process. External callers only
  see serializable values (integer stmt_ids, rows, column names, etc.).
  """

  use GenServer

  require Logger

  alias Exqlite.Sqlite3

  defstruct [
    :db,
    :path,
    statements: %{},
    cursors: %{},
    next_stmt_id: 1,
    next_cursor_id: 1,
    transaction_status: :idle,
    config: []
  ]

  # -------------------------------------------------------------------
  # Client API
  # -------------------------------------------------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  # -------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    config = Keyword.get(opts, :config, [])

    case Sqlite3.open(path, parse_open_flags(config)) do
      {:ok, db} ->
        state = %__MODULE__{db: db, path: path, config: config}

        case apply_pragmas(state) do
          {:ok, state} -> {:ok, state}
          {:error, reason} -> {:stop, reason}
        end

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def terminate(_reason, %__MODULE__{db: db, statements: statements}) do
    for {_id, ref} <- statements do
      Sqlite3.release(db, ref)
    end

    Sqlite3.close(db)
  end

  @impl true
  def handle_call(:ping, _from, state) do
    {:reply, :ok, state}
  end

  def handle_call(:transaction_status, _from, state) do
    {:reply, state.transaction_status, state}
  end

  def handle_call(:changes, _from, state) do
    {:reply, {:ok, Sqlite3.changes(state.db)}, state}
  end

  def handle_call({:prepare, sql}, _from, state) do
    case Sqlite3.prepare(state.db, sql) do
      {:ok, nif_ref} ->
        stmt_id = state.next_stmt_id
        statements = Map.put(state.statements, stmt_id, nif_ref)
        state = %{state | statements: statements, next_stmt_id: stmt_id + 1}
        {:reply, {:ok, stmt_id}, state}

      {:error, reason} ->
        {:reply, {:error, to_string(reason)}, state}
    end
  end

  def handle_call({:execute, stmt_id, params}, _from, state) do
    case Map.fetch(state.statements, stmt_id) do
      {:ok, nif_ref} ->
        case bind_and_step_all(state.db, nif_ref, params) do
          {:ok, columns, rows} ->
            {:ok, changes} = Sqlite3.changes(state.db)
            {:reply, {:ok, columns, rows, changes}, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      :error ->
        {:reply, {:error, "unknown statement id: #{stmt_id}"}, state}
    end
  end

  def handle_call({:prepare_and_execute, sql, params}, _from, state) do
    case Sqlite3.prepare(state.db, sql) do
      {:ok, nif_ref} ->
        stmt_id = state.next_stmt_id
        statements = Map.put(state.statements, stmt_id, nif_ref)
        state = %{state | statements: statements, next_stmt_id: stmt_id + 1}

        case bind_and_step_all(state.db, nif_ref, params) do
          {:ok, columns, rows} ->
            {:ok, changes} = Sqlite3.changes(state.db)
            {:reply, {:ok, stmt_id, columns, rows, changes}, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      {:error, reason} ->
        {:reply, {:error, to_string(reason)}, state}
    end
  end

  def handle_call({:close_statement, stmt_id}, _from, state) do
    case Map.pop(state.statements, stmt_id) do
      {nil, _} ->
        {:reply, :ok, state}

      {nif_ref, statements} ->
        Sqlite3.release(state.db, nif_ref)
        {:reply, :ok, %{state | statements: statements}}
    end
  end

  def handle_call({:begin, mode}, _from, state) do
    sql =
      case mode do
        :deferred -> "BEGIN DEFERRED"
        :immediate -> "BEGIN IMMEDIATE"
        :exclusive -> "BEGIN EXCLUSIVE"
        _ -> "BEGIN"
      end

    case exec_raw(state.db, sql) do
      :ok ->
        {:reply, :ok, %{state | transaction_status: :transaction}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:commit, _from, state) do
    case exec_raw(state.db, "COMMIT") do
      :ok ->
        {:reply, :ok, %{state | transaction_status: :idle}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:rollback, savepoint}, _from, state) do
    sql =
      if savepoint do
        "ROLLBACK TO SAVEPOINT #{savepoint}"
      else
        "ROLLBACK"
      end

    case exec_raw(state.db, sql) do
      :ok ->
        status = if savepoint, do: state.transaction_status, else: :idle
        {:reply, :ok, %{state | transaction_status: status}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:declare_cursor, sql, params, max_rows}, _from, state) do
    case Sqlite3.prepare(state.db, sql) do
      {:ok, nif_ref} ->
        case Sqlite3.bind(nif_ref, params) do
          :ok ->
            cursor_id = state.next_cursor_id
            cursor = %{ref: nif_ref, max_rows: max_rows}
            cursors = Map.put(state.cursors, cursor_id, cursor)
            state = %{state | cursors: cursors, next_cursor_id: cursor_id + 1}
            {:reply, {:ok, cursor_id}, state}

          {:error, reason} ->
            Sqlite3.release(state.db, nif_ref)
            {:reply, {:error, to_string(reason)}, state}
        end

      {:error, reason} ->
        {:reply, {:error, to_string(reason)}, state}
    end
  end

  def handle_call({:fetch_cursor, cursor_id, max_rows}, _from, state) do
    case Map.fetch(state.cursors, cursor_id) do
      {:ok, %{ref: nif_ref}} ->
        batch_size = max_rows || 500
        {rows, status} = fetch_rows(state.db, nif_ref, batch_size, [])
        columns = Sqlite3.columns(state.db, nif_ref) |> elem(1)
        {:reply, {:ok, columns, rows, status}, state}

      :error ->
        {:reply, {:error, "unknown cursor id: #{cursor_id}"}, state}
    end
  end

  def handle_call({:deallocate_cursor, cursor_id}, _from, state) do
    case Map.pop(state.cursors, cursor_id) do
      {nil, _} ->
        {:reply, :ok, state}

      {%{ref: nif_ref}, cursors} ->
        Sqlite3.release(state.db, nif_ref)
        {:reply, :ok, %{state | cursors: cursors}}
    end
  end

  # -------------------------------------------------------------------
  # Private helpers
  # -------------------------------------------------------------------

  defp bind_and_step_all(db, nif_ref, params) do
    case Sqlite3.bind(nif_ref, params) do
      :ok ->
        columns =
          case Sqlite3.columns(db, nif_ref) do
            {:ok, cols} -> cols
            _ -> []
          end

        rows = collect_rows(db, nif_ref, [])
        {:ok, columns, rows}

      {:error, reason} ->
        {:error, to_string(reason)}
    end
  end

  defp collect_rows(db, nif_ref, acc) do
    case Sqlite3.step(db, nif_ref) do
      {:row, row} -> collect_rows(db, nif_ref, [row | acc])
      :done -> Enum.reverse(acc)
      {:error, _reason} -> Enum.reverse(acc)
    end
  end

  defp fetch_rows(_db, _nif_ref, 0, acc) do
    {Enum.reverse(acc), :cont}
  end

  defp fetch_rows(db, nif_ref, remaining, acc) do
    case Sqlite3.step(db, nif_ref) do
      {:row, row} -> fetch_rows(db, nif_ref, remaining - 1, [row | acc])
      :done -> {Enum.reverse(acc), :halt}
      {:error, _} -> {Enum.reverse(acc), :halt}
    end
  end

  defp exec_raw(db, sql) do
    case Sqlite3.execute(db, sql) do
      :ok -> :ok
      {:error, reason} -> {:error, to_string(reason)}
    end
  end

  defp apply_pragmas(state) do
    pragmas = [
      {"journal_mode", Keyword.get(state.config, :journal_mode, :wal)},
      {"busy_timeout", Keyword.get(state.config, :busy_timeout, 2000)},
      {"foreign_keys", if(Keyword.get(state.config, :foreign_keys, true), do: 1, else: 0)},
      {"cache_size", Keyword.get(state.config, :cache_size, -64000)}
    ]

    Enum.reduce_while(pragmas, {:ok, state}, fn {key, value}, {:ok, s} ->
      case exec_raw(s.db, "PRAGMA #{key} = #{value}") do
        :ok -> {:cont, {:ok, s}}
        {:error, reason} -> {:halt, {:error, "Failed to set PRAGMA #{key}: #{reason}"}}
      end
    end)
  end

  defp parse_open_flags(config) do
    flags = Keyword.get(config, :flags, [])

    case flags do
      [] -> []
      _ -> [flags: flags]
    end
  end
end
