defmodule Ecto.Adapters.ClusterLite.Connection do
  @moduledoc """
  SQL.Connection implementation for ClusterLite.

  Delegates all SQL generation to `Ecto.Adapters.SQLite3.Connection`,
  reusing its 2000+ lines of tested SQLite SQL generation. Only
  overrides connection management to use ClusterLite.Connection instead
  of Exqlite.Connection.
  """

  @behaviour Ecto.Adapters.SQL.Connection

  alias ClusterLite.Query

  @sqlite3_conn Ecto.Adapters.SQLite3.Connection

  # -------------------------------------------------------------------
  # Connection management (overridden)
  # -------------------------------------------------------------------

  @impl true
  def child_spec(options) do
    DBConnection.child_spec(ClusterLite.Connection, options)
  end

  # -------------------------------------------------------------------
  # Query execution (use ClusterLite structs)
  # -------------------------------------------------------------------

  @impl true
  def prepare_execute(conn, name, sql, params, options) do
    query = %Query{statement: sql, name: name}

    case DBConnection.prepare_execute(conn, query, params, options) do
      {:ok, query, result} -> {:ok, query, result}
      {:error, err} -> raise_or_error(err)
    end
  end

  @impl true
  def execute(conn, %Query{ref: ref} = query, params, options) when ref != nil do
    case DBConnection.execute(conn, query, params, options) do
      {:ok, _query, result} -> {:ok, result}
      {:ok, result} -> {:ok, result}
      {:error, err} -> raise_or_error(err)
    end
  end

  def execute(conn, sql, params, options) when is_binary(sql) do
    query = %Query{statement: sql}

    case DBConnection.prepare_execute(conn, query, params, options) do
      {:ok, _query, result} -> {:ok, result}
      {:error, err} -> raise_or_error(err)
    end
  end

  def execute(conn, %Query{} = query, params, options) do
    case DBConnection.prepare_execute(conn, query, params, options) do
      {:ok, _query, result} -> {:ok, result}
      {:error, err} -> raise_or_error(err)
    end
  end

  @impl true
  def query(conn, sql, params, options) do
    query = %Query{statement: sql}

    case DBConnection.prepare_execute(conn, query, params, options) do
      {:ok, _query, result} -> {:ok, result}
      {:error, err} -> raise_or_error(err)
    end
  end

  @impl true
  def query_many(_conn, _sql, _params, _options) do
    raise "query_many is not supported by ClusterLite"
  end

  @impl true
  def stream(_conn, _sql, _params, _options) do
    raise "stream is not supported by ClusterLite"
  end

  # -------------------------------------------------------------------
  # Error handling
  # -------------------------------------------------------------------

  @impl true
  def to_constraints(%ClusterLite.Error{message: message}, _opts) when is_binary(message) do
    cond do
      String.contains?(message, "UNIQUE constraint failed: index ") ->
        [unique: message |> String.replace("UNIQUE constraint failed: index ", "")]

      String.contains?(message, "UNIQUE constraint failed: ") ->
        constraint =
          message
          |> String.replace("UNIQUE constraint failed: ", "")
          |> String.split(", ")
          |> Enum.map(&String.split(&1, "."))
          |> Enum.map(&List.last/1)
          |> Enum.join(", ")

        [unique: constraint]

      String.contains?(message, "FOREIGN KEY constraint failed") ->
        [foreign_key: "foreign_key"]

      true ->
        []
    end
  end

  def to_constraints(_, _opts), do: []

  # -------------------------------------------------------------------
  # SQL generation (delegated to ecto_sqlite3)
  # -------------------------------------------------------------------

  @impl true
  defdelegate all(query, as_prefix \\ []), to: @sqlite3_conn

  @impl true
  defdelegate update_all(query, prefix \\ []), to: @sqlite3_conn

  @impl true
  defdelegate delete_all(query), to: @sqlite3_conn

  @impl true
  defdelegate insert(prefix, table, header, rows, on_conflict, returning, placeholders),
    to: @sqlite3_conn

  @impl true
  defdelegate update(prefix, table, fields, filters, returning), to: @sqlite3_conn

  @impl true
  defdelegate delete(prefix, table, filters, returning), to: @sqlite3_conn

  # -------------------------------------------------------------------
  # DDL (delegated to ecto_sqlite3)
  # -------------------------------------------------------------------

  @impl true
  defdelegate execute_ddl(command), to: @sqlite3_conn

  @impl true
  defdelegate ddl_logs(result), to: @sqlite3_conn

  @impl true
  defdelegate table_exists_query(table), to: @sqlite3_conn

  @impl true
  def explain_query(conn, query, params, opts) do
    type = Keyword.get(opts, :type, :query_plan)

    explain_sql =
      case type do
        :query_plan -> "EXPLAIN QUERY PLAN " <> query
        :instructions -> "EXPLAIN " <> query
        _ -> "EXPLAIN QUERY PLAN " <> query
      end

    case query(conn, explain_sql, params, opts) do
      {:ok, result} -> {:ok, format_explain(result, type)}
      {:error, err} -> {:error, err}
    end
  end

  defp format_explain(%{rows: rows}, _type) do
    rows
    |> Enum.map_join("\n", fn row -> Enum.join(row, "|") end)
  end

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  defp raise_or_error(%ClusterLite.Error{} = err), do: {:error, err}
  defp raise_or_error(err), do: {:error, err}
end
