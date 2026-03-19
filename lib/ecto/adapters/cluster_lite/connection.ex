defmodule Ecto.Adapters.ClusterLite.Connection do
  @moduledoc """
  SQL.Connection for ClusterLite.

  Delegates everything to `Ecto.Adapters.SQLite3.Connection` except
  `child_spec/1`, which swaps in `ClusterLite.Connection` as the
  DBConnection backend.
  """

  @behaviour Ecto.Adapters.SQL.Connection

  @sqlite3_conn Ecto.Adapters.SQLite3.Connection

  @impl true
  def child_spec(options) do
    DBConnection.child_spec(ClusterLite.Connection, options)
  end

  # Query execution — delegate to ecto_sqlite3 but it will use our
  # DBConnection pool (started by child_spec above)
  @impl true
  defdelegate prepare_execute(conn, name, sql, params, options), to: @sqlite3_conn

  @impl true
  defdelegate execute(conn, query, params, options), to: @sqlite3_conn

  @impl true
  defdelegate query(conn, sql, params, options), to: @sqlite3_conn

  @impl true
  defdelegate query_many(conn, sql, params, options), to: @sqlite3_conn

  @impl true
  defdelegate stream(conn, sql, params, options), to: @sqlite3_conn

  # Everything else — SQL generation, DDL, constraints, explain
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

  @impl true
  defdelegate execute_ddl(command), to: @sqlite3_conn

  @impl true
  defdelegate ddl_logs(result), to: @sqlite3_conn

  @impl true
  defdelegate table_exists_query(table), to: @sqlite3_conn

  @impl true
  defdelegate to_constraints(error, opts), to: @sqlite3_conn

  @impl true
  defdelegate explain_query(conn, query, params, opts), to: @sqlite3_conn
end
