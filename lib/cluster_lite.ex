defmodule ClusterLite do
  @moduledoc """
  SQLite over RPC for distributed Elixir/Phoenix applications.

  Drop-in replacement for exqlite that proxies SQLite operations to
  remote nodes in a cluster via `:rpc.call/4`.
  """

  alias ClusterLite.Connection

  @type conn :: DBConnection.conn()

  def child_spec(opts), do: DBConnection.child_spec(Connection, opts)
  def start_link(opts), do: DBConnection.start_link(Connection, opts)

  def query(conn, statement, params \\ [], opts \\ []) do
    query = %Exqlite.Query{statement: statement}

    with {:ok, _query, result} <- DBConnection.prepare_execute(conn, query, params, opts) do
      {:ok, result}
    end
  end

  def query!(conn, statement, params \\ [], opts \\ []) do
    case query(conn, statement, params, opts) do
      {:ok, result} -> result
      {:error, err} -> raise err
    end
  end

  def prepare(conn, name, statement, opts \\ []) do
    DBConnection.prepare(conn, %Exqlite.Query{statement: statement, name: name}, opts)
  end

  def execute(conn, query, params \\ [], opts \\ [])
  def execute(conn, %Exqlite.Query{} = query, params, opts), do: DBConnection.execute(conn, query, params, opts)
  def execute(conn, statement, params, opts) when is_binary(statement), do: query(conn, statement, params, opts)

  def transaction(conn, fun, opts \\ []), do: DBConnection.transaction(conn, fun, opts)
  def rollback(conn, reason), do: DBConnection.rollback(conn, reason)
end
