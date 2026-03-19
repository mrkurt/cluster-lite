defmodule ClusterLite do
  @moduledoc """
  SQLite over RPC for distributed Elixir/Phoenix applications.

  Drop-in replacement for exqlite that proxies SQLite operations to
  remote nodes in a cluster via `:rpc.call/4`.
  """

  alias ClusterLite.{Connection, Query, Error}

  @type conn :: DBConnection.conn()

  def child_spec(opts) do
    DBConnection.child_spec(Connection, opts)
  end

  def start_link(opts) do
    DBConnection.start_link(Connection, opts)
  end

  def query(conn, statement, params \\ [], opts \\ []) do
    query = %Query{statement: statement}

    with {:ok, _query, result} <- DBConnection.prepare_execute(conn, query, params, opts) do
      {:ok, result}
    end
  end

  def query!(conn, statement, params \\ [], opts \\ []) do
    case query(conn, statement, params, opts) do
      {:ok, result} -> result
      {:error, %Error{} = err} -> raise err
      {:error, err} -> raise %Error{message: inspect(err)}
    end
  end

  def prepare(conn, name, statement, opts \\ []) do
    query = %Query{statement: statement, name: name}
    DBConnection.prepare(conn, query, opts)
  end

  def execute(conn, query, params \\ [], opts \\ [])

  def execute(conn, %Query{} = query, params, opts) do
    DBConnection.execute(conn, query, params, opts)
  end

  def execute(conn, statement, params, opts) when is_binary(statement) do
    query(conn, statement, params, opts)
  end

  def transaction(conn, fun, opts \\ []) do
    DBConnection.transaction(conn, fun, opts)
  end

  def rollback(conn, reason) do
    DBConnection.rollback(conn, reason)
  end
end
