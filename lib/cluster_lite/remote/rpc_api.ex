defmodule ClusterLite.Remote.RpcApi do
  @moduledoc """
  MFA-safe exported functions for RPC calls from remote nodes.

  Every function in this module is safe to call via `:rpc.call/4` —
  all arguments and return values are serializable (no NIF refs).
  """

  alias ClusterLite.Remote.DbServer

  @timeout 15_000

  def start_db(path, config \\ []) do
    DynamicSupervisor.start_child(
      ClusterLite.Remote.DynamicSupervisor,
      {DbServer, [path: path, config: config]}
    )
  end

  def stop_db(pid) when is_pid(pid) do
    DynamicSupervisor.terminate_child(ClusterLite.Remote.DynamicSupervisor, pid)
  end

  def ping(pid), do: GenServer.call(pid, :ping, @timeout)

  def prepare(pid, sql), do: GenServer.call(pid, {:prepare, sql}, @timeout)

  def execute(pid, stmt_id, params \\ []),
    do: GenServer.call(pid, {:execute, stmt_id, params}, @timeout)

  def prepare_and_execute(pid, sql, params \\ []),
    do: GenServer.call(pid, {:prepare_and_execute, sql, params}, @timeout)

  def close_statement(pid, stmt_id),
    do: GenServer.call(pid, {:close_statement, stmt_id}, @timeout)

  def begin_transaction(pid, mode \\ :deferred),
    do: GenServer.call(pid, {:begin, mode}, @timeout)

  def commit(pid), do: GenServer.call(pid, :commit, @timeout)

  def rollback(pid, savepoint \\ nil),
    do: GenServer.call(pid, {:rollback, savepoint}, @timeout)

  def transaction_status(pid), do: GenServer.call(pid, :transaction_status, @timeout)

  def changes(pid), do: GenServer.call(pid, :changes, @timeout)

  def declare_cursor(pid, sql, params, max_rows \\ 500),
    do: GenServer.call(pid, {:declare_cursor, sql, params, max_rows}, @timeout)

  def fetch_cursor(pid, cursor_id, max_rows \\ nil),
    do: GenServer.call(pid, {:fetch_cursor, cursor_id, max_rows}, @timeout)

  def deallocate_cursor(pid, cursor_id),
    do: GenServer.call(pid, {:deallocate_cursor, cursor_id}, @timeout)
end
