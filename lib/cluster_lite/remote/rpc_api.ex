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

  def open_conn(pid), do: GenServer.call(pid, :open_conn, @timeout)
  def close_conn(pid, conn_id), do: GenServer.call(pid, {:close_conn, conn_id}, @timeout)

  def ping(pid), do: GenServer.call(pid, :ping, @timeout)

  def query(pid, conn_id, sql, params \\ []),
    do: GenServer.call(pid, {:query, conn_id, sql, params}, @timeout)
end
