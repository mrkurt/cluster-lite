defmodule ClusterLite.Remote.DbServerTest do
  use ExUnit.Case, async: true

  alias ClusterLite.Remote.DbServer

  setup do
    db_path = Path.join(System.tmp_dir!(), "db_server_test_#{:erlang.unique_integer([:positive])}.db")

    {:ok, pid} = DbServer.start_link(path: db_path, config: [journal_mode: :wal])
    {:ok, conn_id} = GenServer.call(pid, :open_conn)

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
      File.rm(db_path)
      File.rm("#{db_path}-wal")
      File.rm("#{db_path}-shm")
    end)

    %{pid: pid, conn_id: conn_id, db_path: db_path}
  end

  defp q(pid, conn_id, sql, params \\ []) do
    GenServer.call(pid, {:query, conn_id, sql, params})
  end

  test "ping", %{pid: pid} do
    assert :ok = GenServer.call(pid, :ping)
  end

  test "create table, insert, and select", %{pid: pid, conn_id: c} do
    {:ok, _} = q(pid, c, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
    {:ok, {_, _, 1}} = q(pid, c, "INSERT INTO test (name) VALUES (?)", ["alice"])
    {:ok, {_, _, 1}} = q(pid, c, "INSERT INTO test (name) VALUES (?)", ["bob"])

    {:ok, {columns, rows, 2}} = q(pid, c, "SELECT name FROM test ORDER BY name")

    assert columns == ["name"]
    assert rows == [["alice"], ["bob"]]
  end

  test "transaction via raw SQL", %{pid: pid, conn_id: c} do
    q(pid, c, "CREATE TABLE t (val TEXT)")

    q(pid, c, "BEGIN")
    q(pid, c, "INSERT INTO t VALUES ('a')")
    q(pid, c, "COMMIT")

    q(pid, c, "BEGIN")
    q(pid, c, "INSERT INTO t VALUES ('b')")
    q(pid, c, "ROLLBACK")

    {:ok, {_cols, rows, _n}} = q(pid, c, "SELECT val FROM t")
    assert rows == [["a"]]
  end

  test "update returns affected count", %{pid: pid, conn_id: c} do
    q(pid, c, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
    q(pid, c, "INSERT INTO t VALUES (1, 'x')")
    q(pid, c, "INSERT INTO t VALUES (2, 'y')")

    {:ok, {_, _, 2}} = q(pid, c, "UPDATE t SET val = 'z'")
  end

  test "multiple connections to same db", %{pid: pid, conn_id: c1} do
    {:ok, c2} = GenServer.call(pid, :open_conn)

    q(pid, c1, "CREATE TABLE t (val TEXT)")
    q(pid, c1, "INSERT INTO t VALUES ('hello')")

    # Second connection can read the same data
    {:ok, {_, rows, 1}} = q(pid, c2, "SELECT val FROM t")
    assert rows == [["hello"]]

    GenServer.call(pid, {:close_conn, c2})
  end

  test "error on invalid SQL", %{pid: pid, conn_id: c} do
    {:error, msg} = q(pid, c, "INVALID SQL STATEMENT")
    assert is_binary(msg)
  end

  test "error on unknown conn_id", %{pid: pid} do
    {:error, msg} = q(pid, 99999, "SELECT 1")
    assert msg =~ "unknown conn_id"
  end
end
