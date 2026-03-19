defmodule ClusterLite.Remote.DbServerTest do
  use ExUnit.Case, async: true

  alias ClusterLite.Remote.DbServer

  setup do
    db_path = Path.join(System.tmp_dir!(), "db_server_test_#{:erlang.unique_integer([:positive])}.db")

    {:ok, pid} = DbServer.start_link(path: db_path, config: [journal_mode: :wal])

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
      File.rm(db_path)
      File.rm("#{db_path}-wal")
      File.rm("#{db_path}-shm")
    end)

    %{pid: pid, db_path: db_path}
  end

  test "ping", %{pid: pid} do
    assert :ok = GenServer.call(pid, :ping)
  end

  test "create table, insert, and select", %{pid: pid} do
    {:ok, _} = GenServer.call(pid, {:query, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", []})

    {:ok, {_, _, 1}} = GenServer.call(pid, {:query, "INSERT INTO test (name) VALUES (?)", ["alice"]})
    {:ok, {_, _, 1}} = GenServer.call(pid, {:query, "INSERT INTO test (name) VALUES (?)", ["bob"]})

    {:ok, {columns, rows, 2}} =
      GenServer.call(pid, {:query, "SELECT name FROM test ORDER BY name", []})

    assert columns == ["name"]
    assert rows == [["alice"], ["bob"]]
  end

  test "transaction via raw SQL", %{pid: pid} do
    GenServer.call(pid, {:query, "CREATE TABLE t (val TEXT)", []})

    GenServer.call(pid, {:query, "BEGIN", []})
    GenServer.call(pid, {:query, "INSERT INTO t VALUES ('a')", []})
    GenServer.call(pid, {:query, "COMMIT", []})

    GenServer.call(pid, {:query, "BEGIN", []})
    GenServer.call(pid, {:query, "INSERT INTO t VALUES ('b')", []})
    GenServer.call(pid, {:query, "ROLLBACK", []})

    {:ok, {_cols, rows, _n}} = GenServer.call(pid, {:query, "SELECT val FROM t", []})
    assert rows == [["a"]]
  end

  test "update returns affected count", %{pid: pid} do
    GenServer.call(pid, {:query, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", []})
    GenServer.call(pid, {:query, "INSERT INTO t VALUES (1, 'x')", []})
    GenServer.call(pid, {:query, "INSERT INTO t VALUES (2, 'y')", []})

    {:ok, {_, _, 2}} = GenServer.call(pid, {:query, "UPDATE t SET val = 'z'", []})
  end

  test "error on invalid SQL", %{pid: pid} do
    {:error, msg} = GenServer.call(pid, {:query, "INVALID SQL STATEMENT", []})
    assert is_binary(msg)
  end
end
