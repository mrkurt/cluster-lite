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

  test "prepare and execute", %{pid: pid} do
    GenServer.call(pid, {:prepare_and_execute, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", []})

    {:ok, stmt_id} = GenServer.call(pid, {:prepare, "INSERT INTO test (name) VALUES (?)"})
    assert is_integer(stmt_id)

    {:ok, [], [], 1} = GenServer.call(pid, {:execute, stmt_id, ["alice"]})
    {:ok, [], [], 1} = GenServer.call(pid, {:execute, stmt_id, ["bob"]})

    {:ok, _stmt_id2, columns, rows, _changes} =
      GenServer.call(pid, {:prepare_and_execute, "SELECT name FROM test ORDER BY name", []})

    assert columns == ["name"]
    assert rows == [["alice"], ["bob"]]
  end

  test "close statement", %{pid: pid} do
    GenServer.call(pid, {:prepare_and_execute, "CREATE TABLE t (id INTEGER PRIMARY KEY)", []})
    {:ok, stmt_id} = GenServer.call(pid, {:prepare, "SELECT 1"})
    assert :ok = GenServer.call(pid, {:close_statement, stmt_id})
    # closing again is idempotent
    assert :ok = GenServer.call(pid, {:close_statement, stmt_id})
  end

  test "transaction begin/commit/rollback", %{pid: pid} do
    GenServer.call(pid, {:prepare_and_execute, "CREATE TABLE t (val TEXT)", []})

    assert :idle = GenServer.call(pid, :transaction_status)

    :ok = GenServer.call(pid, {:begin, :deferred})
    assert :transaction = GenServer.call(pid, :transaction_status)

    GenServer.call(pid, {:prepare_and_execute, "INSERT INTO t VALUES ('a')", []})
    :ok = GenServer.call(pid, :commit)
    assert :idle = GenServer.call(pid, :transaction_status)

    :ok = GenServer.call(pid, {:begin, :deferred})
    GenServer.call(pid, {:prepare_and_execute, "INSERT INTO t VALUES ('b')", []})
    :ok = GenServer.call(pid, {:rollback, nil})
    assert :idle = GenServer.call(pid, :transaction_status)

    {:ok, _sid, _cols, rows} = GenServer.call(pid, {:prepare_and_execute, "SELECT val FROM t", []})
    assert rows == [["a"]]
  end

  test "changes", %{pid: pid} do
    GenServer.call(pid, {:prepare_and_execute, "CREATE TABLE t (val TEXT)", []})
    GenServer.call(pid, {:prepare_and_execute, "INSERT INTO t VALUES ('x')", []})
    assert {:ok, 1} = GenServer.call(pid, :changes)
  end

  test "cursor operations", %{pid: pid} do
    GenServer.call(pid, {:prepare_and_execute, "CREATE TABLE nums (n INTEGER)", []})

    for i <- 1..5 do
      GenServer.call(pid, {:prepare_and_execute, "INSERT INTO nums VALUES (?)", [i]})
    end

    {:ok, cursor_id} = GenServer.call(pid, {:declare_cursor, "SELECT n FROM nums ORDER BY n", [], 2})

    {:ok, ["n"], rows1, :cont} = GenServer.call(pid, {:fetch_cursor, cursor_id, 2})
    assert length(rows1) == 2

    {:ok, ["n"], rows2, :cont} = GenServer.call(pid, {:fetch_cursor, cursor_id, 2})
    assert length(rows2) == 2

    {:ok, ["n"], rows3, :halt} = GenServer.call(pid, {:fetch_cursor, cursor_id, 2})
    assert length(rows3) == 1

    all_vals = Enum.flat_map(rows1 ++ rows2 ++ rows3, & &1)
    assert all_vals == [1, 2, 3, 4, 5]

    assert :ok = GenServer.call(pid, {:deallocate_cursor, cursor_id})
  end

  test "error on invalid SQL", %{pid: pid} do
    {:error, msg} = GenServer.call(pid, {:prepare, "INVALID SQL STATEMENT"})
    assert is_binary(msg)
  end
end
