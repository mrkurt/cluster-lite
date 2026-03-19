defmodule ClusterLite.ConnectionTest do
  use ExUnit.Case, async: true

  alias ClusterLite.{Query, Result}

  setup do
    db_path = Path.join(System.tmp_dir!(), "conn_test_#{:erlang.unique_integer([:positive])}.db")

    {:ok, conn} =
      ClusterLite.start_link(
        database: db_path,
        cluster_lite_node: node(),
        pool_size: 1
      )

    on_exit(fn ->
      File.rm(db_path)
      File.rm("#{db_path}-wal")
      File.rm("#{db_path}-shm")
    end)

    %{conn: conn, db_path: db_path}
  end

  test "query creates table and inserts/selects", %{conn: conn} do
    {:ok, _} = ClusterLite.query(conn, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    {:ok, _} = ClusterLite.query(conn, "INSERT INTO items (name) VALUES (?)", ["widget"])
    {:ok, result} = ClusterLite.query(conn, "SELECT name FROM items")

    assert %Result{columns: ["name"], rows: [["widget"]], num_rows: 1} = result
  end

  test "query! raises on error", %{conn: conn} do
    assert_raise ClusterLite.Error, fn ->
      ClusterLite.query!(conn, "SELECT * FROM nonexistent_table")
    end
  end

  test "prepare and execute", %{conn: conn} do
    ClusterLite.query!(conn, "CREATE TABLE t (val TEXT)")
    {:ok, query} = ClusterLite.prepare(conn, "ins", "INSERT INTO t VALUES (?)")

    assert %Query{command: :insert} = query

    {:ok, _query, _result} = ClusterLite.execute(conn, query, ["hello"])
    {:ok, result} = ClusterLite.query(conn, "SELECT val FROM t")
    assert result.rows == [["hello"]]
  end

  test "transaction commit", %{conn: conn} do
    ClusterLite.query!(conn, "CREATE TABLE t (val TEXT)")

    {:ok, _} =
      ClusterLite.transaction(conn, fn conn ->
        ClusterLite.query!(conn, "INSERT INTO t VALUES ('in_tx')")
      end)

    {:ok, result} = ClusterLite.query(conn, "SELECT val FROM t")
    assert result.rows == [["in_tx"]]
  end

  test "transaction rollback", %{conn: conn} do
    ClusterLite.query!(conn, "CREATE TABLE t (val TEXT)")

    {:error, :rollback_reason} =
      ClusterLite.transaction(conn, fn conn ->
        ClusterLite.query!(conn, "INSERT INTO t VALUES ('should_not_persist')")
        ClusterLite.rollback(conn, :rollback_reason)
      end)

    {:ok, result} = ClusterLite.query(conn, "SELECT val FROM t")
    assert result.rows == []
  end

  test "multiple inserts and count", %{conn: conn} do
    ClusterLite.query!(conn, "CREATE TABLE nums (n INTEGER)")

    for i <- 1..10 do
      ClusterLite.query!(conn, "INSERT INTO nums VALUES (?)", [i])
    end

    {:ok, result} = ClusterLite.query(conn, "SELECT COUNT(*) FROM nums")
    assert result.rows == [[10]]
  end

  test "command detection", %{conn: conn} do
    ClusterLite.query!(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY)")

    {:ok, result} = ClusterLite.query(conn, "SELECT 1")
    assert result.command == :select

    {:ok, result} = ClusterLite.query(conn, "INSERT INTO t VALUES (1)")
    assert result.command == :insert
  end
end
