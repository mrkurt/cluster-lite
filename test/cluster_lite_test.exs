defmodule ClusterLiteTest do
  use ExUnit.Case

  test "query struct command detection" do
    assert ClusterLite.Query.command_from_sql("SELECT * FROM t") == :select
    assert ClusterLite.Query.command_from_sql("INSERT INTO t VALUES (1)") == :insert
    assert ClusterLite.Query.command_from_sql("UPDATE t SET x = 1") == :update
    assert ClusterLite.Query.command_from_sql("DELETE FROM t") == :delete
    assert ClusterLite.Query.command_from_sql("CREATE TABLE t (id INT)") == :create
    assert ClusterLite.Query.command_from_sql("  SELECT 1") == :select
    assert ClusterLite.Query.command_from_sql("PRAGMA journal_mode") == :pragma
    assert ClusterLite.Query.command_from_sql("WITH cte AS (SELECT 1) SELECT * FROM cte") == :select
  end

  test "error struct" do
    err = %ClusterLite.Error{message: "test error", statement: "SELECT 1"}
    assert Exception.message(err) == "test error - SELECT 1"

    err2 = %ClusterLite.Error{message: "simple error"}
    assert Exception.message(err2) == "simple error"
  end

  test "result struct defaults" do
    result = %ClusterLite.Result{}
    assert result.command == nil
    assert result.columns == nil
    assert result.rows == nil
    assert result.num_rows == 0
  end
end
