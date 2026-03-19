defmodule ClusterLiteTest do
  use ExUnit.Case

  test "uses exqlite structs directly" do
    result = %Exqlite.Result{columns: ["a"], rows: [[1]], num_rows: 1}
    assert result.columns == ["a"]
    assert result.num_rows == 1

    query = %Exqlite.Query{statement: "SELECT 1"}
    assert query.statement == "SELECT 1"
  end
end
