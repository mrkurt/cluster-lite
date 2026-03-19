defmodule ClusterLite.EctoAdapterTest do
  use ExUnit.Case

  alias ClusterLite.TestRepo
  alias ClusterLite.TestSchema.User

  setup_all do
    db_path = Path.join(System.tmp_dir!(), "ecto_test_#{:erlang.unique_integer([:positive])}.db")

    Application.put_env(:cluster_lite, TestRepo,
      database: db_path,
      cluster_lite_node: node(),
      pool_size: 1,
      journal_mode: :wal
    )

    {:ok, _} = TestRepo.start_link()

    # Run migrations inline
    TestRepo.query!("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, age INTEGER, inserted_at TEXT NOT NULL, updated_at TEXT NOT NULL)")
    TestRepo.query!("CREATE TABLE IF NOT EXISTS posts (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, body TEXT, user_id INTEGER REFERENCES users(id), inserted_at TEXT NOT NULL, updated_at TEXT NOT NULL)")

    on_exit(fn ->
      File.rm(db_path)
      File.rm("#{db_path}-wal")
      File.rm("#{db_path}-shm")
    end)

    %{db_path: db_path}
  end

  setup do
    TestRepo.query!("DELETE FROM posts")
    TestRepo.query!("DELETE FROM users")
    :ok
  end

  test "insert and get" do
    {:ok, user} = TestRepo.insert(%User{name: "Alice", email: "alice@example.com", age: 30})
    assert user.id != nil
    assert user.name == "Alice"

    fetched = TestRepo.get!(User, user.id)
    assert fetched.name == "Alice"
    assert fetched.email == "alice@example.com"
  end

  test "update" do
    {:ok, user} = TestRepo.insert(%User{name: "Bob", email: "bob@test.com", age: 25})
    changeset = Ecto.Changeset.change(user, name: "Robert")
    {:ok, updated} = TestRepo.update(changeset)
    assert updated.name == "Robert"

    fetched = TestRepo.get!(User, user.id)
    assert fetched.name == "Robert"
  end

  test "delete" do
    {:ok, user} = TestRepo.insert(%User{name: "Charlie", email: "c@test.com", age: 40})
    {:ok, _} = TestRepo.delete(user)
    assert TestRepo.get(User, user.id) == nil
  end

  test "all with query" do
    TestRepo.insert!(%User{name: "D1", email: "d1@test.com", age: 20})
    TestRepo.insert!(%User{name: "D2", email: "d2@test.com", age: 30})
    TestRepo.insert!(%User{name: "D3", email: "d3@test.com", age: 40})

    import Ecto.Query

    users = TestRepo.all(from u in User, where: u.age >= 30, order_by: u.name)
    assert length(users) == 2
    assert Enum.map(users, & &1.name) == ["D2", "D3"]
  end

  test "transaction" do
    result =
      TestRepo.transaction(fn ->
        TestRepo.insert!(%User{name: "TxUser", email: "tx@test.com", age: 1})
        TestRepo.all(User)
      end)

    assert {:ok, [%User{name: "TxUser"}]} = result
  end

  test "raw SQL query" do
    TestRepo.insert!(%User{name: "Raw", email: "raw@test.com", age: 99})

    {:ok, %{rows: rows}} = TestRepo.query("SELECT name, age FROM users WHERE age = ?", [99])
    assert rows == [["Raw", 99]]
  end

  test "aggregate" do
    TestRepo.insert!(%User{name: "A1", email: "a1@test.com", age: 10})
    TestRepo.insert!(%User{name: "A2", email: "a2@test.com", age: 20})

    count = TestRepo.aggregate(User, :count)
    assert count == 2
  end
end
