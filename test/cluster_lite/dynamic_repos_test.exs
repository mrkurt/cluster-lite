defmodule ClusterLite.DynamicReposTest do
  use ExUnit.Case

  alias ClusterLite.DynamicTestRepo
  alias ClusterLite.TestSchema.User

  setup do
    db_paths =
      for i <- 1..3 do
        Path.join(System.tmp_dir!(), "dynamic_test_#{i}_#{:erlang.unique_integer([:positive])}.db")
      end

    on_exit(fn ->
      for path <- db_paths do
        File.rm(path)
        File.rm("#{path}-wal")
        File.rm("#{path}-shm")
      end
    end)

    %{db_paths: db_paths}
  end

  test "start multiple dynamic repos", %{db_paths: db_paths} do
    pids =
      for path <- db_paths do
        {:ok, pid} = ClusterLite.DynamicRepos.start_repo(DynamicTestRepo, node(), path)
        DynamicTestRepo.put_dynamic_repo(pid)

        DynamicTestRepo.query!("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, age INTEGER, inserted_at TEXT NOT NULL, updated_at TEXT NOT NULL)")
        pid
      end

    # Insert different data into each repo
    for {pid, i} <- Enum.with_index(pids, 1) do
      DynamicTestRepo.put_dynamic_repo(pid)
      DynamicTestRepo.insert!(%User{name: "User#{i}", email: "u#{i}@test.com", age: i * 10})
    end

    # Verify each repo has its own data
    for {pid, i} <- Enum.with_index(pids, 1) do
      DynamicTestRepo.put_dynamic_repo(pid)
      users = DynamicTestRepo.all(User)
      assert length(users) == 1
      assert hd(users).name == "User#{i}"
    end

    # Cleanup
    for pid <- pids do
      ClusterLite.DynamicRepos.stop_repo(pid)
    end
  end

  test "with_dynamic_repo helper", %{db_paths: [path | _]} do
    {:ok, pid} = ClusterLite.DynamicRepos.start_repo(DynamicTestRepo, node(), path)

    ClusterLite.DynamicRepos.with_dynamic_repo(DynamicTestRepo, pid, fn ->
      DynamicTestRepo.query!("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, age INTEGER, inserted_at TEXT NOT NULL, updated_at TEXT NOT NULL)")
      DynamicTestRepo.insert!(%User{name: "Helper", email: "h@test.com", age: 42})
      users = DynamicTestRepo.all(User)
      assert length(users) == 1
      assert hd(users).name == "Helper"
    end)

    ClusterLite.DynamicRepos.stop_repo(pid)
  end
end
