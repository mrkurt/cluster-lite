defmodule ClusterLite.DynamicRepos do
  @moduledoc """
  Manages dynamically created Ecto repos, each pointing to a different
  SQLite database potentially on different nodes.
  """

  use DynamicSupervisor

  def start_link(opts) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Starts a dynamic Ecto repo for the given database path on the given node.

  Returns `{:ok, repo_pid}` which can be used with `Repo.put_dynamic_repo/1`.

  ## Options

    * `:pool_size` - defaults to 1
    * Any other options are passed to the repo's `start_link`
  """
  def start_repo(repo_module, node, db_path, opts \\ []) do
    repo_opts =
      opts
      |> Keyword.put(:database, db_path)
      |> Keyword.put(:cluster_lite_node, node)
      |> Keyword.put_new(:pool_size, 1)
      |> Keyword.put_new(:name, nil)

    child_spec = %{
      id: make_ref(),
      start: {repo_module, :start_link, [repo_opts]},
      restart: :temporary
    }

    DynamicSupervisor.start_child(__MODULE__, child_spec)
  end

  @doc """
  Stops a dynamic repo.
  """
  def stop_repo(pid) when is_pid(pid) do
    DynamicSupervisor.terminate_child(__MODULE__, pid)
  end

  @doc """
  Executes a function with the given dynamic repo set.
  """
  def with_dynamic_repo(repo_module, repo_pid, fun) when is_function(fun, 0) do
    repo_module.put_dynamic_repo(repo_pid)
    fun.()
  end
end
