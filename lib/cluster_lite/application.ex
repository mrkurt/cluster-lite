defmodule ClusterLite.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    role = Application.get_env(:cluster_lite, :role, :both)

    children =
      case role do
        :remote -> remote_children()
        :local -> local_children()
        :both -> remote_children() ++ local_children()
      end

    opts = [strategy: :one_for_one, name: ClusterLite.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp remote_children do
    [ClusterLite.Remote.Supervisor]
  end

  defp local_children do
    [ClusterLite.DynamicRepos]
  end
end
