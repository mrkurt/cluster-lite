defmodule ClusterLite.Remote.Supervisor do
  @moduledoc """
  Supervisor for remote-side components.

  Starts a DynamicSupervisor to manage DbServer processes.
  """

  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {DynamicSupervisor, name: ClusterLite.Remote.DynamicSupervisor, strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
