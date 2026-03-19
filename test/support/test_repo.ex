defmodule ClusterLite.TestRepo do
  use Ecto.Repo,
    otp_app: :cluster_lite,
    adapter: Ecto.Adapters.ClusterLite
end

defmodule ClusterLite.DynamicTestRepo do
  use Ecto.Repo,
    otp_app: :cluster_lite,
    adapter: Ecto.Adapters.ClusterLite
end
