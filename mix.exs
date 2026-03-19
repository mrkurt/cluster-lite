defmodule ClusterLite.MixProject do
  use Mix.Project

  def project do
    [
      app: :cluster_lite,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {ClusterLite.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:exqlite, "~> 0.35"},
      {:db_connection, "~> 2.5"},
      {:ecto_sql, "~> 3.11"},
      {:ecto_sqlite3, "~> 0.17"}
    ]
  end
end
