defmodule ClusterLite.TestSchema.User do
  use Ecto.Schema

  schema "users" do
    field :name, :string
    field :email, :string
    field :age, :integer
    timestamps()
  end
end

defmodule ClusterLite.TestSchema.Post do
  use Ecto.Schema

  schema "posts" do
    field :title, :string
    field :body, :string
    belongs_to :user, ClusterLite.TestSchema.User
    timestamps()
  end
end
