defmodule ClusterLite.Query do
  @moduledoc """
  Query struct for ClusterLite.
  """

  @type t :: %__MODULE__{
          statement: iodata(),
          name: String.t(),
          command: atom()
        }

  defstruct statement: nil,
            name: "",
            command: nil

  @doc false
  def command_from_sql(sql) when is_list(sql), do: command_from_sql(IO.iodata_to_binary(sql))

  def command_from_sql(sql) when is_binary(sql) do
    sql
    |> String.trim_leading()
    |> String.split(~r/\s+/, parts: 2)
    |> List.first("")
    |> String.downcase()
    |> case do
      "select" -> :select
      "insert" -> :insert
      "update" -> :update
      "delete" -> :delete
      "create" -> :create
      "drop" -> :drop
      "alter" -> :alter
      "begin" -> :begin
      "commit" -> :commit
      "rollback" -> :rollback
      "pragma" -> :pragma
      "explain" -> :explain
      "with" -> :select
      _ -> :execute
    end
  end
end

defimpl DBConnection.Query, for: ClusterLite.Query do
  def parse(query, _opts), do: query
  def describe(query, _opts), do: query

  def encode(_query, params, _opts), do: params
  def decode(_query, result, _opts), do: result
end

defimpl String.Chars, for: ClusterLite.Query do
  def to_string(%ClusterLite.Query{statement: statement}), do: IO.iodata_to_binary(statement)
end
