defmodule ClusterLite.Result do
  @moduledoc """
  Result struct returned from queries.
  """

  @type t :: %__MODULE__{
          command: atom(),
          columns: [String.t()] | nil,
          rows: [[term()]] | nil,
          num_rows: non_neg_integer()
        }

  defstruct command: nil,
            columns: nil,
            rows: nil,
            num_rows: 0
end
