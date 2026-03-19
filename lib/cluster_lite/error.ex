defmodule ClusterLite.Error do
  @moduledoc """
  Exception struct for ClusterLite errors.
  """

  defexception [:message, :statement]

  @impl true
  def message(%__MODULE__{message: message, statement: nil}), do: message
  def message(%__MODULE__{message: message, statement: statement}), do: "#{message} - #{statement}"
end
