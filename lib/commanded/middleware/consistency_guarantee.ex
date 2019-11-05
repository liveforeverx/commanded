defmodule Commanded.Middleware.ConsistencyGuarantee do
  @moduledoc """
  A `Commanded.Middleware` that blocks after successful command dispatch until
  the requested dispatch consistency has been met.

  Only applies when the requested consistency is `:strong`. Has no effect for
  `:eventual` consistency.
  """

  @behaviour Commanded.Middleware

  require Logger

  alias Commanded.Middleware.Pipeline
  alias Commanded.Subscriptions

  import Pipeline

  def before_dispatch(%Pipeline{} = pipeline) do
    dispatcher_pid = Process.get({Commanded, :dispatcher_pid}) || self()
    Pipeline.assign(pipeline, :dispatcher_pid, dispatcher_pid)
  end

  def after_dispatch(%Pipeline{consistency: :eventual} = pipeline), do: pipeline

  def after_dispatch(%Pipeline{assigns: %{events: []}} = pipeline), do: pipeline

  def after_dispatch(%Pipeline{consistency: consistency, assigns: assigns} = pipeline) do
    %{
      aggregate_uuid: aggregate_uuid,
      aggregate_version: aggregate_version,
      dispatcher_pid: dispatcher_pid
    } = assigns

    opts = [consistency: consistency, exclude: dispatcher_pid]

    case Subscriptions.wait_for(aggregate_uuid, aggregate_version, opts) do
      :ok ->
        pipeline

      {:error, :timeout} ->
        Logger.warn(fn ->
          "Consistency timeout waiting for aggregate #{inspect(aggregate_uuid)} at version #{
            inspect(aggregate_version)
          }"
        end)

        respond(pipeline, {:error, :consistency_timeout})
    end
  end

  def after_failure(%Pipeline{} = pipeline), do: pipeline
end
