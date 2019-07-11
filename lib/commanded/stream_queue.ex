defmodule Commanded.StreamQueue do
  alias Commanded.EventStore.RecordedEvent

  defstruct queue: %{}, pending: %{}

  @doc """

  """
  def new(), do: %__MODULE__{}

  @doc """

  """
  def append_events(%__MODULE__{queue: queue} = stream_queue, events) do
    queue =
      Enum.reduce(events, queue, fn %RecordedEvent{stream_id: stream_id} = event, queue ->
        Map.update(queue, stream_id, [event], &(&1 ++ [event]))
      end)

    %__MODULE__{stream_queue | queue: queue}
  end

  @doc """

  """
  def all_next(%__MODULE__{queue: queue, pending: pending} = stream_queue) do
    Enum.flat_map_reduce(queue, stream_queue, fn {stream_id, [event | events]}, stream_queue ->
      if Map.has_key?(pending, stream_id) do
        {[], stream_queue}
      else
        %__MODULE__{queue: queue, pending: pending} = stream_queue

        new_stream_queue = %__MODULE__{
          stream_queue
          | pending: Map.put(pending, stream_id, true),
            queue: update_queue(queue, stream_id, events)
        }

        {[event], new_stream_queue}
      end
    end)
  end

  defp update_queue(queue, stream_id, []), do: Map.delete(queue, stream_id)
  defp update_queue(queue, stream_id, events), do: Map.put(queue, stream_id, events)

  @doc """

  """
  def ack_event(%__MODULE__{pending: pending} = stream_queue, %RecordedEvent{stream_id: stream_id}) do
    %__MODULE__{stream_queue | pending: Map.delete(pending, stream_id)}
  end

  @doc """

  """
  def empty?(%__MODULE__{queue: queue}), do: map_size(queue) == 0
end
