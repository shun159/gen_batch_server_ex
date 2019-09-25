# GenBatchServer

Elixir wrapper of `rabbitmq/gen-batch-server`

usage:

```elixir
defmodule Stack do
  @moduledoc """
  Example for a very simple-minded Stack server
  """

  use GenBatchServer

  def push(element) do
    GenBatchServer.cast(__MODULE__, {:push, element})
  end

  def pop do
    GenBatchServer.call(__MODULE__, :pop)
  end

  @spec start_link() :: GenBatchServer.on_start()
  def start_link do
    GenBatchServer.start_link(__MODULE__, [:hello], name: __MODULE__)
  end

  @impl GenBatchServer
  def init(stack) do
    {:ok, stack}
  end

  @impl GenBatchServer
  def handle_batch(commands, state) do
    handle_commands([], commands, state)
  end

  # private functions

  defp handle_commands(actions, [], state),
    do: {:ok, actions, state}

  defp handle_commands(actions, [command | rest], state) do
    case command do
      {:cast, {:push, element}} ->
        handle_commands(actions, rest, [element | state])

      {:call, from, :pop} ->
        action = {:reply, from, hd(state)}
        handle_commands([action | actions], rest, tl(state))
    end
  end
end
```
