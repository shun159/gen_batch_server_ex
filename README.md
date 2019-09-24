# GenBatchServerEx

Elixir wrapper of `rabbitmq/gen-batch-server`

usage:

```elixir
defmodule Stack do
  use GenBatchServerEx

  def init(stack) do
    {:ok, stack}
  end

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
