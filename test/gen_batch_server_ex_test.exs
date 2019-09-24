defmodule GenBatchServerExTest do
  use ExUnit.Case, async: true

  defmodule Stack do
    use GenBatchServerEx

    def init(stack) do
      {:ok, stack}
    end

    def handle_batch(commands, state) do
      handle_commands([], commands, state)
    end

    def terminate(_reason, _state) do
      # There is a race condition if the agent is
      # restarted too fast and it is registered.
      self()
      |> Process.info(:registered_name)
      |> elem(1)
      |> Process.unregister()

      :ok
    rescue
      _ -> :ok
    end

  # private functions

    defp handle_commands(actions, [], state),
      do: {:ok, actions, state}

    defp handle_commands(actions, [command | rest], state) do
      case command do
        {:cast, {:push, element}} ->
          handle_commands(actions, rest, [element | state])

        {:call, from, :pop} ->
          handle_commands([{:reply, from, hd(state)} | actions], rest, tl(state))

        {:call, _from, :noreply} ->
          handle_commands(actions, rest, state)

        {:call, from, :stop_self} ->
          reason = catch_exit(GenBatchServerEx.stop(self()))
          action = {:reply, from, reason}
          handle_commands([action | actions], rest, state)
      end
    end
  end

  test "generates child_spec/1" do
    assert Stack.child_spec([:hello]) == %{
             id: Stack,
             start: {Stack, :start_link, [[:hello]]}
           }

    defmodule CustomStack do
      use GenBatchServerEx,
        id: :id,
        restart: :temporary,
        shutdown: :infinity,
        start: {:foo, :bar, []}

      def init(args) do
        {:ok, args}
      end
    end

    assert CustomStack.child_spec([:hello]) == %{
             id: :id,
             restart: :temporary,
             shutdown: :infinity,
             start: {:foo, :bar, []}
           }
  end

  test "start_link/3" do
    assert_raise ArgumentError, ~r"expected :name option to be one of the following:", fn ->
      GenBatchServerEx.start_link(Stack, [:hello], name: "my_gen_server_name")
    end

    assert_raise ArgumentError, ~r"expected :name option to be one of the following:", fn ->
      GenBatchServerEx.start_link(Stack, [:hello], name: {:invalid_tuple, "my_gen_server_name"})
    end

    assert_raise ArgumentError, ~r"expected :name option to be one of the following:", fn ->
      GenBatchServerEx.start_link(Stack, [:hello], name: {:via, "Via", "my_gen_server_name"})
    end

    assert_raise ArgumentError, ~r/Got: "my_gen_server_name"/, fn ->
      GenBatchServerEx.start_link(Stack, [:hello], name: "my_gen_server_name")
    end
  end

  test "start_link/3 with via" do
    GenBatchServerEx.start_link(Stack, [:hello], name: {:via, :global, :via_stack})
    assert GenBatchServerEx.call({:via, :global, :via_stack}, :pop) == :hello
  end

  test "start_link/3 with global" do
    GenBatchServerEx.start_link(Stack, [:hello], name: {:global, :global_stack})
    assert GenBatchServerEx.call({:global, :global_stack}, :pop) == :hello
  end

  test "start_link/3 with local" do
    GenBatchServerEx.start_link(Stack, [:hello], name: :stack)
    assert GenBatchServerEx.call(:stack, :pop) == :hello
  end

  test "start_link/2, call/2, cast/2 and cast_batch/2" do
    {:ok, pid} = GenBatchServerEx.start_link(Stack, [:hello])

    {:links, links} = Process.info(self(), :links)
    assert pid in links

    assert GenBatchServerEx.call(pid, :pop) == :hello
    assert GenBatchServerEx.cast(pid, {:push, :world}) == :ok
    assert GenBatchServerEx.cast_batch(pid, [{:cast, {:push, :world}}]) == :ok
    assert GenBatchServerEx.call(pid, :pop) == :world
    assert GenBatchServerEx.stop(pid) == :ok

    assert GenBatchServerEx.cast({:global, :foo}, {:push, :world}) == :ok
    assert GenBatchServerEx.cast({:via, :foo, :bar}, {:push, :world}) == :ok
    assert GenBatchServerEx.cast(:foo, {:push, :world}) == :ok

    assert GenBatchServerEx.cast_batch({:global, :foo}, [{:cast, {:push, :world}}]) == :ok
    assert GenBatchServerEx.cast_batch({:via, :foo, :bar}, [{:cast, {:push, :world}}]) == :ok
    assert GenBatchServerEx.cast_batch(:foo, [{:cast, {:push, :world}}]) == :ok
  end

  @tag capture_log: true
  test "call/3 exit messages" do
    name = :self
    Process.register(self(), name)
    :global.register_name(name, self())
    {:ok, pid} = GenBatchServerEx.start_link(Stack, [:hello])
    {:ok, stopped_pid} = GenBatchServerEx.start(Stack, [:hello])
    GenBatchServerEx.stop(stopped_pid)

    assert catch_exit(GenBatchServerEx.call(name, :pop, 5000)) ==
             {:calling_self, {GenBatchServerEx, :call, [name, :pop, 5000]}}

    assert catch_exit(GenBatchServerEx.call({:global, name}, :pop, 5000)) ==
             {:calling_self, {GenBatchServerEx, :call, [{:global, name}, :pop, 5000]}}

    assert catch_exit(GenBatchServerEx.call({:via, :global, name}, :pop, 5000)) ==
             {:calling_self, {GenBatchServerEx, :call, [{:via, :global, name}, :pop, 5000]}}

    assert catch_exit(GenBatchServerEx.call(self(), :pop, 5000)) ==
             {:calling_self, {GenBatchServerEx, :call, [self(), :pop, 5000]}}

    assert catch_exit(GenBatchServerEx.call(pid, :noreply, 1)) ==
             {:timeout, {GenBatchServerEx, :call, [pid, :noreply, 1]}}

    assert catch_exit(GenBatchServerEx.call(nil, :pop, 5000)) ==
             {:noproc, {GenBatchServerEx, :call, [nil, :pop, 5000]}}

    assert catch_exit(GenBatchServerEx.call(stopped_pid, :pop, 5000)) ==
             {:noproc, {GenBatchServerEx, :call, [stopped_pid, :pop, 5000]}}

    assert catch_exit(GenBatchServerEx.call({:stack, :bogus_node}, :pop, 5000)) ==
             {{:nodedown, :bogus_node},
              {GenBatchServerEx, :call, [{:stack, :bogus_node}, :pop, 5000]}}
  end

  test "nil name" do
    {:ok, pid} = GenBatchServerEx.start_link(Stack, [:hello], name: nil)
    assert Process.info(pid, :registered_name) == {:registered_name, []}
  end

  test "start/2" do
    {:ok, pid} = GenBatchServerEx.start(Stack, [:hello])
    {:links, links} = Process.info(self(), :links)
    refute pid in links
    GenBatchServerEx.stop(pid)
  end

  test "whereis/1" do
    name = :whereis_server

    {:ok, pid} = GenBatchServerEx.start_link(Stack, [], name: name)
    assert GenBatchServerEx.whereis(name) == pid
    assert GenBatchServerEx.whereis({name, node()}) == pid
    assert GenBatchServerEx.whereis({name, :another_node}) == {name, :another_node}
    assert GenBatchServerEx.whereis(pid) == pid
    assert GenBatchServerEx.whereis(:whereis_bad_server) == nil

    {:ok, pid} = GenBatchServerEx.start_link(Stack, [], name: {:global, name})
    assert GenBatchServerEx.whereis({:global, name}) == pid
    assert GenBatchServerEx.whereis({:global, :whereis_bad_server}) == nil
    assert GenBatchServerEx.whereis({:via, :global, name}) == pid
    assert GenBatchServerEx.whereis({:via, :global, :whereis_bad_server}) == nil
  end

  test "stop/3", %{test: name} do
    {:ok, pid} = GenBatchServerEx.start(Stack, [])
    assert GenBatchServerEx.stop(pid, :normal) == :ok

    stopped_pid = pid

    assert catch_exit(GenBatchServerEx.stop(stopped_pid)) ==
             {:noproc, {GenBatchServerEx, :stop, [stopped_pid, :normal, :infinity]}}

    assert catch_exit(GenBatchServerEx.stop(nil)) ==
             {:noproc, {GenBatchServerEx, :stop, [nil, :normal, :infinity]}}

    {:ok, pid} = GenBatchServerEx.start(Stack, [])

    assert GenBatchServerEx.call(pid, :stop_self) ==
             {:calling_self, {GenBatchServerEx, :stop, [pid, :normal, :infinity]}}

    {:ok, _} = GenBatchServerEx.start(Stack, [], name: name)
    assert GenBatchServerEx.stop(name, :normal) == :ok
  end
end
