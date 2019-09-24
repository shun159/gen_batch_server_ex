defmodule GenBatchServerEx do
  @moduledoc """
  A behaviour module for implementing the batch server.

  `GenBatchServerEx` is a stateful generic server similar to
  GenServer that instead of processing incoming requests
  one by one gathers them into batches before they are
  passed to the behaviour implementation.

  Batches are processed _either_ when the Erlang process mailbox has no further
  messages to batch _or_ when the number of messages in the current batch reaches the maximum batch size
  limit. `GenBatchServerEx` tries to trade latency for throughput by automatically growing the max batch
  size limit when message ingress is high and shrinks it down again as ingress
  reduces.

  This behaviour makes it suitable for use as a data sink, proxy or other kinds of
  aggregator that benefit from processing messages in batches. Examples
  would be a log writer that needs to flush messages to disk using `file:sync/1`
  without undue delay or a metrics sink that aggregates metrics from multiple
  processes and writes them to an external service. It could also be beneficial
  to use `GenBatchServerEx` to proxy a bunch of processes that want to update
  some resource (such as a `dets` table) that doesn't handle casts.

  Let's start with a code example and then explore the available callbacks.
  Imagine we want a GenBatchServerEx that works like a stack, allowing us to push and pop elements:

        defmodule Stack do
          use GenBatchServerEx

          def init(stack) do
            {:ok, stack}
          end

          def handle_batch(batch, state) do
            do_handle_batch([], batch, state)
          end

          # private functions

          defp do_handle_batch(actions, [], state),
            do: {:ok, actions, state}

          defp do_handle_batch(actions, [{:cast, {:push, element}} | rest], state),
            do: do_handle_batch(actions, rest, [element | state])

          defp do_handle_batch(actions, [{:call, from, :pop} | rest], [head | tail]),
            do: do_handle_batch([{:reply, from, head} | actions], rest, tail)
        end

        # Start the server
        {:ok, pid} = GenBatchServerEx.start_link(Stack, [:hello])

        # This is the client
        GenBatchServerEx.call(pid, :pop)

        GenBatchServerEx.cast(pid, {:push, :world})

        GenBatchServerEx.call(pid, :pop)

  We start our `Stack` by calling `start_link/2`, passing the module
  with the server implementation and its initial argument (a list
  representing the stack containing the element `:hello`). We can primarily
  interact with the server by sending two types of messages. **call**
  messages expect a reply from the server (and are therefore synchronous)
  while **cast** messages do not.
  """

  @typedoc "Debug options supported by the `start*` functions"
  @type debug :: [:trace | :log | :statistics | {:log_to_file, Path.t()}]

  @typedoc "The GenBatchServerEx name"
  @type name :: atom | {:global, term} | {:via, module, term}

  @type option ::
          {:debug, debug}
          | {:min_batch_size, non_neg_integer}
          | {:max_batch_size, non_neg_integer}
          | {:name, name}
          | {:timeout, timeout}
          | {:spawn_opt, Process.spawn_opt()}
          | {:hibernate_after, timeout}

  @type on_start :: {:ok, pid} | :ignore | {:error, {:already_started, pid} | term}

  @type server :: pid | name | {atom, node}

  @type from :: {pid, tag :: term}

  @type op ::
          {:cast, pid, user_op :: any}
          | {:call, from, user_op :: any}
          | {:info, user_op :: any}

  @type action :: {:reply, from, msg :: any}

  @callback init(init_arg :: term) ::
              {:ok, state}
              | {:stop, reason :: any}
            when state: any

  @callback handle_batch([op], state) ::
              {:ok, state}
              | {:ok, [action], state}
              | {:stop, reason :: any}
            when state: any

  @callback terminate(reason :: any, state) :: any when state: any

  @callback format_status(state) :: any when state: any

  @optional_callbacks handle_batch: 2,
                      terminate: 2,
                      format_status: 1

  @doc false
  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      if Module.get_attribute(__MODULE__, :doc) == nil do
        @doc """
        Returns a specification to start this module under a supervisor.
        See `Supervisor`.
        """
      end

      def child_spec(init_arg) do
        default = %{id: __MODULE__, start: {__MODULE__, :start_link, [init_arg]}}
        Supervisor.child_spec(default, unquote(Macro.escape(opts)))
      end

      defoverridable child_spec: 1

      @doc false
      def handle_batch(batch, state) do
        proc =
          case Process.info(self(), :registered_name) do
            {_, []} -> self()
            {_, name} -> name
          end

        # We do this to trick Dialyzer to not complain about non-local returns.
        case :erlang.phash2(1, 1) do
          0 ->
            raise "attempted to cast GenBatchServerEx #{inspect(proc)} but no handle_batch/2 clause was provided"

          1 ->
            {:stop, {:bad_batch, batch}, state}
        end
      end

      @doc false
      def terminate(_reason, _state) do
        :ok
      end

      defoverridable handle_batch: 2, terminate: 2
    end
  end

  defmacro __before_compile__(env) do
    unless Module.defines?(env.module, {:init, 1}) do
      message = """
      function init/1 required by behaviour GenBatchServerEx is not implemented \
      (in module #{inspect(env.module)}).
      We will inject a default implementation for now:

          def init(init_arg) do
            {:ok, init_arg}
          end

      You can copy the implementation above or define your own that converts \
      the arguments given to GenBatchServerEx.start_link/3 to the server state.
      """

      IO.warn(message, Macro.Env.stacktrace(env))

      quote do
        @doc false
        def init(init_arg) do
          {:ok, init_arg}
        end

        defoverridable init: 1
      end
    end
  end

  @spec start_link(module, any, [option]) :: on_start
  def start_link(module, init_arg, options \\ [])

  def start_link(module, init_arg, options)
      when is_atom(module) and is_list(options) do
    do_start(:link, module, init_arg, put_default_options(options))
  end

  @spec start(module, any, [option]) :: on_start
  def start(module, init_arg, options \\ [])

  def start(module, init_arg, options)
      when is_atom(module) and is_list(options) do
    do_start(:nolink, module, init_arg, put_default_options(options))
  end

  @default_min_batch_size 32
  @default_max_batch_size 8192

  defp put_default_options(options) do
    options
    |> Keyword.put_new(:min_batch_size, @default_min_batch_size)
    |> Keyword.put_new(:max_batch_size, @default_max_batch_size)
  end

  defp do_start(link, module, init_arg, options) do
    case Keyword.pop(options, :name) do
      {nil, opts} ->
        :gen.start(:gen_batch_server, link, module, init_arg, opts)

      {atom, opts} when is_atom(atom) ->
        :gen.start(:gen_batch_server, link, {:local, atom}, module, init_arg, opts)

      {{:global, _term} = tuple, opts} ->
        :gen.start(:gen_batch_server, link, tuple, module, init_arg, opts)

      {{:via, via_module, _term} = tuple, opts} when is_atom(via_module) ->
        :gen.start(:gen_batch_server, link, tuple, module, init_arg, opts)

      {other, _} ->
        raise ArgumentError, """
        expected :name option to be one of the following:

          * nil
          * atom
          * {:global, term}
          * {:via, module, term}

        Got: #{inspect(other)}
        """
    end
  end

  @spec stop(server, reason :: term, timeout) :: :ok
  def stop(server, reason \\ :normal, timeout \\ :infinity)

  def stop(server, reason, timeout) do
    case whereis(server) do
      nil ->
        exit({:noproc, {__MODULE__, :stop, [server, reason, timeout]}})

      pid when pid == self() ->
        exit({:calling_self, {__MODULE__, :stop, [server, reason, timeout]}})

      pid ->
        try do
          :proc_lib.stop(pid, reason, timeout)
        catch
          :exit, err ->
            exit({err, {__MODULE__, :stop, [server, reason, timeout]}})
        end
    end
  end

  @spec call(server, term, timeout) :: term
  def call(server, request, timeout \\ 5000)
      when (is_integer(timeout) and timeout >= 0) or timeout == :infinity do
    case whereis(server) do
      nil ->
        exit({:noproc, {__MODULE__, :call, [server, request, timeout]}})

      pid when pid == self() ->
        exit({:calling_self, {__MODULE__, :call, [server, request, timeout]}})

      pid ->
        try do
          :gen.call(pid, :"$gen_call", request, timeout)
        catch
          :exit, reason ->
            exit({reason, {__MODULE__, :call, [server, request, timeout]}})
        else
          {:ok, res} -> res
        end
    end
  end

  @spec cast(server, term) :: :ok
  def cast(server, request)

  def cast({:global, name}, request) do
    :global.send(name, cast_msg(request))
    :ok
  catch
    _, _ -> :ok
  end

  def cast({:via, mod, name}, request) do
    mod.send(name, cast_msg(request))
    :ok
  catch
    _, _ -> :ok
  end

  def cast({name, node}, request) when is_atom(name) and is_atom(node),
    do: do_send({name, node}, cast_msg(request))

  def cast(dest, request) when is_atom(dest) or is_pid(dest),
    do: do_send(dest, cast_msg(request))

  defp cast_msg(req),
    do: {:"$gen_cast", req}

  @spec cast_batch(server, term) :: :ok
  def cast_batch({:global, name}, batch) when is_list(batch) do
    :global.send(name, cast_batch_msg(batch))
    :ok
  catch
    _, _ -> :ok
  end

  def cast_batch({:via, mod, name}, batch) when is_list(batch) do
    mod.send(name, cast_batch_msg(batch))
  catch
    _, _ -> :ok
  end

  def cast_batch({name, node}, request) when is_atom(name) and is_atom(node),
    do: do_send({name, node}, cast_batch_msg(request))

  def cast_batch(dest, request) when is_atom(dest) or is_pid(dest),
    do: do_send(dest, cast_batch_msg(request))

  defp cast_batch_msg(msgs),
    do: {:"$gen_cast_batch", msgs, length(msgs)}

  defp do_send(dest, msg) do
    send(dest, msg)
    :ok
  catch
    _, _ -> :ok
  end

  @doc """
  Returns the `pid` or `{name, node}` of a GenServer process, or `nil` if
  no process is associated with the given `server`.
  ## Examples
  For example, to lookup a server process, monitor it and send a cast to it:
      process = GenServer.whereis(server)
      monitor = Process.monitor(process)
      GenServer.cast(process, :hello)
  """
  @spec whereis(server) :: pid | {atom, node} | nil
  def whereis(server)

  def whereis(pid) when is_pid(pid),
    do: pid

  def whereis(name) when is_atom(name),
    do: Process.whereis(name)

  def whereis({:global, name}) do
    case :global.whereis_name(name) do
      pid when is_pid(pid) -> pid
      :undefined -> nil
    end
  end

  def whereis({:via, mod, name}) do
    case apply(mod, :whereis_name, [name]) do
      pid when is_pid(pid) -> pid
      :undefined -> nil
    end
  end

  def whereis({name, local}) when is_atom(name) and local == node(),
    do: Process.whereis(name)

  def whereis({name, node} = server) when is_atom(name) and is_atom(node),
    do: server
end
