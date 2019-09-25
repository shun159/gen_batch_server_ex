defmodule GenBatchServer do
  @moduledoc """
  A behaviour module for implementing the batch server.

  `GenBatchServer` is a stateful generic server similar to
  GenServer that instead of processing incoming requests
  one by one gathers them into batches before they are
  passed to the behaviour implementation.

  Batches are processed _either_ when the Erlang process mailbox has no further
  messages to batch _or_ when the number of messages in the current batch reaches the maximum batch size
  limit. `GenBatchServer` tries to trade latency for throughput by automatically growing the max batch
  size limit when message ingress is high and shrinks it down again as ingress
  reduces.

  This behaviour makes it suitable for use as a data sink, proxy or other kinds of
  aggregator that benefit from processing messages in batches. Examples
  would be a log writer that needs to flush messages to disk using `file:sync/1`
  without undue delay or a metrics sink that aggregates metrics from multiple
  processes and writes them to an external service. It could also be beneficial
  to use `GenBatchServer` to proxy a bunch of processes that want to update
  some resource (such as a `dets` table) that doesn't handle casts.

  ## How to supervise

  A `GenBatchServer` can be started under a supervision tree as well as `GenServer`.
  When we invoke `use GenBatchServer`, it automatically defines a  child_spec/1`
  function that allows us to start the `Stack` directly under a supervisor.
  To start a default stack of `[:hello]` under a supervisor, one may do:

      children = [
        {Stack, [:hello]}
      ]

      Supervisor.start_link(children, strategy: :one_for_all)

  Note you can also start it simply as `Stack`, which is the same as `{Stack, []}`:

      children = [
        Stack # The same as {Stack, []}
      ]

      Supervisor.start_link(children, strategy: :one_for_all)

  In both cases, `Stack.start_link/1` is always invoked.

  `use GenBatchServer` also accepts a list of options which configures the child
  specification and therefore how it runs under a supervisor. The generated `child_spec/1`
  can be customized with the following options:

    * `:id` - the child specification identifier, defaults to the current module
    * `:restart` - when the child should be restarted, defaults to `:permanent`
    * `:shutdown` - how to shut down the child, either immediately or by giving it time to shut down

  For example:

      use GenBatchServer, restart: :transient, shutdown: 10_000

  See the "Child specification" section in the `Supervisor` module for more detailed information.
  The `@doc` annotation immediately preceding `use GenBatchServer` will be attached to the geenrated
  `child_spec/1` function.

  ## Name registration

  Both `start_link/3` and `start/3` support the `GenBatchServer` to register
  a name on start vi  `:name` option. Registered anmes are also automatically cleaned up on
  termination. The supported values are:

    * an atom: the GenBatchServer is registered locally with the given name using `Proesss.register/2`
    * `{:global, term}` - the GenBatchServer is registered globally with the given term using
      the functions in the [`:global` module](http://www.erlang.org/doc/man/global.html).
    * `{:via, module, term}` - the GenBatchServer is registered with the given mechanism and name.
      The `:via` option expects a module that exports `registered_name/2`, `unregistered_name/1`, `whereis_name/1`
      and `send/2`. One such example is the [`:global` module](http://www.erlang.org/doc/man/global.html)
      which uses these functions for keeping the list of names of processes and their associated PIDs that are
      available globally for a network of Elixir nodes. Elixir also ships with a local, decentralized and scalable
      registery called `Registry` for locally storing names that are generated dynamically.

  For example, we could start and register our `Stack` server locally as follows:

      # Start the server and register it locally with name MyStack
      {:ok, _pid} = GenBatchServer.start_link(Stack, [:hello], name: MyStack)

      # Now messages can be sent directly to MyStack
      GenBatchServer.call(MyStack, :pop)

  Once the server is started, the remaining functions in this module (`call/3`, `cast/2` and friends)
  will also accept an atom, or any `{:global, ...}` or `{:via, ...}` tuples.
  In general, the following formats are supported:

    * a PID
    * an atom if the server is locally registered
    * `{atom, node}` if the server is locally registered at another node
    * `{:global, term}` if the server is globally registered
    * `{:via, module, name}` if the server is registered through an alternative registry

  If there is an interest to register dynamic names locally, do not use atoms, as atoms are never garbage-collected.
  For such cases, you can set up your own local registry by using the `Registry` module.

  ## Receiving "regular" messages

  As well as GenServer, the goal of a GenBatchServer is to abstract the "receive" loop for developers,
  automatically handling system messages and treating regular messages as batching.
  Therefore, you should never call your own "receive" inside the GenBatchServer callbacks
  as doing so will cause the GenBatchServer to misbehave.

  Besides the synchronous and asynchronous communication provided by `call/3` and `cast/2`,
  "regular" messages sent by functions such as `Kernel.send/2`, `Process.send/2`, Process.send_after/4`
  and similar, can be handled `{:info, any}` message inside the `c:handle_batch/2` callback.

  `{:info, any}` can be used in many situations, such as handling monitor `DOWN` messages sent by `Process.monitor/1`.
  Another use case for `{:info, any}` is to perform periodic work, with the help of `Process.send_after/4`.


      defmodule MyApp.Periodically do
        use GenBatchServer

        def start_link do
          GenBatchServer.start_link(__MODULE__, %{})
        end

        @impl true
        def init(state) do
          # Schedule work to be performed on start
          schedule_work()
          {:ok, state}
        end

        @impl true
        def handle_batch([{:info, :work}], state) do
          # Do the desired work here
          # ...

          # Reschedule once more
          schedule_work()

          {:ok, state}
        end

        defp schedule_work do
          # In 2 hours
          Process.send_after(self(), :work, 2 * 60 * 60 * 1000)
        end
      end
  """

  @typedoc "Debug options supported by the `start*` functions"
  @type debug :: [:trace | :log | :statistics | {:log_to_file, Path.t()}]

  @typedoc "The GenBatchServer name"
  @type name :: atom | {:global, term} | {:via, module, term}

  @typedoc "Option values used by the `start*` functions"
  @type option ::
          {:debug, debug}
          | {:min_batch_size, non_neg_integer}
          | {:max_batch_size, non_neg_integer}
          | {:name, name}
          | {:timeout, timeout}
          | {:spawn_opt, Process.spawn_opt()}
          | {:hibernate_after, timeout}

  @typedoc "Return values of `start*` functions"
  @type on_start :: {:ok, pid} | :ignore | {:error, {:already_started, pid} | term}

  @typedoc """
  The server reference.

  This is either a plain PID or a value representing a registered name.
  See the "Name registration" section of this document for more information.
  """
  @type server :: pid | name | {atom, node}

  @typedoc """
  Tuple describing the client of a call request.

  `pid` is the PID of the caller and `tag` is a unique term used to identify the
  call.
  """
  @type from :: {pid, tag :: term}

  @type op ::
          {:cast, pid, user_op :: any}
          | {:call, from, user_op :: any}
          | {:info, user_op :: any}

  @type action :: {:reply, from, msg :: any}

  @doc """
  Invoked when the server is started. `start_link/3` or `start/3` will
  block until it returns.

  `init_arg` is the argument term (second argument) passed to `start_link/3`.

  Returning `{:ok, state}` will cause `start_link/3` to return
  `{:ok, pid}` and the process to enter its loop.

  Returning `{:stop, reason}` will cause `start_link/3` to return
  `{:error, reason}` and the process to exit with reason `reason` without
  entering the loop or calling `c:terminate/2`.
  """
  @callback init(init_arg :: term) ::
              {:ok, state}
              | {:stop, reason :: any}
            when state: any

  @callback handle_batch([op], state) ::
              {:ok, state}
              | {:ok, [action], state}
              | {:stop, reason :: any}
            when state: any

  @doc """
  Invoked when the server is about to exit. It should do any cleanup required.

  `reason` is exit reason and `state` is the current state of the `GenBatchServer`.
  The return value is ignored.

  `c:terminate/2` is called if a callback (except `c:init/1`) does one of the
  following:

    * returns a `:stop` tuple
    * raises
    * calls `Kernel.exit/1`
    * returns an invalid value
    * the `GenBatchServer` traps exits (using `Process.flag/2`) *and* the parent
      process sends an exit signal

  If part of a supervision tree, a `GenBatchServer` will receive an exit
  signal when the tree is shutting down. The exit signal is based on
  the shutdown strategy in the child's specification, where this
  value can be:

    * `:brutal_kill`: the `GenBatchServer` is killed and so `c:terminate/2` is not called.

    * a timeout value, where the supervisor will send the exit signal `:shutdown` and
      the `GenBatchServer` will have the duration of the timeout to terminate.
      If after duration of this timeout the process is still alive, it will be killed
      immediately.

  For a more in-depth explanation, please read the "Shutdown values (:shutdown)"
  section in the `Supervisor` module.

  If the `GenBatchServer` receives an exit signal (that is not `:normal`) from any
  process when it is not trapping exits it will exit abruptly with the same
  reason and so not call `c:terminate/2`. Note that a process does *NOT* trap
  exits by default and an exit signal is sent when a linked process exits or its
  node is disconnected.

  Therefore it is not guaranteed that `c:terminate/2` is called when a `GenBatchServer`
  exits. For such reasons, we usually recommend important clean-up rules to
  happen in separated processes either by use of monitoring or by links
  themselves. There is no cleanup needed when the `GenBatchServer` controls a `port` (e.g.
  `:gen_tcp.socket`) or `t:File.io_device/0`, because these will be closed on
  receiving a `GenBatchServer`'s exit signal and do not need to be closed manually
  in `c:terminate/2`.

  If `reason` is neither `:normal`, `:shutdown`, nor `{:shutdown, term}` an error is
  logged.

  This callback is optional.
  """
  @callback terminate(reason :: any, state) :: any when state: any

  @doc """
  Invoked in some cases to retrieve a formatted version of the `GenBatchServer` status.

  This callback can be useful to control the *appearance* of the status of the
  `GenBatchServer`. For example, it can be used to return a compact representation of
  the `GenBatchServer`'s state to avoid having large state terms printed.

  * one of `:sys.get_status/1` or `:sys.get_status/2` is invoked to get the
  status of the `GenBatchServer`; in such cases, `reason` is `:normal`

  * the `GenBatchServer` terminates abnormally and logs an error; in such cases,
  `reason` is `:terminate`

  `pdict_and_state` is a two-elements list `[pdict, state]` where `pdict` is a
  list of `{key, value}` tuples representing the current process dictionary of
  the `GenBatchServer` and `state` is the current state of the `GenBatchServer`.
  """
  @callback format_status(state) :: any when state: any

  @optional_callbacks handle_batch: 2,
                      terminate: 2,
                      format_status: 1

  @doc false
  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      @behaviour GenBatchServer

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
            raise "attempted to cast GenBatchServer #{inspect(proc)} but no handle_batch/2 clause was provided"

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
      function init/1 required by behaviour GenBatchServer is not implemented \
      (in module #{inspect(env.module)}).
      We will inject a default implementation for now:

          def init(init_arg) do
            {:ok, init_arg}
          end

      You can copy the implementation above or define your own that converts \
      the arguments given to GenBatchServer.start_link/3 to the server state.
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

  @doc """
  Starts a `GenBatchServer` process linked to the current process.

  This is often used to start the `GenBatchServer` as part of a supervision tree.

  Once the server is started, the `c:init/1` function of the given `module` is
  called with `init_arg` as its argument to initialize the server. To ensure a
  synchronized start-up procedure, this function does not return until `c:init/1`
  has returned.

  Note that a `GenBatchServer` started with `start_link/3` is linked to the
  parent process and will exit in case of crashes from the parent. The GenBatchServer
  will also exit due to the `:normal` reasons in case it is configured to trap
  exits in the `c:init/1` callback.

  ## Options

    * `:min_batch_size` - the minimum batch size

    * `:max_batch_size` - the maximum batch size

    * `:name` - used for name registration as described in the "Name
      registration" section in the documentation for `GenBatchServer`

    * `:timeout` - if present, the server is allowed to spend the given number of
      milliseconds initializing or it will be terminated and the start function
      will return `{:error, :timeout}`

    * `:debug` - if present, the corresponding function in the [`:sys` module](http://www.erlang.org/doc/man/sys.html) is invoked

    * `:spawn_opt` - if present, its value is passed as options to the
      underlying process as in `Process.spawn/4`

    * `:hibernate_after` - if present, the GenBatchServer process awaits any message for
      the given number of milliseconds and if no message is received, the process goes
      into hibernation automatically (by calling `:proc_lib.hibernate/3`).

  ## Return values

  If the server is successfully created and initialized, this function returns
  `{:ok, pid}`, where `pid` is the PID of the server. If a process with the
  specified server name already exists, this function returns
  `{:error, {:already_started, pid}}` with the PID of that process.

  If the `c:init/1` callback fails with `reason`, this function returns
  `{:error, reason}`. Otherwise, if it returns `{:stop, reason}`
  or `:ignore`, the process is terminated and this function returns
  `{:error, reason}` or `:ignore`, respectively.
  """
  @spec start_link(module, any, [option]) :: on_start
  def start_link(module, init_arg, options \\ [])

  def start_link(module, init_arg, options)
      when is_atom(module) and is_list(options) do
    do_start(:link, module, init_arg, put_default_options(options))
  end

  @doc """
  Starts a `GenBatchServer` process without links (outside of a supervision tree).

  See `start_link/3` for more information.
  """
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

  @doc """
  Synchronously stops the server with the given `reason`.

  The `c:terminate/2` callback of the given `server` will be invoked before
  exiting. This function returns `:ok` if the server terminates with the
  given reason; if it terminates with another reason, the call exits.

  This function keeps OTP semantics regarding error reporting.
  If the reason is any other than `:normal`, `:shutdown` or
  `{:shutdown, _}`, an error report is logged.
  """
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

  @doc """
  Makes a synchronous call to the `gen_batch_server` and waits for the response provided
  by `Module:handle_batch/2`.
  The timeout is optional and defaults to 5000ms.
  """
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

  @doc """
  Sends an asynchronous request to the `gen_batch_server returning` `ok` immediately.
  The request tuple (`{cast, Request}`) is included in the list of operations passed
  to `Module:handle_batch/2`.
  """
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

  @doc """
  Sends an asynchronous request to the `gen_batch_server returning` `ok` immediately.
  The request tuple (`{cast, Request}`) is included in the list of operations passed
  to `Module:handle_batch/2`.
  """
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
  Returns the `pid` or `{name, node}` of a GenBatchServer process, or `nil` if
  no process is associated with the given `server`.
  ## Examples
  For example, to lookup a server process, monitor it and send a cast to it:
      process = GenBatchServer.whereis(server)
      monitor = Process.monitor(process)
      GenBatchServer.cast(process, :hello)
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
