defmodule GenBatchServerEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :gen_batch_server_ex,
      version: "0.1.0",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:gen_batch_server, "~> 0.8.0"}
    ]
  end
end
