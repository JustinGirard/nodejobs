namespace Nodejobs.Client;

using System.Diagnostics;
using System.Globalization;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

public class ClientService
{
    public ClientData Data { get; }
    public ClientAuth Auth { get; }
    private readonly Dictionary<string, RuntimeState> _runtimeByAlias = new();
    private readonly HttpClient _http = new();

    private sealed class RuntimeState
    {
        public string DbPath { get; set; }
        public string JobId { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }
    }

    public ClientService(ClientData data, ClientAuth auth)
    {
        Data = data;
        Auth = auth;
    }

    public async Task<Dictionary<string, string>> StartServerAsync(Dictionary<string, object> payload)
    {
        var aliasPayload = new ServerAliasSpec(payload);
        var alias = aliasPayload.ServerAlias;
        var spec = Data.GetServer(payload);
        if (spec.Lifecycle == null)
        {
            throw new Exception("lifecycle_required_for_start");
        }
        var lifecycle = new LifecycleSpec(spec.Lifecycle);
        var dbPath = lifecycle.DbPath;
        var jobId = !string.IsNullOrWhiteSpace(lifecycle.JobId)
            ? lifecycle.JobId
            : $"__CLIENT_LIFECYCLE__{alias}__{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
        var py = !string.IsNullOrWhiteSpace(lifecycle.Python)
            ? lifecycle.Python
            : Environment.GetEnvironmentVariable("PYTHON");
        if (string.IsNullOrWhiteSpace(py))
        {
            throw new Exception("python_required_for_lifecycle");
        }

        var runArgs = new List<string>
        {
            "-m", "clients.python.lifecycle_ops",
            "--action", "start",
            "--db-path", dbPath,
            "--command-json", DJson.Stringify(lifecycle.LaunchCommand),
        };
        if (!string.IsNullOrWhiteSpace(lifecycle.LaunchCwd))
        {
            runArgs.Add("--cwd");
            runArgs.Add(lifecycle.LaunchCwd);
        }

        var extraEnv = new Dictionary<string, string>
        {
            ["NODEJOBS_LIFECYCLE_JOB_ID"] = jobId,
        };
        var res = await RunProcessAsync(py, runArgs, lifecycle.LaunchCwd, extraEnv);
        if (res.ExitCode != 0) throw new Exception(res.StdErr + res.StdOut);

        _runtimeByAlias[alias] = new RuntimeState
        {
            DbPath = dbPath,
            JobId = jobId,
            Host = spec.Host,
            Port = spec.Port,
        };
        return new Dictionary<string, string> { { "status", "ok" }, { "job_id", jobId } };
    }

    public async Task<Dictionary<string, string>> WaitReadyAsync(Dictionary<string, object> payload)
    {
        var waitPayload = new WaitReadySpec(payload);
        var alias = waitPayload.ServerAlias;
        var timeoutS = waitPayload.TimeoutS;
        if (!_runtimeByAlias.TryGetValue(alias, out var runtime))
        {
            throw new Exception($"server_not_started:{alias}");
        }
        var spec = Data.GetServer(new Dictionary<string, object> { [ServerAliasSpec.f_server_alias] = alias });
        if (spec.Lifecycle == null)
        {
            throw new Exception("lifecycle_required_for_wait");
        }
        var lifecycle = new LifecycleSpec(spec.Lifecycle);
        var py = !string.IsNullOrWhiteSpace(lifecycle.Python)
            ? lifecycle.Python
            : Environment.GetEnvironmentVariable("PYTHON");
        if (string.IsNullOrWhiteSpace(py))
        {
            throw new Exception("python_required_for_lifecycle");
        }

        var args = new List<string>
        {
            "-m", "clients.python.lifecycle_ops",
            "--action", "wait",
            "--db-path", runtime.DbPath,
            "--job-id", runtime.JobId,
            "--host", runtime.Host,
            "--port", runtime.Port.ToString(CultureInfo.InvariantCulture),
            "--timeout-s", timeoutS.ToString(CultureInfo.InvariantCulture),
        };

        var res = await RunProcessAsync(py, args, lifecycle.LaunchCwd);
        if (res.ExitCode != 0)
        {
            throw new Exception(res.StdErr + res.StdOut);
        }
        return new Dictionary<string, string> { { "status", "ok" } };
    }

    public async Task<object> InvokeAsync(Dictionary<string, object> payload)
    {
        var validated = Data.ValidateInvoke(payload);
        var invoke = validated.invoke;
        var server = validated.server;
        var command = validated.command;
        var body = validated.validatedData;
        var transport = server.Transport.Trim().ToLowerInvariant();
        var wireCommand = string.IsNullOrWhiteSpace(command.TransportCommand) ? invoke.CommandId : command.TransportCommand;
        var baseUrl = server.BaseUrl.TrimEnd('/');

        if (transport == "http")
        {
            var route = server.Path.Trim();
            if (!route.StartsWith("/")) throw new Exception("server_path_must_start_with_slash");
            var envelope = new Dictionary<string, object>
            {
                ["command_id"] = wireCommand.Trim(),
                ["data"] = body,
            };
            var json = DJson.Stringify(envelope);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            var request = new HttpRequestMessage(HttpMethod.Post, $"{baseUrl}{route}") { Content = content };
            var response = await _http.SendAsync(request);
            var raw = await response.Content.ReadAsStringAsync();
            var code = (int)response.StatusCode;
            if (code != 200 && code != 201)
            {
                throw new Exception($"{code}:{raw}");
            }
            return ParseJsonLike(raw);
        }

        if (transport == "mcp")
        {
            var route = server.Path.Trim();
            if (!route.StartsWith("/")) throw new Exception("server_path_must_start_with_slash");
            var endpoint = new Uri($"{baseUrl}{route}");
            var clientTransport = new HttpClientTransport(new HttpClientTransportOptions
            {
                Endpoint = endpoint,
                TransportMode = HttpTransportMode.StreamableHttp,
            });
            await using var mcpClient = await McpClient.CreateAsync(clientTransport);
            var mcpResult = await mcpClient.CallToolAsync(invoke.CommandId, body);
            if (mcpResult.StructuredContent.HasValue)
            {
                return DJson.ParseGeneric(mcpResult.StructuredContent.Value.GetRawText());
            }
            if (mcpResult.Content.Count == 1 && mcpResult.Content[0] is TextContentBlock textBlock)
            {
                return ParseJsonLike(textBlock.Text);
            }
            throw new Exception("mcp_response_missing_payload");
        }

        if (transport == "cli")
        {
            if (server.LaunchCommand == null) throw new Exception("launch_command_required_for_cli");
            var serveIndex = server.LaunchCommand.FindIndex(x => string.Equals(x, "serve", StringComparison.Ordinal));
            if (serveIndex < 0) throw new Exception("cli_launch_command_missing_serve");
            var prefix = server.LaunchCommand.GetRange(0, serveIndex);
            if (prefix.Count == 0) throw new Exception("cli_launch_command_missing_prefix");
            var cliArgs = new List<string> { invoke.CommandId };
            foreach (var kv in body)
            {
                var value = kv.Value is string s ? s : DJson.Stringify(kv.Value);
                cliArgs.Add($"{kv.Key}={value}");
            }
            var command = new List<string>();
            command.AddRange(prefix);
            command.AddRange(cliArgs);
            var cli = await RunProcessAsync(command[0], command.Skip(1).ToList(), server.LaunchCwd);
            if (cli.ExitCode != 0) throw new Exception($"cli_invoke_failed:{cli.ExitCode}\nstdout:\n{cli.StdOut}\nstderr:\n{cli.StdErr}");
            var lines = cli.StdOut.Split('\n').Select(x => x.Trim()).Where(x => x.Length > 0).ToList();
            if (lines.Count == 0) throw new Exception("cli_empty_stdout");
            var rawOut = lines[^1];
            return ParseJsonLike(rawOut);
        }
        throw new Exception($"unsupported_transport:{transport}");
    }

    public async Task<Dictionary<string, string>> StopServerAsync(Dictionary<string, object> payload)
    {
        var aliasPayload = new ServerAliasSpec(payload);
        var alias = aliasPayload.ServerAlias;
        if (!_runtimeByAlias.ContainsKey(alias))
        {
            return new Dictionary<string, string> { { "status", "ok" } };
        }
        var runtime = _runtimeByAlias[alias];
        var spec = Data.GetServer(new Dictionary<string, object> { [ServerAliasSpec.f_server_alias] = alias });
        if (spec.Lifecycle == null)
        {
            _runtimeByAlias.Remove(alias);
            return new Dictionary<string, string> { { "status", "ok" } };
        }
        var lifecycle = new LifecycleSpec(spec.Lifecycle);
        var py = !string.IsNullOrWhiteSpace(lifecycle.Python)
            ? lifecycle.Python
            : Environment.GetEnvironmentVariable("PYTHON");
        if (string.IsNullOrWhiteSpace(py))
        {
            throw new Exception("python_required_for_lifecycle");
        }
        var args = new List<string>
        {
            "-m", "clients.python.lifecycle_ops",
            "--action", "stop",
            "--db-path", runtime.DbPath,
            "--job-id", runtime.JobId,
            "--wait-time", "1",
        };
        var res = await RunProcessAsync(py, args, lifecycle.LaunchCwd);
        _runtimeByAlias.Remove(alias);
        if (res.ExitCode != 0) throw new Exception(res.StdErr + res.StdOut);
        return new Dictionary<string, string> { { "status", "ok" } };
    }

    public Dictionary<string, string> StartServer(Dictionary<string, object> payload) => StartServerAsync(payload).GetAwaiter().GetResult();
    public Dictionary<string, string> WaitReady(Dictionary<string, object> payload) => WaitReadyAsync(payload).GetAwaiter().GetResult();
    public object Invoke(Dictionary<string, object> payload) => InvokeAsync(payload).GetAwaiter().GetResult();
    public Dictionary<string, string> StopServer(Dictionary<string, object> payload) => StopServerAsync(payload).GetAwaiter().GetResult();

    private static object ParseJsonLike(string raw)
    {
        var t = raw.Trim();
        if (string.Equals(t, "null", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }
        try
        {
            var parsed = JsonConvert.DeserializeObject(t);
            if (parsed is JObject obj)
            {
                return DJson.Parse(obj.ToString(Formatting.None));
            }
            if (parsed is JArray arr)
            {
                return DJson.ParseGeneric(arr.ToString(Formatting.None));
            }
            if (parsed is JValue jv)
            {
                return jv.Value;
            }
            if (parsed != null)
            {
                return parsed;
            }
        }
        catch
        {
        }
        if (double.TryParse(t, NumberStyles.Float, CultureInfo.InvariantCulture, out var n))
        {
            return n;
        }
        return t;
    }

    private static async Task<(int ExitCode, string StdOut, string StdErr)> RunProcessAsync(
        string fileName,
        List<string> arguments,
        string? workingDirectory,
        Dictionary<string, string>? extraEnv = null
    )
    {
        var psi = new ProcessStartInfo
        {
            FileName = fileName,
            Arguments = string.Join(" ", arguments.Select(QuoteArg)),
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        if (!string.IsNullOrWhiteSpace(workingDirectory))
        {
            psi.WorkingDirectory = workingDirectory;
        }
        if (extraEnv != null)
        {
            foreach (var kv in extraEnv)
            {
                psi.Environment[kv.Key] = kv.Value;
            }
        }
        using var proc = Process.Start(psi)!;
        var outTask = proc.StandardOutput.ReadToEndAsync();
        var errTask = proc.StandardError.ReadToEndAsync();
        await Task.Run(() => proc.WaitForExit());
        var stdout = await outTask;
        var stderr = await errTask;
        return (proc.ExitCode, stdout, stderr);
    }

    private static string QuoteArg(string arg)
    {
        var v = arg.Replace("\\", "\\\\").Replace("\"", "\\\"");
        return $"\"{v}\"";
    }
}
