namespace Nodejobs.Client;
using System.Diagnostics;
using System.Net.Http;
using System.Text;
using System.Globalization;

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
        public string Path { get; set; }
    }

    private string GetArtifactRoot(string testName)
    {
        var root = Path.Combine(FindRepoRoot(), "tests", "client", "artifacts", $"client_{testName}");
        if (Directory.Exists(root)) Directory.Delete(root, true);
        Directory.CreateDirectory(root);
        return root;
    }

    private (int ExitCode, string StdOut, string StdErr) RunLifecycleService(
        string python,
        List<string> args,
        string workingDirectory,
        Dictionary<string, string>? extraEnv = null
    )
    {
        var joined = string.Join(" ", args.ConvertAll(QuoteArg));
        return RunProcess(python, $"-m nodejobs.client.lifecycle_ops {joined}", workingDirectory, extraEnv);
    }

    private static string QuoteArg(string arg)
    {
        var v = arg.Replace("\\", "\\\\").Replace("\"", "\\\"");
        return $"\"{v}\"";
    }

    public ClientService(ClientData data, ClientAuth auth)
    {
        Data = data;
        Auth = auth;
    }

    public Dictionary<string, string> StartServer(Dictionary<string, object> payload)
    {
        var aliasPayload = new ServerAliasSpec(payload);
        var alias = aliasPayload.ServerAlias;
        var spec = Data.GetServer(payload);
        var repoRoot = FindRepoRoot();
        var root = GetArtifactRoot(spec.TestName);
        var dbPath = Path.Combine(root, "job_db");
        var jobId = $"__CLIENT_LIFECYCLE__{spec.TestName}__{alias}__{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
        var py = spec.Python;
        if (spec.LaunchCommand == null)
        {
            throw new Exception("launch_command_required");
        }
        var commandJson = DJson.Stringify(spec.LaunchCommand);
        var runArgs = new List<string>
        {
            "--action", "start",
            "--db-path", dbPath,
            "--command-json", commandJson
        };
        if (!string.IsNullOrWhiteSpace(spec.LaunchCwd))
        {
            runArgs.Add("--cwd");
            runArgs.Add(spec.LaunchCwd);
        }
        var res = RunLifecycleService(
            py,
            runArgs,
            repoRoot,
            new Dictionary<string, string>
            {
                ["NODEJOBS_LIFECYCLE_JOB_ID"] = jobId
            }
        );
        if (res.ExitCode != 0) throw new Exception(res.StdErr + res.StdOut);
        _runtimeByAlias[alias] = new RuntimeState { DbPath = dbPath, JobId = jobId, Host = spec.Host, Port = spec.Port, Path = spec.Path };
        return new Dictionary<string, string> { { "status", "ok" }, { "job_id", jobId } };
    }

    public Dictionary<string, string> WaitReady(Dictionary<string, object> payload)
    {
        var waitPayload = new WaitReadySpec(payload);
        var alias = waitPayload.ServerAlias;
        var timeoutS = waitPayload.TimeoutS;
        if (!_runtimeByAlias.TryGetValue(alias, out var runtime))
        {
            throw new Exception($"server_not_started:{alias}");
        }
        var spec = Data.GetServer(new Dictionary<string, object> { [ServerAliasSpec.f_server_alias] = alias });
        var res = RunLifecycleService(
            spec.Python,
            new List<string>
            {
                "--action", "wait",
                "--db-path", runtime.DbPath,
                "--job-id", runtime.JobId,
                "--host", runtime.Host,
                "--port", runtime.Port.ToString(),
                "--timeout-s", timeoutS.ToString(System.Globalization.CultureInfo.InvariantCulture),
            },
            FindRepoRoot()
        );
        if (res.ExitCode != 0)
        {
            throw new Exception(res.StdErr + res.StdOut);
        }
        return new Dictionary<string, string> { { "status", "ok" } };
    }

    public Dictionary<string, object> Invoke(Dictionary<string, object> payload)
    {
        var validated = Data.ValidateInvoke(payload);
        var invoke = validated.invoke;
        var server = validated.server;
        var command = validated.command;
        var body = validated.validatedData;
        var transport = server.Transport.Trim().ToLowerInvariant();
        var wireCommand = string.IsNullOrWhiteSpace(command.TransportCommand) ? invoke.CommandId : command.TransportCommand;
        var endpoint = wireCommand.TrimStart('/');
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
            var request = new HttpRequestMessage(HttpMethod.Post, $"{baseUrl}{route}")
            {
                Content = content
            };
            var response = _http.SendAsync(request).GetAwaiter().GetResult();
            var raw = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
            var code = (int)response.StatusCode;
            if (code != 200 && code != 201)
            {
                throw new Exception($"{code}:{raw}");
            }
            var trimmed = raw.Trim();
            if (trimmed.StartsWith("{"))
            {
                return DJson.Parse(trimmed);
            }
            if (double.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out var n))
            {
                return new Dictionary<string, object> { ["data"] = n };
            }
            return new Dictionary<string, object> { ["data"] = trimmed };
        }

        if (transport == "mcp")
        {
            var py = server.Python;
            var mcpReq = DJson.Stringify(body);
            var escapedReq = mcpReq.Replace("\"", "\\\"");
            var mcp = RunProcess(py, $"-m nodejobs.client.mcp_invoke --base-url \"{baseUrl}\" --path \"{server.Path}\" --tool \"{invoke.CommandId}\" --data-json \"{escapedReq}\" --timeout-s 8", FindRepoRoot());
            if (mcp.ExitCode != 0) throw new Exception(mcp.StdErr + mcp.StdOut);
            var mcpObj = DJson.Parse(mcp.StdOut);
            var mcpData = DJson.TryGetKeyAt(mcpObj, new List<string> { "data" }, null);
            if (mcpData == null) throw new Exception("mcp_response_missing_data");
            if (mcpData is Dictionary<string, object> mcpDataObj)
            {
                return mcpDataObj;
            }
            return new Dictionary<string, object>
            {
                ["data"] = mcpData
            };
        }

        if (transport == "cli")
        {
            if (server.LaunchCommand == null) throw new Exception("launch_command_required_for_cli");
            var serveIndex = server.LaunchCommand.FindIndex(x => string.Equals(x, "serve", StringComparison.Ordinal));
            if (serveIndex < 0) throw new Exception("cli_launch_command_missing_serve");
            var prefix = server.LaunchCommand.GetRange(0, serveIndex);
            if (prefix.Count == 0) throw new Exception("cli_launch_command_missing_prefix");
            var args = new List<string> { invoke.CommandId };
            foreach (var kv in body)
            {
                var value = kv.Value is string s ? s : DJson.Stringify(kv.Value);
                args.Add($"{kv.Key}={value}");
            }
            var fileName = prefix[0];
            var allArgs = string.Join(" ", prefix.Skip(1).Concat(args).Select(QuoteArg));
            var cli = RunProcess(fileName, allArgs, string.IsNullOrWhiteSpace(server.LaunchCwd) ? FindRepoRoot() : server.LaunchCwd);
            if (cli.ExitCode != 0) throw new Exception($"cli_invoke_failed:{cli.ExitCode}\nstdout:\n{cli.StdOut}\nstderr:\n{cli.StdErr}");
            var lines = cli.StdOut.Split('\n').Select(x => x.Trim()).Where(x => x.Length > 0).ToList();
            if (lines.Count == 0) throw new Exception("cli_empty_stdout");
            var rawOut = lines[^1];
            if (double.TryParse(rawOut, out var n))
            {
                return new Dictionary<string, object> { ["data"] = n };
            }
            return new Dictionary<string, object> { ["data"] = rawOut };
        }
        throw new Exception($"unsupported_transport:{transport}");
    }

    public Dictionary<string, string> StopServer(Dictionary<string, object> payload)
    {
        var aliasPayload = new ServerAliasSpec(payload);
        var alias = aliasPayload.ServerAlias;
        if (!_runtimeByAlias.ContainsKey(alias))
        {
            return new Dictionary<string, string> { { "status", "ok" } };
        }
        var runtime = _runtimeByAlias[alias];
        var spec = Data.GetServer(new Dictionary<string, object> { [ServerAliasSpec.f_server_alias] = alias });
        var repoRoot = FindRepoRoot();
        var py = spec.Python;
        var res = RunLifecycleService(py, new List<string>
        {
            "--action", "stop",
            "--db-path", runtime.DbPath,
            "--job-id", runtime.JobId,
            "--wait-time", "1",
        }, repoRoot);
        _runtimeByAlias.Remove(alias);
        if (res.ExitCode != 0) throw new Exception(res.StdErr + res.StdOut);
        return new Dictionary<string, string> { { "status", "ok" } };
    }

    private static (int ExitCode, string StdOut, string StdErr) RunProcess(
        string fileName,
        string arguments,
        string workingDirectory,
        Dictionary<string, string>? extraEnv = null
    )
    {
        var psi = new ProcessStartInfo
        {
            FileName = fileName,
            Arguments = arguments,
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        if (extraEnv != null)
        {
            foreach (var kv in extraEnv)
            {
                psi.Environment[kv.Key] = kv.Value;
            }
        }
        using var proc = Process.Start(psi)!;
        var stdout = proc.StandardOutput.ReadToEnd();
        var stderr = proc.StandardError.ReadToEnd();
        proc.WaitForExit();
        return (proc.ExitCode, stdout, stderr);
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null)
        {
            var pyproject = Path.Combine(dir.FullName, "pyproject.toml");
            var nodejobsPkg = Path.Combine(dir.FullName, "nodejobs");
            if (File.Exists(pyproject) && Directory.Exists(nodejobsPkg))
            {
                return dir.FullName;
            }
            dir = dir.Parent;
        }
        throw new Exception("repo root not found from AppContext.BaseDirectory");
    }
}
