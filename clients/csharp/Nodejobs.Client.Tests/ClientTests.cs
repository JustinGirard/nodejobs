using Nodejobs.Client;
using System;
using System.IO;
using Xunit;

namespace Nodejobs.Client.Tests;

public class ClientTests
{
    private static readonly bool DEBUG = Environment.GetEnvironmentVariable("CLIENT_TEST_VERBOSE") == "1";

    private static void DPrint(string line)
    {
        if (DEBUG)
        {
            Console.WriteLine(line);
        }
    }

    [Fact]
    public void ClientClassesSmoke()
    {
        var service = new ClientService(
            new ClientData(
                new Dictionary<string, object>
                {
                    ["servers"] = new Dictionary<string, object>
                    {
                        ["hello"] = new Dictionary<string, object>
                        {
                            ["transport"] = "http",
                            ["host"] = "127.0.0.1",
                            ["port"] = 8872,
                            ["base_url"] = "http://127.0.0.1:8872",
                            ["python"] = "python",
                            ["test_name"] = "csharp_http_client",
                            ["path"] = "/mcp",
                            ["commands"] = new Dictionary<string, object>(),
                        }
                    }
                }
            ),
            new ClientAuth()
        );
        Assert.NotNull(service);
    }

    [Fact]
    public void ClientLifecycle_StartWaitInvokeStop_CreatesArtifacts()
    {
        var repoRoot = FindRepoRoot();
        var py = Environment.GetEnvironmentVariable("PYTHON");
        var commands = new Dictionary<string, object>
        {
            ["now"] = new Dictionary<string, object>
            {
                ["required_params"] = new List<string> { "tz_offset" },
                ["optional_params"] = new List<string>(),
                ["transport_command"] = "now",
            }
        };
        var httpTemplate = new Dictionary<string, object>
        {
            ["host"] = "127.0.0.1",
            ["port"] = 8872,
            ["base_url"] = "http://127.0.0.1:8872",
            ["python"] = py,
            ["test_name"] = "csharp_http_case",
            ["path"] = "/cmd",
            ["launch_command"] = new List<string>
            {
                py,
                "-m",
                "nodejobs.dependencies.TestService",
                "launch",
                "host=127.0.0.1",
                "port=8872",
                "path=/cmd",
            },
            ["launch_cwd"] = repoRoot,
            ["commands"] = commands,
        };
        var mcpTemplate = new Dictionary<string, object>
        {
            ["host"] = "127.0.0.1",
            ["port"] = 8875,
            ["base_url"] = "http://127.0.0.1:8875",
            ["python"] = py,
            ["test_name"] = "csharp_mcp_case",
            ["path"] = "/mcp",
            ["launch_command"] = new List<string>
            {
                py,
                "-m",
                "nodejobs.dependencies.TestService",
                "serve",
                "transport=http",
                "host=127.0.0.1",
                "port=8875",
                "path=/mcp",
            },
            ["launch_cwd"] = repoRoot,
            ["commands"] = commands,
        };
        var cliTemplate = new Dictionary<string, object>
        {
            ["host"] = "127.0.0.1",
            ["port"] = 0,
            ["base_url"] = "http://127.0.0.1:0",
            ["python"] = py,
            ["test_name"] = "csharp_cli_case",
            ["path"] = "/unused",
            ["launch_command"] = new List<string>
            {
                py,
                "-m",
                "nodejobs.dependencies.TestService",
                "serve",
                "transport=http",
                "host=127.0.0.1",
                "port=9999",
                "path=/mcp",
            },
            ["launch_cwd"] = repoRoot,
            ["commands"] = commands,
        };
        var service = new ClientService(
            new ClientData(
                new Dictionary<string, object>
                {
                    ["servers"] = new Dictionary<string, object>
                    {
                        ["http_case"] = new Dictionary<string, object> { ["transport"] = "http", ["host"] = httpTemplate["host"], ["port"] = httpTemplate["port"], ["base_url"] = httpTemplate["base_url"], ["python"] = httpTemplate["python"], ["test_name"] = httpTemplate["test_name"], ["path"] = httpTemplate["path"], ["launch_command"] = httpTemplate["launch_command"], ["launch_cwd"] = httpTemplate["launch_cwd"], ["commands"] = httpTemplate["commands"] },
                        ["mcp_case"] = new Dictionary<string, object> { ["transport"] = "mcp", ["host"] = mcpTemplate["host"], ["port"] = mcpTemplate["port"], ["base_url"] = mcpTemplate["base_url"], ["python"] = mcpTemplate["python"], ["test_name"] = mcpTemplate["test_name"], ["path"] = mcpTemplate["path"], ["launch_command"] = mcpTemplate["launch_command"], ["launch_cwd"] = mcpTemplate["launch_cwd"], ["commands"] = mcpTemplate["commands"] },
                        ["cli_case"] = new Dictionary<string, object> { ["transport"] = "cli", ["host"] = cliTemplate["host"], ["port"] = cliTemplate["port"], ["base_url"] = cliTemplate["base_url"], ["python"] = cliTemplate["python"], ["test_name"] = cliTemplate["test_name"], ["path"] = cliTemplate["path"], ["launch_command"] = cliTemplate["launch_command"], ["launch_cwd"] = cliTemplate["launch_cwd"], ["commands"] = cliTemplate["commands"] },
                    }
                }
            ),
            new ClientAuth()
        );
        var cases = new List<(string alias, string mode, bool needsServer)>
        {
            ("http_case", "http", true),
            ("mcp_case", "mcp", true),
            ("cli_case", "cli", false),
        };
        var offsets = new List<double> { 1.0, -2.5 };

        try
        {
            foreach (var c in cases)
            {
                if (c.needsServer)
                {
                    service.StartServer(new Dictionary<string, object> { ["server_alias"] = c.alias });
                    service.WaitReady(new Dictionary<string, object> { ["server_alias"] = c.alias, ["timeout_s"] = 8.0 });
                }
                foreach (var tzOffset in offsets)
                {
                    var outObj = service.Invoke(
                        new Dictionary<string, object>
                        {
                            ["server_alias"] = c.alias,
                            ["command_id"] = "now",
                            ["data"] = new Dictionary<string, object> { ["tz_offset"] = tzOffset }
                        }
                    );
                    var outData = DJson.TryGetKeyAt(outObj, new List<string> { "data" }, null);
                    Assert.NotNull(outData);
                    var expected = tzOffset + 10.0;
                    DPrint($"PROOF CS mode={c.mode} request: command_id=now data={{\"tz_offset\":{tzOffset}}}");
                    DPrint($"PROOF CS mode={c.mode} response: {DJson.Stringify(outObj)} expected={expected}");
                    Assert.Equal(expected, Convert.ToDouble(outData));
                    Assert.NotNull(DJson.Stringify(outObj));
                }
                if (c.needsServer)
                {
                    var spec = service.Data.GetServer(new Dictionary<string, object> { ["server_alias"] = c.alias });
                    var dbPath = Path.Combine(repoRoot, "tests", "client", "artifacts", $"client_{spec.TestName}", "job_db");
                    Assert.True(Directory.Exists(dbPath));
                    service.StopServer(new Dictionary<string, object> { ["server_alias"] = c.alias });
                }
            }
        }
        finally
        {
            foreach (var c in cases)
            {
                service.StopServer(new Dictionary<string, object> { ["server_alias"] = c.alias });
            }
        }
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
