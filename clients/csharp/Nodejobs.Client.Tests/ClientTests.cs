using Nodejobs.Client;
using System;
using System.IO;
using System.Linq;
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

    private static HashSet<string> SelectedTransports()
    {
        var raw = Environment.GetEnvironmentVariable("CLIENT_TEST_TRANSPORTS");
        if (raw == null)
        {
            return new HashSet<string>(new[] { "http", "mcp", "cli" });
        }
        var parts = raw.Split(',').Select(p => p.Trim().ToLowerInvariant()).ToList();
        if (parts.Any(p => p.Length == 0))
        {
            throw new Exception("invalid_transport_filter_empty");
        }
        var allowed = new HashSet<string>(new[] { "http", "mcp", "cli" });
        var unknown = parts.Where(p => !allowed.Contains(p)).ToList();
        if (unknown.Count > 0)
        {
            throw new Exception($"invalid_transport_filter:{string.Join(",", unknown)}");
        }
        return new HashSet<string>(parts);
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
                            ["port"] = 8872,
                        }
                    }
                }
            ),
            new ClientAuth()
        );
        Assert.NotNull(service);
    }

    [Fact]
    public async Task ClientLifecycle_StartWaitInvokeStop_CreatesArtifacts()
    {
        var repoRoot = FindRepoRoot();
        var py = Environment.GetEnvironmentVariable("PYTHON");
        var tempRoot = Path.Combine(Path.GetTempPath(), $"nodejobs-client-cs-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        var commands = new Dictionary<string, object>
        {
            ["now"] = new Dictionary<string, object>
            {
                ["required_params"] = new List<string> { "tz_offset" },
                ["optional_params"] = new List<string>(),
                ["transport_command"] = "now",
            },
            ["fixture"] = new Dictionary<string, object>
            {
                ["required_params"] = new List<string> { "kind" },
                ["optional_params"] = new List<string>(),
                ["transport_command"] = "fixture",
            }
        };
        var httpTemplate = new Dictionary<string, object>
        {
            ["transport"] = "http",
            ["port"] = 8872,
            ["path"] = "/cmd",
            ["commands"] = commands,
            ["lifecycle"] = new Dictionary<string, object>
            {
                ["db_path"] = Path.Combine(tempRoot, "jobs_http.db"),
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
                ["python"] = py,
                ["launch_cwd"] = repoRoot,
            },
        };
        var mcpTemplate = new Dictionary<string, object>
        {
            ["transport"] = "mcp",
            ["port"] = 8875,
            ["path"] = "/mcp",
            ["commands"] = commands,
            ["lifecycle"] = new Dictionary<string, object>
            {
                ["db_path"] = Path.Combine(tempRoot, "jobs_mcp.db"),
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
                ["python"] = py,
                ["launch_cwd"] = repoRoot,
            },
        };
        var cliTemplate = new Dictionary<string, object>
        {
            ["transport"] = "cli",
            ["port"] = 0,
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
                        ["http_case"] = httpTemplate,
                        ["mcp_case"] = mcpTemplate,
                        ["cli_case"] = cliTemplate,
                    }
                }
            ),
            new ClientAuth()
        );
        var selected = SelectedTransports();
        var cases = new List<(string alias, string mode, bool needsServer)>
        {
            ("http_case", "http", true),
            ("mcp_case", "mcp", true),
            ("cli_case", "cli", false),
        }.Where(c => selected.Contains(c.mode)).ToList();
        if (cases.Count == 0)
        {
            throw new Exception("no_transport_cases_selected");
        }
        var offsets = new List<double> { 1.0, -2.5 };

        try
        {
            foreach (var c in cases)
            {
                if (c.needsServer)
                {
                    await service.StartServerAsync(new Dictionary<string, object> { ["server_alias"] = c.alias });
                    await service.WaitReadyAsync(new Dictionary<string, object> { ["server_alias"] = c.alias, ["timeout_s"] = 8.0 });
                }
                foreach (var tzOffset in offsets)
                {
                    var outObj = await service.InvokeAsync(
                        new Dictionary<string, object>
                        {
                            ["server_alias"] = c.alias,
                            ["command_id"] = "now",
                            ["data"] = new Dictionary<string, object> { ["tz_offset"] = tzOffset }
                        }
                    );
                    Assert.NotNull(outObj);
                    var expected = tzOffset + 10.0;
                    DPrint($"PROOF CS mode={c.mode} request: command_id=now data={{\"tz_offset\":{tzOffset}}}");
                    DPrint($"PROOF CS mode={c.mode} response: {outObj} expected={expected}");
                    Assert.Equal(expected, Convert.ToDouble(outObj));
                }
                if (c.needsServer)
                {
                    await service.StopServerAsync(new Dictionary<string, object> { ["server_alias"] = c.alias });
                }
            }

            await service.StartServerAsync(new Dictionary<string, object> { ["server_alias"] = "http_case" });
            await service.WaitReadyAsync(new Dictionary<string, object> { ["server_alias"] = "http_case", ["timeout_s"] = 8.0 });
            await service.StartServerAsync(new Dictionary<string, object> { ["server_alias"] = "mcp_case" });
            await service.WaitReadyAsync(new Dictionary<string, object> { ["server_alias"] = "mcp_case", ["timeout_s"] = 8.0 });

            var fixtureKinds = new[] { "int", "float", "string", "bool", "dict", "list", "null", "nested" };
            foreach (var kind in fixtureKinds)
            {
                var http = await service.InvokeAsync(new Dictionary<string, object>
                {
                    ["server_alias"] = "http_case",
                    ["command_id"] = "fixture",
                    ["data"] = new Dictionary<string, object> { ["kind"] = kind },
                });
                var mcp = await service.InvokeAsync(new Dictionary<string, object>
                {
                    ["server_alias"] = "mcp_case",
                    ["command_id"] = "fixture",
                    ["data"] = new Dictionary<string, object> { ["kind"] = kind },
                });
                var cli = await service.InvokeAsync(new Dictionary<string, object>
                {
                    ["server_alias"] = "cli_case",
                    ["command_id"] = "fixture",
                    ["data"] = new Dictionary<string, object> { ["kind"] = kind },
                });
                Assert.Equal(DJson.Stringify(http), DJson.Stringify(mcp));
                Assert.Equal(DJson.Stringify(http), DJson.Stringify(cli));
            }
            await service.StopServerAsync(new Dictionary<string, object> { ["server_alias"] = "mcp_case" });
            await service.StopServerAsync(new Dictionary<string, object> { ["server_alias"] = "http_case" });

            await service.StartServerAsync(new Dictionary<string, object> { ["server_alias"] = "http_case" });
            await service.WaitReadyAsync(new Dictionary<string, object> { ["server_alias"] = "http_case", ["timeout_s"] = 8.0 });
            var concurrentTasks = new List<Task<object>>();
            for (var i = 0; i < 20; i++)
            {
                concurrentTasks.Add(service.InvokeAsync(new Dictionary<string, object>
                {
                    ["server_alias"] = "http_case",
                    ["command_id"] = "fixture",
                    ["data"] = new Dictionary<string, object> { ["kind"] = "int" },
                }));
            }
            var concurrent = await Task.WhenAll(concurrentTasks);
            Assert.True(concurrent.All(item => Convert.ToInt64(item) == 3));
            await service.StopServerAsync(new Dictionary<string, object> { ["server_alias"] = "http_case" });
        }
        finally
        {
            foreach (var c in cases)
            {
                try
                {
                    await service.StopServerAsync(new Dictionary<string, object> { ["server_alias"] = c.alias });
                }
                catch
                {
                }
            }
            try
            {
                Directory.Delete(tempRoot, true);
            }
            catch
            {
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
