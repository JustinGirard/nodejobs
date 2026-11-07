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
        var lifecycleDir = Path.Combine(repoRoot, "test-results", "client-tests", "lifecycle");
        Directory.CreateDirectory(lifecycleDir);

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
                ["db_path"] = Path.Combine(lifecycleDir, "jobs_lifecycle.db"),
                ["job_id"] = "__CLIENT_LIFECYCLE__http_case_cs",
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
                ["db_path"] = Path.Combine(lifecycleDir, "jobs_lifecycle.db"),
                ["job_id"] = "__CLIENT_LIFECYCLE__mcp_case_cs",
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
        var expectedFixtures = new Dictionary<string, object>
        {
            ["int"] = 3L,
            ["float"] = 2.5,
            ["string"] = "abc",
            ["bool"] = true,
            ["dict"] = new Dictionary<string, object> { ["a"] = 1L, ["error"] = "app-level" },
            ["list"] = new List<object> { 1L, "x", false },
            ["null"] = null!,
            ["nested"] = new Dictionary<string, object>
            {
                ["x"] = new List<object> { 1L, new Dictionary<string, object> { ["y"] = null! } }
            },
        };

        try
        {
            foreach (var c in cases)
            {
                if (c.needsServer)
                {
                    var pre = await service.StopServerAsync(new Dictionary<string, object> { ["server_alias"] = c.alias });
                    Assert.Equal("ok", pre["status"]);
                    var started = await service.StartServerAsync(new Dictionary<string, object> { ["server_alias"] = c.alias });
                    Assert.Equal("ok", started["status"]);
                    var ready = await service.WaitReadyAsync(new Dictionary<string, object> { ["server_alias"] = c.alias, ["timeout_s"] = 8.0 });
                    Assert.Equal("ok", ready["status"]);
                }
                foreach (var tzOffset in offsets)
                {
                    var outObj = await service.Invoke(
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
                foreach (var kv in expectedFixtures)
                {
                    var outObj = await service.Invoke(new Dictionary<string, object>
                    {
                        ["server_alias"] = c.alias,
                        ["command_id"] = "fixture",
                        ["data"] = new Dictionary<string, object> { ["kind"] = kv.Key },
                    });
                    if (kv.Key == "int")
                    {
                        Assert.True(outObj is int or long);
                        Assert.Equal(3L, Convert.ToInt64(outObj));
                    }
                    else if (kv.Key == "float")
                    {
                        Assert.True(outObj is float or double or decimal);
                        Assert.Equal(2.5, Convert.ToDouble(outObj), 8);
                    }
                    else if (kv.Key == "string")
                    {
                        Assert.IsType<string>(outObj);
                        Assert.Equal("abc", (string)outObj);
                    }
                    else if (kv.Key == "bool")
                    {
                        Assert.IsType<bool>(outObj);
                        Assert.True((bool)outObj);
                    }
                    else if (kv.Key == "null")
                    {
                        Assert.Null(outObj);
                    }
                    else
                    {
                        Assert.Equal(DJson.Stringify(kv.Value), DJson.Stringify(outObj));
                    }
                }
                if (c.needsServer)
                {
                    var stopped = await service.StopServerAsync(new Dictionary<string, object> { ["server_alias"] = c.alias });
                    Assert.Equal("ok", stopped["status"]);
                }
            }
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
            service.Close();
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
