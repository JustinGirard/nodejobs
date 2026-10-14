namespace Nodejobs.Client;

public sealed class ServerAliasSpec : BaseData
{
    public const string f_server_alias = "server_alias";
    public string ServerAlias => Get<string>(f_server_alias);

    public ServerAliasSpec(Dictionary<string, object> input) : base(input) { }

    protected override Dictionary<string, Type> RequiredKeys() => new()
    {
        [f_server_alias] = typeof(string),
    };

    protected override Dictionary<string, object> DoPreProcess(Dictionary<string, object> input)
    {
        if (input.ContainsKey(f_server_alias) && input[f_server_alias] is string s)
        {
            input[f_server_alias] = s.Trim();
        }
        return input;
    }

    protected override object DoValidation(string key, object value)
    {
        if (key == f_server_alias && ((string)value).Length == 0)
        {
            throw new Exception("server_alias_required");
        }
        return value;
    }
}

public sealed class WaitReadySpec : BaseData
{
    public const string f_server_alias = "server_alias";
    public const string f_timeout_s = "timeout_s";
    public string ServerAlias => Get<string>(f_server_alias);
    public double TimeoutS => Get<double>(f_timeout_s);

    public WaitReadySpec(Dictionary<string, object> input) : base(input) { }

    protected override Dictionary<string, Type> RequiredKeys() => new()
    {
        [f_server_alias] = typeof(string),
        [f_timeout_s] = typeof(double),
    };

    protected override Dictionary<string, object> DoPreProcess(Dictionary<string, object> input)
    {
        if (input.ContainsKey(f_server_alias) && input[f_server_alias] is string s)
        {
            input[f_server_alias] = s.Trim();
        }
        if (input.ContainsKey(f_timeout_s) && input[f_timeout_s] is int i)
        {
            input[f_timeout_s] = (double)i;
        }
        return input;
    }

    protected override object DoValidation(string key, object value)
    {
        if (key == f_server_alias && ((string)value).Length == 0)
        {
            throw new Exception("server_alias_required");
        }
        if (key == f_timeout_s && (double)value <= 0)
        {
            throw new Exception("timeout_s_must_be_positive");
        }
        return value;
    }
}

public sealed class InvokeSpec : BaseData
{
    public const string f_server_alias = "server_alias";
    public const string f_command_id = "command_id";
    public const string f_data = "data";

    public string ServerAlias => Get<string>(f_server_alias);
    public string CommandId => Get<string>(f_command_id);
    public Dictionary<string, object> Data => Get<Dictionary<string, object>>(f_data);

    public InvokeSpec(Dictionary<string, object> input) : base(input) { }

    protected override Dictionary<string, Type> RequiredKeys() => new()
    {
        [f_server_alias] = typeof(string),
        [f_command_id] = typeof(string),
        [f_data] = typeof(Dictionary<string, object>),
    };

    protected override Dictionary<string, object> DoPreProcess(Dictionary<string, object> input)
    {
        if (input.ContainsKey(f_server_alias) && input[f_server_alias] is string s1)
        {
            input[f_server_alias] = s1.Trim();
        }
        if (input.ContainsKey(f_command_id) && input[f_command_id] is string s2)
        {
            input[f_command_id] = s2.Trim();
        }
        return input;
    }

    protected override object DoValidation(string key, object value)
    {
        if (key == f_server_alias && ((string)value).Length == 0)
        {
            throw new Exception("server_alias_required");
        }
        if (key == f_command_id && ((string)value).Length == 0)
        {
            throw new Exception("command_id_required");
        }
        return value;
    }
}

public sealed class CommandSpec : BaseData
{
    public const string f_required_params = "required_params";
    public const string f_optional_params = "optional_params";
    public const string f_transport_command = "transport_command";

    public List<string> RequiredParams => Get<List<string>>(f_required_params);
    public List<string> OptionalParams => GetOptional<List<string>>(f_optional_params) ?? new List<string>();
    public string? TransportCommand => GetOptional<string>(f_transport_command);

    public CommandSpec(Dictionary<string, object> input) : base(input) { }

    protected override Dictionary<string, Type> RequiredKeys() => new()
    {
        [f_required_params] = typeof(List<string>),
    };

    protected override Dictionary<string, Type> OptionalKeys() => new()
    {
        [f_optional_params] = typeof(List<string>),
        [f_transport_command] = typeof(string),
    };

    protected override object DoValidation(string key, object value)
    {
        if (key == f_required_params || key == f_optional_params)
        {
            var vals = (List<string>)value;
            foreach (var item in vals)
            {
                if (item.Trim().Length == 0)
                {
                    throw new Exception($"invalid_param_name:{item}");
                }
            }
        }
        return value;
    }
}

public sealed class ServerSpec : BaseData
{
    public const string f_transport = "transport";
    public const string f_host = "host";
    public const string f_port = "port";
    public const string f_base_url = "base_url";
    public const string f_python = "python";
    public const string f_test_name = "test_name";
    public const string f_path = "path";
    public const string f_commands = "commands";
    public const string f_launch_command = "launch_command";
    public const string f_launch_cwd = "launch_cwd";

    public string Transport => Get<string>(f_transport);
    public string Host => Get<string>(f_host);
    public int Port => Get<int>(f_port);
    public string BaseUrl => Get<string>(f_base_url);
    public string Python => Get<string>(f_python);
    public string TestName => Get<string>(f_test_name);
    public string Path => Get<string>(f_path);
    public Dictionary<string, object> Commands => Get<Dictionary<string, object>>(f_commands);
    public List<string>? LaunchCommand => GetOptional<List<string>>(f_launch_command);
    public string? LaunchCwd => GetOptional<string>(f_launch_cwd);

    public ServerSpec(Dictionary<string, object> input) : base(input) { }

    protected override Dictionary<string, Type> RequiredKeys() => new()
    {
        [f_transport] = typeof(string),
        [f_host] = typeof(string),
        [f_port] = typeof(int),
        [f_base_url] = typeof(string),
        [f_python] = typeof(string),
        [f_test_name] = typeof(string),
        [f_path] = typeof(string),
        [f_commands] = typeof(Dictionary<string, object>),
    };

    protected override Dictionary<string, Type> OptionalKeys() => new()
    {
        [f_launch_command] = typeof(List<string>),
        [f_launch_cwd] = typeof(string),
    };

    protected override Dictionary<string, object> DoPreProcess(Dictionary<string, object> input)
    {
        if (input.ContainsKey(f_transport) && input[f_transport] is string s)
        {
            input[f_transport] = s.Trim().ToLowerInvariant();
        }
        return input;
    }

    protected override object DoValidation(string key, object value)
    {
        if (key == f_transport && (value is not string s0 || s0.Length == 0)) throw new Exception("transport_required");
        if (key == f_host && (value is not string s1 || s1.Length == 0)) throw new Exception("host_required");
        if (key == f_base_url && (value is not string s2 || s2.Length == 0)) throw new Exception("base_url_required");
        if (key == f_python && (value is not string s3 || s3.Length == 0)) throw new Exception("python_required");
        if (key == f_test_name && (value is not string s4 || s4.Length == 0)) throw new Exception("test_name_required");
        if (key == f_path && (value is not string s5 || s5.Length == 0)) throw new Exception("path_required");
        return value;
    }
}

public sealed class CommandPayloadSpec : BaseData
{
    public const string f_required_params = "required_params";
    public const string f_data = "data";
    public Dictionary<string, object> Data => Get<Dictionary<string, object>>(f_data);

    public CommandPayloadSpec(Dictionary<string, object> input) : base(input) { }

    protected override Dictionary<string, Type> RequiredKeys() => new()
    {
        [f_required_params] = typeof(List<string>),
        [f_data] = typeof(Dictionary<string, object>),
    };

    protected override object DoValidation(string key, object value)
    {
        if (key == f_data)
        {
            var dirty = GetDirtyDict();
            var required = (List<string>)dirty[f_required_params];
            var data = (Dictionary<string, object>)value;
            foreach (var req in required)
            {
                if (!data.ContainsKey(req))
                {
                    throw new Exception($"missing_required_param:{req}");
                }
            }
        }
        return value;
    }
}

public sealed class ClientData : BaseData
{
    public const string f_servers = "servers";
    public Dictionary<string, object> Servers => Get<Dictionary<string, object>>(f_servers);

    public ClientData(Dictionary<string, object> input) : base(input) { }

    protected override Dictionary<string, Type> RequiredKeys() => new()
    {
        [f_servers] = typeof(Dictionary<string, object>),
    };

    public ServerSpec GetServer(Dictionary<string, object> payload)
    {
        var alias = new ServerAliasSpec(payload);
        if (!Servers.ContainsKey(alias.ServerAlias))
        {
            throw new Exception($"unknown_server_alias:{alias.ServerAlias}");
        }
        return new ServerSpec((Dictionary<string, object>)Servers[alias.ServerAlias]);
    }

    public CommandSpec GetCommand(Dictionary<string, object> payload)
    {
        var invoke = new InvokeSpec(payload);
        var server = GetServer(new Dictionary<string, object> { [ServerAliasSpec.f_server_alias] = invoke.ServerAlias });
        if (!server.Commands.ContainsKey(invoke.CommandId))
        {
            throw new Exception($"unknown_command:{invoke.ServerAlias}:{invoke.CommandId}");
        }
        return new CommandSpec((Dictionary<string, object>)server.Commands[invoke.CommandId]);
    }

    public (InvokeSpec invoke, ServerSpec server, CommandSpec command, Dictionary<string, object> validatedData) ValidateInvoke(Dictionary<string, object> payload)
    {
        var invoke = new InvokeSpec(payload);
        var server = GetServer(new Dictionary<string, object> { [ServerAliasSpec.f_server_alias] = invoke.ServerAlias });
        var command = GetCommand(payload);
        var validated = new CommandPayloadSpec(
            new Dictionary<string, object>
            {
                [CommandPayloadSpec.f_required_params] = command.RequiredParams,
                [CommandPayloadSpec.f_data] = invoke.Data,
            }
        );
        return (invoke, server, command, validated.Data);
    }
}
