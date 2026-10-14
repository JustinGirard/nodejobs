from nodejobs.dependencies.BaseData import BaseData
from typing import Any


class ServerAliasSpec(BaseData):
    server_alias: str

    def do_pre_process(self, in_dict):
        out = dict(in_dict)
        if ServerAliasSpec.server_alias in out and isinstance(out[ServerAliasSpec.server_alias], str):
            out[ServerAliasSpec.server_alias] = out[ServerAliasSpec.server_alias].strip()
        return out

    def do_validation(self, key, value):
        if key == ServerAliasSpec.server_alias and len(value) == 0:
            raise Exception("server_alias_required")
        return value, ""


class WaitReadySpec(BaseData):
    server_alias: str
    timeout_s: float

    def do_pre_process(self, in_dict):
        out = dict(in_dict)
        if WaitReadySpec.server_alias in out and isinstance(out[WaitReadySpec.server_alias], str):
            out[WaitReadySpec.server_alias] = out[WaitReadySpec.server_alias].strip()
        return out

    def do_validation(self, key, value):
        if key == WaitReadySpec.server_alias and len(value) == 0:
            raise Exception("server_alias_required")
        if key == WaitReadySpec.timeout_s and value <= 0:
            raise Exception("timeout_s_must_be_positive")
        return value, ""


class InvokeSpec(BaseData):
    server_alias: str
    command_id: str
    data: dict

    def do_pre_process(self, in_dict):
        out = dict(in_dict)
        if InvokeSpec.server_alias in out and isinstance(out[InvokeSpec.server_alias], str):
            out[InvokeSpec.server_alias] = out[InvokeSpec.server_alias].strip()
        if InvokeSpec.command_id in out and isinstance(out[InvokeSpec.command_id], str):
            out[InvokeSpec.command_id] = out[InvokeSpec.command_id].strip()
        return out

    def do_validation(self, key, value):
        if key == InvokeSpec.server_alias and len(value) == 0:
            raise Exception("server_alias_required")
        if key == InvokeSpec.command_id and len(value) == 0:
            raise Exception("command_id_required")
        return value, ""


class ServerSpec(BaseData):
    transport: (str, "http")
    host: (str, "127.0.0.1")
    port: int
    protocol: (str, "http")
    base_url: (str, None)
    path: (str, None)
    commands: (dict, {})
    python: (str, None)
    test_name: (str, None)
    network_service: (str, None)
    launch_command: (list, None)
    launch_cwd: (str, None)
    lifecycle: (dict, None)

    def do_pre_process(self, in_dict):
        out = dict(in_dict)
        if ServerSpec.transport in out and isinstance(out[ServerSpec.transport], str):
            out[ServerSpec.transport] = out[ServerSpec.transport].strip().lower()
        transport = str(out.get(ServerSpec.transport) or "http").strip().lower()
        if ServerSpec.path not in out or out.get(ServerSpec.path) in (None, ""):
            out[ServerSpec.path] = "/mcp" if transport == "mcp" else "/cmd"
        if ServerSpec.base_url not in out or out.get(ServerSpec.base_url) in (None, ""):
            protocol = str(out.get(ServerSpec.protocol) or "http").strip().lower()
            host = str(out.get(ServerSpec.host) or "127.0.0.1").strip()
            port = int(out.get(ServerSpec.port))
            out[ServerSpec.base_url] = f"{protocol}://{host}:{port}"
        return out

    def do_validation(self, key, value):
        if key == ServerSpec.transport and len(value) == 0:
            raise Exception("transport_required")
        if key == ServerSpec.host and len(value) == 0:
            raise Exception("host_required")
        if key == ServerSpec.base_url and value is not None and len(value) == 0:
            raise Exception("base_url_empty")
        if key == ServerSpec.path and len(value) == 0:
            raise Exception("path_required")
        if key == ServerSpec.network_service and value is not None and len(value) == 0:
            raise Exception("network_service_required_when_set")
        if key == ServerSpec.launch_command and value is not None and len(value) == 0:
            raise Exception("launch_command_empty")
        return value, ""


class LifecycleSpec(BaseData):
    db_path: str
    launch_command: list
    python: (str, None)
    launch_cwd: (str, None)
    job_id: (str, None)


class CommandSpec(BaseData):
    required_params: list
    optional_params: (list, [])
    transport_command: (str, None)

    def do_validation(self, key, value):
        if key in (CommandSpec.required_params, CommandSpec.optional_params):
            for item in value:
                if (not isinstance(item, str)) or len(item.strip()) == 0:
                    raise Exception(f"invalid_param_name:{item}")
        return value, ""


class CommandPayloadSpec(BaseData):
    required_params: list
    data: dict

    def do_validation(self, key, value):
        if key == CommandPayloadSpec.data:
            dirty = self.GetDirtyDict()
            if CommandPayloadSpec.required_params not in dirty:
                raise Exception("required_params_missing")
            required_params = dirty[CommandPayloadSpec.required_params]
            for req in required_params:
                if req not in value:
                    raise Exception(f"missing_required_param:{req}")
        return value, ""


class ClientData(BaseData):
    servers: dict

    def get_server(self, payload) -> ServerSpec:
        payload = ServerAliasSpec(payload)
        servers = self[ClientData.servers]
        if payload.server_alias not in servers:
            raise Exception(f"unknown_server_alias:{payload.server_alias}")
        return ServerSpec(servers[payload.server_alias])

    def get_command(self, payload) -> CommandSpec:
        payload = InvokeSpec(payload)
        server = self.get_server({ServerAliasSpec.server_alias: payload.server_alias})
        if isinstance(server.commands, dict) and payload.command_id in server.commands:
            return CommandSpec(server.commands[payload.command_id])
        return CommandSpec({CommandSpec.required_params: []})

    def validate_invoke(self, payload):
        payload = InvokeSpec(payload)
        server = self.get_server({ServerAliasSpec.server_alias: payload.server_alias})
        command = self.get_command(payload)
        validated = CommandPayloadSpec(
            {
                CommandPayloadSpec.required_params: command.required_params,
                CommandPayloadSpec.data: payload.data,
            }
        )
        return payload, server, command, validated.data


class InvokeResultSpec(BaseData):
    data: Any
