from nodejobs.dependencies.BaseData import BaseData
from thinhost.simple_web_server import SimpleWebServer


class CommandRegistryHost(SimpleWebServer):
    INFRA_COMMANDS = {"launch", "serve", "list_commands"}

    class tServerSpec(BaseData):
        # Bind address and HTTP endpoint.
        host: str
        port: str
        command_uri: str
        public_root: str

        # Advertised addresses; these do not change the bind address.
        http_base_url: str
        mcp_base_url: str

        # MCP endpoint and application-owned settings.
        mcp_port: int
        mcp_path: str
        options: dict

    def __init__(self, host_spec: dict = None):
        if host_spec is not None:
            # The current command runtime is classmethod-based: configuration,
            # like registrations, is shared by this host class in this process.
            type(self)._server_spec = self.tServerSpec(host_spec)

    @classmethod
    def get_server_spec(cls):
        return cls.__dict__.get("_server_spec")

    @classmethod
    def _cmd_launch(cls, **kwargs):
        spec = cls.get_server_spec()
        if spec is None:
            return super()._cmd_launch(**kwargs)
        launch_args = {
            "host": spec.host,
            "port": spec.port,
            "command_uri": spec.command_uri,
            "public_root": spec.public_root,
        }
        launch_args.update(kwargs)
        return super()._cmd_launch(**launch_args)

    @classmethod
    def bind_auth_processor(cls, processor):
        """Bind a callable(command_id, payload) returning a status envelope."""
        if processor is not None and not callable(processor):
            raise TypeError("auth_processor_must_be_callable")
        cls._auth_processor = processor

    @classmethod
    def authorize_command(cls, command_id: str, *, transport=None, auth=None, kwargs=None):
        processor = cls.__dict__.get("_auth_processor")
        if processor is None:
            return True, {}

        payload = {key: value for key, value in kwargs.items() if key != "__command"}
        result = processor(command_id=command_id, payload=payload)
        if not isinstance(result, dict) or result.get("status") not in ("ok", "error"):
            raise TypeError("auth_processor_must_return_status_envelope")
        if result["status"] == "ok":
            return True, {}
        return False, result

    @classmethod
    def register_service(cls, service_cls):
        service_map = service_cls.get_command_map()
        if not isinstance(service_map, dict):
            raise TypeError("service_command_map_must_be_dict")

        registered = dict(cls.__dict__.get("_registered_command_map", {}))
        reserved = set(super().get_command_map()) | cls.INFRA_COMMANDS
        for command_id in service_map:
            if command_id in reserved or command_id in registered:
                raise ValueError(f"duplicate_command_id:{command_id}")
        registered.update(service_map)
        cls._registered_command_map = registered

    @classmethod
    def get_domain_command_map(cls):
        return dict(cls.__dict__.get("_registered_command_map", {}))

    @classmethod
    def get_command_map(cls):
        commands = super().get_command_map()
        commands.update(
            {
                "serve": {"required_args": [], "method": cls.serve},
                "list_commands": {"required_args": [], "method": cls.list_commands},
            }
        )
        commands.update(cls.get_domain_command_map())
        return commands

    @classmethod
    def list_commands(cls):
        out = {}
        for command_id, entry in cls.get_command_map().items():
            out[command_id] = {
                "required_args": list(entry.get("required_args", [])),
                "infra": command_id in cls.INFRA_COMMANDS,
            }
        return out

    @classmethod
    def _run_command_from_mcp(cls, command_id: str, payload: dict):
        if command_id in cls.INFRA_COMMANDS:
            raise Exception(f"infra_command_not_exposed:{command_id}")
        if not isinstance(payload, dict):
            raise Exception("mcp_payload_must_be_object")
        return cls.run(__command=[command_id], **payload)

    @classmethod
    def serve(
        cls,
        transport: str = "http",
        host: str = None,
        port: int = None,
        path: str = None,
    ):
        from fastmcp import FastMCP

        spec = cls.get_server_spec()
        if spec is not None:
            host = spec.host if host is None else host
            port = spec.mcp_port if port is None else port
            path = spec.mcp_path if path is None else path
        else:
            host = "127.0.0.1" if host is None else host
            port = 8765 if port is None else port
            path = "/mcp" if path is None else path

        mcp = FastMCP(cls.__name__)

        @mcp.tool(name="list_commands")
        def list_commands() -> dict:
            return cls.list_commands()

        for command_id, entry in cls.get_command_map().items():
            if command_id in cls.INFRA_COMMANDS:
                continue
            required = list(entry.get("required_args", []))

            def _make_tool(_command_id: str, _required: list[str]):
                @mcp.tool(name=_command_id)
                def _tool(spec: dict = None, **kwargs):
                    payload = {}
                    if isinstance(spec, dict):
                        payload.update(spec)
                    payload.update(kwargs)
                    for arg in _required:
                        if arg not in payload:
                            raise Exception(f"missing_required_arg:{_command_id}.{arg}")
                    return cls._run_command_from_mcp(_command_id, payload)

                return _tool

            _make_tool(command_id, required)

        mcp.run(
            transport=str(transport),
            host=str(host),
            port=int(port),
            path=str(path),
        )
