from nodejobs.dependencies.command_registry_host import CommandRegistryHost


class TimeCommands:
    @classmethod
    def get_command_map(cls):
        return {
            "now": {"required_args": ["tz_offset"], "method": cls.now},
        }

    @classmethod
    def now(cls, tz_offset: float) -> float:
        return float(tz_offset) + 10


class FixtureCommands:
    @classmethod
    def get_command_map(cls):
        return {
            "fixture": {"required_args": ["kind"], "method": cls.fixture},
        }

    @classmethod
    def fixture(cls, kind: str):
        fixtures = {
            "int": 3,
            "float": 2.5,
            "string": "abc",
            "bool": True,
            "dict": {"a": 1, "error": "app-level"},
            "list": [1, "x", False],
            "null": None,
            "nested": {"x": [1, {"y": None}]},
        }
        if kind not in fixtures:
            raise ValueError(f"unknown_fixture_kind:{kind}")
        return fixtures[kind]


# Assembly, not inheritance: each service contributes commands to the host.
host = CommandRegistryHost({
    "host": "127.0.0.1",
    "port": "8870",
    "command_uri": "/cmd",
    "public_root": ".",
    "http_base_url": "http://127.0.0.1:8870",
    "mcp_base_url": "http://127.0.0.1:8873",
    "mcp_port": 8873,
    "mcp_path": "/mcp",
    "options": {"service_name": "test-service-composable"},
})
host.register_service(TimeCommands)
host.register_service(FixtureCommands)

# Example only; no authentication policy is installed by default.
# def example_auth_processor(*, command_id, payload):
#     if not authorized_for_request(command_id, payload):
#         return {"status": "error", "error": "unauthorized"}
#     return {"status": "ok"}
#
# host.bind_auth_processor(example_auth_processor)

if __name__ == "__main__":
    host.run_cli()
