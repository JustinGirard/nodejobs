import asyncio
import os
import sys
import tempfile
from pathlib import Path

from clients.python import (
    ClientService,
    ClientData,
    ClientAuth,
    ServerAliasSpec,
    WaitReadySpec,
    InvokeSpec,
)

DEBUG = os.environ.get("CLIENT_TEST_VERBOSE") == "1"


def dprint(msg: str):
    if DEBUG:
        print(msg, flush=True)


def _selected_transports() -> set[str]:
    raw = os.environ.get("CLIENT_TEST_TRANSPORTS")
    if raw is None:
        return {"http", "mcp", "cli"}
    parts = [p.strip().lower() for p in raw.split(",")]
    if any(len(p) == 0 for p in parts):
        raise Exception("invalid_transport_filter_empty")
    unknown = [p for p in parts if p not in {"http", "mcp", "cli"}]
    if len(unknown) > 0:
        raise Exception(f"invalid_transport_filter:{','.join(unknown)}")
    return set(parts)


def test_client_http_server_lifecycle_and_hello():
    repo_root = Path(__file__).resolve().parents[2]
    py = sys.executable
    temp_root = tempfile.TemporaryDirectory(prefix="nodejobs-client-py-")
    tmp = Path(temp_root.name)

    commands = {
        "now": {
            "required_params": ["tz_offset"],
            "optional_params": [],
            "transport_command": "now",
        },
        "fixture": {
            "required_params": ["kind"],
            "optional_params": [],
            "transport_command": "fixture",
        },
    }

    http_template = {
        "transport": "http",
        "port": 8870,
        "path": "/cmd",
        "commands": commands,
        "lifecycle": {
            "db_path": str(tmp / "jobs_http.db"),
            "launch_command": [
                py,
                "-m",
                "nodejobs.dependencies.TestService",
                "launch",
                "host=127.0.0.1",
                "port=8870",
                "path=/cmd",
            ],
            "python": py,
            "launch_cwd": str(repo_root),
        },
    }
    mcp_template = {
        "transport": "mcp",
        "port": 8873,
        "path": "/mcp",
        "commands": commands,
        "lifecycle": {
            "db_path": str(tmp / "jobs_mcp.db"),
            "launch_command": [
                py,
                "-m",
                "nodejobs.dependencies.TestService",
                "serve",
                "transport=http",
                "host=127.0.0.1",
                "port=8873",
                "path=/mcp",
            ],
            "python": py,
            "launch_cwd": str(repo_root),
        },
    }
    cli_template = {
        "transport": "cli",
        "port": 0,
        "path": "/unused",
        "launch_command": [
            py,
            "-m",
            "nodejobs.dependencies.TestService",
            "serve",
            "transport=http",
            "host=127.0.0.1",
            "port=9999",
            "path=/mcp",
        ],
        "launch_cwd": str(repo_root),
        "commands": commands,
    }
    data = ClientData(
        {
            "servers": {
                "http_case": http_template,
                "mcp_case": mcp_template,
                "cli_case": cli_template,
            }
        }
    )
    client = ClientService(data=data, auth=ClientAuth(provider="local"))
    selected_transports = _selected_transports()
    cases = [
        ("http_case", "http", True),
        ("mcp_case", "mcp", True),
        ("cli_case", "cli", False),
    ]
    cases = [c for c in cases if c[1] in selected_transports]
    if len(cases) == 0:
        raise Exception("no_transport_cases_selected")
    offsets = [1.0, -2.5]
    expected_fixtures = {
        "int": 3,
        "float": 2.5,
        "string": "abc",
        "bool": True,
        "dict": {"a": 1, "error": "app-level"},
        "list": [1, "x", False],
        "null": None,
        "nested": {"x": [1, {"y": None}]},
    }

    try:
        for alias, mode, needs_server in cases:
            if needs_server:
                asyncio.run(client.start_server_async({ServerAliasSpec.server_alias: alias}))
                asyncio.run(client.wait_ready_async({WaitReadySpec.server_alias: alias, WaitReadySpec.timeout_s: 8.0}))
            for tz_offset in offsets:
                out = client.invoke(
                    {
                        InvokeSpec.server_alias: alias,
                        InvokeSpec.command_id: "now",
                        InvokeSpec.data: {"tz_offset": tz_offset},
                    }
                ).result(timeout=8.0)
                expected = float(tz_offset) + 10.0
                dprint(f"PROOF PY mode={mode} request: command_id=now data={{'tz_offset': {tz_offset}}}")
                dprint(f"PROOF PY mode={mode} response: {out!r} expected={expected!r}")
                assert isinstance(out, (int, float))
                assert float(out) == expected
            for kind, expected in expected_fixtures.items():
                out = client.invoke(
                    {
                        InvokeSpec.server_alias: alias,
                        InvokeSpec.command_id: "fixture",
                        InvokeSpec.data: {"kind": kind},
                    }
                ).result(timeout=8.0)
                assert out == expected
                assert type(out) is type(expected)
            if needs_server:
                asyncio.run(client.stop_server_async({ServerAliasSpec.server_alias: alias}))
    finally:
        for alias, _, _ in cases:
            try:
                asyncio.run(client.stop_server_async({ServerAliasSpec.server_alias: alias}))
            except Exception:
                pass
        try:
            client.close()
        except Exception:
            pass
        temp_root.cleanup()
