"""Opt-in authenticated CommandRegistryHost example (not started by default).

Supply EXAMPLE_ADMIN_USERNAME, EXAMPLE_ADMIN_PASSWORD and
EXAMPLE_ADMIN_TOKEN_FILE in the launch environment. Keep the HTTP server on
loopback until a protected transport is configured.
"""

import os
from pathlib import Path

from nodejobs.dependencies.BaseService import BaseService
from nodejobs.dependencies.command_registry_host import CommandRegistryHost
from nodejobs.dependencies.single_user_auth import SingleUserAuth


class ExampleCommands(BaseService):
    auth = None

    @classmethod
    def get_command_map(cls):
        return {
            "login": {"required_args": ["username", "password"], "method": cls.login},
            "profile": {"required_args": ["auth_token"], "method": cls.profile},
        }

    @classmethod
    def login(cls, username, password):
        return cls.auth.login(username, password)

    @classmethod
    def profile(cls, auth_token):
        return {"status": "ok", "username": cls.auth.username}


if __name__ == "__main__":
    class ExampleHost(CommandRegistryHost):
        pass

    ExampleCommands.auth = SingleUserAuth(
        username=os.environ["EXAMPLE_ADMIN_USERNAME"],
        password=os.environ["EXAMPLE_ADMIN_PASSWORD"],
        token_file=os.environ["EXAMPLE_ADMIN_TOKEN_FILE"],
    )
    host = ExampleHost({
        "host": "127.0.0.1", "port": "8875", "command_uri": "/cmd",
        "public_root": str(Path(__file__).resolve().parent / "auth_example_public"),
        "http_base_url": "http://127.0.0.1:8875",
        "mcp_base_url": "http://127.0.0.1:8876", "mcp_port": 8876,
        "mcp_path": "/mcp", "options": {"service_name": "auth-example"},
    })
    host.register_service(ExampleCommands)
    host.bind_auth_processor(lambda *, command_id, payload: (
        {"status": "ok"} if command_id in ("launch", "serve", "login")
        or ExampleCommands.auth.valid(payload.get("auth_token"))
        else {"status": "error", "error": "auth_required"}
    ))
    host.run_cli()
