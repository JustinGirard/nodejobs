# TestServiceMCPLauncher.py
from decelium_wallet.commands.BaseService import BaseService


class MPCServiceLauncher(BaseService):
    @classmethod
    def get_command_map(cls):
        return {
            "serve": {"required_args": [], "method": cls.serve},
        }

    @classmethod
    def serve(cls, transport: str = "http", host: str = "127.0.0.1", port: int = 8765, path: str = "/mcp"):
        from fastmcp import FastMCP
        from nodejobs.dependencies.TestService import TestService
        mcp = FastMCP("test-service")

        @mcp.tool(name="now")
        def now(tz_offset: float) -> float:
            return TestService.now(tz_offset=tz_offset)

        mcp.run(transport=str(transport), host=str(host), port=int(port), path=str(path))


if __name__ == "__main__":
    MPCServiceLauncher.run_cli()
    # python3 ex_mcp_service.py serve - Spins up the MCP server in the shell
