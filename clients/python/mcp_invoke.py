import argparse
import asyncio
import json
import time

from fastmcp import Client
from nodejobs.dependencies.BaseService import BaseService


class McpNetworkService(BaseService):
    @classmethod
    def get_command_map(cls):
        return {
            "invoke": {
                "required_args": ["invoke", "server", "command", "validated_data", "run_process"],
                "method": cls.invoke,
            }
        }

    @classmethod
    async def invoke(cls, invoke, server, command, validated_data, run_process):
        base_url = str(server.base_url).rstrip("/")
        path = str(server.path).strip()
        tool = str(invoke.command_id).strip()
        payload = await _run_once(base_url, path, tool, validated_data)
        if isinstance(payload, dict) and set(payload.keys()) == {"data"}:
            return payload["data"]
        return payload

    @classmethod
    async def dispatch_async(cls, command_id: str, **kwargs):
        command_map = cls.get_command_map()
        if command_id not in command_map:
            raise ValueError(f"Unknown command: {command_id}")
        command_info = command_map[command_id]
        required_args = command_info.get("required_args", [])
        for arg in required_args:
            assert arg in kwargs, f"Missing required argument: {arg} for command {command_id}()"
        method = command_info["method"]
        return await method(**kwargs)


async def _run_once(base_url: str, path: str, tool: str, data: dict):
    client = Client(f"{base_url.rstrip('/')}{path}")
    async with client:
        await client.ping()
        res = await client.call_tool(tool, data)
        return getattr(res, "data", res)


async def _run(base_url: str, path: str, tool: str, data: dict, timeout_s: float, poll_ms: int):
    deadline = time.time() + max(0.1, float(timeout_s))
    last_err = None
    while time.time() < deadline:
        try:
            payload = await _run_once(base_url, path, tool, data)
            return {"data": payload}
        except Exception as exc:
            last_err = exc
            await asyncio.sleep(max(0.01, float(poll_ms) / 1000.0))
    if last_err is not None:
        raise last_err
    raise RuntimeError("mcp_invoke_timeout")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--path", required=True)
    parser.add_argument("--tool", required=True)
    parser.add_argument("--data-json", required=True)
    parser.add_argument("--timeout-s", type=float, default=8.0)
    parser.add_argument("--poll-ms", type=int, default=100)
    args = parser.parse_args()

    data = json.loads(args.data_json)
    out = asyncio.run(_run(args.base_url, args.path, args.tool, data, args.timeout_s, args.poll_ms))
    print(json.dumps(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
