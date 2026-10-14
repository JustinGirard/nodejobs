import httpx
from nodejobs.dependencies.BaseService import BaseService


class HttpNetworkService(BaseService):
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
        endpoint_id = command.transport_command
        if endpoint_id is None:
            endpoint_id = invoke.command_id
        path = str(server.path).strip()
        if not path.startswith("/"):
            raise Exception("server_path_must_start_with_slash")
        req = {"command_id": str(endpoint_id).strip(), "data": validated_data}
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{base_url}{path}", json=req)
        if resp.status_code not in (200, 201):
            raise Exception(f"{resp.status_code}:{resp.text}")
        return resp.json()

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
