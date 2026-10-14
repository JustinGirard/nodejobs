import json
from nodejobs.dependencies.BaseService import BaseService


class ClientCli:
    def __init__(self, service):
        self.service = service


class CliNetworkService(BaseService):
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
        if server.launch_command is None:
            raise Exception("launch_command_required_for_cli")
        prefix = []
        found_serve = False
        for token in server.launch_command:
            if str(token) == "serve":
                found_serve = True
                break
            prefix.append(str(token))
        if not found_serve:
            raise Exception("cli_launch_command_missing_serve")
        if len(prefix) == 0:
            raise Exception("cli_launch_command_missing_prefix")

        def _enc(v):
            if isinstance(v, str):
                return v
            return json.dumps(v)

        cmd = prefix + [str(invoke.command_id).strip()] + [f"{k}={_enc(v)}" for k, v in validated_data.items()]
        rc, out, err = await run_process(cmd, cwd=str(server.launch_cwd) if server.launch_cwd else None)
        if rc != 0:
            raise Exception(f"cli_invoke_failed:{rc}\\nstdout:\\n{out}\\nstderr:\\n{err}")
        lines = [ln.strip() for ln in out.splitlines() if len(ln.strip()) > 0]
        if len(lines) == 0:
            raise Exception("cli_empty_stdout")
        raw = lines[-1]
        try:
            return json.loads(raw)
        except Exception:
            pass
        try:
            return float(raw)
        except Exception:
            return raw

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
