# MCP Service Guide

Top-level architecture guide: `nodejobs/docs/unified_service_guide.md`

## First MCP Server (Step-by-Step)

1) Creating Your First MCP Server

```python
# my_mcp_service.py
from decelium_wallet.commands.BaseService import BaseService

class MyMCPService(BaseService):
    @classmethod
    def get_command_map(cls):
        return {"serve": {"required_args": [], "method": cls.serve}}

    @classmethod
    def serve(cls, transport: str = "http", host: str = "127.0.0.1", port: int = 8765, path: str = "/mcp"):
        from fastmcp import FastMCP
        mcp = FastMCP("my-first-mcp")

        # Design the 'now' tool: return tz_offset + 10.0 (simple demo)
        @mcp.tool(name="now")
        def now(tz_offset: float) -> float:
            return float(tz_offset) + 10.0

        mcp.run(transport=str(transport), host=str(host), port=int(port), path=str(path))

if __name__ == "__main__":
    MyMCPService.run_cli()
```

2) Running The Server With nodejobs

```python
import sys, time
from nodejobs.jobs import Jobs, JobRecord

jobs = Jobs(db_path="./mcp_jobs_db")
job_id = "my_first_mcp_http"
cmd = [
    sys.executable, "my_mcp_service.py",
    "serve",
    "transport=http",
    "host=127.0.0.1",
    "port=8765",
    "path=/mcp",
]
jobs.run(command=cmd, job_id=job_id)

# Wait for startup
timeout = time.time() + 5
while jobs.get_status(job_id).status == JobRecord.Status.c_starting:
    assert time.time() < timeout, "Server failed to start in time"
    time.sleep(0.1)
```

3) Connecting With A Python Client

```python
import asyncio
from fastmcp import Client

async def main():
    client = Client("http://127.0.0.1:8765/mcp")
    async with client:
        await client.ping()
        res = await client.call_tool("now", {"tz_offset": 1})
        print(float(res.data))  # 11.0

asyncio.run(main())
```

4) Shutting Down Your Server

```python
jobs.stop(job_id)
```

2b) Combining Server And Job Management (MCPService)

Define a single class that:
- Exposes the MCP server via BaseService CLI (`serve`)
- Manages lifecycle via nodejobs (`start`/`stop`/`restart`/`status`/`logs`)

Use this "sb-styled" class pattern as the standard going forward.

```python
# mcp_service.py
import inspect, sys
from decelium_wallet.commands.BaseService import BaseService
from nodejobs.jobs import Jobs, JobRecord

class MCPService(BaseService):
    SERVICE_NAME = "my-first-mcp"
    DEFAULT_DB = "./mcp_jobs_db"

    # ---- CLI routing (BaseService) ----
    @classmethod
    def get_command_map(cls):
        # Only expose 'serve' as a CLI command; management happens via Python helpers below
        return {"serve": {"required_args": [], "method": cls.serve}}

    @classmethod
    def serve(cls, transport: str = "http", host: str = "127.0.0.1",
              port: int = 8765, path: str = "/mcp"):
        from fastmcp import FastMCP
        mcp = FastMCP(cls.SERVICE_NAME)

        # Example tool: 'now' returns tz_offset + 10.0
        @mcp.tool(name="now")
        def now(tz_offset: float) -> float:
            return float(tz_offset) + 10.0

        mcp.run(transport=str(transport), host=str(host), port=int(port), path=str(path))

    # ---- Helpers for building commands ----
    @classmethod
    def _script_path(cls) -> str:
        # Launch the same file this class is defined in
        return inspect.getfile(cls)

    @classmethod
    def _job_id_for(cls, host: str, port: int, name: str | None = None) -> str:
        base = (name or cls.SERVICE_NAME).replace(" ", "_").lower()
        return f"{base}_{host}_{port}"

    @classmethod
    def _serve_args(cls, transport: str, host: str, port: int, path: str) -> list[str]:
        # BaseService expects key=value pairs for args
        return ["serve",
                f"transport={transport}",
                f"host={host}",
                f"port={int(port)}",
                f"path={path}"]

    # ---- Job lifecycle (nodejobs) ----
    @classmethod
    def start(cls, *, db_path: str | None = None,
              transport: str = "http", host: str = "127.0.0.1",
              port: int = 8765, path: str = "/mcp",
              job_id: str | None = None, cwd: str | None = None) -> JobRecord:
        jobs = Jobs(db_path=db_path or cls.DEFAULT_DB)
        jid = job_id or cls._job_id_for(host, port)
        cmd = [sys.executable, cls._script_path(), *cls._serve_args(transport, host, port, path)]
        rec = jobs.run(command=cmd, job_id=jid, cwd=cwd)

        # Optionally wait for transition out of 'starting'
        return rec  # caller may poll/get_status; see `status()` below

    @classmethod
    def stop(cls, job_id: str, *, db_path: str | None = None) -> JobRecord:
        jobs = Jobs(db_path=db_path or cls.DEFAULT_DB)
        return jobs.stop(job_id)

    @classmethod
    def restart(cls, job_id: str, *, db_path: str | None = None,
                transport: str = "http", host: str = "127.0.0.1",
                port: int = 8765, path: str = "/mcp",
                cwd: str | None = None) -> JobRecord:
        cls.stop(job_id=job_id, db_path=db_path)
        return cls.start(db_path=db_path, transport=transport, host=host,
                         port=port, path=path, job_id=job_id, cwd=cwd)

    @classmethod
    def status(cls, job_id: str, *, db_path: str | None = None) -> JobRecord:
        jobs = Jobs(db_path=db_path or cls.DEFAULT_DB)
        return jobs.get_status(job_id)

    @classmethod
    def logs(cls, job_id: str, *, db_path: str | None = None) -> tuple[str, str]:
        jobs = Jobs(db_path=db_path or cls.DEFAULT_DB)
        return jobs.job_logs(job_id)

if __name__ == "__main__":
    MCPService.run_cli()
```

Usage patterns:

- Start under nodejobs (programmatic):
  ```python
  rec = MCPService.start(host="127.0.0.1", port=8765, path="/mcp")
  job_id = rec.self_id  # or recompute via MCPService._job_id_for(...)
  ```

- Check readiness:
  ```python
  from time import time, sleep
  from nodejobs.jobs import JobRecord

  timeout = time() + 5
  while MCPService.status(job_id).status == JobRecord.Status.c_starting:
      assert time() < timeout, "Server failed to start in time"
      sleep(0.1)
  ```

- Inspect logs:
  ```python
  stdout, stderr = MCPService.logs(job_id)
  print(stdout); print(stderr)
  ```

- Stop / Restart:
  ```python
  MCPService.stop(job_id)
  # or
  MCPService.restart(job_id, host="127.0.0.1", port=8765, path="/mcp")
  ```

- Run directly via CLI (no nodejobs), still through BaseService:
  - `python3 mcp_service.py serve transport=http host=127.0.0.1 port=8765 path=/mcp`

This unifies (1) service definition and (2) nodejobs lifecycle in one class while preserving the BaseService CLI.

This guide shows how to:
- Wrap an MCP server with a thin BaseService CLI
- Run and manage it under nodejobs
- Debug output via persisted logs and live streams
- Reuse the Jobs module patterns shown in `docs/class/jobs.md` and `docs/class_overview.md`

It references these files:
- Example service: `nodejobs/ex_mcp_service.py`
- Example tests: `nodejobs/ex_mcp_service_tests.py`
- Service CLI base: `/Users/computercomputer/justinops/decelium_wallet/decelium_wallet/commands/BaseService.py`
- Jobs docs: `docs/class/jobs.md`, `docs/class_overview.md`

Prereqs:
- Python 3.9+
- `fastmcp` installed (server and client)
- `nodejobs` importable (this repo)
- `BaseService` importable (decelium_wallet)


## Quick Start

- Run the example MCP service (HTTP):
  - From repo root:
    - `python3 ex_mcp_service.py serve transport=http host=127.0.0.1 port=8765 path=/mcp`

- Call it (async) via `fastmcp.Client`:

```python
import asyncio
from fastmcp import Client

async def main():
    client = Client("http://127.0.0.1:8765/mcp")
    async with client:
        await client.ping()
        res = await client.call_tool("now", {"tz_offset": 1})
        print(float(res.data))  # 11.0

asyncio.run(main())
```

- Run the same service managed by nodejobs:

```python
import sys, time
from nodejobs.jobs import Jobs, JobRecord

jobs = Jobs(db_path="./mcp_jobs_db")
job_id = "testservice_mcp_http"
cmd = [
    sys.executable, "ex_mcp_service.py",
    "serve",
    "transport=http",
    "host=127.0.0.1",
    "port=8765",
    "path=/mcp",
]
jobs.run(command=cmd, job_id=job_id)

# Wait for startup
timeout = time.time() + 5
while jobs.get_status(job_id).status == JobRecord.Status.c_starting:
    assert time.time() < timeout, "Server failed to start in time"
    time.sleep(0.1)

# …connect a client and use tools…

jobs.stop(job_id)
```


## Wrapping Your MCP Server

The `BaseService` wrapper gives you:
- A small, declarative CLI (command + key=value args)
- A stable entrypoint that nodejobs can run, log, and stop

Skeleton (adapt from `nodejobs/ex_mcp_service.py`):

```python
# my_mcp_service.py
from decelium_wallet.commands.BaseService import BaseService

class MyMCPServiceLauncher(BaseService):
    @classmethod
    def get_command_map(cls):
        return {
            "serve": {"required_args": [], "method": cls.serve},
        }

    @classmethod
    def serve(cls, transport: str = "http", host: str = "127.0.0.1", port: int = 8765, path: str = "/mcp"):
        from fastmcp import FastMCP
        # Import your application logic (pure functions you’ll expose as tools)
        from my_package.service import do_thing, get_time

        mcp = FastMCP("my-service")

        @mcp.tool(name="do_thing")
        def _do_thing(x: int) -> int:
            return do_thing(int(x))

        @mcp.tool(name="now")
        def _now(tz_offset: float) -> float:
            return get_time(float(tz_offset))

        mcp.run(transport=str(transport), host=str(host), port=int(port), path=str(path))

if __name__ == "__main__":
    MyMCPServiceLauncher.run_cli()
# Run: python3 my_mcp_service.py serve transport=http host=127.0.0.1 port=8765 path=/mcp
```

Notes:
- Define tools with `@mcp.tool` and route to your “real” functions; keep these pure where possible.
- Cast CLI strings to the right types in your service method (`int(port)`, `float(tz_offset)`).


## CLI Contract (BaseService)

How `BaseService.run_cli` parses arguments:
- Command is the first positional token(s): e.g., `serve`
- Options are key=value pairs: e.g., `host=127.0.0.1 port=8765 path=/mcp`
- Quotes around values are stripped: `"path"="/mcp"`
- Booleans `true`/`false` map to `True`/`False`
- Dotted keys and indexes become nested via `add_depth`: `cfg.db.host=...`, `items.1.name=...`
- Required args per command are enforced by `get_command_map()`

Minimal usage examples:
- `python3 my_mcp_service.py serve transport=http host=127.0.0.1 port=8765 path=/mcp`
- `python3 -m nodejobs.dependencies.TestService now tz_offset=1`


## Manage Under nodejobs

Use `Jobs` to orchestrate lifecycle and logs (see `docs/class/jobs.md`):

- Start:

```python
jobs = Jobs(db_path="./jobs_db")
jobs.run(command=[sys.executable, "my_mcp_service.py", "serve", "transport=http", "host=127.0.0.1", "port=8765", "path=/mcp"], job_id="my_mcp_01")
```

- Monitor:

```python
from nodejobs.jobs import JobRecord
status = jobs.get_status("my_mcp_01").status
# statuses include: starting, running, stopped, finished, failed
```

- Logs (persisted per job):

```python
stdout, stderr = jobs.job_logs("my_mcp_01")
print(stdout); print(stderr)
```

- Live stream (local generator):

```python
for ev in jobs.bind("my_mcp_01", include=("stdout","stderr"), from_beginning=True):
    if ev.type in ("stdout","stderr"):
        print(ev.type, ev.text, end="")
    elif ev.type == "status" and ev.status in ("finished","stopped","failed"):
        break
```

- Stop:

```python
jobs.stop("my_mcp_01")
```

Tips:
- Provide `cwd=` if your service expects a working directory.
- Use a list command (recommended) to avoid shell quoting issues.
- Ensure unique `job_id` per instance.


## Testing Pattern

See `nodejobs/ex_mcp_service_tests.py` for an end-to-end pattern:
- Start the MCP service under Jobs on a free port
- Wait until not `starting`
- Connect with `fastmcp.Client`, `await client.ping()`
- Call a tool and assert results
- Stop the job and assert a terminal status

Minimal excerpt:

```python
import asyncio, socket, sys, time, unittest
from nodejobs.jobs import Jobs, JobRecord
from fastmcp import Client

def _free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close(); return port

class MyMCPTests(unittest.TestCase):
    def test_http(self):
        jobs = Jobs(db_path="./test_nodejobs_db")
        host, port, path = "127.0.0.1", _free_port(), "/mcp"
        job_id = "myservice_http"
        cmd = [sys.executable, "ex_mcp_service.py", "serve", "transport=http", f"host={host}", f"port={port}", f"path={path}"]
        jobs.run(command=cmd, job_id=job_id)

        timeout = time.time() + 5
        while jobs.get_status(job_id).status == JobRecord.Status.c_starting:
            self.assertTrue(time.time() < timeout, "Server failed to start in time")
            time.sleep(0.1)

        async def talk():
            client = Client(f"http://{host}:{port}{path}")
            async with client:
                await client.ping()
                res = await client.call_tool("now", {"tz_offset": 1})
                self.assertEqual(float(res.data), 11.0)

        try:
            asyncio.run(talk())
        finally:
            jobs.stop(job_id)
```


## Logs & Debugging

- Where logs go: `Jobs` captures stdout/stderr to append-only files under its db/log directories (see `docs/class_overview.md` “Streaming Architecture”).
- Triage:
  - Use `jobs.job_logs(job_id)` for quick inspection
  - Use `jobs.bind(job_id, include=("stdout","stderr"))` for live tail
  - In HTTP mode, `await client.ping()` is a simple readiness probe
- Common issues:
  - Port in use: change `port=...`
  - Wrong path: make sure client URL matches `path` (e.g., `/mcp`)
  - Tool name mismatch: confirm `@mcp.tool(name="...")` equals what the client calls
  - Type conversion: cast CLI strings to numbers/bools inside your tool methods
- Developer visibility:
  - Print or logging calls in your tool functions go to stdout/stderr and are captured by `Jobs`
  - For long-lived services, prefer periodic logging so `bind()` shows liveness


## Lifecycle & Status

You will typically see:
- `starting`: process spawned; server booting
- `running`: process alive (note: reachability still depends on server readiness; use `ping()`)
- `stopped` or `finished`: exited normally or was stopped
- `failed`: nonzero exit with error (check `stderr`)

Production hint: combine `running` with a quick HTTP/Client `ping()` to confirm readiness before traffic.


## Common Recipes

- Run a module instead of a file:

```python
jobs.run(command=[sys.executable, "-m", "my_package.my_mcp_service", "serve", "transport=http"], job_id="svc01")
```

- Set working directory:

```python
jobs.run(command=cmd, job_id="svc02", cwd="/opt/myapp")
```

- Call from CLI (no Jobs) for local dev:
  - `python3 my_mcp_service.py serve transport=http host=127.0.0.1 port=9000 path=/mcp`

- Use an existing CLI service directly (no MCP) via `BaseService`:
  - `python3 -m nodejobs.dependencies.TestService now tz_offset=1`

## NJ Hierarchical Service Wrappers (Jobs + Schedules)

The Nodejobs service layer now includes:
- `JobsService` (`nodejobs/jobs_service.py`) for job lifecycle operations
- `SchedulesService` (`nodejobs/schedules_service.py`) for schedule lifecycle operations
- `NJService` (`nodejobs/nj.py`) as a core router command (`nj`) that delegates to either service

All commands use the BaseService CLI grammar: `command key=value`.

### Direct JobsService usage

- List jobs:
  - `python3 nodejobs/jobs_service.py list`
- Start a job with dotted list args:
  - `python3 nodejobs/jobs_service.py run job_id=my_job command.1=python command.2=-c command.3='print("hi")'`
- Check status:
  - `python3 nodejobs/jobs_service.py status job_id=my_job`
- Stop:
  - `python3 nodejobs/jobs_service.py stop job_id=my_job wait_time=1`
- Fetch logs:
  - `python3 nodejobs/jobs_service.py logs job_id=my_job`

### Direct SchedulesService usage

- Set a schedule with dotted keys:
  - `python3 nodejobs/schedules_service.py set schedule.schedule_id=s1 schedule.job_id=my_job schedule.command.1=python schedule.command.2=-c schedule.command.3='print("scheduled")' schedule.next_run_at=2026-08-05T00:00:00`
- List schedules:
  - `python3 nodejobs/schedules_service.py list`
- Remove a schedule:
  - `python3 nodejobs/schedules_service.py remove schedule_id=s1`

### Routed NJService usage

- Route to JobsService:
  - `python3 nodejobs/nj.py nj service=jobs op=status job_id=my_job`
- Route to SchedulesService:
  - `python3 nodejobs/nj.py nj service=schedules op=list`

### JSON file reference techniques (both supported)

- Key-scoped JSON replacement:
  - `python3 nodejobs/nj.py nj service=schedules op=set schedule=[[schedule_payload.json]]`
- Whole-arg JSON merge:
  - `python3 nodejobs/nj.py nj [[bulk]]=[[nj_route_payload.json]]`

### Merge precedence behavior

When whole-arg JSON merge is used and keys collide, values from the merged JSON overwrite existing keys. This behavior is inherited from `BaseService._expand_message_json`.


## Reference: Example Files

- `nodejobs/ex_mcp_service.py`: Minimal HTTP MCP server exposing `now`
- `nodejobs/ex_mcp_service_tests.py`: Direct call, CLI subprocess under `Jobs`, MCP HTTP test
- `/Users/computercomputer/justinops/decelium_wallet/decelium_wallet/commands/BaseService.py`:
  - CLI bridge: `run_cli()` parses `command key=value`
  - Routing: `get_command_map()` maps command → method
  - Nested args: `add_depth()` turns dotted keys/indices into nested structures
  - Extras: `flatten()`, `to_arg_string()`, optional `compile_for()` via Nuitka
