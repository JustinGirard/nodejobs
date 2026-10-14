from .client_data import (
    ClientData,
    ServerAliasSpec,
    WaitReadySpec,
    InvokeSpec,
    LifecycleSpec,
)
import asyncio
import json
import os
import sys
import threading
import traceback
from .client_auth import ClientAuth
from .network_http import HttpNetworkService
from .network_cli import CliNetworkService
from .mcp_invoke import McpNetworkService
from nodejobs.dependencies.CachingComponent import CachingComponent
from nodejobs.dependencies.BaseSession import DataSession
import time


class ClientService(CachingComponent):
    _network_registry = {}

    @classmethod
    def bind_network_service(cls, network_id: str, service_factory):
        key = str(network_id).strip().lower()
        if len(key) == 0:
            raise Exception("network_id_required")
        cls._network_registry[key] = service_factory

    @classmethod
    def _bind_defaults(cls):
        if len(cls._network_registry) > 0:
            return
        cls.bind_network_service("http", HttpNetworkService)
        cls.bind_network_service("mcp", McpNetworkService)
        cls.bind_network_service("cli", CliNetworkService)

    def __init__(self, data, auth):
        cache_spec = {
            CachingComponent.do_purge: False,
            CachingComponent.cache_path: ".client_runtime",
            CachingComponent.data_session: DataSession(
                {DataSession.f_root: os.getcwd(), DataSession.f_unlocked: True}
            ),
        }
        super().__init__(cache_spec, trim=False)
        self.awake()
        self.data = data
        self.auth = auth
        self._invoke_loop = None
        self._invoke_thread = None
        self._invoke_lock = threading.Lock()
        self._invoke_futures = set()
        self._closed = False
        self._bind_defaults()
        runtime_map = self.load_cache("runtime_by_alias")
        if runtime_map is None:
            self.save_cache("runtime_by_alias", {})

    async def _run_process(self, command: list[str], cwd: str | None = None, env: dict | None = None):
        proc = await asyncio.create_subprocess_exec(
            *[str(x) for x in command],
            cwd=cwd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out_b, err_b = await proc.communicate()
        return proc.returncode, out_b.decode("utf-8"), err_b.decode("utf-8")

    def _load_runtime_by_alias(self):
        runtime_map = self.load_cache("runtime_by_alias")
        if not isinstance(runtime_map, dict):
            raise Exception("invalid_runtime_by_alias_cache")
        return runtime_map

    def _save_runtime_by_alias(self, runtime_map: dict):
        self.save_cache("runtime_by_alias", runtime_map)

    def _get_network_service(self, network_id: str):
        key = str(network_id).strip().lower()
        if key not in self._network_registry:
            raise Exception(f"unsupported_network_service:{key}")
        return self._network_registry[key]

    def _ensure_invoke_loop(self):
        with self._invoke_lock:
            if self._closed:
                raise Exception("client_closed")
            if self._invoke_loop is not None and self._invoke_thread is not None and self._invoke_thread.is_alive():
                return self._invoke_loop
            ready = threading.Event()

            def _runner():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                self._invoke_loop = loop
                ready.set()
                loop.run_forever()
                loop.close()

            thread = threading.Thread(target=_runner, name="nodejobs-client-invoke-loop", daemon=True)
            self._invoke_thread = thread
            thread.start()
            ready.wait()
            return self._invoke_loop

    async def start_server_async(self, payload):
        payload = ServerAliasSpec(payload)
        spec = self.data.get_server(payload)
        if spec.lifecycle is None:
            raise Exception("lifecycle_required_for_start")
        lifecycle = LifecycleSpec(spec.lifecycle)
        host = str(spec.host)
        port = int(spec.port)
        db_path = str(lifecycle.db_path)
        ts_ms = int(time.time() * 1000)
        job_id = str(lifecycle.job_id) if lifecycle.job_id is not None else f"__CLIENT_LIFECYCLE__{payload.server_alias}__{ts_ms}"
        py = str(lifecycle.python) if lifecycle.python is not None else sys.executable
        cmd = [
            py,
            "-m",
            "clients.python.lifecycle_ops",
            "--action",
            "start",
            "--db-path",
            db_path,
            "--command-json",
            json.dumps(lifecycle.launch_command),
        ]
        if lifecycle.launch_cwd:
            cmd += ["--cwd", str(lifecycle.launch_cwd)]
        rc, out, err = await self._run_process(
            cmd,
            cwd=str(lifecycle.launch_cwd) if lifecycle.launch_cwd else None,
            env={**os.environ, "NODEJOBS_LIFECYCLE_JOB_ID": job_id},
        )
        if rc != 0:
            raise Exception(err or out)
        runtime_map = self._load_runtime_by_alias()
        runtime_map[payload.server_alias] = {
            "db_path": db_path,
            "job_id": job_id,
            "host": host,
            "port": port,
            "path": str(spec.path),
        }
        self._save_runtime_by_alias(runtime_map)
        return {"status": "ok", "job_id": job_id}

    async def wait_ready_async(self, payload):
        payload = WaitReadySpec(payload)
        runtime_map = self._load_runtime_by_alias()
        if payload.server_alias not in runtime_map:
            raise Exception(f"server_not_started:{payload.server_alias}")
        spec = self.data.get_server({ServerAliasSpec.server_alias: payload.server_alias})
        if spec.lifecycle is None:
            raise Exception("lifecycle_required_for_wait")
        lifecycle = LifecycleSpec(spec.lifecycle)
        runtime = runtime_map[payload.server_alias]
        py = str(lifecycle.python) if lifecycle.python is not None else sys.executable
        cmd = [
            py,
            "-m",
            "clients.python.lifecycle_ops",
            "--action",
            "wait",
            "--db-path",
            runtime["db_path"],
            "--job-id",
            runtime["job_id"],
            "--host",
            runtime["host"],
            "--port",
            str(runtime["port"]),
            "--timeout-s",
            str(float(payload.timeout_s)),
        ]
        rc, out, err = await self._run_process(cmd, cwd=str(lifecycle.launch_cwd) if lifecycle.launch_cwd else None)
        if rc != 0:
            raise Exception(err or out)
        return {"status": "ok"}

    async def invoke_async(self, payload):
        payload = InvokeSpec(payload)
        invoke, server, command, validated_data = self.data.validate_invoke(payload)
        network_id = server.network_service
        if network_id is None:
            network_id = server.transport
        network_service = self._get_network_service(network_id)
        return await network_service.dispatch_async(
            "invoke",
            invoke=invoke,
            server=server,
            command=command,
            validated_data=validated_data,
            run_process=self._run_process,
        )

    async def stop_server_async(self, payload):
        payload = ServerAliasSpec(payload)
        runtime_map = self._load_runtime_by_alias()
        if payload.server_alias not in runtime_map:
            return {"status": "ok"}
        runtime = runtime_map[payload.server_alias]
        spec = self.data.get_server({ServerAliasSpec.server_alias: payload.server_alias})
        if spec.lifecycle is None:
            runtime_map.pop(payload.server_alias, None)
            self._save_runtime_by_alias(runtime_map)
            return {"status": "ok"}
        lifecycle = LifecycleSpec(spec.lifecycle)
        py = str(lifecycle.python) if lifecycle.python is not None else sys.executable
        cmd = [
            py,
            "-m",
            "clients.python.lifecycle_ops",
            "--action",
            "stop",
            "--db-path",
            runtime["db_path"],
            "--job-id",
            runtime["job_id"],
            "--wait-time",
            "1",
        ]
        rc, out, err = await self._run_process(cmd, cwd=str(lifecycle.launch_cwd) if lifecycle.launch_cwd else None)
        if rc != 0:
            raise Exception(err or out)
        runtime_map.pop(payload.server_alias, None)
        self._save_runtime_by_alias(runtime_map)
        return {"status": "ok"}

    def start_server(self, payload):
        return asyncio.run(self.start_server_async(payload))

    def wait_ready(self, payload):
        return asyncio.run(self.wait_ready_async(payload))

    def invoke(self, payload, on_success=None, on_failure=None, on_disconnect=None, on_progress=None):
        """
        Submit invoke_async() on the client-owned background loop and return a native Future.
        Callbacks execute on the background loop thread and must not block.
        """
        loop = self._ensure_invoke_loop()
        future = asyncio.run_coroutine_threadsafe(self.invoke_async(payload), loop)
        with self._invoke_lock:
            if self._closed:
                future.cancel()
                raise Exception("client_closed")
            self._invoke_futures.add(future)

        def _done(done_future):
            with self._invoke_lock:
                self._invoke_futures.discard(done_future)
            try:
                result = done_future.result()
            except Exception as exc:
                if on_failure is not None:
                    try:
                        on_failure(exc)
                    except Exception as cb_exc:
                        traceback.print_exception(type(cb_exc), cb_exc, cb_exc.__traceback__, file=sys.stderr)
                else:
                    traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)
                return
            if on_success is not None:
                try:
                    on_success(result)
                except Exception as cb_exc:
                    traceback.print_exception(type(cb_exc), cb_exc, cb_exc.__traceback__, file=sys.stderr)

        future.add_done_callback(_done)
        return future

    def close(self):
        with self._invoke_lock:
            if self._closed:
                return
            self._closed = True
            futures = list(self._invoke_futures)
            self._invoke_futures.clear()
            loop = self._invoke_loop
            thread = self._invoke_thread
        for future in futures:
            future.cancel()
        if loop is not None and thread is not None and thread.is_alive():
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=5.0)
        with self._invoke_lock:
            self._invoke_loop = None
            self._invoke_thread = None

    def stop_server(self, payload):
        return asyncio.run(self.stop_server_async(payload))
