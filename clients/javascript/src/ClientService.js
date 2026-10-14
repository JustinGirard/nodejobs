import { ClientData, ServerAliasSpec, WaitReadySpec, InvokeSpec, LifecycleSpec } from "./ClientData.js";
import { ClientAuth } from "./ClientAuth.js";
import { spawn } from "node:child_process";

export class ClientService {
  constructor(data, auth) {
    this.data = data;
    this.auth = auth;
    this.runtimeByAlias = new Map();
  }

  async _runCommandAsync(command, cwd = null, extraEnv = null) {
    return await new Promise((resolve) => {
      const env = extraEnv ? { ...process.env, ...extraEnv } : process.env;
      const child = spawn(String(command[0]), command.slice(1).map((x) => String(x)), {
        cwd: cwd ? String(cwd) : undefined,
        env,
      });
      let stdout = "";
      let stderr = "";
      child.stdout.on("data", (d) => {
        stdout += d.toString("utf8");
      });
      child.stderr.on("data", (d) => {
        stderr += d.toString("utf8");
      });
      child.on("close", (code) => {
        resolve({ status: code ?? 0, stdout, stderr });
      });
    });
  }

  async startServer(payload) {
    const specPayload = new ServerAliasSpec(payload);
    const serverAlias = specPayload.serverAlias;
    const spec = this.data.getServer(specPayload);
    if (!spec.lifecycle) {
      throw new Error("lifecycle_required_for_start");
    }
    const lifecycle = new LifecycleSpec(spec.lifecycle);
    const host = String(spec.host);
    const port = Number(spec.port);
    const dbPath = String(lifecycle.dbPath);
    const jobId = lifecycle.jobId ? String(lifecycle.jobId) : `__CLIENT_LIFECYCLE__${serverAlias}__${Date.now()}`;
    const py = lifecycle.python ? String(lifecycle.python) : process.env.PYTHON;
    if (!py) {
      throw new Error("python_required_for_lifecycle");
    }
    const args = [
      "--action", "start",
      "--db-path", dbPath,
      "--command-json", JSON.stringify(lifecycle.launchCommand),
    ];
    if (lifecycle.launchCwd) {
      args.push("--cwd", String(lifecycle.launchCwd));
    }
    const out = await this._runCommandAsync(
      [py, "-m", "nodejobs.client.lifecycle_ops", ...args],
      lifecycle.launchCwd ?? null,
      { NODEJOBS_LIFECYCLE_JOB_ID: jobId },
    );
    if (out.status !== 0) {
      throw new Error(out.stderr || out.stdout);
    }
    this.runtimeByAlias.set(serverAlias, { dbPath, jobId, host, port });
    return { status: "ok", jobId };
  }

  async waitReady(payload) {
    const specPayload = new WaitReadySpec(payload);
    const serverAlias = specPayload.serverAlias;
    const timeoutS = specPayload.timeoutS;
    if (!this.runtimeByAlias.has(serverAlias)) {
      throw new Error(`server_not_started:${serverAlias}`);
    }
    const spec = this.data.getServer({ serverAlias });
    if (!spec.lifecycle) {
      throw new Error("lifecycle_required_for_wait");
    }
    const lifecycle = new LifecycleSpec(spec.lifecycle);
    const py = lifecycle.python ? String(lifecycle.python) : process.env.PYTHON;
    if (!py) {
      throw new Error("python_required_for_lifecycle");
    }
    const runtime = this.runtimeByAlias.get(serverAlias);
    const out = await this._runCommandAsync(
      [
        py,
        "-m",
        "nodejobs.client.lifecycle_ops",
        "--action", "wait",
        "--db-path", runtime.dbPath,
        "--job-id", runtime.jobId,
        "--host", runtime.host,
        "--port", String(runtime.port),
        "--timeout-s", String(timeoutS),
      ],
      lifecycle.launchCwd ?? null,
    );
    if (out.status !== 0) {
      throw new Error(out.stderr || out.stdout);
    }
    return { status: "ok" };
  }

  async invoke(payload) {
    const invokePayload = new InvokeSpec(payload);
    const { invoke, validatedData, server, command } = this.data.validateInvoke(invokePayload);
    const transport = String(server.transport).trim().toLowerCase();

    if (transport === "http") {
      const baseUrl = String(server.baseUrl).replace(/\/+$/, "");
      const endpointId = command.transportCommand ? String(command.transportCommand) : invoke.commandId;
      const route = String(server.path).trim();
      if (!route.startsWith("/")) {
        throw new Error("server_path_must_start_with_slash");
      }
      const resp = await fetch(`${baseUrl}${route}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ command_id: endpointId.trim(), data: validatedData }),
      });
      if (!(resp.status === 200 || resp.status === 201)) {
        throw new Error(`invoke_failed:${resp.status}`);
      }
      return await resp.json();
    }

    if (transport === "mcp") {
      const baseUrl = String(server.baseUrl).replace(/\/+$/, "");
      const mcpPath = String(server.path).trim();
      const py = (server.lifecycle && server.lifecycle.python) ? String(server.lifecycle.python) : process.env.PYTHON;
      if (!py) {
        throw new Error("python_required_for_mcp_bridge");
      }
      const out = await this._runCommandAsync([
        py,
        "-m",
        "nodejobs.client.mcp_invoke",
        "--base-url",
        baseUrl,
        "--path",
        mcpPath,
        "--tool", String(invoke.commandId),
        "--timeout-s",
        "8",
        "--data-json",
        JSON.stringify(validatedData),
      ]);
      if (out.status !== 0) {
        throw new Error(out.stderr || out.stdout);
      }
      return JSON.parse(out.stdout).data;
    }

    if (transport === "cli") {
      if (!server.launchCommand) throw new Error("launch_command_required_for_cli");
      const serveIdx = server.launchCommand.findIndex((x) => String(x) === "serve");
      if (serveIdx < 0) throw new Error("cli_launch_command_missing_serve");
      const prefix = server.launchCommand.slice(0, serveIdx).map((x) => String(x));
      if (prefix.length === 0) throw new Error("cli_launch_command_missing_prefix");
      const cliArgs = Object.entries(validatedData).map(([k, v]) => `${k}=${typeof v === "string" ? v : JSON.stringify(v)}`);
      const out = await this._runCommandAsync([...prefix, String(invoke.commandId), ...cliArgs], server.launchCwd ?? null);
      if (out.status !== 0) throw new Error(`cli_invoke_failed:${out.status}\nstdout:\n${out.stdout}\nstderr:\n${out.stderr}`);
      const lines = out.stdout.split(/\r?\n/).map((s) => s.trim()).filter((s) => s.length > 0);
      if (lines.length === 0) throw new Error("cli_empty_stdout");
      const raw = lines[lines.length - 1];
      try { return JSON.parse(raw); } catch (_) {}
      const n = Number(raw);
      if (!Number.isNaN(n)) return n;
      return raw;
    }
    throw new Error(`unsupported_transport:${transport}`);
  }

  async stopServer(payload) {
    const specPayload = new ServerAliasSpec(payload);
    const serverAlias = specPayload.serverAlias;
    if (!this.runtimeByAlias.has(serverAlias)) {
      return { status: "ok" };
    }
    const spec = this.data.getServer({ serverAlias });
    const runtime = this.runtimeByAlias.get(serverAlias);
    if (!spec.lifecycle) {
      this.runtimeByAlias.delete(serverAlias);
      return { status: "ok" };
    }
    const lifecycle = new LifecycleSpec(spec.lifecycle);
    const py = lifecycle.python ? String(lifecycle.python) : process.env.PYTHON;
    if (!py) {
      throw new Error("python_required_for_lifecycle");
    }
    const out = await this._runCommandAsync(
      [
        py,
        "-m",
        "nodejobs.client.lifecycle_ops",
        "--action", "stop",
        "--db-path", runtime.dbPath,
        "--job-id", runtime.jobId,
        "--wait-time", "1",
      ],
      lifecycle.launchCwd ?? null,
    );
    this.runtimeByAlias.delete(serverAlias);
    if (out.status !== 0) {
      throw new Error(out.stderr || out.stdout);
    }
    return { status: "ok" };
  }
}
