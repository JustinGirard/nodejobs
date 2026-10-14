import { ClientData, ServerAliasSpec, WaitReadySpec, InvokeSpec } from "./ClientData.js";
import { ClientAuth } from "./ClientAuth.js";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

export class ClientService {
  constructor(data, auth) {
    this.data = data;
    this.auth = auth;
    this.runtimeByAlias = new Map();
  }

  _repoRoot() {
    return path.resolve(process.cwd(), "../..");
  }

  _runPython(args, extraEnv = null) {
    const py = process.env.PYTHON;
    let env = process.env;
    if (extraEnv !== null) {
      env = { ...process.env, ...extraEnv };
    }
    return spawnSync(py, args, { cwd: this._repoRoot(), encoding: "utf8", env });
  }

  _artifactRoot(testName) {
    const root = path.join(this._repoRoot(), "tests", "client", "artifacts", `client_${testName}`);
    fs.rmSync(root, { recursive: true, force: true });
    fs.mkdirSync(root, { recursive: true });
    return root;
  }

  _runCommand(command, cwd = null) {
    const runCwd = cwd ? String(cwd) : this._repoRoot();
    return spawnSync(String(command[0]), command.slice(1).map((x) => String(x)), { cwd: runCwd, encoding: "utf8", env: process.env });
  }

  _runLifecycle(args) {
    return this._runPython(["-m", "nodejobs.client.lifecycle_ops", ...args]);
  }

  startServer(payload) {
    const specPayload = new ServerAliasSpec(payload);
    const serverAlias = specPayload.serverAlias;
    const spec = this.data.getServer(specPayload);
    const host = String(spec.host);
    const port = Number(spec.port);
    const testName = String(spec.testName);
    const root = this._artifactRoot(testName);
    const dbPath = path.join(root, "job_db");
    const jobId = `__CLIENT_LIFECYCLE__${testName}__${serverAlias}__${Date.now()}`;
    const args = [
      "--action", "start",
      "--db-path", dbPath,
      "--command-json", JSON.stringify(spec.launchCommand),
    ];
    if (spec.launchCwd) {
      args.push("--cwd", String(spec.launchCwd));
    }
    const out = this._runPython(
      ["-m", "nodejobs.client.lifecycle_ops", ...args],
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
    const runtime = this.runtimeByAlias.get(serverAlias);
    const out = this._runLifecycle([
      "--action", "wait",
      "--db-path", runtime.dbPath,
      "--job-id", runtime.jobId,
      "--host", runtime.host,
      "--port", String(runtime.port),
      "--timeout-s", String(timeoutS),
    ]);
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
      const out = this._runPython([
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
      const out = this._runCommand([...prefix, String(invoke.commandId), ...cliArgs], server.launchCwd ?? null);
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

  stopServer(payload) {
    const specPayload = new ServerAliasSpec(payload);
    const serverAlias = specPayload.serverAlias;
    if (!this.runtimeByAlias.has(serverAlias)) {
      return { status: "ok" };
    }
    const runtime = this.runtimeByAlias.get(serverAlias);
    const out = this._runLifecycle([
      "--action", "stop",
      "--db-path", runtime.dbPath,
      "--job-id", runtime.jobId,
      "--wait-time", "1",
    ]);
    this.runtimeByAlias.delete(serverAlias);
    if (out.status !== 0) {
      throw new Error(out.stderr || out.stdout);
    }
    return { status: "ok" };
  }
}
