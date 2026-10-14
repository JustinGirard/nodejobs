import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { ClientService } from "../src/ClientService.js";
import { ClientData } from "../src/ClientData.js";
import { ClientAuth } from "../src/ClientAuth.js";

const DEBUG = process.env.CLIENT_TEST_VERBOSE === "1";
function dprint(line) {
  if (DEBUG) {
    console.log(line);
  }
}

test("javascript client lifecycle + hello", async () => {
  const repoRoot = path.resolve(process.cwd(), "../..");
  const py = process.env.PYTHON;
  const commands = {
    now: {
      requiredParams: ["tz_offset"],
      optionalParams: [],
      transportCommand: "now",
    },
  };
  const httpTemplate = {
    host: "127.0.0.1",
    port: 8871,
    baseUrl: "http://127.0.0.1:8871",
    python: py,
    testName: "javascript_http_case",
    path: "/cmd",
    launchCommand: [
      py,
      "-m",
      "nodejobs.dependencies.TestService",
      "launch",
      "host=127.0.0.1",
      "port=8871",
      "path=/cmd",
    ],
    launchCwd: repoRoot,
    commands,
  };
  const mcpTemplate = {
    host: "127.0.0.1",
    port: 8874,
    baseUrl: "http://127.0.0.1:8874",
    python: py,
    testName: "javascript_mcp_case",
    path: "/mcp",
    launchCommand: [
      py,
      "-m",
      "nodejobs.dependencies.TestService",
      "serve",
      "transport=http",
      "host=127.0.0.1",
      "port=8874",
      "path=/mcp",
    ],
    launchCwd: repoRoot,
    commands,
  };
  const cliTemplate = {
    host: "127.0.0.1",
    port: 0,
    baseUrl: "http://127.0.0.1:0",
    python: py,
    testName: "javascript_cli_case",
    path: "/unused",
    launchCommand: [
      py,
      "-m",
      "nodejobs.dependencies.TestService",
      "serve",
      "transport=http",
      "host=127.0.0.1",
      "port=9999",
      "path=/mcp",
    ],
    launchCwd: repoRoot,
    commands,
  };
  const data = new ClientData({
    servers: {
      httpCase: { transport: "http", ...httpTemplate },
      mcpCase: { transport: "mcp", ...mcpTemplate },
      cliCase: { transport: "cli", ...cliTemplate },
    },
  });
  const client = new ClientService(data, new ClientAuth("local"));
  const cases = [
    { alias: "httpCase", mode: "http", needsServer: true },
    { alias: "mcpCase", mode: "mcp", needsServer: true },
    { alias: "cliCase", mode: "cli", needsServer: false },
  ];
  const offsets = [1, -2.5];

  try {
    for (const c of cases) {
      if (c.needsServer) {
        client.startServer({ serverAlias: c.alias });
        await client.waitReady({ serverAlias: c.alias, timeoutS: 8 });
      }
      for (const tzOffset of offsets) {
        const out = await client.invoke({ serverAlias: c.alias, commandId: "now", data: { tz_offset: tzOffset } });
        const expected = tzOffset + 10;
        dprint(`PROOF JS mode=${c.mode} request: commandId=now data={"tz_offset":${tzOffset}}`);
        dprint(`PROOF JS mode=${c.mode} response: ${JSON.stringify(out)} expected=${expected}`);
        assert.equal(typeof out, "number");
        assert.equal(out, expected);
      }
      if (c.needsServer) {
        const srv = data.getServer({ serverAlias: c.alias });
        const dbPath = path.join(repoRoot, "tests", "client", "artifacts", `client_${srv.testName}`, "job_db");
        assert.ok(fs.existsSync(dbPath));
        client.stopServer({ serverAlias: c.alias });
      }
    }
  } finally {
    for (const c of cases) {
      client.stopServer({ serverAlias: c.alias });
    }
  }
});
