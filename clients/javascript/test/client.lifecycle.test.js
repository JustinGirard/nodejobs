import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
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
  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "nodejobs-client-js-"));
  const commands = {
    now: {
      requiredParams: ["tz_offset"],
      optionalParams: [],
      transportCommand: "now",
    },
    fixture: {
      requiredParams: ["kind"],
      optionalParams: [],
      transportCommand: "fixture",
    },
  };
  const httpTemplate = {
    transport: "http",
    port: 8871,
    path: "/cmd",
    commands,
    lifecycle: {
      dbPath: path.join(tmpRoot, "jobs_http.db"),
      launchCommand: [
        py,
        "-m",
        "nodejobs.dependencies.TestService",
        "launch",
        "host=127.0.0.1",
        "port=8871",
        "path=/cmd",
      ],
      python: py,
      launchCwd: repoRoot,
    },
  };
  const mcpTemplate = {
    transport: "mcp",
    port: 8874,
    path: "/mcp",
    commands,
    lifecycle: {
      dbPath: path.join(tmpRoot, "jobs_mcp.db"),
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
      python: py,
      launchCwd: repoRoot,
    },
  };
  const cliTemplate = {
    transport: "cli",
    port: 0,
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
      httpCase: httpTemplate,
      mcpCase: mcpTemplate,
      cliCase: cliTemplate,
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
        await client.startServer({ serverAlias: c.alias });
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
        await client.stopServer({ serverAlias: c.alias });
      }
    }

    await client.startServer({ serverAlias: "httpCase" });
    await client.waitReady({ serverAlias: "httpCase", timeoutS: 8 });
    await client.startServer({ serverAlias: "mcpCase" });
    await client.waitReady({ serverAlias: "mcpCase", timeoutS: 8 });
    const fixtureKinds = ["int", "float", "string", "bool", "dict", "list", "null", "nested"];
    for (const kind of fixtureKinds) {
      const http = await client.invoke({ serverAlias: "httpCase", commandId: "fixture", data: { kind } });
      const mcp = await client.invoke({ serverAlias: "mcpCase", commandId: "fixture", data: { kind } });
      const cli = await client.invoke({ serverAlias: "cliCase", commandId: "fixture", data: { kind } });
      assert.deepEqual(http, mcp);
      assert.deepEqual(http, cli);
    }
    await client.stopServer({ serverAlias: "mcpCase" });
    await client.stopServer({ serverAlias: "httpCase" });

    await client.startServer({ serverAlias: "httpCase" });
    await client.waitReady({ serverAlias: "httpCase", timeoutS: 8 });
    const concurrent = await Promise.all(
      Array.from({ length: 20 }, () => client.invoke({ serverAlias: "httpCase", commandId: "fixture", data: { kind: "int" } })),
    );
    assert.ok(concurrent.every((x) => x === 3));
    await client.stopServer({ serverAlias: "httpCase" });
  } finally {
    for (const c of cases) {
      try {
        await client.stopServer({ serverAlias: c.alias });
      } catch (_) {
      }
    }
    fs.rmSync(tmpRoot, { recursive: true, force: true });
  }
});
