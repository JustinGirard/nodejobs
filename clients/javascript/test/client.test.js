import test from "node:test";
import assert from "node:assert/strict";
import { ClientService } from "../src/ClientService.js";
import { ClientData } from "../src/ClientData.js";
import { ClientAuth } from "../src/ClientAuth.js";

test("javascript client smoke", () => {
  const service = new ClientService(
    new ClientData({
      servers: {
        hello: {
          transport: "http",
          host: "127.0.0.1",
          port: 8871,
          baseUrl: "http://127.0.0.1:8871",
          python: "python",
          testName: "javascript_http_client",
          path: "/mcp",
        },
      },
    }),
    new ClientAuth("local"),
  );
  assert.ok(service);
});
