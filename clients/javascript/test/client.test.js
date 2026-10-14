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
          port: 8871,
        },
      },
    }),
    new ClientAuth("local"),
  );
  assert.ok(service);
});
