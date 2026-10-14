import { BaseData } from "./BaseData.js";

export class ServerAliasSpec extends BaseData {
  static serverAlias = "serverAlias";

  static schema() {
    return { required: { [ServerAliasSpec.serverAlias]: "string" }, optional: {} };
  }

  do_pre_process(inData) {
    const out = { ...inData };
    if (ServerAliasSpec.serverAlias in out && typeof out.serverAlias === "string") {
      out.serverAlias = out.serverAlias.trim();
    }
    return out;
  }

  do_validation(key, value) {
    if (key === ServerAliasSpec.serverAlias && value.length === 0) {
      throw new Error("server_alias_required");
    }
    return [value, ""];
  }
}

export class WaitReadySpec extends BaseData {
  static serverAlias = "serverAlias";
  static timeoutS = "timeoutS";

  static schema() {
    return { required: { [WaitReadySpec.serverAlias]: "string", [WaitReadySpec.timeoutS]: "number" }, optional: {} };
  }

  do_pre_process(inData) {
    const out = { ...inData };
    if (WaitReadySpec.serverAlias in out && typeof out.serverAlias === "string") {
      out.serverAlias = out.serverAlias.trim();
    }
    return out;
  }

  do_validation(key, value) {
    if (key === WaitReadySpec.serverAlias && value.length === 0) {
      throw new Error("server_alias_required");
    }
    if (key === WaitReadySpec.timeoutS && value <= 0) {
      throw new Error("timeout_s_must_be_positive");
    }
    return [value, ""];
  }
}

export class InvokeSpec extends BaseData {
  static serverAlias = "serverAlias";
  static commandId = "commandId";
  static data = "data";

  static schema() {
    return {
      required: {
        [InvokeSpec.serverAlias]: "string",
        [InvokeSpec.commandId]: "string",
        [InvokeSpec.data]: "object",
      },
      optional: {},
    };
  }

  do_pre_process(inData) {
    const out = { ...inData };
    if (InvokeSpec.serverAlias in out && typeof out.serverAlias === "string") {
      out.serverAlias = out.serverAlias.trim();
    }
    if (InvokeSpec.commandId in out && typeof out.commandId === "string") {
      out.commandId = out.commandId.trim();
    }
    return out;
  }

  do_validation(key, value) {
    if (key === InvokeSpec.serverAlias && value.length === 0) {
      throw new Error("server_alias_required");
    }
    if (key === InvokeSpec.commandId && value.length === 0) {
      throw new Error("command_id_required");
    }
    return [value, ""];
  }
}

export class ServerSpec extends BaseData {
  static schema() {
    return {
      required: {
        transport: "string",
        port: "number",
      },
      optional: {
        host: "string",
        protocol: "string",
        baseUrl: "string",
        path: "string",
        commands: { mapOf: "object" },
        launchCommand: "array",
        launchCwd: "string",
        python: "string",
        testName: "string",
        lifecycle: "object",
      },
    };
  }

  do_pre_process(inData) {
    const out = { ...inData };
    out.transport = typeof out.transport === "string" ? out.transport.trim().toLowerCase() : "http";
    if (!("host" in out)) out.host = "127.0.0.1";
    if (!("protocol" in out)) out.protocol = "http";
    if (!("path" in out) || !out.path) out.path = out.transport === "mcp" ? "/mcp" : "/cmd";
    if (!("baseUrl" in out) || !out.baseUrl) out.baseUrl = `${out.protocol}://${out.host}:${out.port}`;
    return out;
  }
}

export class LifecycleSpec extends BaseData {
  static schema() {
    return {
      required: { dbPath: "string", launchCommand: "array" },
      optional: { python: "string", launchCwd: "string", jobId: "string" },
    };
  }
}

export class CommandSpec extends BaseData {
  static schema() {
    return {
      required: { requiredParams: { listOf: "string" } },
      optional: { optionalParams: { listOf: "string" }, transportCommand: "string" },
    };
  }

  do_validation(key, value) {
    if (key === "requiredParams" || key === "optionalParams") {
      for (const item of value) {
        if (item.trim().length === 0) {
          throw new Error(`invalid_param_name:${item}`);
        }
      }
    }
    return [value, ""];
  }
}

export class CommandPayloadSpec extends BaseData {
  static schema() {
    return { required: { requiredParams: { listOf: "string" }, data: "object" }, optional: {} };
  }

  do_validation(key, value) {
    if (key === "data") {
      const dirty = this.GetDirtyDict();
      const required = dirty.requiredParams;
      for (const req of required) {
        if (!(req in value)) {
          throw new Error(`missing_required_param:${req}`);
        }
      }
    }
    return [value, ""];
  }
}

export class ClientData extends BaseData {
  static schema() {
    return { required: { servers: { mapOf: "object" } }, optional: {} };
  }

  getServer(aliasPayload) {
    const payload = new ServerAliasSpec(aliasPayload);
    if (!(payload.serverAlias in this.servers)) {
      throw new Error(`unknown_server_alias:${payload.serverAlias}`);
    }
    return new ServerSpec(this.servers[payload.serverAlias]);
  }

  getCommand(invokePayload) {
    const payload = new InvokeSpec(invokePayload);
    const server = this.getServer({ serverAlias: payload.serverAlias });
    if ("commands" in server && server.commands && payload.commandId in server.commands) {
      return new CommandSpec(server.commands[payload.commandId]);
    }
    return new CommandSpec({ requiredParams: [] });
  }

  validateInvoke(invokePayload) {
    const payload = new InvokeSpec(invokePayload);
    const server = this.getServer({ serverAlias: payload.serverAlias });
    const command = this.getCommand(payload);
    const validated = new CommandPayloadSpec({ requiredParams: command.requiredParams, data: payload.data });
    return { invoke: payload, server, command, validatedData: validated.data };
  }
}

export class InvokeResultSpec extends BaseData {
  static data = "data";

  static schema() {
    return { required: { [InvokeResultSpec.data]: "any" }, optional: {} };
  }
}
