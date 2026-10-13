# CLI Service Guide

Top-level architecture guide: `nodejobs/docs/unified_service_guide.md`

This guide defines how to design, implement, and maintain CLI-capable services in this repository.
It is the CLI counterpart to `nodejobs/docs/mcp_service_guide.md`.

It is intended for service authors building classes that expose one or more of:

- CLI (`run_cli`)
- HTTP (`launch` / `/cmd`)
- MCP (`serve_mcp`)

The primary goal is **one command core with multiple adapters**, so behavior stays consistent across surfaces.

---

## 1) Scope and Guarantees

This guide standardizes:

1. CLI grammar and parsing behavior.
2. Command routing and execution lifecycle.
3. Spec/schema design for durable service contracts.
4. How CLI, HTTP, and MCP should share a command core.
5. Testing, drift prevention, and lifecycle operations under `nodejobs`.

This guide does **not** redefine domain logic for specific services; it defines the service-framework contract.

---

## 2) Service Archetype (CLI + HTTP + MCP)

A service class should be designed as:

- **One command core** (`get_command_map` + handlers)
- **Three optional adapters**:
  - CLI (`run_cli`)
  - HTTP (`SimpleWebServer` launch + `/cmd`)
  - MCP (`FastMCP` tools in `serve_mcp`)

### Core Principle

Transport layers are adapters only.
Business logic belongs in command handlers, not in parser/routes/tool wrappers.

---

## 3) Framework Layering (Actual Base Classes)

### Base layer

- `nodejobs/nodejobs/dependencies/BaseService.py` (`BaseService`)
  - `parse_args`
  - `_expand_message_json`
  - `run`
  - `run_cli`
  - `authorize_command`

### HTTP bridge layer (optional)

- `thinhost/thinhost/simple_web_server.py` (`SimpleWebServer(BaseService)`)
  - HTTP host spec
  - static serving + `/cmd` command endpoint
  - extension hook: `_after_launch_endpoints`

### MCP layer (optional)

- `fastmcp.FastMCP` runtime with explicit `@mcp.tool(...)` registrations.

### Extenders

- `ai_services/ai_services/node_mcp.py` (`NodeMcpServer(SimpleWebServer)`)
- `ai_services/component_host.py` (`ComponentHost(SimpleWebServer)`)

---

## 4) CLI Contract (Canonical Grammar)

`BaseService.run_cli` expects:

- One command token (first positional command)
- `key=value` arguments

### Supported parsing behavior

- Quotes are stripped from key/value wrappers.
- `"true"` / `"false"` become booleans.
- Dotted keys become nested objects via `add_depth`.
- Numeric path segments become list indexes (1-based in flattened path conventions).
- JSON spec expansion with `[[...]]` is supported by `_expand_message_json`.

### Required command semantics

- `__command` must resolve to exactly one command id.
- Command id must exist in `get_command_map`.
- Required args are enforced before handler execution.

---

## 5) JSON Expansion and Nested Args

`_expand_message_json` supports two useful forms:

1. **Value-level replacement**
   - `spec=[[./mcpSpec.json]]`
   - Replaces that value with parsed JSON object.

2. **Whole-arg merge**
   - `[[bulk]]=[[./payload.json]]`
   - Merges file keys into root kwargs.

### Merge precedence

When merged keys collide, merged JSON values overwrite existing keys.
Use this intentionally and document command-level expectations.

---

## 6) Dispatch Lifecycle (CLI -> Handler)

Execution flow:

1. `run_cli()` reads argv.
2. `parse_args()` builds kwargs and `__command`.
3. `add_depth()` nests dotted keys.
4. `_expand_message_json()` injects JSON payloads.
5. `run()` loads `get_command_map()`.
6. required args validated.
7. `authorize_command(...)` evaluated.
8. target handler invoked.

This is the canonical lifecycle that other transports should mirror semantically.

---

## 7) Rules for Creating a New CLI-Capable Service

1. Start from `BaseService` (or `SimpleWebServer` if HTTP is required).
2. Define typed specs with `BaseData` for command payloads and service config.
3. Implement `get_command_map()` with explicit command ids.
4. Keep handler logic transport-neutral.
5. Add `run_cli()` entrypoint.
6. If adding MCP, tools must delegate into shared command core.
7. If adding HTTP `/cmd`, it must dispatch through command map.
8. Add auth checks via `authorize_command` (fail closed).

---

## 8) HostSpec Pattern for Multi-Service Repos

For maintainability, define explicit spec objects:

- `CoreServiceSpec`: service identity/version/debug/cwd
- `HttpSpec`: host/port/command_uri/public_root/cors/static policy
- `McpSpec`: transport/host/port/path/tool policy
- `AuthSpec`: auth directory + authorize rules + policy mode

### Requirements

- Use typed `BaseData` specs.
- Clearly mark required vs optional fields.
- Add `spec_version`.
- Evolve by adding optional fields; breaking required-field changes require version bump + migration notes.

---

## 9) CLI/HTTP/MCP Parity Rules

When a command exists on more than one surface:

- It must map to the same command id and same core handler.
- Input validation semantics must match.
- Error envelope semantics must match (or be explicitly documented).
- Auth behavior must match policy intent.

No transport-specific business forks unless formally documented.

---

## 10) Operations With nodejobs

Use `Jobs` for service lifecycle:

- `run(...)`
- `get_status(...)`
- `job_logs(...)`
- `bind(...)`
- `stop(...)`

### Operational guidelines

- Prefer command arrays over shell strings.
- Use explicit `cwd`.
- Use deterministic `job_id` conventions.
- Use readiness probes (e.g., ping or lightweight command) before traffic.

---

## 11) Maintenance Without Surface Drift

### Command add/change checklist

1. Add/modify handler.
2. Update `get_command_map`.
3. Update CLI examples.
4. Update HTTP adapter exposure (if present).
5. Update MCP tool wrapper (if present).
6. Update tests and docs.

### Deprecation checklist

1. Mark deprecated command.
2. Keep compatibility shim for migration window.
3. Add warning behavior and tests.
4. Remove only after callers/docs are migrated.

---

## 12) Testing Matrix for CLI Surfaces

Minimum coverage:

- Parser grammar (`key=value`, booleans, dotted paths, JSON injection)
- Command-map validation and required-arg enforcement
- Authorization behavior (`authorize_command`)
- CLI subprocess invocation behavior
- Jobs-managed lifecycle behavior
- Parity tests when command is shared with HTTP/MCP

---

## 13) Debugging and Failure Triage

Typical failure categories:

- unknown command id
- missing required arg
- malformed nested arg path
- invalid JSON spec path
- auth denied
- service startup failures (port/cwd/import issues)

Recommended triage order:

1. reproduce exact CLI command
2. inspect parsed kwargs
3. inspect command map + required args
4. inspect authorize decision
5. inspect handler exception path
6. inspect `Jobs` stdout/stderr logs

---

## 14) Security Guidance for CLI Services

- Treat CLI input as untrusted by default in shared environments.
- Validate spec/object shapes with typed `BaseData`.
- Avoid hidden fallback paths for sensitive config.
- Keep auth decisions centralized and explicit.
- Fail closed on ambiguous auth/policy states.

Reference: `docs/back_end_security_policy.md`.

---

## 15) Minimal Service Template (CLI + optional HTTP/MCP)

```python
from nodejobs.dependencies.BaseService import BaseService

class MyService(BaseService):
    @classmethod
    def get_command_map(cls):
        return {
            "ping": {"required_args": [], "method": cls.ping},
            "do_work": {"required_args": ["x"], "method": cls.do_work},
            # Optional:
            # "launch": {"required_args": [], "method": cls.launch},
            # "serve_mcp": {"required_args": [], "method": cls.serve_mcp},
        }

    @classmethod
    def ping(cls, **kwargs):
        return {"status": "ok"}

    @classmethod
    def do_work(cls, x, **kwargs):
        return {"status": "ok", "result": int(x) + 1}

if __name__ == "__main__":
    MyService.run_cli()
```

---

## 16) Architecture References

- `nodejobs/nodejobs/dependencies/BaseService.py`
- `thinhost/thinhost/simple_web_server.py`
- `nodejobs/docs/mcp_service_guide.md`
- `ai_services/docs/mcp_extension_guide.md`
- `docs/node_mcp_command_dev_guide.md`
- `docs/back_end_security_policy.md`
