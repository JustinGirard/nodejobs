# Unified Service Guide (CLI + HTTP + MCP)

This is the top-level architecture and operations guide for building and maintaining services in this repository.
It unifies the surface-specific guides:

- `nodejobs/docs/cli_service_guide.md`
- `nodejobs/docs/http_service_guide.md`
- `nodejobs/docs/mcp_service_guide.md`

Use this guide first for system shape and governance. Use the surface guides for implementation details.

---

## 1) Service Model (One Core, Many Surfaces)

The standard service architecture is:

1. **One command core**
   - `get_command_map()` defines command API.
   - Handlers own business logic.
2. **Multiple adapters**
   - CLI (`run_cli`)
   - HTTP (`launch` + `/cmd`)
   - MCP (`serve_mcp` + `@mcp.tool(...)`)
3. **Shared behavior**
   - Same command IDs and semantics across surfaces.
   - Same auth and error policy expectations.

Design goal: no transport-specific business forks.

---

## 2) Base Class Stack (Actual Runtime Layers)

### Layer A: Command runtime

- `nodejobs/nodejobs/dependencies/BaseService.py` (`BaseService`)
  - command dispatch: `run(**kwargs)`
  - command registry contract: `get_command_map()`
  - CLI parser/entry: `parse_args()`, `run_cli()`
  - auth gate hook: `authorize_command(...)`
  - JSON payload expansion: `_expand_message_json(...)`

### Layer B: HTTP runtime

- `thinhost/thinhost/simple_web_server.py` (`SimpleWebServer(BaseService)`)
  - HTTP serving
  - command envelope adapter at `/cmd`
  - host spec parsing/validation
  - extension hook: `_after_launch_endpoints(app, hostSpec)`

### Layer C: MCP runtime

- `fastmcp.FastMCP`
  - MCP tool exposure and transport runtime
  - explicit tool registration via `@mcp.tool(...)`

### Layer D: Service extenders

- Examples:
  - `ai_services/ai_services/node_mcp.py`
  - `ai_services/component_host.py`

These are extenders of the base layers, not the base layers themselves.

---

## 3) Unified Contract Surface

## 3.1 Command identity

- Command IDs are stable service API identifiers.
- Renames require migration plan + compatibility window.

## 3.2 Shared command semantics

When command `X` is exposed on multiple surfaces, it must preserve:

- payload semantics
- validation semantics
- auth semantics
- result semantics
- error semantics (or explicit documented exception)

## 3.3 Error envelope policy

Each service must document one canonical error envelope and reuse it across surfaces.
At minimum include:

- `error`
- `error_type`
- `traceback` (debug policy controlled)

---

## 4) Surface Contracts (Quick Index)

## 4.1 CLI

- Grammar: `command key=value`
- Supports dotted keys -> nested objects
- Supports `[[...]]` JSON expansion
- Canonical details: `nodejobs/docs/cli_service_guide.md`

## 4.2 HTTP

- Canonical command endpoint: `/cmd`
- Envelope: `{ "command_id": str, "data": dict }`
- Additional routes via `_after_launch_endpoints(...)`
- Canonical details: `nodejobs/docs/http_service_guide.md`

## 4.3 MCP

- Tools exposed explicitly with `@mcp.tool(...)`
- Tools delegate to shared command core
- Canonical details: `nodejobs/docs/mcp_service_guide.md` and `ai_services/docs/mcp_extension_guide.md`

---

## 5) Spec Architecture and Versioning

For long-lived services, use explicit typed specs (`BaseData`) for:

- `CoreServiceSpec` (identity, version, debug, cwd)
- `HttpSpec` (host, port, command_uri, public_root, etc.)
- `McpSpec` (transport, host, port, path, tool policy)
- `AuthSpec` (auth provider wiring, authorize rules, policy mode)

Rules:

1. Required vs optional fields must be explicit.
2. Defaults must be deterministic and documented.
3. Add `spec_version` to durable specs.
4. Backward-compatible evolution is additive optional fields.
5. Required-field breaks require version bump and migration notes.

---

## 6) Build a NEW Service (Peer, Not Child)

Use this sequence when creating a new service from base classes.

1. Define typed payload and host specs (`BaseData`).
2. Implement command handlers (business logic only).
3. Register handlers in `get_command_map()`.
4. Add CLI entry (`run_cli`).
5. Add HTTP adapter (`SimpleWebServer` launch + `/cmd`) if required.
6. Add MCP adapter (`serve_mcp` + explicit `@mcp.tool(...)`) if required.
7. Implement `authorize_command(...)` integration.
8. Add surface parity tests.
9. Add service docs with command contracts and examples.

Decision hint:

- Need HTTP/static/`/cmd`? Start with `SimpleWebServer`.
- Need only CLI+MCP? `BaseService` + `FastMCP` is valid.

---

## 7) Change Governance (No Surface Drift)

For any command change:

1. Update handler logic.
2. Update `get_command_map`.
3. Update CLI examples.
4. Update HTTP exposure/docs.
5. Update MCP wrappers/docs.
6. Update tests for all exposed surfaces.

Forbidden anti-patterns:

- Business logic in route wrappers.
- MCP tool logic diverging from command core.
- Hidden hostSpec fallbacks for critical auth/root settings.
- Inconsistent error envelopes for same command.

---

## 8) Testing and Audit Matrix

## 8.1 Minimum test coverage

- parser behavior (CLI)
- envelope validation (HTTP)
- tool invocation (MCP)
- command-map required args
- auth allow/deny path
- error-shape behavior
- lifecycle behavior under `Jobs`
- parity tests for shared commands

## 8.2 Audit checklist (review gate)

### Architecture
- [ ] One command core, many adapters
- [ ] Correct base class chosen
- [ ] No adapter-level business forks

### Contract
- [ ] Command IDs documented
- [ ] Envelope formats documented
- [ ] Spec fields + version documented

### Security
- [ ] `authorize_command` integrated
- [ ] Fail-closed behavior for ambiguous auth/policy
- [ ] Path/static containment validated

### Reliability
- [ ] Lifecycle and readiness strategy defined
- [ ] Logs/debug path documented
- [ ] Error policy explicit

### Maintenance
- [ ] Deprecation policy applied where needed
- [ ] Tests and docs updated with code change

---

## 9) Lifecycle Operations Under nodejobs

Manage service processes using `Jobs`:

- start: `run(...)`
- observe: `get_status(...)`, `bind(...)`
- inspect: `job_logs(...)`
- stop: `stop(...)`

Operational standards:

- prefer command list form over shell strings
- set explicit `cwd`
- use deterministic `job_id` naming
- require readiness probe before traffic

---

## 10) Canonical References

- `nodejobs/nodejobs/dependencies/BaseService.py`
- `thinhost/thinhost/simple_web_server.py`
- `nodejobs/docs/cli_service_guide.md`
- `nodejobs/docs/http_service_guide.md`
- `nodejobs/docs/mcp_service_guide.md`
- `ai_services/docs/mcp_extension_guide.md`
- `ai_services/ai_services/node_mcp.py`
- `ai_services/component_host.py`
- `/Users/computercomputer/justinops/git_backup/ui/ProcessingGraphManager/docs/back_end_security_policy.md`

---

## 11) Practical Reading Order

1. Read this unified guide first.
2. Read the surface guide you are implementing:
   - CLI, HTTP, or MCP.
3. Read service-specific extension guide (for example `ai_services/docs/mcp_extension_guide.md`) if extending that domain.
4. Implement and test all exposed surfaces before merge.
