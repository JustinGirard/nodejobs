# HTTP Service Guide

Top-level architecture guide: `nodejobs/docs/unified_service_guide.md`

This guide defines the standard way to design, implement, and maintain HTTP-capable services in this repository.
It is a peer to:

- `nodejobs/docs/mcp_service_guide.md`
- `nodejobs/docs/cli_service_guide.md`

It is intended for service authors building new services from base classes, not only extending existing domain servers.

---

## 0) What This Guide Covers

This guide covers:

1. The framework layering for HTTP services.
2. The canonical HTTP command contract (`/cmd`).
3. How to create a NEW HTTP server from base classes.
4. HostSpec/schema standards for long-lived multi-service repos.
5. Drift prevention and audit checklists.

It does not define your domain business logic; it defines the service interface architecture.

---

## 1) Framework Layering (Actual Base Classes)

### 1.1 Base runtime

- `nodejobs/nodejobs/dependencies/BaseService.py` (`BaseService`)
  - `get_command_map()` command registry
  - `run(**kwargs)` command dispatch
  - `authorize_command(...)` command auth gate
  - `parse_args()` and `run_cli()` CLI adapter
  - `_expand_message_json()` `[[...]]` JSON expansion

### 1.2 HTTP runtime

- `thinhost/thinhost/simple_web_server.py` (`SimpleWebServer(BaseService)`)
  - Generic HTTP hosting and static serving
  - Command envelope parsing
  - `/cmd` adapter that dispatches into `BaseService.run(...)`
  - Extension hook: `_after_launch_endpoints(app, hostSpec)`

### 1.3 Extender examples

- `ai_services/ai_services/node_mcp.py` (`NodeMcpServer(SimpleWebServer)`)
- `ai_services/component_host.py` (`ComponentHost(SimpleWebServer)`)

Key point: those are extenders. The HTTP base contract lives in `BaseService` + `SimpleWebServer`.

---

## 2) Canonical HTTP Contract

## 2.1 Default command endpoint

`SimpleWebServer._cmd_launch(...)` binds a command endpoint at `command_uri` (usually `/cmd`), and dispatches:

1. Parse request object into dict (`get_request()` / `parse_http_request_object()`).
2. Validate as `SimpleWebServer.Command`.
3. Call `cls.run(__command=[command_id], **data)`.
4. Serialize response as JSON string.

The envelope shape is:

```json
{
  "command_id": "some_command",
  "data": {
    "...": "..."
  }
}
```

Fields are defined by `SimpleWebServer.Command`:

- `command_id: str`
- `data: dict`

## 2.2 Request parsing behavior

`SimpleWebServer.parse_http_request_object(request_obj)` supports:

- `POST`: JSON body must be a dict.
- `GET`:
  - if `__encoded` exists: decode base64url JSON object.
  - otherwise use query args dict.

## 2.3 Optional response mode

`SimpleWebServer.respond_mode_aware(...)` supports:

- RPC mode (default): returns JSON payload.
- HTML/file mode when request payload sets `_mode=html` and file spec is provided.

## 2.4 Static file behavior

`SimpleWebServer._cmd_launch(...)` serves:

- `/` and `/<path>` from `public_root`, with `index.html` fallback.

Containment and path rules are enforced in helper functions like `serve_file_from(...)`.

---

## 3) Base HostSpec for HTTP

`SimpleWebServer.tHostSpec` defines required host config keys:

- `host: str`
- `port: str`
- `command_uri: str`
- `public_root: str`
- optional `cwd: str`

Startup preconditions in `_cmd_launch(...)` include:

- `public_root` must exist
- `command_uri` must start with `/`

---

## 4) Rules for Creating a NEW HTTP Server (from Base Classes)

Use this as normative guidance.

1. **Choose the right base**
   - Subclass `SimpleWebServer` if you need `/cmd` command endpoint and static/file support.
   - Subclass `BaseService` only if you do not need built-in HTTP hosting.

2. **Keep one command core**
   - Implement business logic in command handlers referenced by `get_command_map()`.
   - Do not put business logic directly in Flask route methods.

3. **Use typed specs**
   - Validate command payloads with `BaseData` classes in handlers.
   - Validate server config with explicit host spec classes.

4. **Treat HTTP as adapter layer**
   - Route parses envelope, dispatches to `run`, serializes result.
   - Command semantics live in handlers.

5. **Centralize auth**
   - Use `authorize_command(...)` and policy integrations.
   - Fail closed on ambiguous auth/policy states.

6. **Stabilize error shape**
   - Define a canonical error envelope for your service.
   - Avoid transport-specific error formats for shared commands.

7. **Define command IDs as API surface**
   - Command ids are stable interface identifiers.
   - Rename only with migration path and compatibility shim.

8. **Test parity if multi-surface**
   - If command also exists via CLI or MCP, enforce same semantics across surfaces.

---

## 5) NEW HTTP Server Build Sequence (Step-by-Step)

1. Define command payload specs (`BaseData`).
2. Define HTTP host spec (explicit required/optional fields).
3. Implement command handlers (business logic).
4. Register handlers in `get_command_map()`.
5. Use inherited `launch`/`_cmd_launch` flow from `SimpleWebServer`.
6. Add custom routes only through `_after_launch_endpoints(...)` when needed.
7. Add `run_cli()` entrypoint.
8. Add tests and docs for command contracts.

---

## 6) Minimal Template: New HTTP Service

```python
from nodejobs.dependencies.BaseData import BaseData
from thinhost.simple_web_server import SimpleWebServer


class WorkSpec(BaseData):
    x: int


class MyHttpService(SimpleWebServer):
    @classmethod
    def get_command_map(cls):
        m = super().get_command_map()  # includes "launch"
        m.update(
            {
                "ping": {"required_args": [], "method": cls._cmd_ping},
                "do_work": {"required_args": ["x"], "method": cls._cmd_do_work},
            }
        )
        return m

    @classmethod
    def _cmd_ping(cls, **kwargs):
        return {"status": "ok"}

    @classmethod
    def _cmd_do_work(cls, **kwargs):
        spec = WorkSpec(kwargs)
        return {"status": "ok", "result": int(spec.x) + 1}

    @classmethod
    def _after_launch_endpoints(cls, app, hostSpec):
        @app.route("/healthz", methods=["GET"])
        def healthz():
            return {"status": "ok"}, 200


if __name__ == "__main__":
    MyHttpService.run_cli()
```

Run:

```bash
python3 my_http_service.py launch host=127.0.0.1 port=8765 command_uri=/cmd public_root=/tmp
```

Or with JSON expansion:

```bash
python3 my_http_service.py launch [[server=./hostSpec.json]]
```

---

## 7) HostSpec Pattern for Multi-Service Repositories

For long-lived repos, define explicit versioned specs:

- `CoreServiceSpec` (identity/version/debug/cwd)
- `HttpSpec` (host/port/command_uri/public_root/cors/static policy)
- `AuthSpec` (auth directory/rules/policy mode)

Requirements:

- typed schema (`BaseData`)
- explicit required vs optional fields
- deterministic defaults
- `spec_version` included
- additive optional changes for backward compatibility
- required-field breaks require version bump + migration notes

---

## 8) Status Codes and Error Envelope Policy

Baseline `SimpleWebServer` behavior returns success response body with HTTP 201 for `/cmd`.
For new services, define and document your policy explicitly.

Recommended:

- `200`: normal read/compute success
- `201`: command accepted/created semantics (if keeping existing convention)
- `400`: malformed request/envelope
- `401`/`403`: auth/authz denied
- `404`: missing resource/route
- `409`: conflict state
- `422`: validation failures
- `500`: unhandled internal failure

Error payload should include:

- `error` (message/code)
- `error_type` (exception/category)
- `traceback` (when debug policy allows)

---

## 9) Maintenance Without Drift

## 9.1 Command change checklist

For every command add/change:

1. Handler implemented/updated.
2. `get_command_map` updated.
3. Required args updated.
4. HTTP contract examples updated.
5. Error contract updated.
6. Tests updated.

## 9.2 Deprecation checklist

1. Mark command deprecated in docs.
2. Keep compatibility shim for migration window.
3. Add tests for deprecated behavior.
4. Remove only after clients and docs migrate.

## 9.3 Anti-patterns (forbidden)

- Business logic in Flask routes.
- Multiple diverging handlers for same logical command across surfaces.
- Hidden hostSpec fallbacks for critical auth/root settings.
- Inconsistent error shape per route.

---

## 10) Testing Matrix for HTTP Services

Minimum expected coverage:

1. Command envelope parsing and validation.
2. Required-arg enforcement from command map.
3. `authorize_command(...)` decision path.
4. `/cmd` command dispatch and response behavior.
5. Custom endpoint behavior from `_after_launch_endpoints`.
6. Static/media path behavior if enabled.
7. Parity tests with CLI/MCP when commands are shared.

Useful references in this repo:

- `ai_services/ai_services/test_node_host_http.py`
- `ai_services/tests/test_component_host_http.py`
- `projects/microsite_utils/scripts/test_node_host_http_e2e.py`

---

## 11) Operations Under nodejobs

Manage HTTP services with `Jobs`:

- `run(command=[...], job_id=..., cwd=...)`
- `get_status(job_id)`
- `job_logs(job_id)`
- `bind(job_id, include=("stdout","stderr"), ...)`
- `stop(job_id)`

Operational recommendations:

- prefer list-form command (avoid shell quoting ambiguity)
- set explicit `cwd`
- ensure deterministic `job_id` naming
- perform readiness probe before traffic

---

## 12) Audit Checklist (Yes/No)

Use this list for reviews.

### Architecture

- [ ] Service subclasses correct base (`SimpleWebServer` for HTTP host use case).
- [ ] One command core used by all surfaces.
- [ ] No business logic inside route wrappers.

### Contract

- [ ] `/cmd` envelope documented and tested.
- [ ] Required args enforced by command map.
- [ ] HostSpec required/optional fields documented.
- [ ] `spec_version` present and tracked.

### Security

- [ ] `authorize_command` integrated and tested.
- [ ] Fail-closed behavior on ambiguous auth/policy states.
- [ ] Path/static containment rules verified.

### Reliability

- [ ] Error envelope shape consistent.
- [ ] Status-code policy explicit and tested.
- [ ] Jobs lifecycle and readiness behavior tested.

### Drift prevention

- [ ] Command change checklist followed.
- [ ] Deprecation protocol followed where applicable.
- [ ] Docs and tests updated in same change set.

---

## 13) Reference Locations

- `nodejobs/nodejobs/dependencies/BaseService.py`
- `thinhost/thinhost/simple_web_server.py`
- `nodejobs/docs/mcp_service_guide.md`
- `nodejobs/docs/cli_service_guide.md`
- `ai_services/docs/mcp_extension_guide.md`
- `ai_services/ai_services/node_mcp.py`
- `ai_services/component_host.py`
- `/Users/computercomputer/justinops/git_backup/ui/ProcessingGraphManager/docs/back_end_security_policy.md`
