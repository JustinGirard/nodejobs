# Nodejobs Clients Module Guide

This guide documents the cross-language client module in `clients/` and the client test suite under `tests/client`.

## Purpose & Scope

### What this module does
- Provides a common client model for invoking server commands across three transports:
  - HTTP
  - MCP
  - CLI
- Exposes language-specific client APIs with similar behavior:
  - Python: `nodejobs.client.ClientService`
  - JavaScript: `clients/javascript/src/ClientService.js`
  - C#: `clients/csharp/Nodejobs.Client/ClientService.cs`
- Supports server lifecycle orchestration for test/dev scenarios:
  - start
  - wait-ready
  - stop

### What this module does not do
- It is not a production service orchestration platform.
- It does not manage deployment, load balancing, or long-running infra health.
- It does not define business-domain commands; command schemas are provided by caller configuration.

### Where it fits
- These clients are adapters between user code and transport-specific execution paths.
- Python implementation is the reference behavior; JS and C# mirror it and rely on Python for some bridge operations.

## Public Contract

### API surface
- Python (`nodejobs/client/client_service.py`)
  - Async: `start_server_async`, `wait_ready_async`, `invoke_async`, `stop_server_async`
  - Sync wrappers: `start_server`, `wait_ready`, `invoke`, `stop_server`
- JavaScript (`clients/javascript/src/ClientService.js`)
  - Async methods: `startServer`, `waitReady`, `invoke`, `stopServer`
- C# (`clients/csharp/Nodejobs.Client/ClientService.cs`)
  - Async methods: `StartServerAsync`, `WaitReadyAsync`, `InvokeAsync`, `StopServerAsync`
  - Sync wrappers: `StartServer`, `WaitReady`, `Invoke`, `StopServer`

### Input/output schemas
- Core config object is `ClientData` with `servers` map.
- Per-server config includes:
  - `transport`, `host`, `port`, `protocol`, `base_url`, `path`
  - `commands` map with `required_params`, `optional_params`, `transport_command`
  - optional lifecycle config:
    - `db_path`, `launch_command`, `python`, `launch_cwd`, `job_id`
- Invoke payload requires:
  - `server_alias` / `serverAlias`
  - `command_id` / `commandId`
  - `data` object

### Error model
- Validation and runtime failures raise exceptions/errors with stable string markers where possible, for example:
  - `unknown_server_alias:<alias>`
  - `missing_required_param:<name>`
  - `lifecycle_required_for_start`
  - `server_not_started:<alias>`
  - `unsupported_transport:<name>`

### Sync/async behavior
- All transports ultimately run async internals.
- Python and C# include sync wrappers over async methods.
- JS surface is async-only.

### Versioning and backward compatibility
- Keep payload keys and error marker strings stable.
- Treat transport parity behavior as a compatibility requirement:
  - same command + same input -> equivalent output across HTTP/MCP/CLI.

## Dependency & Runtime Requirements

### Languages and runtimes
- Python: project runtime from `pyproject.toml` (`nodejobs` package).
- JavaScript: Node.js with ESM support (`"type": "module"` in `clients/javascript/package.json`).
- C#:
  - Library target: `netstandard2.1`
  - Test target: `net8.0`

### Required libraries
- Python: project dependencies in root `pyproject.toml`.
- JavaScript: built-in Node test runner (`node --test`), no heavy external test framework.
- C#: `xunit`, `Microsoft.NET.Test.Sdk`, `xunit.runner.visualstudio`, plus `Newtonsoft.Json` in client lib.

### Environment variables
- `PYTHON`:
  - Required for JS and C# lifecycle and MCP bridge paths.
  - Recommended to set to the same interpreter used for Python tests.
- `CLIENT_TEST_VERBOSE=1`:
  - Enables proof/debug lines in Python/JS/C# lifecycle tests.
- `NODEJOBS_LIFECYCLE_JOB_ID`:
  - Internal lifecycle identifier (normally set by client code, not manually).

### External services and flags
- No external cloud dependency for the client tests.
- Tests spin up local process-based fixtures (`nodejobs.dependencies.TestService`).

## Core Design

### Key abstractions
- `ClientData` / spec classes: schema and validation for config and invoke payloads.
- `ClientService`: transport dispatch + lifecycle orchestration.
- `lifecycle_ops` (Python module): process/job-db backed lifecycle control.

### Data flow
- Caller -> `ClientData.validateInvoke(...)` -> transport path -> parse response -> return typed result.
- Lifecycle calls write runtime state per alias and use job DB + health checks.

### State model
- Runtime state is alias-scoped (`_runtime_by_alias` / `runtimeByAlias` map):
  - `db_path`
  - `job_id`
  - host/port

### Concurrency model
- Multiple invokes are allowed and tested (parallel invoke batches).
- Lifecycle state mutations are per alias and should be treated as non-reentrant unless explicitly coordinated.

### Extension points
- New transports can be added in `ClientService` dispatch with matching spec validation.
- New commands are schema-driven via `servers[*].commands`.

## Test Architecture

### Test pyramid
- Unit/smoke:
  - Python: `tests/client/test_client_cli.py`, `tests/client/test_client_service.py`
  - JS and C#: class construction smoke and basic behavior checks
- Integration:
  - Lifecycle + invoke parity tests in each language
- End-to-end aggregate:
  - Python runner executes Python, JS, and C# suites and collects artifacts
- Principle: tests prove real behavior, not simulated behavior.

### Real-code-only policy
- Mocking is prohibited in this module's test suites.
- Do not use mocks, stubs, fakes, monkeypatches, or transport shims for core client paths.
- Every test must execute real code paths end-to-end for the scope it covers.
- Transport tests must run real HTTP, real MCP bridge calls, and real CLI subprocess invocations.
- Lifecycle tests must run real process start/wait/stop behavior against real job DB state.
- If a test is slow or flaky, fix the real code path or fixture orchestration; do not replace with mocking.

### Fixtures and determinism
- Fixture provider command: `fixture(kind=...)` in `TestService`.
- Use deterministic fixture kinds for parity:
  - `int`, `float`, `string`, `bool`, `dict`, `list`, `null`, `nested`
- Keep random/time usage explicit in tests; avoid hidden non-determinism.

### Transport parity matrix
- Command parity cases (all languages):
  - `now` with numeric offsets across HTTP/MCP/CLI
  - `fixture` typed payload parity across HTTP/MCP/CLI
- Pass condition:
  - equal semantic values for same command and same payload across transports.

### Contract tests
- Validate required params and command wiring:
  - missing required parameter fails with explicit marker.
- Validate response parsing behavior for:
  - numbers, booleans, null, dict/object, list/array, nested payloads.

### Concurrency tests
- Batch invoke test (20 parallel calls) validates independent successful responses.
- Keep assertions strict on value and completion.

### Failure-mode tests
- Expected coverage:
  - timeout waiting for readiness
  - malformed invoke payload
  - unsupported transport
  - non-2xx HTTP response
  - CLI non-zero exit

### Golden tests
- Prefer assertion on typed values and explicit schemas.
- Use snapshot/golden data only for stable, intentionally versioned outputs.

### CI mapping and timing budget
- PR gate (recommended):
  - smoke + lifecycle/parity suites, aggregate runner
  - target: <= 5 minutes on normal CI hardware
- Nightly (recommended):
  - extended concurrency and failure injection suites
  - target: <= 15 minutes

## How to Run & Debug

### Common commands
- Aggregate (Python + JS + C#):
  - `python -m nodejobs.client.test_runner -v`
  - or installed script: `nodejobs-client-test -v`
- Python only:
  - `python -m pytest tests/client -q`
- JavaScript only:
  - `cd clients/javascript && PYTHON=$(which python) node --test test/client.test.js test/client.lifecycle.test.js`
- C# only:
  - `cd clients/csharp/Nodejobs.Client.Tests && PYTHON=$(which python) dotnet test`

### Artifacts and logs
- Aggregate logs:
  - `test-results/client-tests/python.log`
  - `test-results/client-tests/javascript.log`
  - `test-results/client-tests/csharp.log`
- Structured reports:
  - `test-results/pytest-client.xml`
  - `clients/csharp/Nodejobs.Client.Tests/test-results/csharp-client.trx`
- Test fixture process artifacts:
  - `tests/client/artifacts/...`

### Known failure signatures
- Parity mismatch on bool across transports:
  - `True` vs `'True'` indicates CLI output parsing/encoding mismatch.
- C# suite appears as not discovered in aggregate:
  - often a compile error prevented TRX generation.

## Change Playbook

When changing:
- Transport request/response shape:
  - update Python, JS, and C# client implementations.
  - update parity tests in all three suites.
- Validation schemas or key naming:
  - update corresponding spec classes in each language.
  - update tests that construct payloads/config.
- Lifecycle command construction:
  - update integration tests and aggregate expectations.
- Test runner output/reporting:
  - update artifact paths and troubleshooting docs.
- Test strategy:
  - reject any new test that bypasses client transport/lifecycle production paths with mocks, stubs, fakes, or monkeypatching.

## Examples

### Minimal use-case
- Build `ClientData` with one server alias and call `invoke("now")` with `tz_offset`.

### Edge-case use-case
- Run the same typed fixture (`kind="nested"`) through HTTP, MCP, and CLI and assert semantic equality.

## Operational Notes

- Performance:
  - lifecycle-based tests spawn subprocesses; keep them scoped and always tear down in `finally`.
- Observability:
  - use `CLIENT_TEST_VERBOSE=1` for proof lines.
  - inspect aggregate logs first before deep debugging.
- Security:
  - do not pass secrets in command-line payloads for CLI transport tests.
- Rollback:
  - if parity breaks, revert to last known good transport response shape and re-run aggregate.

## Troubleshooting FAQ

1. Why does JS/C# fail with python bridge errors?
   - Ensure `PYTHON` is set and points to an interpreter with `nodejobs` importable.
2. Why does parity fail for bool/null values?
   - Check CLI serialization path and final stdout parsing behavior.
3. Why does C# show no discovered tests in aggregate?
   - Check `test-results/client-tests/csharp.log` for compile errors before discovery.
4. Why does wait-ready timeout?
   - Verify lifecycle launch command, host/port/path, and fixture process logs in `tests/client/artifacts`.
5. Why do tests pass individually but fail in aggregate?
   - Check port collisions, stale lifecycle processes, and alias runtime cleanup in finalizers.
