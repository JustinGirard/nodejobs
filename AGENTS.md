# nodejobs runtime

For new Python command servers, use
`nodejobs/dependencies/command_registry_host.py` as the authoritative
server template. Register BaseService subclasses with one
CommandRegistryHost instead of duplicating CLI, HTTP, or MCP dispatch.
See `README.md` → “Building command servers”.

Start here:
- `README.md`
- `docs/class_overview.md`
- `docs/events_and_streaming.md`

For job launch, status, or logs from a terminal, start with the
`python3 -m nodejobs.jobs_service` CLI example in `README.md`;
use `docs/cli_service_guide.md` for argument grammar.
