# Events & Streaming Guide

This guide explains how to log, read, search, and stream structured events using the nodejobs events stack. It pulls directly from `nodejobs/events/ndjson_events.py`, `watcher.py`, and `events_tests.py`, so you can trust the examples to match current behavior. Use it when you need durable NDJSON event logs with incremental readers and watchers.

## 1. Components Overview

- **DataSessionAppend** (`nodejobs/events/data_session_append.py`)
  - Sandboxed filesystem session used by writers/readers/watchers
  - Ensures paths stay under a configured root, creates parent directories, supports atomic `replace()`

- **NDJSONWriter**
  - Appends or upserts `EventData` records to an NDJSON file
  - Enforces schema via `EventData`, injects `_writerNonce` for dedup tracking
  - Composite key enforcement (list of JSON paths) for append/upsert operations

- **NDJSONReader**
  - Reads all events, tail reads from byte offsets, or searches by labels/text/time
  - Validates JSON and schema as it reads

- **NDJSONWatcher / AsyncNDJSONWatcher**
  - Polls an NDJSON file incrementally with filters and truncation policies
  - `tail()` yields events; `watch(callback)` invokes your handler per event

- **EventData (from `nodejobs/events/models.py`)**
  - Defines the schema: `datetime`, `labels`, `sources`, `destinations`, `content`, optional `ttl`, etc.


## 2. Create a Session & Writer

```python
from nodejobs.events.data_session_append import DataSessionAppend
from nodejobs.events.ndjson_events import NDJSONWriter
from nodejobs.events.models import EventData

session = DataSessionAppend({
    DataSessionAppend.f_root: "./event_logs",  # sandbox root
    DataSessionAppend.f_verbose: False,
})

writer = NDJSONWriter(session, path="logs/app.ndjson")

key_paths = ["labels.job"]  # composite key = labels.job ("" if missing)

offset = writer.append({
    EventData.content: {"msg": "job started"},
    EventData.labels: {"job": "alpha", "ok": True},
    EventData.sources: ["scheduler"],
    EventData.destinations: "worker-1",
}, key_paths)

print("new EOF offset", offset)
```

Notes:
- `key_paths` is required for append/upsert; each path is dot notation into the event JSON (missing path contributes empty string).
- `EventData.datetime` is optional; if omitted, NDJSONWriter uses current UTC and normalizes to ISO Z.
- Writer ensures `_writerNonce` is present; re-appends the same object keeps original nonce.
- `EventData.labels` must use primitive values (`str`, `int`, `float`, `bool`, `None`).
- `EventData.sources`/`destinations` accept `str` or `list[str]`; stored as lists.
- TTL accepts `int` or `str`; floats raise `InvalidEventError`.


## 3. Upsert by Composite Key

```python
# Ensure labels.job exists; key = "alpha"
writer.upsert({
    EventData.content: {"msg": "job alpha finished", "status": "success"},
    EventData.labels: {"job": "alpha"},
}, key_paths)
```

Behavior:
- Finds the last event whose composite key matches (`labels.job == "alpha"`).
- Replaces the entire line if content differs; preserves `_writerNonce` when updating.
- If no match, appends a new line.
- Only the most recent line with that key is updated (“last wins”).


## 4. Reading & Incremental Offsets

```python
from nodejobs.events.ndjson_events import NDJSONReader

reader = NDJSONReader(session, path="logs/app.ndjson")

# Read everything
events = reader.read_all()

# Stream from start, keep offset
batch, off = reader.read_from_offset(0)
print(len(batch), off)

# Append more events, then resume
writer.append({EventData.content: {"msg": "another"}, EventData.labels: {"job": "beta"}}, key_paths)
batch2, off2 = reader.read_from_offset(off)

# Nothing left
batch3, off3 = reader.read_from_offset(off2)
```

Rules:
- `read_from_offset` takes a byte offset (>=0). If offset is mid-line, reader skips to next newline to ensure whole events.
- If offset > EOF, raises `StreamReadError` (file truncated/rotated past your cursor).
- Use returned offset as the new resume point.


## 5. Searching Events

```python
hits = reader.search(
    labels={"job": "alpha", "ok": True},
    labels_mode="all",  # or "any"
    text="job",
    since="2024-01-01T00:00:00Z",
    until="2024-01-02T00:00:00Z",
)

for ev in hits:
    print(ev.datetime, ev.labels, ev.content)
```

Search filters:
- `labels` dict + `labels_mode` (`all` requires every key/value match; `any` passes if any match).
- `text` substring search across serialized `content` (exact substring match).
- `since`/`until` accept ISO strings or `datetime` objects; inclusive range.


## 6. Watching Streams (Synchronous)

```python
from nodejobs.events.watcher import WatchConfig, WatchFilter, NDJSONWatcher

cfg = WatchConfig({
    WatchConfig.path: "logs/app.ndjson",
    WatchConfig.poll_ms: 200,
    WatchConfig.from_end: True,      # start from EOF (only new events)
    WatchConfig.wait_for_create: True,
    WatchConfig.on_truncate: "end",  # "end" | "start" | "error"
    WatchConfig.idle_stop_ms: None,
})

flt = WatchFilter({
    WatchFilter.labels: {"job": "alpha"},
    WatchFilter.labels_mode: "any",
    WatchFilter.text: "job",
})

watcher = NDJSONWatcher(session, cfg, flt)

for event in watcher.tail():
    print("tail event", event.to_safe_dict())
```

Key settings:
- `from_end=True`: tail only new events (like `tail -f`). Use `False` to replay existing events before live tailing.
- `wait_for_create=True`: wait for file to exist before starting; otherwise `FileNotFoundError` bubbles out.
- `on_truncate`: controls behavior when file shrinks/rotates.
  - `"end"`: seek to file end, ignore truncated content.
  - `"start"`: restart from beginning (replay entire file).
  - `"error"`: raise `StreamReadError`.
- `idle_stop_ms`: stop tailing if no events for this many milliseconds (`None` means never stop).

`watch(callback)` wraps `tail()` and calls your function per event; exceptions from callback propagate.


## 7. Async Watching

```python
import asyncio
from nodejobs.events.watcher import WatchConfig, AsyncNDJSONWatcher

cfg = WatchConfig({WatchConfig.path: "logs/app.ndjson", WatchConfig.poll_ms: 100})
watcher = AsyncNDJSONWatcher(session, cfg)

async def main():
    async for event in watcher.tail_async():
        print("async event", event.to_safe_dict())

asyncio.run(main())
```

Notes:
- Same config/filter rules; uses `asyncio.sleep` for polling.
- `watch_async(callback)` executes your coroutine per event.


## 8. Handling Truncation & Rotation

Example: enforce policies from `events_tests.py` (`test_watcher_on_truncate_policies`).

- `on_truncate="end"`: watchers jump to EOF and continue; events added after truncation are seen, truncated data is skipped.
- `on_truncate="start"`: watchers restart at byte 0 and replay the file from the beginning (including new data written after truncation).
- `on_truncate="error"`: watchers raise the underlying exception (stop tailing) when a truncation is detected.

Choose policy per use case:
- For log files rotated/truncated frequently, "end" is typical (skip lost data, stay near live tail).
- For data pipelines that require restart, "start" replays everything after truncation.
- For strict pipelines, "error" ensures operators notice and handle truncation manually.


## 9. Watch Filters in Practice

```python
flt = WatchFilter({
    WatchFilter.labels: {"job": "alpha", "ok": True},
    WatchFilter.labels_mode: "all",
    WatchFilter.text: "hello",
    WatchFilter.since: "2020-01-01T00:00:00Z",
    WatchFilter.until: "2020-01-02T00:00:00Z",
})

watcher = NDJSONWatcher(session, cfg, flt)
for ev in watcher.tail():
    print(ev.labels, ev.content)
```

Filtering occurs after events are read:
- Date bounds convert to UTC and compare with each event’s `.datetime`.
- Text search uses JSON-serialized `content` (unicode-safe).
- Label mode `all` requires every key match; `any` allows partial matches.


## 10. Build a CLI Tail

```python
# tail_events.py
import argparse
from nodejobs.events.data_session_append import DataSessionAppend
from nodejobs.events.watcher import WatchConfig, WatchFilter, NDJSONWatcher

parser = argparse.ArgumentParser()
parser.add_argument("--root", default="./event_logs")
parser.add_argument("--path", default="logs/app.ndjson")
parser.add_argument("--job")
args = parser.parse_args()

session = DataSessionAppend({
    DataSessionAppend.f_root: args.root,
    DataSessionAppend.f_verbose: False,
})

cfg = WatchConfig({WatchConfig.path: args.path, WatchConfig.from_end: False, WatchConfig.poll_ms: 100})
flt = WatchFilter({WatchFilter.labels: {"job": args.job}}) if args.job else None

watcher = NDJSONWatcher(session, cfg, flt)

for event in watcher.tail():
    print(event.to_safe_dict())
```

Run:
- `python3 tail_events.py --job alpha`
- Use `--root` to point at different sandbox directories.
- Use `CTRL+C` to stop (tail loop stops when script ends).


## 11. Integrate with Jobs

Tie events to job lifecycle for richer observability:

```python
from nodejobs.jobs import Jobs, JobRecord

jobs = Jobs(db_path="./jobs_db")
session = DataSessionAppend({DataSessionAppend.f_root: "./event_logs", DataSessionAppend.f_verbose: False})
writer = NDJSONWriter(session, "logs/jobs.ndjson")

def record_status(job_id: str):
    status = jobs.get_status(job_id)
    writer.append({
        EventData.labels: {"job_id": job_id, "status": status.status},
        EventData.content: {
            "pid": status.last_pid,
            "cwd": status.cwd,
        },
    }, key_paths=["labels.job_id", "labels.status"])

record_status("svc_1")
```

Then watch `logs/jobs.ndjson` to stream job status changes in real time.


## 12. Troubleshooting & Best Practices

- **Locks**: NDJSONWriter uses lock files (`.lock`) beside the event file with TTL/polling. Don’t delete them manually; writer releases automatically.
- **Composite keys**: Ensure `key_paths` match real fields; missing values produce `""` and make collisions likely.
- **Offsets**: Always use the returned offset from `read_from_offset`. If you see `StreamReadError` with “beyond EOF”, restart from 0 or handle a truncated file.
- **Encoding**: NDJSON is UTF-8; watchers/search handle Unicode (see tests for `こんにちは世界`).
- **Watcher performance**: Set `poll_ms` to balance latency vs. CPU. Use `idle_stop_ms` for finite tails (stop after inactivity).
- **Async watchers**: When integrating with asyncio apps, avoid blocking operations inside `watch_async` callbacks.
- **Error handling**: Wrap watchers in try/except to handle `FileNotFoundError`, `StreamReadError`, or your own callback exceptions.
- **Directory layout**: Keep event files under a dedicated root (e.g., `./event_logs`) for easy cleanup.


## 13. Reference Cheat Sheet

| Component | Key Methods | Description |
|-----------|-------------|-------------|
| `NDJSONWriter` | `append(event, key_paths)`, `upsert(event, key_paths)` | Append or replace events with schema validation, locking, `_writerNonce` handling |
| `NDJSONReader` | `read_all()`, `read_from_offset(offset)`, `search(...)` | Full reads, incremental tails, label/text/time searches |
| `WatchConfig` | fields: `path`, `poll_ms`, `from_end`, `wait_for_create`, `on_truncate`, `idle_stop_ms` | Controls watcher behavior (poll rate, start mode, truncation policy) |
| `WatchFilter` | fields: `labels`, `labels_mode`, `text`, `since`, `until` | Optional filters applied per event |
| `NDJSONWatcher` | `tail()`, `watch(callback)` | Synchronous generator/callback for streaming events |
| `AsyncNDJSONWatcher` | `tail_async()`, `watch_async(callback)` | Async equivalents for `NDJSONWatcher` |
| `DataSessionAppend` | `open(path, mode)`, `replace(src, dst)` | Sandboxed filesystem session that ensures directories and atomic replaces |

Use this guide alongside `docs/class/jobs.md` and `docs/class_overview.md` to wire events into your Job orchestration flows.
