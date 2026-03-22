"""
Lightweight NDJSON event log watcher with incremental polling.

Example:

    from nodejobs.events.watcher import WatchConfig, WatchFilter, NDJSONWatcher

    cfg = WatchConfig({WatchConfig.path: "logs/app.ndjson"})
    flt = WatchFilter({WatchFilter.labels: {"job": "alpha"}, WatchFilter.labels_mode: "any"})
    NDJSONWatcher(session, cfg, flt).watch(lambda ev: print(ev.to_safe_dict_LLMS_DONT_USE()))
"""


import time
from typing import Optional, Dict, Any, Callable, Tuple, Union, List
from datetime import datetime, timezone

from nodejobs.events.models import EventData, _to_iso_utc_z
from nodejobs.events.ndjson_events import NDJSONReader, StreamReadError, InvalidEventError
from nodejobs.dependencies.BaseData import BaseData


class WatchFilter(BaseData):
    labels: (Dict[str, Any], None)
    labels_mode: (str, "all")  # "all" or "any"
    text: (str, None)
    since: (Union[str, datetime, None], None)
    until: (Union[str, datetime, None], None)

    def get_defaults(self):
        return {WatchFilter.labels_mode: "all"}

    def do_pre_process(self, in_dict):
        for k in (WatchFilter.since, WatchFilter.until):
            if k in in_dict and in_dict[k] is not None:
                in_dict[k] = _to_iso_utc_z(in_dict[k])
        if WatchFilter.labels_mode not in in_dict or in_dict[WatchFilter.labels_mode] is None:
            in_dict[WatchFilter.labels_mode] = "all"
        return in_dict

    def do_validation(self, key, value):
        if key == WatchFilter.labels_mode:
            if value not in ("all", "any"):
                raise TypeError("labels_mode must be 'all' or 'any'")
        if key == WatchFilter.labels:
            if value is not None and not isinstance(value, dict):
                raise TypeError("labels must be dict or None")
            if isinstance(value, dict):
                for k, v in value.items():
                    if not isinstance(k, str):
                        raise TypeError("labels keys must be str")
                    if not (isinstance(v, (str, int, float, bool)) or v is None):
                        raise TypeError(f"labels[{k}] has invalid type: {type(v)}")
        return value, ""


class WatchConfig(BaseData):
    path: str
    poll_ms: (int, 500)
    from_end: (bool, True)
    wait_for_create: (bool, True)
    on_truncate: (str, "end")  # "end" | "start" | "error"
    idle_stop_ms: (int, None)

    def do_validation(self, key, value):
        if key == WatchConfig.on_truncate:
            if value not in ("end", "start", "error"):
                raise TypeError("on_truncate must be 'end', 'start', or 'error'")
        return value, ""


class NDJSONWatcher:
    def __init__(self, session, cfg: WatchConfig, flt: Optional[WatchFilter] = None):
        self._session = session
        self._cfg = WatchConfig(cfg)
        self._flt = WatchFilter(flt) if flt is not None else None
        self._reader = NDJSONReader(session, self._cfg.path)

    def _apply_filter(self, ev: EventData) -> bool:
        if not self._flt:
            return True
        from_dt = None
        to_dt = None
        if self._flt.since:
            from_dt = datetime.fromisoformat(self._flt.since.replace("Z", "+00:00")).astimezone(timezone.utc)
        if self._flt.until:
            to_dt = datetime.fromisoformat(self._flt.until.replace("Z", "+00:00")).astimezone(timezone.utc)
        ev_dt = datetime.fromisoformat(ev.datetime.replace("Z", "+00:00")).astimezone(timezone.utc)
        if from_dt and ev_dt < from_dt:
            return False
        if to_dt and ev_dt > to_dt:
            return False
        if self._flt.labels:
            items = list(self._flt.labels.items())
            if self._flt.labels_mode == "all":
                if not all(ev.labels.get(k) == v for k, v in items):
                    return False
            else:
                if not any(ev.labels.get(k) == v for k, v in items):
                    return False
        if self._flt.text:
            import json as _json
            hay = _json.dumps(ev.content, separators=(",", ":"), ensure_ascii=False)
            if self._flt.text not in hay:
                return False
        return True

    def tail(self, start_offset: Optional[int] = None):
        offset: Optional[int] = start_offset
        last_event_time = time.time()

        while True:
            try:
                if offset is None:
                    if self._cfg.from_end:
                        with self._session.open(self._cfg.path, "rb") as f:
                            f.seek(0, 2)
                            offset = f.tell()
                    else:
                        offset = 0
                break
            except FileNotFoundError:
                if self._cfg.wait_for_create:
                    time.sleep(self._cfg.poll_ms / 1000.0)
                    continue
                raise

        while True:
            try:
                events, new_off = self._reader.read_from_offset(offset)
                if events:
                    for ev in events:
                        if self._apply_filter(ev):
                            last_event_time = time.time()
                            yield ev
                offset = new_off
            except StreamReadError:
                try:
                    with self._session.open(self._cfg.path, "rb") as f:
                        f.seek(0, 2)
                        end = f.tell()
                except FileNotFoundError:
                    if not self._cfg.wait_for_create:
                        raise
                    time.sleep(self._cfg.poll_ms / 1000.0)
                    continue
                if self._cfg.on_truncate == "end":
                    offset = end
                elif self._cfg.on_truncate == "start":
                    offset = 0
                else:
                    raise
            except FileNotFoundError:
                if not self._cfg.wait_for_create:
                    raise
                time.sleep(self._cfg.poll_ms / 1000.0)
                continue

            if self._cfg.idle_stop_ms is not None:
                if (time.time() - last_event_time) * 1000.0 >= self._cfg.idle_stop_ms:
                    return

            time.sleep(self._cfg.poll_ms / 1000.0)

    def watch(self, callback: Callable[[EventData], None], start_offset: Optional[int] = None):
        for ev in self.tail(start_offset=start_offset):
            callback(ev)


class AsyncNDJSONWatcher:
    def __init__(self, session, cfg: WatchConfig, flt: Optional[WatchFilter] = None):
        self._sync = NDJSONWatcher(session, cfg, flt)

    async def tail_async(self, start_offset: Optional[int] = None):
        import asyncio
        cfg = self._sync._cfg
        offset = start_offset
        while True:
            try:
                if offset is None:
                    if cfg.from_end:
                        with self._sync._session.open(cfg.path, "rb") as f:
                            f.seek(0, 2)
                            offset = f.tell()
                    else:
                        offset = 0
                break
            except FileNotFoundError:
                if cfg.wait_for_create:
                    await asyncio.sleep(cfg.poll_ms / 1000.0)
                    continue
                raise

        loop = asyncio.get_event_loop()
        last_event_time = loop.time()
        while True:
            try:
                events, new_off = self._sync._reader.read_from_offset(offset)
                if events:
                    for ev in events:
                        if self._sync._apply_filter(ev):
                            last_event_time = loop.time()
                            yield ev
                offset = new_off
            except StreamReadError:
                try:
                    with self._sync._session.open(cfg.path, "rb") as f:
                        f.seek(0, 2)
                        end = f.tell()
                except FileNotFoundError:
                    if not cfg.wait_for_create:
                        raise
                    await asyncio.sleep(cfg.poll_ms / 1000.0)
                    continue
                if cfg.on_truncate == "end":
                    offset = end
                elif cfg.on_truncate == "start":
                    offset = 0
                else:
                    raise
            except FileNotFoundError:
                if not cfg.wait_for_create:
                    raise
                await asyncio.sleep(cfg.poll_ms / 1000.0)
                continue

            if cfg.idle_stop_ms is not None:
                now = loop.time()
                if (now - last_event_time) * 1000.0 >= cfg.idle_stop_ms:
                    return
            await asyncio.sleep(cfg.poll_ms / 1000.0)

    async def watch_async(self, callback: Callable[[EventData], None], start_offset: Optional[int] = None):
        async for ev in self.tail_async(start_offset=start_offset):
            callback(ev)
