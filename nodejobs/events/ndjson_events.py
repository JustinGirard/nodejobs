import json
import os
import threading
import uuid
from typing import Union, List, Optional, Dict, Tuple
from datetime import datetime, timezone

from .models import EventData, EventDataContent, _to_iso_utc_z


class StreamWriteError(Exception):
    pass


class InvalidEventError(Exception):
    pass


class StreamReadError(Exception):
    pass


# -----------------------------
# Co-located TTL Lockfile Helpers
# -----------------------------

# Lock configuration (ms)
_LOCK_TTL_MS = 2000
_LOCK_POLL_MS = 25
_LOCK_MAX_WAIT_MS = 15000

_LOCK_KEY_PID = "pid"
_LOCK_KEY_EXPIRES = "expiresAtMs"


def _lock_path(path: str) -> str:
    lp = f"{path}.lock"
    # Enforce co-location beside the NDJSON file
    if os.path.dirname(lp) != os.path.dirname(path):
        raise StreamWriteError(f"Lockfile must be beside event file: {lp}")
    return lp


def _data_path(path: str) -> str:
    return path[:-7] + ".data.ndjson" if path.endswith(".ndjson") else path + ".data.ndjson"


def _build_data_record(nonce: str, content: dict) -> EventDataContent:
    rec = EventDataContent({EventDataContent._writerNonce: nonce})
    for k, v in (content or {}).items():
        if k == EventDataContent._writerNonce:
            raise StreamWriteError("content contains reserved key _writerNonce")
        rec[k] = v
    return rec


def _payload_from_data_record(data: EventDataContent) -> dict:
    if EventData.content in data and len(data) == 2:
        content = data[EventData.content]
        if not isinstance(content, dict):
            raise StreamReadError("data content must be a dict")
        return content
    data.pop(EventDataContent._writerNonce, None)
    return data


def _content_key_subset(content: dict, key_paths: List[str]) -> dict:
    prefix = f"{EventData.content}."
    out: Dict[str, object] = {}
    for path in key_paths:
        if not path.startswith(prefix):
            continue
        tail = path[len(prefix) :]
        if not tail:
            continue
        cur = content
        ok = True
        parts = tail.split(".")
        for part in parts:
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                ok = False
                break
        if not ok:
            continue
        dest = out
        for part in parts[:-1]:
            next_val = dest.get(part)
            if not isinstance(next_val, dict):
                next_val = {}
                dest[part] = next_val
            dest = next_val
        dest[parts[-1]] = cur
    return out


def _needs_migration(session, path: str) -> bool:
    try:
        with session.open(path, "rb") as _f:
            pass
    except FileNotFoundError:
        return False
    data_path = _data_path(path)
    try:
        with session.open(data_path, "rb") as _f:
            pass
    except FileNotFoundError:
        return True
    return False


def _migrate_sidecar(session, path: str, key_paths: Optional[List[str]] = None) -> None:
    try:
        with session.open(path, "rb") as _f:
            pass
    except FileNotFoundError:
        return
    data_path = _data_path(path)
    backup_path = f"{path}.bak.{_now_ms()}"
    with session.open(path, "rb") as src, session.open(backup_path, "wb") as bak:
        bak.write(src.read())
        bak.flush()

    data_map: Dict[str, dict] = {}
    try:
        with session.open(data_path, "rb") as f:
            for raw in f:
                line = raw.decode("utf-8").strip()
                if not line:
                    continue
                obj = json.loads(line)
                data = EventDataContent(obj)
                nonce = data._writerNonce
                data_map[nonce] = _payload_from_data_record(data)
    except FileNotFoundError:
        pass

    new_events: List[str] = []
    new_data: List[str] = []
    with session.open(path, "rb") as f:
        for raw in f:
            line = raw.decode("utf-8").strip()
            if not line:
                continue
            obj = json.loads(line)
            ev = EventData(obj)
            nonce = ev._writerNonce
            content = ev.content or data_map.get(nonce, {})
            data_rec = _build_data_record(nonce, content)
            ev.content = _content_key_subset(content, key_paths) if key_paths else content
            new_data.append(json.dumps(data_rec, separators=(",", ":"), ensure_ascii=False) + "\n")
            new_events.append(json.dumps(ev, separators=(",", ":"), ensure_ascii=False) + "\n")

    with session.open(path, "wb") as f:
        f.write("".join(new_events).encode("utf-8"))
        f.flush()

    with session.open(data_path, "wb") as f:
        f.write("".join(new_data).encode("utf-8"))
        f.flush()


def _now_ms() -> int:
    import time as _t

    return int(_t.time() * 1000)


def _lock_json(pid: int, ttl_ms: int) -> str:
    return json.dumps(
        {
            _LOCK_KEY_PID: pid,
            _LOCK_KEY_EXPIRES: _now_ms() + int(ttl_ms),
        },
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _read_lock(session, lock_path: str) -> Tuple[Optional[int], int]:
    try:
        with session.open(lock_path, "rb") as f:
            raw = f.read()
        obj = json.loads(raw.decode("utf-8"))
        exp = obj.get(_LOCK_KEY_EXPIRES) if isinstance(obj, dict) else 0
        pid = obj.get(_LOCK_KEY_PID) if isinstance(obj, dict) else None
        exp_val = exp if isinstance(exp, int) else 0
        pid_val = pid if isinstance(pid, int) else None
        return (pid_val, exp_val)
    except FileNotFoundError:
        return (None, 0)
    except Exception:
        return (None, 0)


def _try_create_lock(session, lock_path: str, pid: int, ttl_ms: int) -> bool:
    data = _lock_json(pid, ttl_ms).encode("utf-8")
    try:
        with session.open(lock_path, "xb") as f:  # atomic exclusive create
            f.write(data)
            f.flush()
        return True
    except FileExistsError:
        return False


def _write_lock(session, lock_path: str, pid: int, ttl_ms: int) -> None:
    data = _lock_json(pid, ttl_ms).encode("utf-8")
    with session.open(lock_path, "wb") as f:
        f.write(data)
        f.flush()


def _acquire_lock(
    session,
    path: str,
    ttl_ms: int = _LOCK_TTL_MS,
    poll_ms: int = _LOCK_POLL_MS,
    max_wait_ms: int = _LOCK_MAX_WAIT_MS,
) -> int:
    lock_path = _lock_path(path)
    pid = os.getpid()
    start = _now_ms()
    # Try exclusive create first
    if _try_create_lock(session, lock_path, pid, ttl_ms):
        return pid
    # Else poll until available or expired
    while True:
        now = _now_ms()
        if now - start > max_wait_ms:
            raise StreamWriteError(f"Lock acquire timeout for {path}")
        owner_pid, exp = _read_lock(session, lock_path)
        if owner_pid is None or exp <= now:
            try:
                _write_lock(session, lock_path, pid, ttl_ms)
                owner2, _exp2 = _read_lock(session, lock_path)
                if owner2 == pid:
                    return pid
            except Exception:
                # race; retry
                pass
        import time as _t

        _t.sleep(max(0.0, poll_ms / 1000.0))


def _owns_lock(session, path: str, pid: int) -> bool:
    lock_path = _lock_path(path)
    owner_pid, exp = _read_lock(session, lock_path)
    return owner_pid == pid and exp > _now_ms()


def _release_lock(session, path: str, pid: int) -> None:
    lock_path = _lock_path(path)
    if not _owns_lock(session, path, pid):
        return
    try:
        # Use session absolute if available
        if hasattr(session, "_abs_path"):
            abs_p = session._abs_path(lock_path)  # type: ignore[attr-defined]
            os.remove(abs_p)
        else:
            os.remove(lock_path)
    except FileNotFoundError:
        pass
    except Exception as e:
        raise StreamWriteError(f"Failed to release lock {lock_path}: {e}") from e


def _with_store_lock(session, event_path: str, fn):
    pid = _acquire_lock(session, event_path)
    try:
        return fn()
    finally:
        _release_lock(session, event_path, pid)


class NDJSONWriter:
    def __init__(self, session, path: str):
        # session should behave like BaseSession/DataSession
        self._session = session
        self._path = path
        # Per-writer lock to serialize append/upsert operations
        self._lock = threading.Lock()
    def purge(self) -> None:
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        def _run():
            with self._lock:
                with self._session.open(self._path, "wb") as f:
                    f.write(b"")
                    f.flush()
                data_path = _data_path(self._path)
                with self._session.open(data_path, "wb") as f:
                    f.write(b"")
                    f.flush()

        _with_store_lock(self._session, self._path, _run)

    def _append_data_record(self, nonce: str, content: dict) -> None:
        data_path = _data_path(self._path)
        os.makedirs(os.path.dirname(data_path) or ".", exist_ok=True)
        rec = _build_data_record(nonce, content)
        payload = (
            json.dumps(rec, separators=(",", ":"), ensure_ascii=False)
            + "\n"
        ).encode("utf-8")
        with self._session.open(data_path, "ab") as f:
            f.write(payload)
            f.flush()

    def _read_sidecar_payload_by_nonce(self, nonce: str) -> dict:
        data_path = _data_path(self._path)
        try:
            with self._session.open(data_path, "rb") as f:
                for raw in f:
                    line = raw.decode("utf-8").strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    data = EventDataContent(obj)
                    if data._writerNonce == nonce:
                        return _payload_from_data_record(data)
        except FileNotFoundError:
            return {}
        return {}

    def _find_last_by_nonce(self, path: str, nonce: str) -> Tuple[Optional[int], Optional[int]]:
        try:
            with self._session.open(path, "rb") as f:
                cur = 0
                found_start: Optional[int] = None
                found_end: Optional[int] = None
                while True:
                    raw = f.readline()
                    if not raw:
                        break
                    line = raw.decode("utf-8").strip()
                    if line:
                        obj = json.loads(line)
                        data = EventDataContent(obj)
                        if data._writerNonce == nonce:
                            found_start = cur
                            found_end = cur + len(raw)
                    cur += len(raw)
                return found_start, found_end
        except FileNotFoundError:
            return None, None

    def _delete_sidecar_by_nonce(self, nonce: str) -> None:
        data_path = _data_path(self._path)
        start, end = self._find_last_by_nonce(data_path, nonce)
        if start is None or end is None:
            return
        self._delete_line_at(data_path, start, end)

    def _assert_sidecar_has_nonce(self, nonce: str) -> None:
        data_path = _data_path(self._path)
        try:
            with self._session.open(data_path, "rb") as f:
                for raw in f:
                    line = raw.decode("utf-8").strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    data = EventDataContent(obj)
                    if data._writerNonce == nonce:
                        return
        except FileNotFoundError:
            raise StreamWriteError("Sidecar missing after write")
        raise StreamWriteError(f"Sidecar missing nonce after write: {nonce}")

    def _build_event_line(self, rec: dict, key_paths: List[str]) -> str:
        ev = EventData(rec)
        ev.content = _content_key_subset(ev.content, key_paths)
        return json.dumps(ev, separators=(",", ":"), ensure_ascii=False) + "\n"

    def _upsert_sidecar_by_key(self, rec: dict, key_paths: List[str], comp_key: str) -> None:
        data_path = _data_path(self._path)

        def line_to_obj(s: str) -> dict:
            obj = json.loads(s)
            data = EventDataContent(obj)
            payload = _payload_from_data_record(data)
            content_key = str(EventData.content)
            return {content_key: payload}

        start, end, _old = self._find_last_by_composite_key(
            comp_key, key_paths, line_to_obj=line_to_obj, path=data_path
        )

        ev = EventData(rec)
        data_rec = _build_data_record(ev._writerNonce, ev.content)
        new_line = json.dumps(data_rec, separators=(",", ":"), ensure_ascii=False) + "\n"

        if start is None or end is None:
            with self._session.open(data_path, "ab") as f:
                f.write(new_line.encode("utf-8"))
                f.flush()
            return
        self._replace_line_at(data_path, int(start), end, new_line)

    def append(self, event: Union[EventData, dict], key_paths: List[str]) -> int:
        """Append a single event with a required composite key; returns new EOF offset.
        - key_paths: list of JSON paths to form the composite key ("" if any missing)
        - Always attaches top-level _writerNonce (generated if absent)
        - Pure append; allows duplicates; no scanning/dedup; persists key fields from key_paths
        """
        if not isinstance(key_paths, list) or not all(
            isinstance(p, str) for p in key_paths
        ):
            raise ValueError("key_paths must be a list[str]")

        def _run():
            if _needs_migration(self._session, self._path):
                _migrate_sidecar(self._session, self._path, key_paths)

            # Normalize to dict first to attach identity before schema validation
            if isinstance(event, dict):
                rec = dict(event)
            else:
                raise InvalidEventError(
                    "event must be dict or BaseData"
                )

            content_key = EventData.content
            content_val = rec.get(content_key)
            if content_val is None:
                rec[content_key] = {}
            elif not isinstance(content_val, dict):
                raise InvalidEventError("event content must be a dict")

            # Compute composite key (not persisted); ensures caller provided a usable set
            _ = self._extract_composite_key(rec, key_paths)

            # Ensure top-level nonce exists
            nonce_key = EventData._writerNonce
            if not isinstance(rec.get(nonce_key), str) or not rec.get(nonce_key):
                rec[nonce_key] = str(uuid.uuid4())

            try:
                ev = EventData(rec)
            except Exception as e:
                raise InvalidEventError(f"Invalid event: {e}") from e

            self._append_data_record(ev._writerNonce, ev.content)
            payload = self._build_event_line(rec, key_paths).encode("utf-8")
            try:
                os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
            except Exception as e:
                raise StreamWriteError(
                    f"Failed ensuring directory for {self._path}: {e}"
                ) from e

            with self._lock:
                with self._session.open(self._path, "ab") as f:
                    f.write(payload)
                    f.flush()
                    offset = f.tell()
            self._assert_sidecar_has_nonce(ev._writerNonce)
            return offset

        return _with_store_lock(self._session, self._path, _run)

    def upsert(self, event: Union[EventData, dict], key_paths: List[str]) -> int:
        """
        Replace the last record that matches the composite key defined by key_paths with the provided event,
        but only if content differs. If no prior record exists, append. Returns the byte offset where the
        record resides after operation (start position of the line).

        - key_paths: list of JSON paths like ["name"] or ["person.id"] or ["age","location","person.height"].
          The composite key is the concatenation of each value looked up via these paths ("" if missing).
        - No hashing and no fuzzy logic; exact serialized line equality check determines if replacement is needed.
        """
        if not isinstance(key_paths, list) or not all(
            isinstance(p, str) for p in key_paths
        ):
            raise ValueError("key_paths must be a list[str]")

        def _run():
            if _needs_migration(self._session, self._path):
                _migrate_sidecar(self._session, self._path, key_paths)

            # Normalize to dict first; attach identity before validation
            if isinstance(event, dict):
                rec = dict(event)
            else:
                raise InvalidEventError(
                    "event must be dict or BaseData"
                )

            content_key = EventData.content
            content_val = rec.get(content_key)
            if content_val is None:
                rec[content_key] = {}
            elif not isinstance(content_val, dict):
                raise InvalidEventError("event content must be a dict")

            comp_key = self._extract_composite_key(rec, key_paths)

            # Deterministic serialization prepared after EventData validation below
            new_line = None  # will set after identity handling

            # Ensure directory exists
            try:
                os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
            except Exception as e:
                raise StreamWriteError(
                    f"Failed ensuring directory for {self._path}: {e}"
                ) from e

            with self._lock:
                start, end, old_line = self._find_last_by_composite_key(comp_key, key_paths)
                # Not found → append; attach nonce for inserts (always ensure top-level)
                if start is None:
                    nonce_key = EventData._writerNonce
                    if not isinstance(rec.get(nonce_key), str) or not rec.get(nonce_key):
                        rec[nonce_key] = str(uuid.uuid4())
                    new_line = self._build_event_line(rec, key_paths)
                    with self._session.open(self._path, "ab") as f:
                        pos = f.tell()
                        f.write(new_line.encode("utf-8"))
                        f.flush()
                    self._upsert_sidecar_by_key(rec, key_paths, comp_key)
                    self._assert_sidecar_has_nonce(rec[nonce_key])
                    return pos

                # Found → if identical, no-op
                if start is None:
                    raise StreamWriteError(
                        "Internal error: missing start offset for upsert replace"
                    )
                start_int = int(start)
                if isinstance(old_line, str):
                    # Preserve existing nonce on replace
                    try:
                        old_obj = json.loads(old_line)
                    except Exception:
                        old_obj = {}
                    nonce_key = EventData._writerNonce
                    old_nonce = (old_obj or {}).get(nonce_key)
                    rec[nonce_key] = (
                        old_nonce or rec.get(nonce_key) or str(uuid.uuid4())
                    )
                    old_content = self._read_sidecar_payload_by_nonce(rec[nonce_key])
                    new_content = rec.get(content_key) or {}
                    rec[content_key] = {**old_content, **new_content}
                    new_line = self._build_event_line(rec, key_paths)
                    if old_line == new_line:
                        self._upsert_sidecar_by_key(rec, key_paths, comp_key)
                        self._assert_sidecar_has_nonce(rec[nonce_key])
                        return start_int
                else:
                    # Defensive: missing prior line content; ensure nonce and build new line
                    nonce_key = EventData._writerNonce
                    rec[nonce_key] = rec.get(nonce_key) or str(uuid.uuid4())
                    old_content = self._read_sidecar_payload_by_nonce(rec[nonce_key])
                    new_content = rec.get(content_key) or {}
                    rec[content_key] = {**old_content, **new_content}
                    new_line = self._build_event_line(rec, key_paths)

                if end is None:
                    raise StreamWriteError(
                        "Internal error: missing end offset for upsert replace"
                    )
                self._replace_line_at(self._path, start_int, end, new_line)
                self._upsert_sidecar_by_key(rec, key_paths, comp_key)
                self._assert_sidecar_has_nonce(rec[nonce_key])
                return start_int

        return _with_store_lock(self._session, self._path, _run)

    def delete_by_key(self, event: Union[EventData, dict], key_paths: List[str]) -> int:
        if not isinstance(key_paths, list) or not all(
            isinstance(p, str) for p in key_paths
        ):
            raise ValueError("key_paths must be a list[str]")

        def _run():
            if isinstance(event, dict):
                rec = dict(event)
            else:
                raise InvalidEventError("event must be dict or BaseData")

            content_key = EventData.content
            content_val = rec.get(content_key)
            if content_val is None:
                rec[content_key] = {}
            elif not isinstance(content_val, dict):
                raise InvalidEventError("event content must be a dict")

            comp_key = self._extract_composite_key(rec, key_paths)
            if comp_key == "":
                return -1
            start, end, old_line = self._find_last_by_composite_key(comp_key, key_paths)
            if start is None or end is None or not old_line:
                return -1
            try:
                old_obj = json.loads(old_line)
            except Exception as e:
                raise StreamWriteError(f"Delete target invalid JSON: {e}") from e
            nonce_key = EventData._writerNonce
            nonce = old_obj.get(nonce_key)
            if not isinstance(nonce, str) or not nonce:
                raise StreamWriteError("Delete target missing nonce")
            self._delete_line_at(self._path, start, end)
            self._delete_sidecar_by_nonce(nonce)
            return start

        return _with_store_lock(self._session, self._path, _run)

    def _extract_composite_key(self, obj: dict, key_paths: List[str]) -> str:
        def get_path(o, path: str) -> str:
            cur = o
            for part in path.split("."):
                if isinstance(cur, dict) and part in cur:
                    cur = cur[part]
                else:
                    return ""
            return "" if cur is None else str(cur)

        return "".join(get_path(obj, p) for p in key_paths)

    def _file_size(self) -> int:
        try:
            with self._session.open(self._path, "rb") as f:
                f.seek(0, 2)
                return f.tell()
        except FileNotFoundError:
            return 0

    def _find_last_by_composite_key(
        self,
        comp_key: str,
        key_paths: List[str],
        *,
        line_to_obj=None,
        path: Optional[str] = None,
        chunk_size: int = 65536,  # unused; kept for signature compatibility
        max_scan_bytes: int = 2
        * 1024
        * 1024,  # unused; kept for signature compatibility
    ) -> Tuple[Optional[int], Optional[int], Optional[str]]:
        """Forward-only scan to compute exact byte offsets for last matching line.

        This avoids positional errors from chunked tail scans and guarantees that
        (start, end) correspond exactly to NDJSON line boundaries.
        """
        if line_to_obj is None:
            def _default_line_to_obj(s: str) -> dict:
                return json.loads(s)
            line_to_obj = _default_line_to_obj
        if path is None:
            path = self._path
        try:
            with self._session.open(path, "rb") as f:
                cur = 0
                found_start: Optional[int] = None
                found_end: Optional[int] = None
                found_line: Optional[str] = None
                while True:
                    raw = f.readline()
                    if not raw:
                        break
                    s = raw.decode("utf-8", errors="ignore")
                    try:
                        obj = line_to_obj(s)
                    except Exception:
                        cur += len(raw)
                        continue
                    if self._extract_composite_key(obj, key_paths) == comp_key:
                        found_start = cur
                        found_end = cur + len(raw)
                        found_line = s
                    cur += len(raw)
                return found_start, found_end, found_line
        except FileNotFoundError:
            return None, None, None

    def _replace_line_at(self, path: str, start: int, end: int, new_line: str) -> None:
        tmp = f"{path}.tmp"
        # Use session for I/O where possible; fallback to os.replace for atomic swap
        with (
            self._session.open(path, "rb") as src,
            self._session.open(tmp, "wb") as dst,
        ):
            if start > 0:
                src.seek(0)
                to_copy = start
                while to_copy > 0:
                    chunk = src.read(min(65536, to_copy))
                    if not chunk:
                        break
                    dst.write(chunk)
                    to_copy -= len(chunk)
            # Guard: ensure replacement line has exactly one trailing newline
            if not isinstance(new_line, str) or not new_line.endswith("\n"):
                raise StreamWriteError("Replacement line missing trailing newline")
            dst.write(new_line.encode("utf-8"))
            src.seek(end)
            while True:
                chunk2 = src.read(65536)
                if not chunk2:
                    break
                dst.write(chunk2)
            dst.flush()
            # Some session implementations may not support fileno/fsync; best-effort
            try:
                os.fsync(dst.fileno())  # type: ignore[attr-defined]
            except Exception:
                pass
        # Atomic replace within sandbox using session
        try:
            if hasattr(self._session, "replace"):
                self._session.replace(tmp, path)  # type: ignore[attr-defined]
            else:
                # Fallback to absolute replace if session lacks API
                abs_src = (
                    self._session._abs_path(tmp)
                    if hasattr(self._session, "_abs_path")
                    else tmp
                )
                abs_dst = (
                    self._session._abs_path(path)
                    if hasattr(self._session, "_abs_path")
                    else path
                )
                os.replace(abs_src, abs_dst)
        except Exception as e:
            raise StreamWriteError(
                f"Failed atomic replace: {tmp} -> {path}: {e}"
            ) from e

    def _delete_line_at(self, path: str, start: int, end: int) -> None:
        tmp = f"{path}.tmp"
        with (
            self._session.open(path, "rb") as src,
            self._session.open(tmp, "wb") as dst,
        ):
            if start > 0:
                src.seek(0)
                to_copy = start
                while to_copy > 0:
                    chunk = src.read(min(65536, to_copy))
                    if not chunk:
                        break
                    dst.write(chunk)
                    to_copy -= len(chunk)
            src.seek(end)
            while True:
                chunk2 = src.read(65536)
                if not chunk2:
                    break
                dst.write(chunk2)
            dst.flush()
            try:
                os.fsync(dst.fileno())  # type: ignore[attr-defined]
            except Exception:
                pass
        try:
            if hasattr(self._session, "replace"):
                self._session.replace(tmp, path)  # type: ignore[attr-defined]
            else:
                abs_src = (
                    self._session._abs_path(tmp)
                    if hasattr(self._session, "_abs_path")
                    else tmp
                )
                abs_dst = (
                    self._session._abs_path(path)
                    if hasattr(self._session, "_abs_path")
                    else path
                )
                os.replace(abs_src, abs_dst)
        except Exception as e:
            raise StreamWriteError(
                f"Failed atomic delete: {tmp} -> {path}: {e}"
            ) from e


def _parse_utc(dt_s: str) -> datetime:
    return datetime.fromisoformat(dt_s.replace("Z", "+00:00")).astimezone(timezone.utc)


class NDJSONReader:
    def __init__(self, session, path: str):
        self._session = session
        self._path = path

    def read_all(self) -> List[EventData]:
        def _run():
            if _needs_migration(self._session, self._path):
                _migrate_sidecar(self._session, self._path)
            data_map = self._load_data_map()
            out: List[EventData] = []
            with self._session.open(self._path, "rb") as f:
                lineno = 0
                for raw in f:
                    lineno += 1
                    if not raw:
                        continue
                    line = raw.decode("utf-8").strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                    except Exception as e:
                        raise StreamReadError(
                            f"Malformed JSON at line {lineno}: {e}"
                        ) from e
                    try:
                        ev = EventData(data)
                        if ev._writerNonce in data_map:
                            ev.content = data_map[ev._writerNonce]
                        out.append(ev)
                    except Exception as e:
                        raise StreamReadError(
                            f"Schema violation at line {lineno}: {e}"
                        ) from e
            return out

        return _with_store_lock(self._session, self._path, _run)

    def read_from_offset(self, offset: int) -> Tuple[List[EventData], int]:
        if not isinstance(offset, int) or offset < 0:
            raise ValueError("offset must be a non-negative int")

        def _run():
            if _needs_migration(self._session, self._path):
                _migrate_sidecar(self._session, self._path)

            data_map = self._load_data_map()

            with self._session.open(self._path, "rb") as f:
                f.seek(0, 2)
                end = f.tell()
                if offset > end:
                    raise StreamReadError(
                        f"Offset {offset} beyond EOF {end} (truncated or rotated)"
                    )
                if offset == end:
                    return [], end

                # Position at requested offset
                f.seek(offset, 0)

                # align to next line boundary if mid-line
                if offset > 0:
                    f.seek(offset - 1, 0)
                    prev = f.read(1)
                    if prev != b"\n":
                        f.readline()  # discard remainder of partial
                # else offset == 0: start at file start

                events: List[EventData] = []
                while True:
                    pos_before = f.tell()
                    raw = f.readline()
                    if not raw:
                        break
                    line = raw.decode("utf-8").strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        ev = EventData(data)
                        if ev._writerNonce in data_map:
                            ev.content = data_map[ev._writerNonce]
                        events.append(ev)
                    except Exception as e:
                        raise StreamReadError(f"Error at byte {pos_before}: {e}") from e
                return events, f.tell()

        return _with_store_lock(self._session, self._path, _run)

    def _load_data_map(self) -> Dict[str, dict]:
        data_path = _data_path(self._path)
        out: Dict[str, dict] = {}
        try:
            with self._session.open(data_path, "rb") as f:
                lineno = 0
                for raw in f:
                    lineno += 1
                    if not raw:
                        continue
                    line = raw.decode("utf-8").strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        data = EventDataContent(obj)
                    except Exception as e:
                        raise StreamReadError(
                            f"Malformed data JSON at line {lineno}: {e}"
                        ) from e
                    nonce = data._writerNonce
                    out[nonce] = _payload_from_data_record(data)
        except FileNotFoundError:
            return {}
        return out

    def search(
        self,
        labels: Optional[Dict[str, object]] = None,
        labels_mode: str = "all",  # "all" or "any"
        text: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
    ) -> List[EventData]:
        start_dt = _parse_utc(_to_iso_utc_z(since)) if since else None
        end_dt = _parse_utc(_to_iso_utc_z(until)) if until else None

        matches: List[EventData] = []
        for ev in self.read_all():
            ev_dt = _parse_utc(ev.datetime)
            if start_dt and ev_dt < start_dt:
                continue
            if end_dt and ev_dt > end_dt:
                continue
            if labels:
                items = list(labels.items())
                if labels_mode == "all":
                    if not all(ev.labels.get(k) == v for k, v in items):
                        continue
                elif labels_mode == "any":
                    if not any(ev.labels.get(k) == v for k, v in items):
                        continue
                else:
                    raise ValueError("labels_mode must be 'all' or 'any'")
            if text:
                hay = json.dumps(ev.content, separators=(",", ":"), ensure_ascii=False)
                if text not in hay:
                    continue
            matches.append(ev)
        return matches
