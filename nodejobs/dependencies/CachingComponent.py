import os
import json
import uuid
import base64
from typing import Any, Dict, List, Union
from nodejobs.dependencies.BaseData import BaseData
from nodejobs.dependencies.BaseSession import DataSession
import hashlib
from functools import wraps
import datetime
import time

# import imghdr
from nodejobs.events.models import EventData
from nodejobs.events.ndjson_events import (
    NDJSONWriter,
    NDJSONReader,
    InvalidEventError,
    StreamReadError,
)
from nodejobs.events.watcher import WatchConfig, WatchFilter, NDJSONWatcher
from nodejobs.events.data_session_append import DataSessionAppend
from nodejobs.jobs import Jobs


class CachingComponent(BaseData):
    do_purge: bool
    cache_path: str
    data_session: DataSession
    b_empty, b_ready = "empty", "ready"
    cache_key_dir_size = "cache_dir_size"

    class tCommandSpec(BaseData):
        required_args: list
        method: object

    class tCommandMap(BaseData):
        pass

    class tMediaFetchResult(BaseData):
        path: str
        size: int
        encoding: str
        data_b64: str

    def _guard_direct_use(self):
        return

    def __init__(self, in_dict=None, trim=True):
        super().__init__(in_dict, trim=trim)
        self._guard_direct_use()

    def do_pre_process(self, in_dict):
        if CachingComponent.do_purge not in in_dict:
            in_dict[CachingComponent.do_purge] = False
        return super().do_pre_process(in_dict)

    def log(self, messgage, is_error=False):
        # Logging must never crash execution. Unity may launch MCP with stdout/stderr
        # redirected; after domain reload the reader can disappear, causing BrokenPipeError.
        try:
            print(messgage, flush=True)
            return
        except (BrokenPipeError, ValueError):
            pass
        except Exception:
            pass

        # Fallback: append to a per-node log file under the cache namespace.
        try:
            root = self.data_session.get_root()
            base = os.path.normpath(os.path.join(root, self.cache_path))
            exec_dir = os.path.join(base, "executions")
            os.makedirs(exec_dir, exist_ok=True)
            path = os.path.join(exec_dir, "mcp.log")
            with open(path, "a", encoding="utf-8") as f:
                f.write(str(messgage))
                f.write("\n")
        except Exception:
            return

    def get_command_map(self):
        """
        OVERRIDE ME
        """
        self._guard_direct_use()
        return CachingComponent.tCommandMap({})

    def media_fetch(self, path: str):
        self._guard_direct_use()
        if path is None: raise Exception("media requires args.path")
        rel_path = str(path).lstrip("/")
        base = os.path.normpath(os.path.join(self.data_session.get_root(), self.cache_path))
        full = os.path.normpath(os.path.join(base, rel_path))
        if not full.startswith(base + os.sep) and full != base:
            raise Exception("media path escapes cache root")
        if not os.path.isfile(full):
            raise FileNotFoundError("media not found")
        with open(full, "rb") as f:
            raw = f.read()
        b64 = base64.b64encode(raw).decode("ascii")
        return CachingComponent.tMediaFetchResult({
            CachingComponent.tMediaFetchResult.path: rel_path,
            CachingComponent.tMediaFetchResult.size: len(raw),
            CachingComponent.tMediaFetchResult.encoding: "base64",
            CachingComponent.tMediaFetchResult.data_b64: b64,
        })

    def awake(self):
        session = DataSession(self.data_session)
        if self.do_purge == True:
            if session.exists(self.cache_path):
                print(f"... purging {self.cache_path}")
                session.delete(self.cache_path)

        if not session.exists(self.cache_path):
            session.mkdir(self.cache_path)
        # self.set_behaviour(self.b_ready)

    def get_full_data_session(self) -> DataSession:
        return DataSession(self.data_session)

    def get_rel_cache_path(self) -> str:
        return self.cache_path

    def get_cache_data_session(self) -> DataSession:
        ds = DataSession(
            {
                DataSession.f_root: os.path.join(
                    self.data_session.get_root(), self.cache_path
                )
            }
        )
        if DataSession.f_unlocked in self.data_session:
            ds[DataSession.f_unlocked] = self.data_session[DataSession.f_unlocked]
        return ds

    def resolve_session_paths(self, value):
        if isinstance(value, str):
            if value.startswith("/") or value.startswith("\\"):
                full = os.path.abspath(value)
                if os.path.exists(full):
                    return full
                root = os.path.abspath(self.data_session.get_root())
                if os.path.commonpath([root, full]) == root:
                    return value
                resolved = os.path.join(root, value.lstrip("/\\"))
                if not os.path.exists(resolved):
                    return value
                return resolved
            return value
        if isinstance(value, list):
            return [self.resolve_session_paths(v) for v in value]
        if isinstance(value, dict):
            return {k: self.resolve_session_paths(v) for k, v in value.items()}
        return value

    # @classmethod
    # def get_job_db_location(cls,data_session:DataSession):
    #     db_path = os.path.join(data_session.get_root(), "nodejobs_db")
    #     return db_path

    def peek_events(self, **kwargs) -> str:
        job_id = kwargs["job_id"]
        db_path = kwargs["job_db_path"]
        # cache_ds = self.get_cache_data_session()
        # db_path = self.get_job_db_location(cache_ds)
        stdout_text, stderr_text = Jobs(db_path=db_path, verbose=False).job_logs(job_id)
        return {
            "job_id": job_id,
            "db_path": db_path,
            "stdout": stdout_text,
            "stderr": stderr_text,
            "events": [],
        }

    def _generate_cache_filepath(self, key: str, prefix: str) -> str:
        h = hashlib.md5(key.encode("utf-8")).hexdigest()
        safe = str(h)
        if prefix:
            return os.path.join(prefix, f"{safe}.json")
        return os.path.join(f"{safe}.json")

    def _sanitize_store_id(self, store: str | None) -> str:
        s = store if isinstance(store, str) and len(store) > 0 else "default"
        # Allow alnum, dash, underscore only; replace others with '_'
        return "".join(ch if (ch.isalnum() or ch in ("-", "_")) else "_" for ch in s)

    def _event_store_relpath(self, store: str = "default") -> str:
        sid = self._sanitize_store_id(store)
        return f"event_store.{sid}.ndjson"

    def sniff_image_ext(self, b: bytes) -> str | None:
        if b.startswith(b"\x89PNG\r\n\x1a\n"):
            return "png"
        if b.startswith(b"\xff\xd8"):
            return "jpg"  # JPEG
        if b[:6] in (b"GIF87a", b"GIF89a"):
            return "gif"
        if (b[:4] == b"II*\x00") or (b[:4] == b"MM\x00*"):
            return "tiff"
        if b[:2] == b"BM":
            return "bmp"
        if b[:4] == b"RIFF" and b[8:12] == b"WEBP":
            return "webp"
        if b[:4] == b"\x00\x00\x01\x00":
            return "ico"
        return None

    def _blob_relpath(self, sha256_hex: str) -> str:
        return os.path.join("_blobs", sha256_hex)

    def _json_default_factory(self, session, base_dir: str):
        def _default(obj):
            if isinstance(obj, datetime.datetime):  # NEW
                if obj.tzinfo is None or obj.tzinfo.utcoffset(obj) is None:
                    return {
                        "$datetime_naive": obj.isoformat(timespec="microseconds")
                    }  # NEW
                else:
                    return {
                        "$datetime_utc": obj.astimezone(
                            datetime.timezone.utc
                        ).isoformat()
                    }  # NEW
            if isinstance(obj, datetime.date) and not isinstance(
                obj, datetime.datetime
            ):  # NEW
                return {"$date": obj.isoformat()}  # NEW
            if isinstance(obj, datetime.time):  # NEW
                if obj.tzinfo is None or obj.tzinfo.utcoffset(None) is None:
                    return {"$time": obj.isoformat(timespec="microseconds")}  # NEW
                else:
                    # Normalize aware times to UTC for portability  # NEW
                    _dt = datetime.datetime.combine(
                        datetime.date(1970, 1, 1), obj
                    ).astimezone(datetime.timezone.utc)  # NEW
                    return {"$time_utc": _dt.timetz().isoformat()}  # NEW

            if isinstance(obj, (bytes, bytearray, memoryview)):
                b = bytes(obj)
                sha = hashlib.sha256(b).hexdigest()
                ext = self.sniff_image_ext(b)
                if ext != None and ext != "None":
                    rel = os.path.join(base_dir, self._blob_relpath(sha) + f".{ext}")
                else:
                    rel = os.path.join(base_dir, self._blob_relpath(sha))

                # rel = os.path.join(base_dir,self._blob_relpath( sha))
                d = os.path.dirname(rel)
                if not session.exists(d):
                    session.mkdir(d)
                if not session.exists(rel):
                    with session.open(rel, "wb") as fh:
                        fh.write(b)
                return {
                    "$binary_ref": self._blob_relpath(sha) + f".{ext}",
                    "sha256": sha,
                }
            # Let json raise for unsupported types
            raise TypeError(
                f"Object of type {type(obj).__name__} is not JSON serializable"
            )

        return _default

    def _json_object_hook_factory(self, session, base_dir):
        def _hook(obj):
            if isinstance(obj, dict):
                v = obj.get("$datetime_utc")
                if isinstance(v, str):
                    return datetime.datetime.fromisoformat(v)
                v = obj.get("$datetime_naive")
                if isinstance(v, str):
                    return datetime.datetime.fromisoformat(v)
                v = obj.get("$date")
                if isinstance(v, str):
                    return date.fromisoformat(v)
                v = obj.get("$time_utc")
                if isinstance(v, str):
                    return time.fromisoformat(v)
                v = obj.get("$time")
                if isinstance(v, str):
                    return time.fromisoformat(v)  # naive  # NEW
            ref = obj.get("$binary_ref") if isinstance(obj, dict) else None
            if isinstance(ref, str):
                try:
                    with session.open(os.path.join(base_dir, ref), "rb") as fh:
                        b = fh.read()
                except:
                    if ref.endswith(".None"):
                        with session.open(
                            os.path.join(base_dir, ref.replace(".None", "")), "rb"
                        ) as fh:
                            b = fh.read()

                # sha = obj.get("sha256")
                # with session.open(os.path.join(base_dir,self._blob_relpath( sha)+ f".{ext}"), "rb") as fh:
                #    b = fh.read()
                # if sha and hashlib.sha256(b).hexdigest() != sha:
                #    raise ValueError(f"Binary blob hash mismatch for {ref}")
                return b
            return obj

        return _hook

    def make_cache_key(self, data: dict, excluded_keys: list[str]) -> str:
        d = dict(data)
        for k in excluded_keys:
            try:
                del d[k]
            except KeyError:
                pass
        return hashlib.md5(json.dumps(d, sort_keys=True).encode()).hexdigest()

    def load_processed_cache(self, query, cache_key, prefix=None):
        if query["overwrite"] == True:
            # print("PURGING")
            purged = self.purge_cache(key=cache_key, prefix=prefix)
            assert purged == True, (
                "Could not remove a previous cache. You need to debug this."
            )
        # print("Loading")
        cached = self.load_cache(key=cache_key, prefix=prefix)
        # print(f"Loading {cache_key}:{str(cached)[0:100]}")
        return cached

    def write_cache_key(self, prefix, key, id: str, value: str) -> None:
        data = self.load_cache(prefix=prefix, key=key, raw_mode=False)
        try:
            meta = json.load(data)
        except Exception:
            meta = {}
        meta[key] = value
        self.save_cache(prefix=prefix, key=key, data=data)

    # --- Updated cache I/O using hooks ---------------------------------------
    def load_cache(self, key: str, prefix: str = None, raw_mode: bool = False) -> Any:
        rel_path = self._generate_cache_filepath(key, prefix)
        return self.load_cache_path(rel_path, raw_mode)

    def load_cache_path(self, rel_path: str, raw_mode: bool = False) -> Any:
        session = DataSession(self.data_session)
        dir_rel = os.path.dirname(os.path.join(self.cache_path, rel_path))
        if dir_rel == None or len(dir_rel) == 0:
            assert len(rel_path) > 0
            dir_rel = "./"

        # print(f"[+] load_ing_cache {key}:{rel_path}")
        if not session.exists(os.path.join(self.cache_path, rel_path)):
            return None
        with session.open(os.path.join(self.cache_path, rel_path), "r") as f:
            # print(f"[+] FOUND load_cache {key}:{os.path.join(session.get_root(), rel_path)}")
            if raw_mode == False:
                return json.load(
                    f, object_hook=self._json_object_hook_factory(session, dir_rel)
                )
            else:
                return json.load(f)

    def save_cache(self, key: str, data: Dict[str, Any], prefix: str = None) -> None:
        # session = DataSession(self.data_session)
        rel_path = self._generate_cache_filepath(key, prefix)
        return self.save_cache_path(rel_path, data)

    def save_cache_path(self, rel_path: str, data: Dict[str, Any]) -> None:
        session = DataSession(self.data_session)
        dir_rel = os.path.dirname(os.path.join(self.cache_path, rel_path))
        if dir_rel == None or len(dir_rel) == 0:
            assert len(rel_path) > 0
            dir_rel = "./"
        # print(f"[-] sav_ing_cache {dir_rel}:({rel_path})")
        if not session.exists(dir_rel):
            session.mkdir(dir_rel)
        with session.open(os.path.join(self.cache_path, rel_path), "w") as f:
            # print(f"[-] WRTITING sav_ing_cache {os.path.join(session.get_root(), rel_path)}")
            json.dump(
                data,
                f,
                indent=2,
                default=self._json_default_factory(session, dir_rel),
            )

    # ---- Run Guard (cache-backed, best-effort concurrency gate) --------------
    def _run_guard_key(self, cache_key: str) -> str:
        return f"{cache_key}_run_guard"

    def load_run_guard(self, cache_key: str, prefix: str | None = None) -> dict | None:
        g = self.load_cache(
            key=self._run_guard_key(cache_key), prefix=prefix, raw_mode=True
        )
        if not isinstance(g, dict):
            return None
        now = time.time()

        # If the heartbeat hasn't updated in >2 minutes, assume the process died
        # and unblock future executions by purging the guard.
        last = g.get("updated_at")
        if not isinstance(last, (int, float)):
            last = g.get("started_at")
        if isinstance(last, (int, float)) and (now - float(last)) > 120.0:
            try:
                self.purge_cache(key=self._run_guard_key(cache_key), prefix=prefix)
            except Exception:
                pass
            return None

        exp = g.get("expires_at")
        if isinstance(exp, (int, float)) and exp > now:
            return g
        return None

    def try_acquire_run_guard(
        self,
        cache_key: str,
        prefix: str | None,
        owner_uuid: str,
        ttl_sec: int = 600,
    ) -> tuple[bool, dict]:
        existing = self.load_run_guard(cache_key, prefix=prefix)
        if isinstance(existing, dict) and existing.get("owner_uuid") != owner_uuid:
            return False, existing
        now = time.time()
        g = {
            "owner_uuid": owner_uuid,
            "status": "running",
            "cache_key": cache_key,
            "started_at": now,
            "updated_at": now,
            "expires_at": now + float(ttl_sec),
            "heartbeat": 0,
        }
        self.save_cache(key=self._run_guard_key(cache_key), data=g, prefix=prefix)
        return True, g

    def update_run_guard(
        self,
        cache_key: str,
        prefix: str | None,
        owner_uuid: str,
        *,
        status: str | None = None,
        message: str | None = None,
        stdout_tail: str | None = None,
        nodes_done: list[str] | None = None,
        extend_ttl_sec: int = 600,
    ) -> dict:
        key = self._run_guard_key(cache_key)
        g = self.load_cache(key=key, prefix=prefix, raw_mode=True)
        if not isinstance(g, dict):
            raise Exception(f"run_guard missing: {key}")
        if g.get("owner_uuid") != owner_uuid:
            raise Exception(f"run_guard owner mismatch: {key} owner={g.get('owner_uuid')} ours={owner_uuid}")
        now = time.time()
        g["updated_at"] = now
        g["expires_at"] = now + float(extend_ttl_sec)
        g["heartbeat"] = int(g.get("heartbeat") or 0) + 1
        if status is not None:
            g["status"] = status
        if message is not None:
            g["message"] = message
        if stdout_tail is not None:
            g["stdout_tail"] = stdout_tail
        if nodes_done is not None:
            g["nodes_done"] = nodes_done
        self.save_cache(key=key, data=g, prefix=prefix)
        return g

    def release_run_guard(
        self,
        cache_key: str,
        prefix: str | None,
        owner_uuid: str,
        *,
        keep: bool = False,
        status: str = "done",
        final_message: str | None = None,
        keep_ttl_sec: int = 60,
    ) -> None:
        if not keep:
            self.purge_cache(key=self._run_guard_key(cache_key), prefix=prefix)
            return
        self.update_run_guard(
            cache_key,
            prefix,
            owner_uuid,
            status=status,
            message=final_message,
            extend_ttl_sec=keep_ttl_sec,
        )

    def _purge_cache_walk(self, session: DataSession, base_dir: str, obj: Any):
        if isinstance(
            obj, dict
        ):  # TODO - MINOR: Kinda gross but not terrible. Something to look into
            ref = obj.get("$binary_ref")
            if isinstance(ref, str) and ref.startswith("_blobs/"):
                target = os.path.join(base_dir, ref)
                try:
                    if session.exists(target):
                        with session.open(target, "rb"):
                            pass
                        session.delete(target)
                except Exception:
                    pass
            for v in obj.values():
                self._purge_cache_walk(session, base_dir, v)

        elif isinstance(obj, list):
            for v in obj:
                self._purge_cache_walk(session, base_dir, v)

    def purge_cache(self, key: str, prefix: str | None = None) -> bool:
        payload = self.load_cache(key, prefix, raw_mode=True)
        if payload is None:
            return True
        self._purge_cache_walk(
            self.get_full_data_session(),
            os.path.dirname(
                os.path.join(
                    self.cache_path, self._generate_cache_filepath(key, prefix)
                )
            ),
            payload,
        )
        self.get_full_data_session().delete(
            os.path.join(self.cache_path, self._generate_cache_filepath(key, prefix))
        )
        return True

    def get_cache_dir_size(self, overwrite: bool = False) -> Dict[str, Any]:
        cache_key = self.cache_key_dir_size
        if not overwrite:
            cached = self.load_cache(cache_key)
            if cached is not None:
                return cached
        session = self.get_full_data_session()
        if not session.exists(self.cache_path):
            res = {"total": 0.00, "units": "mb"}
            self.save_cache(cache_key, res)
            return res
        abs_dir = os.path.join(session.get_root(), self.cache_path)
        if not os.path.isdir(abs_dir):
            raise ValueError(f"cache_path is not a directory: {abs_dir}")
        total_bytes = 0
        for root, _, files in os.walk(abs_dir):
            for name in files:
                total_bytes += os.path.getsize(os.path.join(root, name))
        total_mb = round(total_bytes / (1024 * 1024), 2)
        res = {"total": total_mb, "units": "mb"}
        self.save_cache(cache_key, res)
        return res

    def clear_data(self) -> bool:
        return True

    # ---- Event Store Facade (emit/read/search/tail/watch) --------------------
    def emit(self, event, store: str = "default") -> int:
        """
        Append a single EventData to this component's canonical event store file.
        Returns new EOF byte offset.
        """
        ev = EventData(event)  # validate immediately; may raise InvalidEventError
        cache_sess = self.get_cache_data_session()
        # Use append-capable session for event writes (supports 'ab')
        append_sess = DataSessionAppend(
            {
                DataSessionAppend.f_root: cache_sess.get_root(),
                DataSessionAppend.f_verbose: False,
            }
        )
        if DataSessionAppend.f_unlocked in cache_sess:
            append_sess[DataSessionAppend.f_unlocked] = cache_sess[
                DataSessionAppend.f_unlocked
            ]
        writer = NDJSONWriter(append_sess, self._event_store_relpath(store))
        return writer.append(ev, [])

    def read_all(self, store: str = "default"):
        reader = NDJSONReader(
            self.get_cache_data_session(), self._event_store_relpath(store)
        )
        return reader.read_all()

    def read_from_offset(self, offset: int, store: str = "default"):
        reader = NDJSONReader(
            self.get_cache_data_session(), self._event_store_relpath(store)
        )
        return reader.read_from_offset(offset)

    def search(
        self,
        store: str = "default",
        labels: dict | None = None,
        labels_mode: str = "all",
        text: str | None = None,
        since=None,
        until=None,
    ):
        reader = NDJSONReader(
            self.get_cache_data_session(), self._event_store_relpath(store)
        )
        return reader.search(
            labels=labels, labels_mode=labels_mode, text=text, since=since, until=until
        )

    def tail(
        self,
        store: str = "default",
        poll_ms: int = 500,
        from_end: bool = True,
        wait_for_create: bool = True,
        on_truncate: str = "end",
        idle_stop_ms: int | None = None,
        labels: dict | None = None,
        labels_mode: str = "all",
        text: str | None = None,
        since=None,
        until=None,
        start_offset: int | None = None,
    ):
        session = self.get_cache_data_session()
        path = self._event_store_relpath(store)
        cfg = WatchConfig(
            {
                WatchConfig.path: path,
                WatchConfig.poll_ms: int(poll_ms),
                WatchConfig.from_end: bool(from_end),
                WatchConfig.wait_for_create: bool(wait_for_create),
                WatchConfig.on_truncate: str(on_truncate),
                WatchConfig.idle_stop_ms: idle_stop_ms,
            }
        )
        flt = None
        if (
            any(v is not None for v in (labels, text, since, until))
            or labels_mode != "all"
        ):
            flt = WatchFilter(
                {
                    WatchFilter.labels: labels,
                    WatchFilter.labels_mode: labels_mode,
                    WatchFilter.text: text,
                    WatchFilter.since: since,
                    WatchFilter.until: until,
                }
            )
        watcher = NDJSONWatcher(session, cfg, flt)
        for ev in watcher.tail(start_offset=start_offset):
            yield ev

    def watch(
        self,
        callback,
        store: str = "default",
        poll_ms: int = 500,
        from_end: bool = True,
        wait_for_create: bool = True,
        on_truncate: str = "end",
        idle_stop_ms: int | None = None,
        labels: dict | None = None,
        labels_mode: str = "all",
        text: str | None = None,
        since=None,
        until=None,
        start_offset: int | None = None,
    ):
        session = self.get_cache_data_session()
        path = self._event_store_relpath(store)
        cfg = WatchConfig(
            {
                WatchConfig.path: path,
                WatchConfig.poll_ms: int(poll_ms),
                WatchConfig.from_end: bool(from_end),
                WatchConfig.wait_for_create: bool(wait_for_create),
                WatchConfig.on_truncate: str(on_truncate),
                WatchConfig.idle_stop_ms: idle_stop_ms,
            }
        )
        flt = None
        if (
            any(v is not None for v in (labels, text, since, until))
            or labels_mode != "all"
        ):
            flt = WatchFilter(
                {
                    WatchFilter.labels: labels,
                    WatchFilter.labels_mode: labels_mode,
                    WatchFilter.text: text,
                    WatchFilter.since: since,
                    WatchFilter.until: until,
                }
            )
        NDJSONWatcher(session, cfg, flt).watch(callback, start_offset=start_offset)
