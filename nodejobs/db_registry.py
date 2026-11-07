import json
import os
import time
from pathlib import Path

from nodejobs.dependencies.BaseData import BaseData


class KnownDbRegistryConfig(BaseData):
    file_path: str
    legacy_file_path: (str, "")
    temp_db_path: str


class KnownDbPathList(BaseData):
    paths: (list, [])


class KnownDbRegistrationResult(BaseData):
    db_path: str
    paths: (list, [])
    wrote_primary: bool


class KnownDbRegistry:
    _WRITE_ATTEMPTS = 3

    def __init__(self, config):
        self.config = (
            config if isinstance(config, KnownDbRegistryConfig) else KnownDbRegistryConfig(config)
        )

    @classmethod
    def from_home(cls, temp_db_path: str):
        home = str(Path.home())
        return cls(
            {
                KnownDbRegistryConfig.file_path: os.path.join(home, ".nodejobs_known_dbs.json"),
                KnownDbRegistryConfig.legacy_file_path: os.path.join(home, ".nodejobs_ui_known_dbs.json"),
                KnownDbRegistryConfig.temp_db_path: str(temp_db_path or "").strip(),
            }
        )

    def load_paths(self) -> list[str]:
        primary = self._safe_read_disk_paths(str(self.config.file_path or "").strip())
        legacy = self._safe_read_disk_paths(str(self.config.legacy_file_path or "").strip())
        return self._merge_paths(primary + legacy)

    def remember(self, db_path: str) -> list[str]:
        result = self.remember_many([db_path])
        return list(result.paths)

    def register_from_job_start(self, db_path: str) -> KnownDbRegistrationResult:
        return self._merge_append_and_write([db_path])

    def remember_many(self, db_paths: list[str]) -> KnownDbRegistrationResult:
        return self._merge_append_and_write(db_paths)

    def _safe_read_disk_paths(self, file_path: str) -> list[str]:
        if not file_path:
            return []
        try:
            return self._read_disk_paths(file_path)
        except Exception:
            return []

    def _read_disk_paths(self, file_path: str) -> list[str]:
        if not os.path.isfile(file_path):
            return []
        with open(file_path, "r") as f:
            raw = json.load(f)
        if not isinstance(raw, list):
            raise ValueError("known db file must be a JSON list")
        payload = KnownDbPathList({KnownDbPathList.paths: raw})
        return self._merge_paths(payload.paths)

    def _write_disk_paths(self, paths: list[str]) -> None:
        payload = KnownDbPathList({KnownDbPathList.paths: self._merge_paths(paths)})
        file_path = str(self.config.file_path or "").strip()
        if not file_path:
            raise ValueError("known db file_path is required")
        parent = os.path.dirname(file_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(file_path, "w") as f:
            json.dump(list(payload.paths), f, indent=2)

    def _write_disk_paths_atomic(self, paths: list[str]) -> None:
        payload = KnownDbPathList({KnownDbPathList.paths: self._merge_paths(paths)})
        file_path = str(self.config.file_path or "").strip()
        if not file_path:
            raise ValueError("known db file_path is required")
        parent = os.path.dirname(file_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        stamp = int(time.time() * 1000)
        temp_name = f".{os.path.basename(file_path)}.tmp.{os.getpid()}.{stamp}"
        temp_path = os.path.join(parent if parent else ".", temp_name)
        try:
            with open(temp_path, "w") as f:
                json.dump(list(payload.paths), f, indent=2)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass
            os.replace(temp_path, file_path)
        finally:
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass

    def _normalize_many(self, db_paths: list[str]) -> list[str]:
        out = []
        if not isinstance(db_paths, list):
            return out
        for raw in db_paths:
            text = self._normalize_path(raw)
            if text and text not in out:
                out.append(text)
        return out

    def _merge_append_and_write(self, db_paths: list[str]) -> KnownDbRegistrationResult:
        candidates = self._normalize_many(db_paths)
        if len(candidates) == 0:
            return KnownDbRegistrationResult(
                {
                    KnownDbRegistrationResult.db_path: "",
                    KnownDbRegistrationResult.paths: self.load_paths(),
                    KnownDbRegistrationResult.wrote_primary: False,
                }
            )

        last_paths = []
        for _ in range(self._WRITE_ATTEMPTS):
            latest = self.load_paths()
            merged = self._merge_paths(latest + candidates)
            self._write_disk_paths_atomic(merged)
            last_paths = self.load_paths()
            if all(candidate in last_paths for candidate in candidates):
                return KnownDbRegistrationResult(
                    {
                        KnownDbRegistrationResult.db_path: candidates[0],
                        KnownDbRegistrationResult.paths: last_paths,
                        KnownDbRegistrationResult.wrote_primary: True,
                    }
                )

        return KnownDbRegistrationResult(
            {
                KnownDbRegistrationResult.db_path: candidates[0],
                KnownDbRegistrationResult.paths: last_paths if last_paths else self.load_paths(),
                KnownDbRegistrationResult.wrote_primary: True,
            }
        )

    def _merge_paths(self, paths: list[str]) -> list[str]:
        ordered = []
        temp_path = self._normalize_path(self.config.temp_db_path)
        if temp_path:
            ordered.append(temp_path)
        for raw in paths:
            p = self._normalize_path(raw)
            if p and p not in ordered:
                ordered.append(p)
        return ordered

    def _normalize_path(self, value) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        return os.path.abspath(os.path.expanduser(text))
