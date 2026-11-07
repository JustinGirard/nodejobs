import os

from nodejobs.dependencies.BaseData import BaseData
from nodejobs.jobs import default_db_path


class AppState(BaseData):
    db_path: (str, "")
    dev_mode: (bool, False)
    connected: (bool, False)
    error: (str, "")

    @classmethod
    def from_env(cls):
        dev_raw = os.environ.get("NODEJOBS_UI_DEV_MODE", "").strip().lower()
        is_dev = dev_raw in ("1", "true", "yes", "on")
        env_db_path = os.environ.get("NODEJOBS_UI_DB_PATH", "").strip()
        resolved_db_path = env_db_path if env_db_path else default_db_path()
        return cls(
            {
                cls.db_path: resolved_db_path,
                cls.dev_mode: is_dev,
                cls.connected: bool(resolved_db_path),
                cls.error: "",
            }
        )

    def has_db_path(self) -> bool:
        return bool(str(self.db_path).strip())

    def is_dev_mode(self) -> bool:
        return bool(self.dev_mode)

    def connect_db(self, db_path: str) -> None:
        self.db_path = str(db_path or "").strip()
        self.connected = bool(self.db_path)
        self.error = ""

    def disconnect_db(self, error: str = "") -> None:
        self.connected = False
        self.error = str(error or "")
