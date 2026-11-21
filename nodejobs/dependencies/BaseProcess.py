"""A minimal convention for managing an operational process."""

import json
from typing import Any

from nodejobs.dependencies.BaseData import BaseData


class ProcessStatus(BaseData):
    status: str
    description: Any

    def do_validation(self, key, value):
        if key == ProcessStatus.status and value not in ("ok", "error"):
            return value, "process_status_must_be_ok_or_error"
        if key == ProcessStatus.description:
            try:
                json.dumps(value)
            except (TypeError, ValueError) as error:
                return value, f"process_description_must_be_json:{error}"
        return value, ""


class BaseProcess:
    def ensure(self, **kwargs):
        raise NotImplementedError

    def teardown(self, **kwargs):
        raise NotImplementedError

    def status(self, **kwargs):
        raise NotImplementedError

    def restart(self, **kwargs):
        raise NotImplementedError

    @staticmethod
    def status_result(result):
        """Retain a provider's JSON operation detail in the standard envelope."""
        return ProcessStatus({
            ProcessStatus.status: "ok" if result.ok else "error",
            ProcessStatus.description: result,
        })
