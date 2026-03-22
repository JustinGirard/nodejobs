from typing import Union, Any, Dict, List
from datetime import datetime, timezone

from nodejobs.dependencies.BaseData import BaseData


def _to_iso_utc_z(dt: Union[str, datetime]) -> str:
    """Normalize input to ISO8601 UTC with trailing 'Z'."""
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        s = dt.isoformat().replace("+00:00", "Z")
        if not s.endswith("Z"):
            s = s + "Z"
        return s
    # string path
    try:
        if isinstance(dt, str) and dt.endswith("Z"):
            return dt
        parsed = datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
        parsed = parsed.astimezone(timezone.utc)
        return parsed.isoformat().replace("+00:00", "Z")
    except Exception:
        raise ValueError(f"Invalid datetime format (expect ISO8601 UTC): {dt}")


class EventData(BaseData):
    # required
    datetime: str
    content: Dict[str, Any]
    labels: Dict[str, Any]
    _writerNonce: (str,"")
    # optional
    ttl: (Union[int, str], None)
    sources: List[str]
    destinations: List[str]

    def get_defaults(self):
        # Default containers (created fresh each init via BaseData call path)
        return {
            EventData.labels: {},
            EventData.sources: [],
            EventData.destinations: [],
        }

    def do_pre_process(self, in_dict):
        # Fill/normalize datetime
        dt_key = EventData.datetime
        if dt_key not in in_dict or in_dict[dt_key] is None:
            in_dict[dt_key] = _to_iso_utc_z(datetime.now(timezone.utc))
        else:
            in_dict[dt_key] = _to_iso_utc_z(in_dict[dt_key])

        # Ensure containers exist before validation
        if EventData.labels not in in_dict or in_dict[EventData.labels] is None:
            in_dict[EventData.labels] = {}
        if EventData.sources not in in_dict or in_dict[EventData.sources] is None:
            in_dict[EventData.sources] = []
        if (
            EventData.destinations not in in_dict
            or in_dict[EventData.destinations] is None
        ):
            in_dict[EventData.destinations] = []
        # Provide empty content if omitted
        if EventData.content not in in_dict or in_dict[EventData.content] is None:
            in_dict[EventData.content] = {}

        # Lift legacy labels._writerNonce to top-level if missing
        if (EventData._writerNonce not in in_dict) or not in_dict.get(
            EventData._writerNonce
        ):
            lbl = in_dict.get(EventData.labels) or {}
            if isinstance(lbl, dict):
                wn = lbl.get("_writerNonce")
                if isinstance(wn, str) and wn:
                    in_dict[EventData._writerNonce] = wn

        # Normalize sources/destinations: single str -> [str]
        for fld in (EventData.sources, EventData.destinations):
            if fld in in_dict and in_dict[fld] is not None:
                v = in_dict[fld]
                if isinstance(v, str):
                    in_dict[fld] = [v]
        return in_dict

    def do_validation(self, key, value):
        # strict labels: flat primitives only
        if key == EventData.labels:
            if not isinstance(value, dict):
                raise TypeError("labels must be a dict")
            for k, v in value.items():
                if not isinstance(k, str):
                    raise TypeError("labels keys must be str")
                if not (isinstance(v, (str, int, float, bool)) or v is None):
                    raise TypeError(f"labels[{k}] has invalid type: {type(v)}")

        if key == EventData._writerNonce:
            if not isinstance(value, str) or not value:
                raise TypeError("_writerNonce must be a non-empty str")

        if key == EventData.content:
            if not isinstance(value, dict):
                raise TypeError("content must be a dict")

        if key in (EventData.sources, EventData.destinations):
            if value is None:
                return [], ""
            if not isinstance(value, list) or not all(
                isinstance(s, str) for s in value
            ):
                raise TypeError(f"{key} must be list[str]")

        if key == EventData.ttl:
            if value is not None and not isinstance(value, (int, str)):
                raise TypeError("ttl must be int or str when set")
        return value, ""


class EventDataContent(BaseData):
    _writerNonce: str
