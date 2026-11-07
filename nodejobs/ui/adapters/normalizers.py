import datetime as dt

from nodejobs.jobdb import JobRecord, ScheduleRecord

from ..types.ui_rows import UiJobRow, UiScheduleRow


def _to_datetime_utc_naive(value):
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None:
            return value.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1]
    try:
        parsed = dt.datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return parsed


def _to_iso_text(value) -> str:
    parsed = _to_datetime_utc_naive(value)
    if parsed is not None:
        return parsed.isoformat() + "Z"
    if value is None:
        return ""
    return str(value).strip()


def _seconds_since(value) -> int:
    parsed = _to_datetime_utc_naive(value)
    if parsed is None:
        return -1
    now_utc = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    delta = int((now_utc - parsed).total_seconds())
    if delta < 0:
        return 0
    return delta


def _normalize_command(value) -> list[str]:
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        return [p for p in text.split() if p]
    return []


def _normalize_envs(value) -> dict:
    if isinstance(value, dict):
        return value
    return {}


def _normalize_tags(value) -> dict:
    if not isinstance(value, dict):
        return {}
    out = {}
    for key, val in value.items():
        key_text = str(key or "").strip()
        val_text = str(val or "").strip()
        if key_text and val_text:
            out[key_text] = val_text
    return out


def _tags_to_text(tags_map: dict) -> str:
    if not isinstance(tags_map, dict) or len(tags_map) == 0:
        return ""
    keys = sorted([str(k).strip() for k in tags_map.keys() if str(k).strip()])
    return ", ".join(keys)


def normalize_job_row(raw) -> UiJobRow:
    rec = JobRecord(raw)
    command_parts = _normalize_command(getattr(rec, "command", None))
    envs_map = _normalize_envs(getattr(rec, "envs", None))
    tags_map = _normalize_tags(getattr(rec, "tags", None))
    return UiJobRow(
        {
            UiJobRow.id: rec.self_id,
            UiJobRow.status: rec.status,
            UiJobRow.command: command_parts,
            UiJobRow.command_text: " ".join(command_parts),
            UiJobRow.envs: envs_map,
            UiJobRow.tags: tags_map,
            UiJobRow.tags_text: _tags_to_text(tags_map),
            UiJobRow.last_pid: rec.last_pid if rec.last_pid is not None else -1,
            UiJobRow.last_update: _to_iso_text(rec.last_update),
            UiJobRow.seconds_since_last_update: _seconds_since(rec.last_update),
            UiJobRow.dirname: rec.dirname if rec.dirname is not None else "",
            UiJobRow.cwd: rec.cwd if rec.cwd is not None else "",
            UiJobRow.logdir: rec.logdir if rec.logdir is not None else "",
            UiJobRow.logfile: rec.logfile if rec.logfile is not None else "",
        }
    )


def normalize_schedule_row(raw) -> UiScheduleRow:
    rec = ScheduleRecord(raw)
    last_update = rec.last_run_at
    tags_map = _normalize_tags(getattr(rec, "tags", None))
    return UiScheduleRow(
        {
            UiScheduleRow.id: rec.schedule_id,
            UiScheduleRow.job_id: rec.job_id,
            UiScheduleRow.next_run_at: _to_iso_text(rec.next_run_at),
            UiScheduleRow.last_update: _to_iso_text(last_update),
            UiScheduleRow.seconds_since_last_update: _seconds_since(last_update),
            UiScheduleRow.interval_days: rec.interval_days,
            UiScheduleRow.interval_hours: rec.interval_hours,
            UiScheduleRow.interval_minutes: rec.interval_minutes,
            UiScheduleRow.interval_sec: rec.interval_sec,
            UiScheduleRow.enabled: bool(rec.enabled),
            UiScheduleRow.cwd: rec.cwd,
            UiScheduleRow.envs: rec.envs,
            UiScheduleRow.tags: tags_map,
            UiScheduleRow.tags_text: _tags_to_text(tags_map),
            UiScheduleRow.last_error: rec.last_error,
            UiScheduleRow.attempt_count: rec.attempt_count if rec.attempt_count is not None else 0,
        }
    )
