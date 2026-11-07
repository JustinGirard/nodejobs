from nodejobs.dependencies.BaseData import BaseData


class UiJobRow(BaseData):
    id: (str, "")
    status: (str, "UNKNOWN")
    command: (list, [])
    command_text: (str, "")
    envs: (dict, {})
    tags: (dict, {})
    tags_text: (str, "")
    last_pid: (int, -1)
    last_update: (str, "")
    seconds_since_last_update: (int, -1)
    dirname: (str, "")
    cwd: (str, "")
    logdir: (str, "")
    logfile: (str, "")


class UiScheduleRow(BaseData):
    id: (str, "")
    job_id: (str, "")
    next_run_at: (str, "")
    last_update: (str, "")
    seconds_since_last_update: (int, -1)
    interval_days: (int, None)
    interval_hours: (int, None)
    interval_minutes: (int, None)
    interval_sec: (int, None)
    enabled: (bool, True)
    cwd: (str, None)
    envs: (dict, None)
    tags: (dict, {})
    tags_text: (str, "")
    last_error: (str, None)
    attempt_count: (int, 0)
