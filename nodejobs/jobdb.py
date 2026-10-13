import datetime
import os
from nodejobs.dependencies.nosqlite import nosqlite
from nodejobs.dependencies.BaseData import BaseData


class JobRecordDict(BaseData):
    f_all = "*"

    def get_keys(self):
        required = {}
        optional = {JobRecordDict.f_all: JobRecord}
        return required, optional


def now_dt():
    dt = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    return dt


class JobRecord(BaseData):
    self_id: str
    status: str
    last_update: (datetime.datetime, now_dt())
    last_pid: (int, None)
    dirname: (str, None)
    cwd: (str, None)
    logdir: (str, None)
    logfile: (str, None)
    command: (list, None)
    envs: (dict, None)
    tags: (dict, None)

    class Status:
        c_stopping = "stopping"
        c_stopped = "stopped"
        c_starting = "starting"
        c_running = "running"
        c_failed = "failed"
        c_finished = "finished"  # Every end condition has its own trigger.
        c_finished_2 = "finished"  # By using 2 vars, we can debug which
        c_failed_start = "failed_start"
        c_failed_stop = "failed_start"

    def __init__(self, in_dict, trim=False):
        if JobRecord.last_update not in in_dict:
            in_dict[JobRecord.last_update] = now_dt()
        super().__init__(in_dict, trim)


class JobFilter(BaseData):
    self_id: (str, None)
    dirname: (str, None)
    logdir: (str, None)
    logfile: (str, None)
    status: (str, None)
    cwd: (str, None)
    last_update: (datetime.datetime, None)
    tags: (dict, None)


class ScheduleRecord(BaseData):
    schedule_id: str
    job_id: str
    next_run_at: datetime.datetime
    interval_days: (int, None)
    interval_hours: (int, None)
    interval_minutes: (int, None)
    interval_sec: (int, None)
    enabled: (bool, True)
    cwd: (str, None)
    envs: (dict, None)
    tags: (dict, None)
    last_run_at: (datetime.datetime, None)
    last_error: (str, None)
    attempt_count: (int, 0)


class ScheduleFilter(BaseData):
    schedule_id: (str, None)
    job_id: (str, None)
    enabled: (bool, None)
    next_run_at: (datetime.datetime, None)
    tags: (dict, None)


class ScheduleRecordDict(BaseData):
    f_all = "*"

    def get_keys(self):
        required = {}
        optional = {ScheduleRecordDict.f_all: ScheduleRecord}
        return required, optional


class JobDB:
    JobRecord = JobRecord
    JobFilter = JobFilter
    ScheduleRecord = ScheduleRecord
    ScheduleFilter = ScheduleFilter

    def __init__(self, db_path: str):
        """
        Initialize the JobDB, pointing to an SQLite file under `db_path`.

        1. If `db_path` is None, raise an exception.
        2. Construct the full file path as `<db_path>/jobs.db`.
        3. Instantiate the underlying NoSQL wrapper (`nosqlite`)
           so subsequent calls will operate on “jobs.db”

        Parameters:
            db_path (str):  Directory in which “jobs.db” should reside.

        Raises:
            Exception:  If `db_path` is None.
        """
        if db_path is None:
            raise Exception("DP path cant be null")
        self.db_path = os.path.join(db_path, "jobs.db")
        # print(f"USING JOB DB AT {self.db_path}")
        self.jobdb = nosqlite(str(self.db_path))

    @staticmethod
    def _coerce_tags(value) -> dict:
        if not isinstance(value, dict):
            return {}
        out = {}
        for key, val in value.items():
            key_text = str(key or "").strip()
            val_text = str(val or "").strip()
            if key_text and val_text:
                out[key_text] = val_text
        return out

    @classmethod
    def _tags_match(cls, record_tags, required_tags) -> bool:
        required = cls._coerce_tags(required_tags)
        if len(required) == 0:
            return True
        actual = cls._coerce_tags(record_tags)
        for key, val in required.items():
            if actual.get(key) != val:
                return False
        return True

    def update_status(self, job):
        """
        Upsert (insert or update) a job record into the “process_status” table.

        1. Wrap the raw `job` dict in a validated `JobRecord`
            and call `.clean()` to
           strip out any extraneous fields.
        2. Perform an “upsert” on the “process_status” table:
           - If a record with the same `self_id` exists,
            update its fields.
           - Otherwise, insert a new record.
        3. Return whatever the underlying `nosqlite.execute(...)`
            returns (e.g., row count or error).

        Parameters:
            job (dict):  Dictionary containing at least:
                        - 'self_id' (str)
                        - 'status'  (str)
                        Optionally other fields allowed by JobRecord.

        Returns:
            Any:  The result of the upsert operation
              (often the new record’s ID or a status code).
        """
        clean_job = JobRecord(job).clean()
        job_id = clean_job.get(JobFilter.self_id)
        existing_rows = self.list_status({JobFilter.self_id: job_id})
        if job_id in existing_rows:
            merged_job = dict(JobRecord(existing_rows[job_id]).clean())
            merged_job.update(clean_job)
            clean_job = JobRecord(merged_job).clean()
        else:
            command_val = clean_job.get(JobRecord.command)
            has_runnable_command = isinstance(command_val, list) and len(command_val) > 0
            if not has_runnable_command:
                return False
        pid = os.getpid()
        # print(f"UPERST LOG pid={pid}")
        resp = self.jobdb.execute(
            qtype="upsert",
            source="process_status",
            filterval={JobFilter.self_id: clean_job[JobFilter.self_id]},
            setval=clean_job,
            limit=None,
            offset=None,
            field=None,
        )
        return resp

    def job_logs(self, self_id):
        """
        Retrieve the stored stdout and stderr logs for a given job.

        1. Query the “process_status” table for a
            record whose 'self_id' matches `self_id`.
        2. If no record is found,
            return two empty strings.
        3. Verify that the record contains both
          'logdir' and 'logfile' fields; if missing,
           return an error message for each missing field.
        4. Construct the full paths:
           - stdout:  "{logdir}/{logfile}_out.txt"
           - stderr:  "{logdir}/{logfile}_errors.txt"
        5. Attempt to open & read each file;
          if opening fails, return an error string instead.
        6. Return a tuple `(stdout_contents, stderr_contents)`.

        Parameters:
            self_id (str):  Unique identifier of the
            job whose logs should be fetched.

        Returns:
            (str, str):  The contents of stdout and stderr log files
                If the job or files cannot be found,
                error messages are returned in place of file contents.
        """
        resp = self.jobdb.execute(
            qtype="find",
            source="process_status",
            filterval={JobFilter.self_id: self_id},
            setval=None,
            limit=None,
            offset=None,
            field=None,
        )
        # {logdir}/{logfile}_out.txt 2>> {logdir}/{logfile}_errors.txt "
        stdlogs = ""
        errlogs = ""
        if len(resp) <= 0:
            stdlogs, errlogs
        doc = resp[0]
        if "logdir" not in doc:
            return "error: no log dir found", "error: no log dir found"
        if "logfile" not in doc:
            return "error: no log file found", "error: no log file found"
        logdir = doc["logdir"]
        logfile = doc["logfile"]

        try:
            with open(f"{logdir}/{logfile}_out.txt", "r") as f:
                stdlogs = f.read()

        except Exception as e:
            stdlogs = f"error: could not open err({e})" \
                + f"{logdir}/{logfile}_out.txt"
        try:
            with open(f"{logdir}/{logfile}_errors.txt", "r") as f:
                errlogs = f.read()
        except Exception as e:
            errlogs = f"error: could not open err({e}) " \
                + f"{logdir}/{logfile}_errors.txt"

        return str(stdlogs), str(errlogs)

    def list_status(self, filter=None):
        """
        Retrieve all job records matching an optional filter.

        1. If `filter` is None, use an empty dictionary (match everything).
           Otherwise, validate and clean the filter via
           `JobFilter(filter, trim=True).clean()`.
        2. Execute a 'find' query on the “process_status”
            table using `clean_filter`.
        3. If the database returns an Exception object, re‐raise it.
        4. Assemble a dict mapping each job’s 'self_id' to its full record.
        5. Return this dict, which may be empty
            if no records match (or table is empty).

        Parameters:
            filter (dict, optional):  Fields to match against job records.
                Keys must belong to JobFilter.  If provided,
                only jobs satisfying all key/value pairs are returned.

        Returns:
            dict:  { self_id: record_dict, … } for each
                   job matching the filter.
                   If no jobs match, returns an empty dict.
                   If an error occurs
                   during query execution, the exception is propagated.
        """
        if filter is None:
            clean_filter = {}
            required_tags = None
        else:
            clean_filter = JobFilter(filter, trim=True)
            required_tags = clean_filter.get(JobFilter.tags)
            if JobFilter.tags in clean_filter:
                del clean_filter[JobFilter.tags]
        # print(f"clean_filter -----> {clean_filter}")
        resp = self.jobdb.execute(
            qtype="find",
            source="process_status",
            filterval=clean_filter,
            setval=None,
            limit=None,
            offset=None,
            field=None,
        )
        if isinstance(resp, Exception):
            raise resp
        jobs_by_id = {}
        for j in resp:
            j = JobRecord(
                j
            )
            if not self._tags_match(j.get(JobRecord.tags), required_tags):
                continue
            jobs_by_id[j.self_id] = j
        return JobRecordDict(jobs_by_id)

    def remove_job(self, job_id: str):
        if not isinstance(job_id, str) or len(job_id.strip()) == 0:
            raise Exception("job_id cant be empty")
        resp = self.jobdb.execute(
            qtype="delete",
            source="process_status",
            filterval={JobFilter.self_id: job_id.strip()},
            setval=None,
            limit=None,
            offset=None,
            field=None,
        )
        return resp

    def update_schedule(self, schedule):
        clean_schedule = ScheduleRecord(schedule).clean()
        schedule_id = clean_schedule[ScheduleRecord.schedule_id]
        resp = self.jobdb.execute(
            qtype="upsert",
            source="job_schedule",
            filterval={ScheduleFilter.schedule_id: schedule_id},
            setval=clean_schedule,
            limit=None,
            offset=None,
            field=None,
        )
        return resp

    def list_schedules(self, filter=None):
        if filter is None:
            clean_filter = {}
            required_tags = None
        else:
            clean_filter = ScheduleFilter(filter, trim=True)
            required_tags = clean_filter.get(ScheduleFilter.tags)
            if ScheduleFilter.tags in clean_filter:
                del clean_filter[ScheduleFilter.tags]
        resp = self.jobdb.execute(
            qtype="find",
            source="job_schedule",
            filterval=clean_filter,
            setval=None,
            limit=None,
            offset=None,
            field=None,
        )
        if isinstance(resp, Exception):
            raise resp
        schedules_by_id = {}
        for s in resp:
            rec = ScheduleRecord(s)
            if not self._tags_match(rec.get(ScheduleRecord.tags), required_tags):
                continue
            schedules_by_id[rec.schedule_id] = rec
        return ScheduleRecordDict(schedules_by_id)

    def remove_schedule(self, schedule_id: str):
        if not schedule_id:
            raise Exception("schedule_id cant be empty")
        resp = self.jobdb.execute(
            qtype="delete",
            source="job_schedule",
            filterval={ScheduleFilter.schedule_id: schedule_id},
            setval=None,
            limit=None,
            offset=None,
            field=None,
        )
        return resp

    def list_due_schedules(self, now_at=None):
        if now_at is None:
            now_at = now_dt()
        if not isinstance(now_at, datetime.datetime):
            raise Exception(f"list_due_schedules expected datetime, got {type(now_at)}")
        resp = self.jobdb.execute(
            qtype="find",
            source="job_schedule",
            filterval={
                ScheduleFilter.enabled: 1,
                ScheduleRecord.next_run_at: {"$lte": now_at},
            },
            setval=None,
            limit=None,
            offset=None,
            field=None,
        )
        if isinstance(resp, Exception):
            raise resp
        due_by_id = {}
        for s in resp:
            rec = ScheduleRecord(s)
            if not isinstance(rec.next_run_at, datetime.datetime):
                raise Exception(
                    f"Schedule '{rec.schedule_id}' has invalid next_run_at type: {type(rec.next_run_at)}"
                )
            if rec.enabled is True and rec.next_run_at <= now_at:
                due_by_id[rec.schedule_id] = rec
        return ScheduleRecordDict(due_by_id)
