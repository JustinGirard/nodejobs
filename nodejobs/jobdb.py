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


class JobRecord(BaseData):
    self_id: str
    status: str
    last_update: (datetime.datetime, datetime.datetime.utcnow())
    last_pid: (int, None)
    dirname: (str, None)
    cwd: (str, None)
    logdir: (str, None)
    logfile: (str, None)

    class Status:
        c_stopping = "stopping"
        c_stopped = "stopped"
        c_starting = "starting"
        c_running = "running"
        c_failed = "failed"
        c_finished = "finished"
        c_failed_start = "failed_start"
        c_failed_stop = "failed_start"

    def __init__(self, in_dict, trim=False):
        if JobRecord.last_update not in in_dict:
            in_dict[JobRecord.last_update] = datetime.datetime.utcnow()
        super().__init__(in_dict, trim)


class JobFilter(BaseData):
    self_id: (str, None)
    dirname: (str, None)
    logdir: (str, None)
    logfile: (str, None)
    status: (str, None)
    cwd: (str, None)
    last_update: (datetime.datetime, None)


class JobDB:
    JobRecord = JobRecord
    JobFilter = JobFilter

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
        print("UPDATING STATUS "+ str(clean_job))
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
        else:
            clean_filter = JobFilter(filter, trim=True)
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
            jobs_by_id[j.self_id] = j
        return JobRecordDict(jobs_by_id)
