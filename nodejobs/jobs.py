from nodejobs.processes import Processes
from nodejobs.jobdb import JobDB, JobFilter, JobRecord, JobRecordDict
from pathlib import Path
import os
import time
from psutil import Process
from typing import Tuple, Union, List
import subprocess
import json
import datetime as _dt
from typing import Iterator, Optional, Dict
from nodejobs.dependencies.BaseData import BaseData


class Jobs:
    def __init__(self, db_path=None, verbose=False):
        self.verbose = verbose
        try:
            # print(f"a. DB jobs working in {db_path }")
            self.db_path = db_path
            os.makedirs(self.db_path, exist_ok=True)
        except Exception as e:

            home = Path.home()
            default_dir = os.path.join(home, "tmp_decelium_job_database")
            os.makedirs(default_dir, exist_ok=True)
            self.db_path = default_dir
            #print(f"Jobs.__init__ ({e}). DB jobs working in {self.db_path}")
        self.jobdb = JobDB(self.db_path)
        self.processes = Processes(self.jobdb, verbose)

    # === Streaming helpers (SSE + event model) ===
    class StreamEvent(BaseData):
        type: str
        job_id: str
        text: (str, None)
        status: (str, None)
        seq: int
        ts: str

    def _now_iso(self) -> str:
        return _dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"

    def _sse_frame(self, event_type: str, data: 'Jobs.StreamEvent', event_id: int) -> str:
        payload = json.dumps(data.to_safe_dict(), ensure_ascii=True, separators=(",", ":"))
        lines = []
        if event_id is not None:
            lines.append(f"id: {event_id}")
        if event_type:
            lines.append(f"event: {event_type}")
        for ln in payload.splitlines() or [""]:
            lines.append(f"data: {ln}")
        lines.append("")
        return "\n".join(lines) + "\n"

    def __find(self, job_id: str):
        assert job_id is not None, "can only select by job_id"
        job = None
        jobs = {}
        if job_id is not None:
            jobs = self.jobdb.list_status({"self_id": job_id})
        if len(jobs) > 0:
            job = list(jobs.values())[0]
        return job

    def __log_paths(self, job_id: str) -> Tuple[str, str]:
        job = self.__find(job_id)
        if job is None:
            raise ValueError(f"Unknown job_id '{job_id}'")
        rec = JobRecord(job)
        if not rec.logdir or not rec.logfile:
            raise ValueError(f"Job '{job_id}' has no logdir/logfile recorded yet")
        std = os.path.join(rec.logdir, f"{rec.logfile}_out.txt")
        err = os.path.join(rec.logdir, f"{rec.logfile}_errors.txt")
        return std, err

    def run(self, command: Union[str, List[str]], job_id: str, cwd: str = None,envs: dict = None):

        assert len(job_id) > 0, " Job name too short"
        if cwd is None:
            cwd = os.getcwd()
        logdir = f"{self.db_path}/job_logs/"
        os.makedirs(logdir, exist_ok=True)

        logfile = job_id
        self.jobdb.update_status(
            JobRecord(
                {
                    JobRecord.self_id: job_id,
                    JobRecord.last_pid: -1,
                    JobRecord.dirname: job_id,
                    JobRecord.cwd: cwd,
                    JobRecord.logdir: logdir,
                    JobRecord.logfile: logfile,
                    JobRecord.status: JobRecord.Status.c_starting,
                }
            )
        )
        if command is str:
            command = command.strip()
            command = command.split(' ')
        start_proc: subprocess.Popen = self.processes.run(
            command=command,
            job_id=job_id,
            cwd=cwd,
            logdir=logdir,
            logfile=logfile,
            envs = envs
        )
        cond = isinstance(start_proc, subprocess.Popen)
        assert cond, "Invalid process detected"
        time.sleep(0.5)
        ret = start_proc.poll()
        # print(f"looking at pid {start_proc.pid}")
        if ret is None:
            result = JobRecord(
                {
                    JobRecord.self_id: job_id,
                    JobRecord.status: JobRecord.Status.c_running,
                    JobRecord.last_pid: start_proc.pid,
                }
            )
        elif ret == 0:
            result = JobRecord(
                {
                    JobRecord.self_id: job_id,
                    JobRecord.status: JobRecord.Status.c_finished,
                    JobRecord.last_pid: start_proc.pid,
                }
            )
        else:
            result = JobRecord(
                {
                    JobRecord.self_id: job_id,
                    JobRecord.status: JobRecord.Status.c_failed_start,
                    JobRecord.last_pid: start_proc.pid,
                }
            )

        self.jobdb.update_status(result)
        return result

    def stop(self, job_id: str, wait_time: int = 1) -> JobRecord:

        assert job_id is not None, "can only select by  job_id"
        job: JobRecord = self.__find(job_id)
        if job is None:
            return None
        job = JobRecord(job)
        job_id = job.self_id
        self.jobdb.update_status(
            JobRecord(
                {JobRecord.self_id: job_id,
                 JobRecord.status: job.Status.c_stopping}
            )
        )
        success = self.processes.stop(job_id=job_id)
        time.sleep(wait_time)
        found_job = self.list_status(
            JobFilter(
                {
                    JobFilter.self_id: job_id,
                }
            )
        )
        assert (
            job_id in found_job
        ), "Could not find a job that was just present. \
            Should be impossible. Race condition?"
        job = found_job[job_id]
        if JobRecord(found_job[job_id]).status == JobRecord.Status.c_running:
            if self.verbose is True:
                print(f"inspecting job (A): {job} -- {job_id}")
            result = JobRecord(
                {
                    JobRecord.last_pid: job.last_pid,
                    JobRecord.self_id: job_id,
                    JobRecord.status: job.Status.c_failed_stop,
                }
            )

        if success:
            if self.verbose is True:
                print(f"inspecting job (B): {job}")
            result = JobRecord(
                {
                    JobRecord.last_pid: job.last_pid,
                    JobRecord.self_id: job.self_id,
                    JobRecord.status: job.status,
                }
            )
        else:
            if self.verbose is True:
                print(f"inspecting job (C): {job}")
            result = JobRecord(
                {
                    job.last_pid: job.last_pid,
                    job.self_id: job.self_id,
                    job.status: job.status,
                }
            )
        db_res = self.jobdb.update_status(result)
        if self.verbose is True:
            print(f"inspecting job (D): {db_res}")
        return result

    def job_logs(self, job_id: str) -> Tuple[str, str]:

        job = self.__find(job_id)
        if job is None:
            return (
                f"error: could not find job_id for {job_id}",
                f"error: could not find job_id for {job_id}",
            )
        job = JobRecord(job)
        job_id = job.self_id
        stdlog, errlog = self.jobdb.job_logs(self_id=job_id)
        return stdlog, errlog

    def _update_status(self):
        running_jobs = {}
        if self.verbose is True:
            print("...updating ...")

        for proc in self.processes.list():
            proc: Process = proc
            if self.verbose is True:
                print(f"...updating proc ... {proc}, as {proc.job_id} ")
            try:
                os.waitpid(proc.pid, os.WNOHANG)
            except Exception as e:
                e
                pass
            running_jobs[proc.job_id] = (
                proc
            )
        running_ids = list(running_jobs.keys())
        if self.verbose is True:
            print(f"...updating ... running_ids {running_ids}")

        for actually_running_id in running_ids:
            self.jobdb.update_status(
                JobRecord(
                    {
                        JobRecord.self_id: actually_running_id,
                        JobRecord.status: JobRecord.Status.c_running,
                    }
                )
            )

        db_runningdict = self.jobdb.list_status(
            JobFilter({JobRecord.status: JobRecord.Status.c_running})
        )
        db_stopping_dict = self.jobdb.list_status(
            JobFilter({JobRecord.status: JobRecord.Status.c_stopping})
        )
        db_review_dict = {**db_runningdict, **db_stopping_dict}
        if self.verbose is True:
            print(f"...updating ... db_running_list {running_ids}")

        for job_id in db_review_dict.keys():
            if self.verbose is True:
                print(f"...reviewing {job_id}")
            if job_id not in running_ids:
                # TODO - Review reason for stop to assign correct final status
                # print(f"RETIRING {job_id}")
                stdlog, errlog = self.jobdb.job_logs(self_id=job_id)
                if len(errlog.strip()) > 0:
                    if self.verbose is True:
                        print(f"...recording failed: \nstdlog{stdlog}:\n\nerrlog{errlog}")
                    if job_id in db_stopping_dict:
                        self.jobdb.update_status(
                            JobRecord(
                                {
                                    JobRecord.self_id: job_id,
                                    JobRecord.status: JobRecord.Status.c_stopped,
                                }
                            )
                        )
                else:
                    self.jobdb.update_status(
                        JobRecord(
                            {
                                JobRecord.self_id: job_id,
                                JobRecord.status: JobRecord.Status.c_finished_2,
                            }
                        )
                    )

    def list_status(self, filter=None) -> JobRecordDict:
        # print(f"------------------- A {filter}")
        if filter is None:
            filter = {}
        filter = JobFilter(filter)
        # print(f"------------------- B {filter}")
        self._update_status()
        # print(f"------------------- C {filter}")
        return JobRecordDict(self.jobdb.list_status(filter))

    def get_status(self, job_id: str) -> JobRecord:

        assert job_id, "can only select by job_id"

        # Build a filter for list_status
        filt = {JobFilter.self_id: job_id}

        # list_status returns a JobRecordDict (mapping IDs to JobRecord)
        recs = self.list_status(filt)
        if not recs:
            return None

        # Return the first JobRecord in that dict
        return next(iter(recs.values()))

    # === Public Streaming APIs ===
    def bind(
        self,
        job_id: str,
        include: Tuple[str, ...] = ("stdout", "stderr"),
        from_beginning: bool = False,
        poll_interval: float = 0.25,
        heartbeat_interval: float = 5.0,
        last_event_id: Optional[int] = None,
    ) -> Iterator['Jobs.StreamEvent']:
        out_path, err_path = self.__log_paths(job_id)
        want_out = "stdout" in include
        want_err = "stderr" in include

        deadline = time.time() + 5.0
        out_f = err_f = None
        while True:
            try:
                if want_out and out_f is None and os.path.exists(out_path):
                    out_f = open(out_path, "rb")
                    out_f.seek(0, os.SEEK_SET if from_beginning else os.SEEK_END)
                if want_err and err_f is None and os.path.exists(err_path):
                    err_f = open(err_path, "rb")
                    err_f.seek(0, os.SEEK_SET if from_beginning else os.SEEK_END)
                if ((not want_out) or out_f is not None) and ((not want_err) or err_f is not None):
                    break
            except Exception:
                pass
            if time.time() > deadline:
                raise FileNotFoundError(f"Log files not ready for job '{job_id}'")
            time.sleep(0.05)

        if isinstance(last_event_id, int):
            seq = last_event_id
        elif isinstance(last_event_id, str) and last_event_id.isdigit():
            seq = int(last_event_id)
        else:
            seq = 0

        status_rec = self.get_status(job_id)
        init_status = status_rec.status if status_rec else "unknown"
        seq += 1
        yield Jobs.StreamEvent({
            Jobs.StreamEvent.type: "status",
            Jobs.StreamEvent.job_id: job_id,
            Jobs.StreamEvent.status: init_status,
            Jobs.StreamEvent.seq: seq,
            Jobs.StreamEvent.ts: self._now_iso(),
        })

        terminal = {
            JobRecord.Status.c_stopped,
            JobRecord.Status.c_finished,
            JobRecord.Status.c_finished_2,
            JobRecord.Status.c_failed,
            JobRecord.Status.c_failed_start,
        }

        last_heartbeat = time.time()

        try:
            while True:
                emitted = False

                if want_out and out_f is not None:
                    data = out_f.read()
                    if data:
                        emitted = True
                        seq += 1
                        yield Jobs.StreamEvent({
                            Jobs.StreamEvent.type: "stdout",
                            Jobs.StreamEvent.job_id: job_id,
                            Jobs.StreamEvent.text: data.decode("utf-8", errors="replace"),
                            Jobs.StreamEvent.seq: seq,
                            Jobs.StreamEvent.ts: self._now_iso(),
                        })

                if want_err and err_f is not None:
                    data = err_f.read()
                    if data:
                        emitted = True
                        seq += 1
                        yield Jobs.StreamEvent({
                            Jobs.StreamEvent.type: "stderr",
                            Jobs.StreamEvent.job_id: job_id,
                            Jobs.StreamEvent.text: data.decode("utf-8", errors="replace"),
                            Jobs.StreamEvent.seq: seq,
                            Jobs.StreamEvent.ts: self._now_iso(),
                        })

                now = time.time()
                if (now - last_heartbeat) >= heartbeat_interval:
                    seq += 1
                    yield Jobs.StreamEvent({
                        Jobs.StreamEvent.type: "heartbeat",
                        Jobs.StreamEvent.job_id: job_id,
                        Jobs.StreamEvent.seq: seq,
                        Jobs.StreamEvent.ts: self._now_iso(),
                    })
                    last_heartbeat = now

                rec = self.get_status(job_id)
                is_terminal = (rec and rec.status in terminal)
                if is_terminal and not emitted:
                    seq += 1
                    yield Jobs.StreamEvent({
                        Jobs.StreamEvent.type: "status",
                        Jobs.StreamEvent.job_id: job_id,
                        Jobs.StreamEvent.status: rec.status,
                        Jobs.StreamEvent.seq: seq,
                        Jobs.StreamEvent.ts: self._now_iso(),
                    })
                    break

                time.sleep(poll_interval)
        finally:
            try:
                if out_f:
                    out_f.close()
            finally:
                if err_f:
                    err_f.close()

    def sse(
        self,
        job_id: str,
        include: Tuple[str, ...] = ("stdout", "stderr"),
        from_beginning: bool = False,
        poll_interval: float = 0.25,
        heartbeat_interval: float = 5.0,
        last_event_id: Optional[str] = None,
    ) -> Iterator[str]:
        parsed = None
        if isinstance(last_event_id, int):
            parsed = last_event_id
        elif isinstance(last_event_id, str) and last_event_id.strip().isdigit():
            parsed = int(last_event_id.strip())

        for ev in self.bind(
            job_id=job_id,
            include=include,
            from_beginning=from_beginning,
            poll_interval=poll_interval,
            heartbeat_interval=heartbeat_interval,
            last_event_id=parsed,
        ):
            ev_type = ev.type or "message"
            ev_id = ev.seq
            yield self._sse_frame(ev_type, ev, ev_id)

    @staticmethod
    def subscribe_sse(url: str, session=None, **requests_kwargs):
        try:
            import requests
            from sseclient import SSEClient  # type: ignore
        except Exception:
            raise ImportError("[stream] events stream error: sseclient-py is required for event streaming")
        if session is None:
            session = requests.Session()
        resp = session.get(
            url,
            stream=True,
            headers={"Accept": "text/event-stream"},
            **requests_kwargs,
        )
        resp.raise_for_status()
        client = SSEClient(resp)
        return client.events()
