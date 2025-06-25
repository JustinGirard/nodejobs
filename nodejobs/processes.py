import subprocess
import psutil
import threading
import os
from typing import Dict, List
import time
from nodejobs.jobdb import JobDB, JobFilter, JobRecord


class Processes:
    def __init__(self, job_db, verbose=False):
        self.verbose = verbose
        assert isinstance(job_db, JobDB)
        self.jobdb = job_db
        self._processes: Dict[str, subprocess.Popen] = {}
        threading.Thread(target=self._reap_loop, daemon=True).start()

    def _reap_loop(self):
        while True:
            if self.verbose is True:
                print("reaping ... ", end="")
            for jid, proc in list(self._processes.items()):
                if self.verbose is True:
                    print(f",  {jid}", end="")
                if proc.poll() is not None:
                    proc.wait()  # reap
                    # optional: update your JobDB here, e.g.
                    # self.jobdb.update_status(jid, proc.returncode)
                    del self._processes[jid]
            if self.verbose is True:
                print(".. reaped")

            time.sleep(1)

    def run(
        self,
        command: str,
        job_id: str,
        envs: dict = None,
        cwd: str = None,
        logdir: str = None,
        logfile: str = None,
    ):

        assert (
            len(job_id) > 0
        ), "Job id is too short. It should be long enough to be unique"
        if envs is None:
            envs = {}
        envs["JOB_ID"] = job_id

        os.makedirs(logdir, exist_ok=True)
        out_path = f"{logdir}/{logfile}_out.txt"
        err_path = f"{logdir}/{logfile}_errors.txt"
        for p in (out_path, err_path):
            if os.path.exists(p):
                os.remove(p)

        out_f = open(out_path, "a")
        err_f = open(err_path, "a")

        process = subprocess.Popen(
            command,
            shell=True,
            cwd=cwd,
            env=envs,
            stdout=out_f,
            stderr=err_f,
            preexec_fn=os.setsid,
        )
        try:
            self._processes
        except Exception:
            self._processes = {}
        self._processes[job_id] = process

        out_f.close()
        err_f.close()
        return process

    def find(self, job_id):
        filter = JobFilter({JobFilter.self_id: job_id})
        jobs: Dict[str, JobRecord] = self.jobdb.list_status(filter=filter)
        if len(jobs) == 0 or job_id not in list(jobs.keys()):
            print(f"Process.find found no job record for {job_id}")
            return None

        job = JobRecord(jobs[job_id])
        try:
            assert (
                JobRecord.last_pid in job and job.last_pid is not None
            ), f"Found a job with a missing pid {job}"
        except Exception:
            print(f"Found a job with a missing pid: {job}")
            return None
        for proc in psutil.process_iter(["pid", "name", "environ"]):
            if proc.pid == job.last_pid:
                return proc
        return None

    def stop(self, job_id):
        for i in [1, 2]:
            proc = self.find(job_id)
            if proc:
                if self.verbose is True:
                    print(" --- stopping ", proc)
                proc.terminate()
                proc.wait()
            else:
                if self.verbose is True:
                    print(" --- NOT stopping ", job_id)

        return True

    def list(self):
        filter = JobFilter({})
        jobs: Dict[str, JobRecord] = self.jobdb.list_status(filter=filter)
        jobs_list: List[JobRecord] = list(jobs.values())
        pid_jobs: Dict[int, JobRecord] = {}
        for j in jobs_list:
            j = JobRecord(j)
            pid_jobs[j.last_pid] = j
        procs = []
        for proc in psutil.process_iter(["pid", "name", "environ"]):
            if proc.info["pid"] in list(pid_jobs.keys()):
                pid = proc.info["pid"]
                proc.job_id = pid_jobs[pid][
                    JobRecord.self_id
                ]
                procs.append(proc)

        return procs
