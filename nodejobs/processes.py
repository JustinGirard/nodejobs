import subprocess
import psutil
import threading
import os
from typing import Dict
import time
import sys
import json


class Processes:
    def __init__(self, job_db=None, verbose=False):
        self.verbose = verbose
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

    def build_run_job_command(
            self,
            job_id: str,
            command: str,
            cwd: str = None,
            envs: dict = None,
            logdir: str = ".") -> list:
        """
        Service function for tests: writes a {job_id}.json spec into logdir
        and returns the list of arguments to invoke run_job.py.
        """
        if envs is None:
            envs = {}
        # envs["JOB_ID"] = job_id

        os.makedirs(logdir, exist_ok=True)
        spec = {
            "command": command,
            "cwd": cwd,
            "job_id": job_id,
            "envs": envs
        }
        spec_path = os.path.join(logdir, f"{job_id}.json")
        with open(spec_path, "w") as f:
            json.dump(spec, f)

        wrapper = os.path.join(os.path.dirname(__file__), "run_job.py")
        assert os.path.exists(wrapper), f"Cant find the run job kernel {wrapper}"
        cmd = [sys.executable, wrapper, "--job_id", job_id, "--json_path", spec_path]
        print(' '.join(cmd))
        return cmd

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

        os.makedirs(logdir, exist_ok=True)
        out_path = f"{logdir}/{logfile}_out.txt"
        err_path = f"{logdir}/{logfile}_errors.txt"
        for p in (out_path, err_path):
            if os.path.exists(p):
                os.remove(p)

        out_f = open(out_path, "a")
        err_f = open(err_path, "a")

        command = self.build_run_job_command(
                            job_id=job_id,
                            command=command,
                            cwd=cwd,
                            envs=envs,
                            logdir=logdir)
        # print(f"running [{command}]")
        process = subprocess.Popen(
            command,
            shell=False,
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
        for proc in psutil.process_iter(["pid", "cmdline"]):
            cmdline = proc.info.get("cmdline") or []
            if job_id in cmdline:
                return proc

        return None

    def stop(self, job_id):
        proc = self.find(job_id)
        if not proc:
            return False

        # 1) kill descendants first
        children = proc.children(recursive=True)
        for c in children:
            c.terminate()
        gone, alive = psutil.wait_procs(children, timeout=1)
        for c in alive:
            c.kill()

        # 2) kill the wrapper itself
        try:
            proc.terminate()
            proc.wait(timeout=1)
        except Exception as e:
            e
        return True

    # def list(self):
    #     procs = []
    #     for proc in psutil.process_iter(['pid', 'cmdline']):
    #         try:
    #             cmd = ' '.join(proc.info.get('cmdline') or [])
    #             if 'run_job.py' in cmd:
    #                 procs.append(proc)
    #         except (psutil.NoSuchProcess, psutil.AccessDenied):
    #             continue
    #     return procs

    def list(self):
        procs = []
        for proc in psutil.process_iter(['pid', 'cmdline']):
            try:
                parts = proc.info.get('cmdline') or []
                whole_cmd = ' '.join(parts)
                if 'run_job.py' in whole_cmd and '--job_id' in parts:
                    idx = parts.index('--job_id')
                    if idx + 1 < len(parts):
                        proc.job_id = parts[idx + 1]
                        procs.append(proc)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return procs
