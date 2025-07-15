#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
import psutil
import time
import threading
from nodejobs.dependencies.BaseData import BaseData
from typing import List


class RunningProcessSpec(BaseData):
    command: List[str]
    cwd: str
    job_id: str
    envs: (dict, None)

    def do_validation(self, key, value):
        if key == RunningProcessSpec.envs and value is None:
            return {}, ""
        return super().do_validation(key, value)


class RunJob:
    def __init__(self, job_id, json_path):
        self.job_id = job_id
        self.json_path = json_path
        self._load_json()

    def _load_json(self):
        with open(self.json_path, 'r') as f:
            raw = json.load(f)
        spec = RunningProcessSpec(raw)
        self.command = spec.command
        self.cwd = spec.cwd
        self.envs = spec.envs
        assert spec.job_id == self.job_id, "job_ids must match {self.job_id }:{spec.job_id}"

    def find_existing(self):
        for proc in psutil.process_iter(['pid', 'cmdline']):
            job_chek = proc.info.get('cmdline')
            if job_chek:
                string = ' '.join(job_chek)
                try:
                    if self.job_id in string and 'run_job.py' not in string:
                        return proc
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        return None

    def _forward_stream(self, stream, target):
        """
        Read from the child’s stream in small chunks and write
        directly to the wrapper’s corresponding stream.
        """
        with stream:
            while True:
                chunk = stream.read(1024)
                if not chunk:
                    break
                # write raw bytes to the underlying buffer
                target.buffer.write(chunk)
                target.flush()

    def run(self):
        existing = self.find_existing()
        if existing:
            raise Exception(
                f"Existing process {existing.pid} found for job {self.job_id}--> {existing.info.get('cmdline')}"
            )

        # launch child with pipes for both streams, unbuffered at Python level

        proc = subprocess.Popen(
            self.command,
            shell=False,
            cwd=self.cwd,
            env={**os.environ, **self.envs},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )

        # start threads to aggressively forward output
        t_out = threading.Thread(
            target=self._forward_stream, args=(proc.stdout, sys.stdout), daemon=True
        )
        t_err = threading.Thread(
            target=self._forward_stream, args=(proc.stderr, sys.stderr), daemon=True
        )
        t_out.start()
        t_err.start()

        # wait for the process to finish, but handle exceptions
        try:
            while proc.poll() is None:
                time.sleep(0.1)
        except Exception:
            # first try a graceful terminate
            proc.terminate()
            deadline = time.time() + 2
            while proc.poll() is None and time.time() < deadline:
                time.sleep(0.1)
            # if still alive, force-kill
            if proc.poll() is None:
                proc.kill()
            # wait for streams to flush
            t_out.join()
            t_err.join()
            # exit with nonzero to signal abnormal termination
            sys.exit(1)
        # ensure all output is flushed
        t_out.join()
        t_err.join()

        # exit with the same code
        sys.exit(proc.returncode)


def main(*, job_id: str, json_path: str):
    """
    Keyword-only main entrypoint.
    """
    RunJob(job_id=job_id, json_path=json_path).run()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Delegate runner for jobs.")
    parser.add_argument(
        '--job_id',
        required=True,
        dest='job_id',
        help="Unique job identifier"
    )
    parser.add_argument(
        '--json_path',
        required=True,
        dest='json_path',
        help="Path to JSON file with job metadata"
    )
    args = parser.parse_args()

    # Must call main with keyword arguments
    main(job_id=args.job_id, json_path=args.json_path)
