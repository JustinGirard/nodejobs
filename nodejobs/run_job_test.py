import os
import shutil
import json
import subprocess
import time
import sys
import psutil
import unittest
from nodejobs.run_job import RunningProcessSpec
import nodejobs
 

class TestRunJobMonolithic(unittest.TestCase):
    def test_run_job_lifecycle(self):
        # 1) Delete old test dir (if present)
        test_dir = "./test_run_data"
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
        assert not os.path.exists(test_dir), "Failed to delete old test directory"

        # 2) Make new test dir
        os.makedirs(test_dir)
        assert os.path.isdir(test_dir), "Failed to create test directory"

        # 3) Create a job spec and save it using BaseData, json, and open
        job_id = "job1_uex67"
        raw_spec = {
            "command": " echo start; sleep 3; echo fin",
            "cwd": './',
            "job_id": job_id,
            "envs": None
        }
        spec = RunningProcessSpec(raw_spec)  # validates via BaseData
        json_path = os.path.join(test_dir, f"{job_id}.json")
        with open(json_path, "w") as f:
            json.dump({
                "command": spec.command,
                "cwd": spec.cwd,
                "job_id": spec.job_id,
                "envs": spec.envs
            }, f)
        assert os.path.exists(json_path), "Spec JSON file was not written"

        # 4) Use subprocess to start RunJob with the sleep command
        # runner = os.path.abspath("run_job.py")
        # output = subprocess.check_output(
        #     [sys.executable, runner, job_id, json_path],
        #     text=True
        # )
        # print(output)
        # assert 'start' in output

        #######
        #######
        #runner = os.path.abspath("run_job.py")
        runner = nodejobs.run_job.__file__
        p = subprocess.Popen(
            [sys.executable, runner, '--job_id', job_id, '--json_path', json_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        # read until we see "start"
        line = p.stdout.readline()
        assert 'start' in line, f"Did not see 'start' in output: {line}"
        pid = p.pid
        assert 'fin' not in line, f"Did not see 'start' in output: {line}"

        #######
        #######
        # 5) Ensure the job is running by searching for job_id
        procs = [
            p for p in psutil.process_iter(['pid', 'cmdline'])
            if job_id in (p.info.get('cmdline') or [])
        ]
        assert any(p.info['pid'] == pid for p in procs), "Job process not found running"

        # 6) Wait until the sleep job should have ended
        time.sleep(5.5)

        # 7) Ensure when sleep stops that the process is gone
        procs = [
            p for p in psutil.process_iter(['pid', 'cmdline'])
            if job_id in (p.info.get('cmdline') or [])
        ]
        for p in procs:
            if p.info['pid'] == pid:
                info = p.info.get('cmdline')
                st = f"Job process is still running {pid}:{info}"
                raise Exception(st)

        p.wait()
        line = p.stdout.readline()
        assert 'fin' in line, f"Did not see 'start' in output: {line}"


if __name__ == "__main__":
    unittest.main()
