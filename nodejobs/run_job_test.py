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
from nodejobs.jobs import Jobs
from nodejobs.jobdb import JobRecord
import uuid


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
            "command": ["bash", "-c", "echo start;sleep 3;echo fin"],
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
        # runner = os.path.abspath("run_job.py")
        runner = nodejobs.run_job.__file__
        print("DEBUG:> "+' '.join([sys.executable, runner, '--job_id', job_id, '--json_path', json_path]))
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

    def test_bind_stream_kill_at_100(self):
        # Arrange: clean DB dir and init Jobs
        test_db = "./test_stream_db"
        if os.path.exists(test_db):
            shutil.rmtree(test_db)
        os.makedirs(test_db, exist_ok=True)
        jobs = Jobs(db_path=test_db, verbose=False)

        # Unique job id per test run
        job_id = f"stream_job_{uuid.uuid4().hex[:8]}"

        # Counter job: prints 1..1000 every 0.1s (~10s to hit 100), unbuffered and cross-platform
        cmd = [
            sys.executable,
            "-u",
            "-c",
            "import time,sys; [print(i, flush=True) or time.sleep(0.1) for i in range(1,1001)]",
        ]

        # Act: start job
        rec = jobs.run(command=cmd, job_id=job_id, cwd=os.getcwd(), envs={})
        self.assertIsNotNone(rec)

        # Bind to stream (chunk mode), read from beginning to simplify parsing
        it = jobs.bind(
            job_id=job_id,
            include=("stdout",),
            from_beginning=True,
            poll_interval=0.05,
            heartbeat_interval=5.0,
            last_event_id=None,
        )

        reached = False
        t0 = time.time()
        max_total_sec = 60  # generous upper bound

        while True:
            print('x',end="-", flush=True)
            if time.time() - t0 > max_total_sec:
                self.fail("Timeout waiting to reach 100 in stream")
            try:
                ev = next(it)
            except StopIteration:
                # Ended before reaching 100 — not expected for a healthy run
                break

            if ev.type != "stdout" or not ev.text:
                continue

            for ln in ev.text.splitlines():
                try:
                    n = int(ln.strip())
                    print(n, end=",", flush=True)
                except Exception:
                    continue
                if n >= 100:
                    reached = True
                    break

            if reached:
                # Stop the job as soon as we observe >=100
                stop_res = jobs.stop(job_id=job_id, wait_time=1)
                self.assertIsNotNone(stop_res, "jobs.stop returned None")

                # Drain the stream until it completes or timeout
                drain_deadline = time.time() + 10
                while True:
                    if time.time() > drain_deadline:
                        self.fail("Stream did not terminate after stopping job")
                    try:
                        _ = next(it)
                    except StopIteration:
                        break
                break

        # Assert: no dangling processes with this job_id
        self.assertIsNone(
            jobs.processes.find(job_id),
            "Wrapper process still present after stop",
        )

        lingering = [
            p for p in psutil.process_iter(["pid", "cmdline"])
            if job_id in (" ".join(p.info.get("cmdline") or []))
        ]
        self.assertFalse(lingering, f"Dangling processes found: {[(p.pid, p.info.get('cmdline')) for p in lingering]}")

        st = jobs.get_status(job_id)
        self.assertIsNotNone(st, "Missing job status after stop")
        self.assertNotEqual(getattr(st, "status", None), JobRecord.Status.c_running, "Job still marked running")




if __name__ == "__main__":
    #unittest.main()
    unittest.main(defaultTest="TestRunJobMonolithic.test_bind_stream_kill_at_100")
