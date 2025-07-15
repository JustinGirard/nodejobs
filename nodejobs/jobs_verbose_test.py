import sys
import time
import unittest
from nodejobs.jobs import Jobs
from nodejobs.jobdb import JobRecord
import subprocess
import shutil


class TestJobsBlackBox(unittest.TestCase):
    def setUp(self):
        # Use a temporary directory for isolation
        self.data_dir = "./test_data"
        try:
            shutil.rmtree(self.data_dir)
        except FileNotFoundError:
            pass
        self.jobs = Jobs(db_path=self.data_dir)
        # print("DONE INIT--------->")

    def tearDown(self):
        # Clean up the test_data directory after each test
        try:
            shutil.rmtree(self.data_dir)
        except FileNotFoundError:
            pass

    def _wait_for_status(
        self, job_id: str, desired_status: str, timeout: float = 5.0
    ) -> bool:
        """
        Poll list_status() until the job’s status matches desired_status
        or until timeout (in seconds) elapses.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            all_jobs = self.jobs.list_status()
            if job_id in all_jobs:
                rec = JobRecord(all_jobs[job_id])  # runtime type‐check
                if rec.status == desired_status:
                    return True
            time.sleep(0.1)
        return False

    def test_run_to_finished(self):
        # 1. run a short shell command → expect “running” → then “finished”
        result = self.jobs.run(
            command=["bash", "-c", 'echo "starting"; sleep 3; echo "done"'], job_id="t1"
        )
        result = JobRecord(result)  # runtime type‐check
        self.assertEqual(result.self_id, "t1")
        self.assertEqual(result.status, JobRecord.Status.c_running)

        time.sleep(1)
        all_jobs = self.jobs.list_status()
        self.assertIn("t1", all_jobs)
        rec = JobRecord(all_jobs["t1"])
        self.assertEqual(rec.status, JobRecord.Status.c_running)

        # Wait up to 7 seconds for it to finish
        finished = self._wait_for_status("t1",
                                         JobRecord.Status.c_finished,
                                         timeout=7.0)
        status_rec = self.jobs.get_status(job_id="t1")
        self.assertTrue(
            finished,
            f"Job t1 did not mv to ‘finished’ ]; status: {status_rec}",
        )

    def test_job_logs_capture(self):
        # 2. run a short Python command that writes to stdout and stderr
        large_id = "fdbskgvbsdjkgbsdjkf"
        py = sys.executable
        py_code = "import sys; print('hi'); sys.stderr.write('err\\n');"
        cmd = [py, "-c", py_code]
        result = self.jobs.run(command=cmd, job_id=large_id)
        result = JobRecord(result)  # runtime type‐check

        self.assertIn(
            result.status,
            (
                JobRecord.Status.c_running,
                JobRecord.Status.c_finished
            )
        )

        # Wait for immediate finish
        finished = self._wait_for_status(large_id,
                                         JobRecord.Status.c_finished,
                                         timeout=2.0)
        self.assertTrue(finished, "Job t2 did not finish in time")

        # Retrieve log contents
        stdout_text, stderr_text = self.jobs.job_logs(job_id=large_id)
        self.assertEqual(stdout_text.strip(), "hi")
        self.assertEqual(stderr_text.strip(), "err")

    def test_stop_long_running_job(self):
        result = self.jobs.run(command=["bash", "-c", "sleep 5"],
                               job_id="t3")
        result = JobRecord(result)  # runtime type‐check
        self.assertEqual(result.status, JobRecord.Status.c_running)

        # Give the subprocess a moment to start
        time.sleep(0.1)

        stop_res = self.jobs.stop(job_id="t3")
        # Stop returns either a JobRecord or None; first runtime‐check:
        stop_res = JobRecord(stop_res)  # type‐check if not None
        self.assertIn(
            stop_res.status,
            (
                JobRecord.Status.c_stopped,
                JobRecord.Status.c_finished
            )
        )

        # After listing status, it should not remain “running”
        all_jobs = self.jobs.list_status()
        self.assertIn("t3", all_jobs)
        rec = JobRecord(all_jobs["t3"])
        self.assertNotEqual(rec.status, JobRecord.Status.c_running)

        # Verify no OS process with that exact PID remains
        pid = stop_res.last_pid
        still_exists = subprocess.call(
            ["ps", "-p", str(pid)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        self.assertNotEqual(still_exists, 0)

    def test_stop_nonexistent_job(self):
        # 4. stopping a job that doesn’t exist → return None
        res = self.jobs.stop(job_id="no_such")
        self.assertIsNone(res)

    def test_list_status_filtering(self):
        # Job “a” - long sleep (will remain running)
        jida = "gdnsbjkdfankl"
        jidb = "fdsgsdgsgsd"
        jidc = "hsfhfshfsfh"
        res_a = self.jobs.run(
            command=["bash", "-c", 'echo "starting"; sleep 10; echo "done"'], job_id=jida
        )
        res_a = JobRecord(res_a)  # runtime type‐check
        time.sleep(0.2)  # let “a” actually enter “running”
        rec_a = JobRecord(self.jobs.list_status()[jida])
        self.assertEqual(rec_a.status, JobRecord.Status.c_running)

        # Job “b” - invalid command → should fail_start
        res_b = self.jobs.run(command="sleabkjep 1", job_id=jidb)
        res_b = JobRecord(res_b)
        self.assertEqual(res_b.status, JobRecord.Status.c_failed_start)

        # Job “c” - immediate finish
        py = sys.executable
        cmd_c = f"{py} -c \"print('x')\""
        res_c = self.jobs.run(command=cmd_c, job_id=jidc)
        res_c = JobRecord(res_c)
        self.assertIn(
            res_c.status,
            (
                JobRecord.Status.c_running,
                JobRecord.Status.c_finished
            )
        )

        # Give time for “b” and “c” to finish; “a” should still be running
        time.sleep(0.5)

        # Filter by running → only “a” should appear
        running_jobs = self.jobs.list_status(
            filter={JobRecord.status: JobRecord.Status.c_running}
        )
        self.assertIn(jida, running_jobs)
        self.assertNotIn(jidb, running_jobs)
        self.assertNotIn(jidc, running_jobs)

        finished_jobs = self.jobs.list_status(
            filter={JobRecord.status: JobRecord.Status.c_finished}
        )
        self.assertIn(jidc, finished_jobs)
        self.assertNotIn(jida, finished_jobs)

        # Filter by failed_start → only “b” should appear
        failed_jobs = self.jobs.list_status(
            filter={JobRecord.status: JobRecord.Status.c_failed_start}
        )
        self.assertIn(jidb, failed_jobs)

        # Filter by dirname → single‐element dict
        single_b = self.jobs.list_status(filter={JobRecord.dirname: jidb})
        self.assertEqual(len(single_b), 1)
        self.assertIn(jidb, single_b)

        # Finally wait for “a” to finish on its own (timeout 10 seconds)
        finished_a = self._wait_for_status(
            jida, JobRecord.Status.c_finished, timeout=10.0
        )
        self.assertTrue(finished_a, "Job a did not finish in time")


if __name__ == "__main__":
    unittest.main()
    # unittest.main(defaultTest="TestJobsBlackBox.test_job_logs_capture")
