import sys
import time
import unittest
import shutil
import psutil
from nodejobs.jobs import Jobs


class TestJobsBlackBox(unittest.TestCase):
    def setUp(self):
        data_dir = "./test_data"
        try:
            shutil.rmtree(data_dir)
        except FileNotFoundError:
            pass
        self.jobs = Jobs(db_path=data_dir)

    def tearDown(self):
        # Restore HOME and clean up
        pass

    def _wait_for_status(self, job_id, desired_status, timeout=5.0):
        """
        Poll list_status() until the job’s status matches desired_status
        or until timeout (in seconds) elapses.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            all_jobs = self.jobs.list_status()
            job = None
            if job_id in all_jobs:
                job = all_jobs[job_id]
            if job and job["status"] == desired_status:
                return True
            time.sleep(1)
        return False

    def test_run_to_finished(self):
        # 1. run “sleep 1” job → expect “running” → then “finished”
        job_id_test = "fbadfkadbkjfda"
        result = self.jobs.run(
            command=["bash", "-c", """echo "starting"; sleep 3; echo "done" """], job_id=job_id_test
        )
        # print(f"INITIAL RESULT {result} ")
        self.assertEqual(result["self_id"], job_id_test)
        self.assertEqual(result["status"], "running")
        time.sleep(1)
        # print("\nLIST STATUS \n\n\n\n")
        all_jobs = self.jobs.list_status()
        self.assertIn(job_id_test, all_jobs)
        self.assertEqual(all_jobs[job_id_test]["status"], "running")
        self.assertEqual(result["last_pid"], all_jobs[job_id_test]["last_pid"])

        # Wait up to 2 seconds for it to finish
        finished = self._wait_for_status(job_id_test, "finished", timeout=7.0)
        status = self.jobs.get_status(job_id=job_id_test)
        self.assertTrue(
            finished,
            f"Job {job_id_test} did not transition to ‘finished’ in time {status}"
        )

    def test_job_logs_capture(self):
        # 2. run a short Python command that writes to stdout and stderr
        py = sys.executable
        large_id = "fdsknfdldsnj"
        #py_code = "import sys; print('hi'); sys.stderr.write('err\\n');"
        #cmd = [py, "-c", py_code]
        #result = self.jobs.run(command=cmd, job_id="t2")
        py_code = "import sys; print('hi'); sys.stderr.write('err\\n');"
        cmd = [py, "-c", py_code]
        result = self.jobs.run(command=cmd, job_id=large_id)
        
        # print(result)
        self.assertIn(result["status"], ["running", "finished"])

        # Wait for immediate finish
        finished = self._wait_for_status(large_id, "finished", timeout=2.0)
        self.assertTrue(finished, f"Job {large_id} did not finish in time")

        # Retrieve log paths
        stdout, stderr = self.jobs.job_logs(job_id=large_id)
        assert stdout.strip() == "hi"
        assert stderr.strip() == "err"

    def test_stop_long_running_job(self):
        result = self.jobs.run(command=['bash','-c',"sleep 5"], job_id="t3")
        self.assertEqual(result["status"], "running")
        stop_res = self.jobs.stop(job_id="t3")
        self.assertIn(stop_res["status"], ("stopped", "finished"))
        # After listing status, it should not remain “running”
        all_jobs = self.jobs.list_status()
        self.assertIn("t3", all_jobs)
        self.assertNotEqual(all_jobs["t3"]["status"], "running")
        self.assertIn(stop_res["status"], ("stopped", "finished"))
        j = all_jobs["t3"]

        found = False
        for proc in psutil.process_iter(["pid", "status"]):
            pid = proc.info.get("pid")
            assert pid is not None, "missing a PID?!"
            if pid == j["last_pid"]:
                print(f"inspecting process: {proc.info}")

            if pid == j["last_pid"] and j["status"] in ["running", "sleeping"]:
                print(f"failed process: {proc.info}")
                found = True
                break
        self.assertFalse(found, f"Found leftover 'sleep 5' pid:{pid} process")

    def test_stop_nonexistent_job(self):
        # 4. stopping a job that doesn’t exist → return None
        # result = self.jobs.run(command="sleep 5", job_id="t3")
        # pass
        res = self.jobs.stop(job_id="no_such")
        self.assertIsNone(res)

    def test_list_status_filtering(self):
        # Job “a” - short sleep
        jida = "fnjdalncklxsa"
        jidb = "fafmadsg"
        jidc = "mflemwlg"

        res_a = self.jobs.run(
            command=["bash", "-c", """echo "starting"; sleep 10; echo "done" """], job_id=jida
        )
        time.sleep(2)
        stdout_path, stderr_path = self.jobs.job_logs(job_id=jida)
        self.assertEqual(res_a["status"], "running")
        res_b = self.jobs.run(command=['bash','-c',"sleabkjep 1"], job_id=jidb)
        self.assertEqual(res_b["status"], "failed_start")

        # Job “c” - immediate finish
        py = sys.executable
        cmd_c = [py, "-c", "\"print('x')\""]
        res_c = self.jobs.run(command=cmd_c, job_id=jidc)
        self.assertIn(res_c["status"], ["running", "finished"])

        # Give time for “b” and “c” to finish, but “a” should still be running
        time.sleep(1)

        # Now filter by running → only “b” should appear
        running_jobs = self.jobs.list_status(filter={"status": "running"})
        self.assertIn(jida, running_jobs)
        self.assertNotIn(jidb, running_jobs)
        self.assertNotIn(jidc, running_jobs)

        # Filter by finished → “a” and “c” should appear
        finished_jobs = self.jobs.list_status(filter={"status": "finished"})
        self.assertIn(jidc, finished_jobs)
        self.assertNotIn(jida, finished_jobs)
        failed_jobs = self.jobs.list_status(filter={"status": "failed_start"})
        self.assertIn(jidb, failed_jobs)

        # Filter by dirname → single‐element dict
        single_b = self.jobs.list_status(filter={"dirname": jidb})
        self.assertEqual(len(single_b), 1)
        self.assertIn(jidb, single_b)

        # Finally wait for “b” to finish and confirm it moves to finished
        finished_b = self._wait_for_status(jida, "finished", timeout=10.0)

        self.assertTrue(finished_b, "Job a did not finish in time")


if __name__ == "__main__":
    unittest.main()
