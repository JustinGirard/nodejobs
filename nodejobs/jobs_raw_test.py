import datetime as _dt
import sys
import time
import unittest
import shutil
import psutil
from nodejobs.jobs import JobDeleteError, Jobs
from nodejobs.jobdb import ScheduleRecord
from nodejobs.ui.adapters import JobsAdapter


class TestJobsBlackBox(unittest.TestCase):
    def setUp(self):
        data_dir = "./test_data"
        try:
            shutil.rmtree(data_dir)
        except FileNotFoundError:
            pass
        self.jobs = Jobs(db_path=data_dir)
        self.jobs_adapter = JobsAdapter(self.jobs)

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

    def _seed_stopped_job(self, job_id: str):
        self.jobs.jobdb.update_status(
            {
                "self_id": job_id,
                "status": "stopped",
                "command": ["echo", "ok"],
            }
        )

    def _read_tags(self, rec):
        if rec is None:
            return None
        if isinstance(rec, dict):
            return rec.get("tags")
        try:
            return rec["tags"]
        except Exception:
            return getattr(rec, "tags", None)

    def test_create_job_draft_without_running_core_api(self):
        job_id = "core_create_job"
        command = ["echo", "ok"]
        tags = {"dev": "dev"}

        rec = self.jobs.create_job(
            command=command,
            job_id=job_id,
            cwd=None,
            envs={},
            tags=tags,
        )
        self.assertEqual(rec["self_id"], job_id)
        self.assertEqual(rec["status"], "stopped")
        self.assertEqual(rec["last_pid"], -1)
        self.assertEqual(rec["command"], command)
        self.assertEqual(rec["tags"], tags)
        persisted = self.jobs.get_status(job_id=job_id)
        self.assertEqual(persisted["status"], "stopped")

    def test_create_job_rejects_duplicate_core_api(self):
        job_id = "core_create_job_dup"
        self.jobs.create_job(
            command=["echo", "ok"],
            job_id=job_id,
            cwd=None,
            envs={},
            tags={"dev": "dev"},
        )
        with self.assertRaises(ValueError) as ctx:
            self.jobs.create_job(command=["echo", "ok"], job_id=job_id)
        self.assertIn("job already exists", str(ctx.exception))

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
        result = self.jobs.run(command=['bash', '-c', "sleep 5"], job_id="t3")
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
        res_b = self.jobs.run(command=['bash', '-c', "sleabkjep 1"], job_id=jidb)
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

    def test_job_tags_patch_add_preserve_and_remove_via_draft_endpoints(self):
        job_id = "tags_draft_job"
        initial_tags = {"team": "team", "prod": "prod"}
        self.jobs_adapter.create_job_draft(
            command=["echo", "ok"],
            job_id=job_id,
            cwd=None,
            envs={},
            tags=initial_tags,
        )

        rec = self.jobs.get_status(job_id=job_id)
        self.assertIsNotNone(rec)
        self.assertEqual(self._read_tags(rec), initial_tags)

        self.jobs_adapter.upsert_job_draft(
            command=["echo", "ok"],
            job_id=job_id,
            cwd=None,
            envs={},
            tags={},
            status="stopped",
        )
        rec2 = self.jobs.get_status(job_id=job_id)
        self.assertIsNotNone(rec2)
        self.assertEqual(self._read_tags(rec2), initial_tags)

        self.jobs_adapter.upsert_job_draft(
            command=["echo", "ok"],
            job_id=job_id,
            cwd=None,
            envs={},
            tags={"prod": ""},
            status="stopped",
        )
        rec3 = self.jobs.get_status(job_id=job_id)
        self.assertIsNotNone(rec3)
        self.assertEqual(self._read_tags(rec3), {"team": "team"})

        self.jobs_adapter.upsert_job_draft(
            command=["echo", "ok"],
            job_id=job_id,
            cwd=None,
            envs={},
            tags={"team": None},
            status="stopped",
        )
        rec4 = self.jobs.get_status(job_id=job_id)
        self.assertIsNotNone(rec4)
        self.assertFalse(bool(self._read_tags(rec4)))

    def test_job_tags_patch_rejects_mismatched_values(self):
        with self.assertRaises(ValueError):
            self.jobs_adapter.create_job_draft(
                command=["echo", "ok"],
                job_id="tags_bad_value_job",
                cwd=None,
                envs={},
                tags={"prod": "production"},
            )

    def test_list_status_filtering_by_tags_subset(self):
        job_a = "tags_filter_a"
        job_b = "tags_filter_b"
        job_c = "tags_filter_c"

        self.jobs_adapter.create_job_draft(
            command=["echo", "a"],
            job_id=job_a,
            tags={"team": "team", "env": "env"},
        )
        self.jobs_adapter.create_job_draft(
            command=["echo", "b"],
            job_id=job_b,
            tags={"team": "team"},
        )
        self.jobs_adapter.create_job_draft(
            command=["echo", "c"],
            job_id=job_c,
            tags={"other": "other"},
        )

        team_matches = self.jobs.list_status(filter={"tags": {"team": "team"}})
        self.assertIn(job_a, team_matches)
        self.assertIn(job_b, team_matches)
        self.assertNotIn(job_c, team_matches)

        strict_matches = self.jobs.list_status(filter={"tags": {"team": "team", "env": "env"}})
        self.assertIn(job_a, strict_matches)
        self.assertNotIn(job_b, strict_matches)
        self.assertNotIn(job_c, strict_matches)

        missing_matches = self.jobs.list_status(filter={"tags": {"missing": "missing"}})
        self.assertEqual(len(missing_matches), 0)

    def test_start_and_stop_npm_run_start(self):
        npm_project_dir = "/Users/computercomputer/justinops/projects/portfolio"  # TODO: set this
        job_id = "npm_run_start_test"

        import os

        npm_bin = "/Users/computercomputer/.nvm/versions/node/v20.11.1/bin"

        envs = {**os.environ}
        envs["PATH"] = f"{npm_bin}:" + envs.get("PATH", "")


        result = self.jobs.run(
            command=["npm", "run", "start"],
            job_id=job_id,
            cwd=npm_project_dir,
            envs=envs
        )

        self.assertEqual(result["self_id"], job_id)
        self.assertEqual(result["status"], "running")

        # Let the dev server spin up a bit
        time.sleep(5)

        stop_res = self.jobs.stop(job_id=job_id)
        self.assertIsNotNone(stop_res)
        self.assertIn(stop_res["status"], ("stopped", "finished"))

        all_jobs = self.jobs.list_status()
        self.assertIn(job_id, all_jobs)
        self.assertNotEqual(all_jobs[job_id]["status"], "running")

    def test_delete_stopped_unscheduled_job(self):
        job_id = "delete_ok_job"
        self._seed_stopped_job(job_id)
        result = self.jobs.delete_job(job_id)
        self.assertTrue(result["deleted"])
        self.assertEqual(result["job_id"], job_id)
        rows = self.jobs.list_status()
        self.assertNotIn(job_id, rows)

    def test_delete_running_job_blocked(self):
        job_id = "delete_block_running"
        run_res = self.jobs.run(command=["bash", "-c", "sleep 5"], job_id=job_id)
        self.assertIn(run_res["status"], ("running", "finished", "failed_start"))
        try:
            with self.assertRaises(JobDeleteError) as ctx:
                self.jobs.delete_job(job_id)
            err = ctx.exception
            self.assertEqual(err.error.code, "job_active")
            self.assertEqual(err.error.job_id, job_id)
            self.assertTrue(err.error.details.status in ("starting", "running", "stopping"))
        finally:
            self.jobs.stop(job_id=job_id)

    def test_delete_scheduled_job_blocked(self):
        job_id = "delete_block_scheduled"
        schedule_id = "sched_delete_block"
        self._seed_stopped_job(job_id)
        self.jobs.set_schedule(
            {
                ScheduleRecord.schedule_id: schedule_id,
                ScheduleRecord.job_id: job_id,
                ScheduleRecord.next_run_at: _dt.datetime.now(_dt.timezone.utc)
                + _dt.timedelta(minutes=5),
                ScheduleRecord.enabled: True,
            }
        )
        with self.assertRaises(JobDeleteError) as ctx:
            self.jobs.delete_job(job_id)
        err = ctx.exception
        self.assertEqual(err.error.code, "job_scheduled")
        self.assertEqual(err.error.job_id, job_id)
        self.assertIn(schedule_id, err.error.details.schedule_ids)

    def test_delete_missing_job_blocked(self):
        with self.assertRaises(JobDeleteError) as ctx:
            self.jobs.delete_job("job_that_does_not_exist")
        err = ctx.exception
        self.assertEqual(err.error.code, "job_not_found")
        self.assertEqual(err.error.job_id, "job_that_does_not_exist")

if __name__ == "__main__":
    unittest.main()
    #unittest.main(defaultTest="TestJobsBlackBox.test_start_and_stop_npm_run_start")
    #unittest.main(defaultTest="TestJobsBlackBox.test_run_to_finished")

    
