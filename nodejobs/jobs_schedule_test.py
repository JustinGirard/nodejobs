import datetime as _dt
import shutil
import sys
import time
import threading
import unittest

from nodejobs.jobs import Jobs
from nodejobs.jobdb import JobFilter, JobRecord, ScheduleRecord


DEBUG = True


def dprint(*a, **k):
    if DEBUG:
        print(*a, **k)


class TestJobsScheduling(unittest.TestCase):
    def setUp(self):
        self.data_dir = "./test_data"
        try:
            shutil.rmtree(self.data_dir)
        except FileNotFoundError:
            pass
        self.jobs = Jobs(db_path=self.data_dir, verbose=False)

    def tearDown(self):
        try:
            shutil.rmtree(self.data_dir)
        except FileNotFoundError:
            pass

    def _is_active_status(self, status: str) -> bool:
        return status in (
            JobRecord.Status.c_starting,
            JobRecord.Status.c_running,
            JobRecord.Status.c_stopping,
        )

    def _status_exists(self, job_id: str) -> bool:
        rows = self.jobs.jobdb.list_status({JobFilter.self_id: job_id})
        return job_id in rows

    def _get_status(self, job_id: str):
        rows = self.jobs.jobdb.list_status({JobFilter.self_id: job_id})
        if job_id not in rows:
            return None
        return JobRecord(rows[job_id])

    def _get_schedule(self, schedule_id: str) -> ScheduleRecord:
        schedules = self.jobs.list_schedules()
        self.assertIn(schedule_id, schedules, f"Missing schedule {schedule_id}")
        return ScheduleRecord(schedules[schedule_id])

    def _wait_until(self, label: str, predicate, timeout: float = 8.0, poll: float = 0.1) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            self.jobs.list_status()
            try:
                if predicate():
                    dprint(f"PROOF wait_until:{label}=True")
                    return True
            except Exception as e:
                dprint(f"PROOF wait_until:{label}: transient_exception={e!r}")
            time.sleep(poll)
        return False

    def _stop_if_active(self, job_id: str):
        rec = self.jobs.get_status(job_id)
        if rec is None:
            return
        if self._is_active_status(rec.status):
            stop_res = self.jobs.stop(job_id=job_id, wait_time=1)
            dprint(f"PROOF stop_if_active: job_id={job_id!r} stop_status={getattr(stop_res, 'status', None)!r}")

    def test_one_shot_due_now_launches_on_pump(self):
        schedule_id = "sched_one_shot_due_now"
        job_id = "sched_one_shot_job"
        now_at = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)

        self.jobs.set_schedule({
            ScheduleRecord.schedule_id: schedule_id,
            ScheduleRecord.job_id: job_id,
            ScheduleRecord.command: [
                sys.executable,
                "-u",
                "-c",
                "import time; print('start', flush=True); time.sleep(1.2); print('done', flush=True)",
            ],
            ScheduleRecord.next_run_at: now_at,
            ScheduleRecord.enabled: True,
        })
        dprint(f"PROOF set_schedule one_shot: schedule_id={schedule_id!r} job_id={job_id!r}")

        self.assertFalse(self._status_exists(job_id), "Job status should not exist before pump")

        self.jobs.list_status()
        launched = self._wait_until("one_shot_launched", lambda: self._status_exists(job_id), timeout=6.0)
        self.assertTrue(launched, "Expected one-shot schedule to launch on pump")

        sched = self._get_schedule(schedule_id)
        dprint(
            f"PROOF one_shot schedule_state: enabled={sched.enabled!r} "
            f"attempt_count={sched.attempt_count!r} last_error={sched.last_error!r}"
        )
        self.assertFalse(sched.enabled, "One-shot should be disabled after launch reservation")
        self.assertGreaterEqual(sched.attempt_count, 1, "attempt_count should increment on launch attempt")
        self.assertIsNone(sched.last_error, "Successful launch should clear last_error")

        self._stop_if_active(job_id)

    def test_recurring_skips_overlap_then_relaunches(self):
        schedule_id = "sched_recurring_overlap"
        job_id = "sched_recurring_job"
        now_at = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)

        self.jobs.set_schedule({
            ScheduleRecord.schedule_id: schedule_id,
            ScheduleRecord.job_id: job_id,
            ScheduleRecord.command: [
                sys.executable,
                "-u",
                "-c",
                "import time; print('loop-start', flush=True); time.sleep(1.5)",
            ],
            ScheduleRecord.next_run_at: now_at,
            ScheduleRecord.interval_sec: 1,
            ScheduleRecord.enabled: True,
        })
        self.jobs.list_status()

        first = self._wait_until(
            "recurring_first_attempt",
            lambda: self._get_schedule(schedule_id).attempt_count >= 1,
            timeout=6.0,
        )
        self.assertTrue(first, "Expected first recurring launch attempt")
        attempt_after_first = self._get_schedule(schedule_id).attempt_count
        dprint(f"PROOF recurring first_attempt_count={attempt_after_first!r}")

        t0 = time.time()
        while time.time() - t0 < 0.8:
            self.jobs.list_status()
            rec = self._get_status(job_id)
            if rec and self._is_active_status(rec.status):
                mid = self._get_schedule(schedule_id).attempt_count
                self.assertEqual(
                    mid,
                    attempt_after_first,
                    "Attempt count should not increase while job is active (overlap skip)",
                )
            time.sleep(0.1)

        relaunch = self._wait_until(
            "recurring_second_attempt",
            lambda: self._get_schedule(schedule_id).attempt_count >= (attempt_after_first + 1),
            timeout=8.0,
        )
        self.assertTrue(relaunch, "Expected recurring schedule to relaunch after terminal state")
        dprint(
            f"PROOF recurring second_attempt_count={self._get_schedule(schedule_id).attempt_count!r} "
            f"first={attempt_after_first!r}"
        )

        self._stop_if_active(job_id)

    def test_remove_schedule_prevents_launch(self):
        schedule_id = "sched_remove_before_pump"
        job_id = "sched_removed_job"
        now_at = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)

        self.jobs.set_schedule({
            ScheduleRecord.schedule_id: schedule_id,
            ScheduleRecord.job_id: job_id,
            ScheduleRecord.command: [sys.executable, "-u", "-c", "import time; time.sleep(1)"],
            ScheduleRecord.next_run_at: now_at,
            ScheduleRecord.enabled: True,
        })
        self.jobs.remove_schedule(schedule_id)
        dprint(f"PROOF remove_schedule: schedule_id={schedule_id!r}")

        schedules = self.jobs.list_schedules()
        self.assertNotIn(schedule_id, schedules, "Schedule should be removed before pump")

        for _ in range(6):
            self.jobs.list_status()
            time.sleep(0.05)

        self.assertFalse(self._status_exists(job_id), "Removed schedule should not launch job")
        self.assertIsNone(self.jobs.processes.find(job_id), "Removed schedule should not create wrapper process")

    def test_failed_start_sets_error_and_backoff(self):
        schedule_id = "sched_fail_backoff"
        job_id = "sched_fail_job"
        now_at = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)

        self.jobs.set_schedule({
            ScheduleRecord.schedule_id: schedule_id,
            ScheduleRecord.job_id: job_id,
            ScheduleRecord.command: ["__definitely_missing_binary__"],
            ScheduleRecord.next_run_at: now_at,
            ScheduleRecord.enabled: True,
        })

        self.jobs.list_status()
        attempted = self._wait_until(
            "failed_start_attempt_recorded",
            lambda: self._get_schedule(schedule_id).attempt_count >= 1,
            timeout=6.0,
        )
        self.assertTrue(attempted, "Expected failed launch attempt to be recorded")

        rec = self._get_schedule(schedule_id)
        now_check = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)
        delta_sec = (rec.next_run_at - now_check).total_seconds()
        dprint(
            f"PROOF failed_start schedule_state: enabled={rec.enabled!r} "
            f"attempt_count={rec.attempt_count!r} last_error={rec.last_error!r} "
            f"next_run_delta_sec={delta_sec!r}"
        )

        self.assertTrue(rec.enabled, "Failed one-shot should be re-enabled for retry")
        self.assertIsInstance(rec.last_error, str)
        self.assertGreater(len(rec.last_error.strip()), 0, "Expected last_error after failed launch")
        self.assertGreaterEqual(rec.attempt_count, 1, "Expected attempt_count increment after failed launch")
        self.assertGreater(delta_sec, 2.0, "Expected delayed retry window after failed one-shot launch")

        attempts_before = rec.attempt_count
        self.jobs.list_status()
        rec_after = self._get_schedule(schedule_id)
        self.assertEqual(
            rec_after.attempt_count,
            attempts_before,
            "Immediate second pump should not retry before backoff expires",
        )

    def test_get_status_triggers_due_schedule_pump(self):
        schedule_id = "sched_get_status_trigger"
        job_id = "sched_get_status_job"
        now_at = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)

        self.jobs.set_schedule({
            ScheduleRecord.schedule_id: schedule_id,
            ScheduleRecord.job_id: job_id,
            ScheduleRecord.command: [
                sys.executable,
                "-u",
                "-c",
                "import time; print('gs', flush=True); time.sleep(1.0)",
            ],
            ScheduleRecord.next_run_at: now_at,
            ScheduleRecord.enabled: True,
        })
        self.assertFalse(self._status_exists(job_id), "Job should not exist before first pump")

        rec = self.jobs.get_status(job_id)
        dprint(f"PROOF get_status_trigger: initial_rec_exists={rec is not None}")

        launched = self._wait_until(
            "get_status_due_launch",
            lambda: self._status_exists(job_id),
            timeout=6.0,
        )
        self.assertTrue(launched, "Expected due schedule to launch via get_status pump path")

        sched = self._get_schedule(schedule_id)
        dprint(
            f"PROOF get_status_trigger schedule_state: enabled={sched.enabled!r} "
            f"attempt_count={sched.attempt_count!r}"
        )
        self.assertFalse(sched.enabled, "One-shot schedule should be disabled after launch reservation")
        self.assertGreaterEqual(sched.attempt_count, 1)
        self._stop_if_active(job_id)

    def test_schedule_persists_and_runs_after_jobs_reinit(self):
        schedule_id = "sched_restart_persist"
        job_id = "sched_restart_job"
        now_at = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)

        self.jobs.set_schedule({
            ScheduleRecord.schedule_id: schedule_id,
            ScheduleRecord.job_id: job_id,
            ScheduleRecord.command: [
                sys.executable,
                "-u",
                "-c",
                "import time; print('persist', flush=True); time.sleep(1.0)",
            ],
            ScheduleRecord.next_run_at: now_at,
            ScheduleRecord.enabled: True,
        })

        jobs2 = Jobs(db_path=self.data_dir, verbose=False)
        self.assertNotIn(job_id, jobs2.jobdb.list_status({JobFilter.self_id: job_id}))

        launched = False
        deadline = time.time() + 8.0
        while time.time() < deadline:
            jobs2.list_status()
            if job_id in jobs2.jobdb.list_status({JobFilter.self_id: job_id}):
                launched = True
                break
            time.sleep(0.1)
        self.assertTrue(launched, "Expected persisted due schedule to launch after Jobs re-init")

        schedules = jobs2.list_schedules()
        self.assertIn(schedule_id, schedules)
        sched = ScheduleRecord(schedules[schedule_id])
        dprint(
            f"PROOF restart_persist schedule_state: enabled={sched.enabled!r} "
            f"attempt_count={sched.attempt_count!r}"
        )
        self.assertFalse(sched.enabled)
        self.assertGreaterEqual(sched.attempt_count, 1)

        rec = jobs2.get_status(job_id)
        if rec is not None and self._is_active_status(rec.status):
            jobs2.stop(job_id=job_id, wait_time=1)

    def test_two_due_schedules_same_job_id_only_one_attempts(self):
        schedule_a = "sched_same_job_a"
        schedule_b = "sched_same_job_b"
        job_id = "sched_same_job_target"
        now_at = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)

        common = {
            ScheduleRecord.job_id: job_id,
            ScheduleRecord.command: [
                sys.executable,
                "-u",
                "-c",
                "import time; print('same-job', flush=True); time.sleep(2.0)",
            ],
            ScheduleRecord.next_run_at: now_at,
            ScheduleRecord.enabled: True,
        }
        self.jobs.set_schedule({ScheduleRecord.schedule_id: schedule_a, **common})
        self.jobs.set_schedule({ScheduleRecord.schedule_id: schedule_b, **common})

        self.jobs.list_status()
        sa = self._get_schedule(schedule_a)
        sb = self._get_schedule(schedule_b)
        dprint(
            f"PROOF same_job_dues attempts: a={sa.attempt_count!r} b={sb.attempt_count!r} "
            f"enabled_a={sa.enabled!r} enabled_b={sb.enabled!r}"
        )

        self.assertEqual(
            sa.attempt_count + sb.attempt_count,
            1,
            "Exactly one schedule should attempt launch for the same active job_id in a pump pass",
        )
        self.assertTrue(
            (sa.enabled is False and sb.enabled is True) or (sb.enabled is False and sa.enabled is True),
            "Attempted one-shot should be disabled; skipped one should remain enabled",
        )
        self._stop_if_active(job_id)

    def test_concurrent_list_status_pump_single_one_shot_attempt(self):
        schedule_id = "sched_concurrent_pump"
        job_id = "sched_concurrent_job"
        now_at = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)

        self.jobs.set_schedule({
            ScheduleRecord.schedule_id: schedule_id,
            ScheduleRecord.job_id: job_id,
            ScheduleRecord.command: [
                sys.executable,
                "-u",
                "-c",
                "import time; print('concurrent', flush=True); time.sleep(2.0)",
            ],
            ScheduleRecord.next_run_at: now_at,
            ScheduleRecord.enabled: True,
        })

        thread_count = 6
        gate = threading.Barrier(thread_count)
        errors = []

        def worker(ix: int):
            try:
                gate.wait(timeout=3.0)
                self.jobs.list_status()
                dprint(f"PROOF concurrent worker_done: idx={ix}")
            except Exception as e:
                errors.append((ix, repr(e)))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(thread_count)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=6.0)

        self.assertEqual(len(errors), 0, f"Concurrent pump workers failed: {errors!r}")

        self.jobs.list_status()
        sched = self._get_schedule(schedule_id)
        wrappers = [p for p in self.jobs.processes.list() if getattr(p, "job_id", None) == job_id]
        dprint(
            f"PROOF concurrent schedule_state: attempt_count={sched.attempt_count!r} "
            f"enabled={sched.enabled!r} wrapper_count={len(wrappers)!r}"
        )

        self.assertEqual(
            sched.attempt_count,
            1,
            "One-shot schedule should record exactly one launch attempt under concurrent pumps",
        )
        self.assertLessEqual(
            len(wrappers),
            1,
            "Should not have multiple run_job wrappers for the same job_id",
        )
        self._stop_if_active(job_id)


if __name__ == "__main__":
    unittest.main()
