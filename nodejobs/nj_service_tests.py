import datetime as _dt
import json
import os
import re
import shutil
import sys
import tempfile
import time
import unittest

from nodejobs.jobs import Jobs
from nodejobs.jobdb import JobRecord
from nodejobs.jobs_service import JobsService
from nodejobs.schedules_service import SchedulesService
from nodejobs.nj import NJService


DEBUG = True


def dprint(*a, **k):
    if DEBUG:
        print(*a, **k)


class CliViaJobs:
    TERMINAL_STATUSES = {
        "stopped",
        "finished",
        "failed",
        "failed_start",
    }

    @staticmethod
    def _field(obj, key, default=None):
        if isinstance(obj, dict):
            return obj.get(key, default)
        try:
            return obj[key]
        except Exception:
            return getattr(obj, key, default)

    @staticmethod
    def wait_for_terminal_status(jobs: Jobs, job_id: str, timeout=12.0, poll=0.1):
        deadline = time.time() + timeout
        last_status = None
        while time.time() < deadline:
            rec = jobs.get_status(job_id=job_id)
            if rec is not None:
                last_status = CliViaJobs._field(rec, "status")
                if last_status in CliViaJobs.TERMINAL_STATUSES:
                    dprint(f"PROOF wait_terminal: job_id={job_id!r} status={last_status!r}")
                    return rec
            time.sleep(poll)
        raise AssertionError(f"Timed out waiting for terminal status for {job_id!r}; last_status={last_status!r}")

    @staticmethod
    def parse_cli_response(stdout_text: str):
        """
        Parse the printed BaseService response line from stdout.
        Works with dict-like repr output (single quotes, possible datetime reprs).
        """
        raw_text = (stdout_text or "").strip()

        # Preferred path: parse JSON object output (single-line or pretty-printed).
        decoder = json.JSONDecoder()
        parsed_obj = None
        for idx, ch in enumerate(raw_text):
            if ch != "{":
                continue
            try:
                candidate, _end = decoder.raw_decode(raw_text[idx:])
            except Exception:
                continue
            if isinstance(candidate, dict) and ("ok" in candidate or "method" in candidate):
                parsed_obj = candidate

        if isinstance(parsed_obj, dict):
            ok_raw = parsed_obj.get("ok")
            ok_val = ok_raw if isinstance(ok_raw, bool) else None
            method_val = parsed_obj.get("method")
            method_val = method_val if isinstance(method_val, str) else None
            err_val = parsed_obj.get("error")
            if isinstance(err_val, (dict, list, tuple, set)):
                err_val = str(err_val)
            return {
                "line": json.dumps(parsed_obj, ensure_ascii=True),
                "ok": ok_val,
                "method": method_val,
                "error": err_val,
            }

        # Backward-compat fallback for historical dict-repr one-liner output.
        lines = [ln.strip() for ln in (stdout_text or "").splitlines() if ln.strip()]
        candidate = ""
        for ln in reversed(lines):
            has_ok = ("'ok':" in ln) or ('"ok":' in ln)
            has_method = ("'method':" in ln) or ('"method":' in ln)
            if has_ok and has_method:
                candidate = ln
                break

        if not candidate:
            return {"line": "", "ok": None, "method": None, "error": None}

        ok_val = None
        if ("'ok': True" in candidate) or ('"ok": true' in candidate.lower()):
            ok_val = True
        elif ("'ok': False" in candidate) or ('"ok": false' in candidate.lower()):
            ok_val = False

        method_match = re.search(r"""["']method["']:\s*["']([^"']+)["']""", candidate)
        method_val = method_match.group(1) if method_match else None

        err_match = re.search(r"""["']error["']:\s*(None|null|["'][^"']*["'])""", candidate, flags=re.IGNORECASE)
        if err_match:
            raw = err_match.group(1)
            if raw in ("None", "null", "NULL"):
                err_val = None
            else:
                err_val = raw.strip("'").strip('"')
        else:
            err_val = None

        return {"line": candidate, "ok": ok_val, "method": method_val, "error": err_val}

    @staticmethod
    def run_cli_through_jobs(
        *,
        jobs: Jobs,
        script_path: str,
        cli_args: list,
        invoke_job_id: str,
        cwd=None,
        envs=None,
        timeout=12.0,
    ):
        command = [sys.executable, script_path] + list(cli_args)
        dprint(f"PROOF run_cli_through_jobs: invoke_job_id={invoke_job_id!r} command={command!r}")
        launch = jobs.run(command=command, job_id=invoke_job_id, cwd=cwd, envs=envs)
        terminal = CliViaJobs.wait_for_terminal_status(jobs, invoke_job_id, timeout=timeout)
        stdout_text, stderr_text = jobs.job_logs(job_id=invoke_job_id)
        parsed = CliViaJobs.parse_cli_response(stdout_text)
        return {
            "launch": launch,
            "terminal": terminal,
            "stdout": stdout_text,
            "stderr": stderr_text,
            "parsed": parsed,
        }

    @staticmethod
    def collect_job_logs_after_terminal(*, jobs: Jobs, job_id: str, timeout=12.0):
        terminal = CliViaJobs.wait_for_terminal_status(jobs, job_id, timeout=timeout)
        stdout_text, stderr_text = jobs.job_logs(job_id=job_id)
        return {
            "terminal": terminal,
            "stdout": stdout_text,
            "stderr": stderr_text,
        }


class NJServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp_root = tempfile.mkdtemp(prefix="nj_service_tests_")
        self.db_path = os.path.join(self.tmp_root, "db")
        self.jobs = Jobs(db_path=self.db_path, verbose=False)
        self.this_dir = os.path.dirname(os.path.abspath(__file__))
        self.nj_script = os.path.join(self.this_dir, "nj.py")
        self.active_jobs = set()

    def tearDown(self):
        for job_id in list(self.active_jobs):
            try:
                rec = self.jobs.get_status(job_id=job_id)
                if rec is None:
                    continue
                status = self._field(rec, "status")
                if status in ("starting", "running", "stopping"):
                    stop_res = self.jobs.stop(job_id=job_id, wait_time=1)
                    dprint(f"PROOF teardown_stop: job_id={job_id!r} stop_status={self._field(stop_res, 'status')!r}")
            except Exception as e:
                dprint(f"PROOF teardown_stop_exception: job_id={job_id!r} exc={e!r}")
        try:
            shutil.rmtree(self.tmp_root)
        except FileNotFoundError:
            pass

    def _field(self, obj, key, default=None):
        if isinstance(obj, dict):
            return obj.get(key, default)
        try:
            return obj[key]
        except Exception:
            return getattr(obj, key, default)

    def _simple_command(self, label: str):
        return [
            sys.executable,
            "-u",
            "-c",
            f"import time; print('{label}', flush=True); time.sleep(0.25)",
        ]

    def _future_iso(self, minutes=30):
        dt_utc = _dt.datetime.now(_dt.UTC) + _dt.timedelta(minutes=minutes)
        return dt_utc.replace(tzinfo=None).isoformat(timespec="microseconds")

    def _write_json(self, filename: str, payload: dict) -> str:
        path = os.path.join(self.tmp_root, filename)
        with open(path, "w") as f:
            json.dump(payload, f)
        dprint(f"PROOF write_json: path={path!r} keys={list(payload.keys())!r}")
        return path

    def _new_id(self, prefix: str) -> str:
        return f"{prefix}_{os.getpid()}_{time.time_ns()}"

    def _seed_job_launch_spec(self, job_id: str, command=None):
        launch_command = command if isinstance(command, list) and len(command) > 0 else self._simple_command("seed")
        self.jobs.jobdb.update_status(
            JobRecord(
                {
                    JobRecord.self_id: job_id,
                    JobRecord.status: JobRecord.Status.c_finished,
                    JobRecord.command: launch_command,
                }
            )
        )
        dprint(f"PROOF seed_job_launch_spec: job_id={job_id!r} command={launch_command!r}")

    def test_jobs_run_via_nj_interface_and_analyze_target_logs(self):
        invoke_id = self._new_id("nj_cli_invoke_run")
        target_job_id = self._new_id("worker_exec_target")
        target_marker = "24681357"
        self.active_jobs.update({invoke_id, target_job_id})

        run_payload = {
            "service": "jobs",
            "op": "run",
            "db_path": self.db_path,
            "job_id": target_job_id,
            "command": [
                sys.executable,
                "-u",
                "-c",
                f"print({target_marker})",
            ],
        }
        run_payload_path = self._write_json("run_payload.json", run_payload)
        cli_args = ["nj", f"[[bulk]]=[[{run_payload_path}]]"]
        invoke_res = CliViaJobs.run_cli_through_jobs(
            jobs=self.jobs,
            script_path=self.nj_script,
            cli_args=cli_args,
            invoke_job_id=invoke_id,
        )
        self.assertEqual(CliViaJobs._field(invoke_res["terminal"], "status"), "finished")
        self.assertTrue(invoke_res["parsed"]["ok"])
        self.assertEqual(invoke_res["parsed"]["method"], "run")

        target_logs = CliViaJobs.collect_job_logs_after_terminal(
            jobs=self.jobs,
            job_id=target_job_id,
        )
        target_status = CliViaJobs._field(target_logs["terminal"], "status")
        dprint(
            f"PROOF target_job_logs: status={target_status!r} "
            f"stdout={target_logs['stdout']!r} stderr={target_logs['stderr']!r}"
        )
        self.assertEqual(
            target_status,
            "finished",
            f"Target job did not finish cleanly. stdout={target_logs['stdout']!r} stderr={target_logs['stderr']!r}",
        )
        self.assertIn(
            target_marker,
            target_logs["stdout"],
            f"Expected marker missing from stdout. stdout={target_logs['stdout']!r} stderr={target_logs['stderr']!r}",
        )

    def test_jobs_create_draft_via_nj_interface_without_running(self):
        invoke_id = self._new_id("nj_cli_invoke_create")
        target_job_id = self._new_id("draft_target")
        self.active_jobs.update({invoke_id, target_job_id})

        draft_command = [
            sys.executable,
            "-u",
            "-c",
            "print('draft command should not run')",
        ]
        create_payload = {
            "service": "jobs",
            "op": "create",
            "db_path": self.db_path,
            "job_id": target_job_id,
            "command": draft_command,
            "cwd": self.tmp_root,
            "envs": {"DRAFT_ONLY": "1"},
        }
        create_payload_path = self._write_json("create_payload.json", create_payload)
        cli_args = ["nj", f"[[bulk]]=[[{create_payload_path}]]"]
        invoke_res = CliViaJobs.run_cli_through_jobs(
            jobs=self.jobs,
            script_path=self.nj_script,
            cli_args=cli_args,
            invoke_job_id=invoke_id,
        )

        self.assertEqual(CliViaJobs._field(invoke_res["terminal"], "status"), "finished")
        self.assertTrue(invoke_res["parsed"]["ok"])
        self.assertEqual(invoke_res["parsed"]["method"], "create")

        created = self.jobs.get_status(job_id=target_job_id)
        self.assertIsNotNone(created)
        self.assertEqual(CliViaJobs._field(created, "status"), "stopped")
        self.assertEqual(CliViaJobs._field(created, "last_pid"), -1)
        self.assertEqual(CliViaJobs._field(created, "self_id"), target_job_id)
        self.assertEqual(CliViaJobs._field(created, "command"), draft_command)

        running = self.jobs.list_status(filter={JobRecord.status: JobRecord.Status.c_running})
        self.assertNotIn(
            target_job_id,
            running,
            f"Draft job unexpectedly running. running={list(running.keys())!r}",
        )

    def test_jobs_create_draft_via_service_interface_without_running(self):
        target_job_id = self._new_id("svc_create_target")
        self.active_jobs.add(target_job_id)
        draft_command = [
            sys.executable,
            "-u",
            "-c",
            "print('service draft should not run')",
        ]

        res = JobsService.create_job(
            job_id=target_job_id,
            command=draft_command,
            cwd=self.tmp_root,
            envs={"DRAFT_ONLY": "1"},
            tags={"dev": "dev"},
            db_path=self.db_path,
        )
        self.assertTrue(self._field(res, "ok"))
        self.assertEqual(self._field(res, "method"), "create")
        created = self._field(res, "result")
        self.assertEqual(self._field(created, "self_id"), target_job_id)
        self.assertEqual(self._field(created, "status"), "stopped")
        self.assertEqual(self._field(created, "last_pid"), -1)
        self.assertEqual(self._field(created, "command"), draft_command)
        self.assertEqual(self._field(created, "tags"), {"dev": "dev"})

        persisted = self.jobs.get_status(job_id=target_job_id)
        self.assertIsNotNone(persisted)
        self.assertEqual(self._field(persisted, "status"), "stopped")
        self.assertEqual(self._field(persisted, "last_pid"), -1)

        running = self.jobs.list_status(filter={JobRecord.status: JobRecord.Status.c_running})
        self.assertNotIn(target_job_id, running)

    def test_jobs_create_draft_via_service_interface_rejects_duplicate(self):
        target_job_id = self._new_id("svc_create_dup")
        self.active_jobs.add(target_job_id)
        cmd = [sys.executable, "-u", "-c", "print('x')"]

        first = JobsService.create_job(
            job_id=target_job_id,
            command=cmd,
            db_path=self.db_path,
        )
        self.assertTrue(self._field(first, "ok"))

        second = JobsService.create_job(
            job_id=target_job_id,
            command=cmd,
            db_path=self.db_path,
        )
        self.assertFalse(self._field(second, "ok"))
        self.assertIn("job already exists", str(self._field(second, "error")))

    def test_jobs_create_draft_via_service_interface_rejects_string_command(self):
        target_job_id = self._new_id("svc_create_str_cmd")
        self.active_jobs.add(target_job_id)

        res = JobsService.create_job(
            job_id=target_job_id,
            command="echo should_fail",
            db_path=self.db_path,
        )
        self.assertFalse(self._field(res, "ok"))
        self.assertIn("list-style", str(self._field(res, "error")))

    def test_schedules_set_list_remove_via_nj_interface(self):
        schedule_id = self._new_id("schedule_cfg")
        schedule_job_id = self._new_id("schedule_job")
        invoke_set = self._new_id("nj_cli_invoke_set")
        invoke_list_1 = self._new_id("nj_cli_invoke_list_1")
        invoke_remove = self._new_id("nj_cli_invoke_remove")
        invoke_list_2 = self._new_id("nj_cli_invoke_list_2")
        self.active_jobs.update({invoke_set, invoke_list_1, invoke_remove, invoke_list_2})
        self._seed_job_launch_spec(schedule_job_id, self._simple_command("sched-set"))

        set_args = [
            "nj",
            "service=schedules",
            "op=set",
            f"db_path={self.db_path}",
            f"schedule.schedule_id={schedule_id}",
            f"schedule.job_id={schedule_job_id}",
            f"schedule.next_run_at={self._future_iso()}",
            "schedule.enabled=true",
        ]
        set_res = CliViaJobs.run_cli_through_jobs(
            jobs=self.jobs,
            script_path=self.nj_script,
            cli_args=set_args,
            invoke_job_id=invoke_set,
        )
        self.assertTrue(set_res["parsed"]["ok"])
        self.assertEqual(set_res["parsed"]["method"], "set")

        list_1_res = CliViaJobs.run_cli_through_jobs(
            jobs=self.jobs,
            script_path=self.nj_script,
            cli_args=["nj", "service=schedules", "op=list", f"db_path={self.db_path}"],
            invoke_job_id=invoke_list_1,
        )
        self.assertTrue(list_1_res["parsed"]["ok"])
        self.assertEqual(list_1_res["parsed"]["method"], "list")
        self.assertIn(schedule_id, list_1_res["stdout"])

        remove_res = CliViaJobs.run_cli_through_jobs(
            jobs=self.jobs,
            script_path=self.nj_script,
            cli_args=["nj", "service=schedules", "op=remove", f"db_path={self.db_path}", f"schedule_id={schedule_id}"],
            invoke_job_id=invoke_remove,
        )
        self.assertTrue(remove_res["parsed"]["ok"])
        self.assertEqual(remove_res["parsed"]["method"], "remove")

        list_2_res = CliViaJobs.run_cli_through_jobs(
            jobs=self.jobs,
            script_path=self.nj_script,
            cli_args=["nj", "service=schedules", "op=list", f"db_path={self.db_path}"],
            invoke_job_id=invoke_list_2,
        )
        self.assertTrue(list_2_res["parsed"]["ok"])
        self.assertEqual(list_2_res["parsed"]["method"], "list")
        self.assertNotIn(schedule_id, list_2_res["stdout"])

    def test_key_scoped_json_ref(self):
        schedule_id = self._new_id("schedule_keyscoped")
        schedule_job_id = self._new_id("schedule_job_keyscoped")
        self._seed_job_launch_spec(schedule_job_id, self._simple_command("key-scoped"))
        sched_payload = {
            "schedule_id": schedule_id,
            "job_id": schedule_job_id,
            "next_run_at": self._future_iso(),
            "enabled": True,
        }
        sched_path = self._write_json("schedule_payload.json", sched_payload)
        invoke_id = self._new_id("nj_cli_invoke_key_scoped")
        self.active_jobs.add(invoke_id)

        res = CliViaJobs.run_cli_through_jobs(
            jobs=self.jobs,
            script_path=self.nj_script,
            cli_args=[
                "nj",
                "service=schedules",
                "op=set",
                f"db_path={self.db_path}",
                f"schedule=[[{sched_path}]]",
            ],
            invoke_job_id=invoke_id,
        )
        dprint(f"PROOF key_scoped_res: ok={res['parsed']['ok']!r} method={res['parsed']['method']!r}")
        self.assertTrue(res["parsed"]["ok"])
        self.assertEqual(res["parsed"]["method"], "set")

        schedules = self.jobs.list_schedules()
        self.assertIn(schedule_id, schedules)

    def test_whole_arg_json_merge_ref_precedence(self):
        schedule_id = self._new_id("schedule_bulk")
        schedule_job_id = self._new_id("schedule_job_bulk")
        self._seed_job_launch_spec(schedule_job_id, self._simple_command("bulk"))
        bulk_payload = {
            "service": "schedules",
            "op": "set",
            "db_path": self.db_path,
            "schedule": {
                "schedule_id": schedule_id,
                "job_id": schedule_job_id,
                "next_run_at": self._future_iso(),
                "enabled": True,
            },
        }
        bulk_path = self._write_json("nj_route_payload.json", bulk_payload)
        invoke_id = self._new_id("nj_cli_invoke_bulk_merge")
        self.active_jobs.add(invoke_id)
        override_db_path = os.path.join(self.tmp_root, "override_db_should_be_replaced")

        res = CliViaJobs.run_cli_through_jobs(
            jobs=self.jobs,
            script_path=self.nj_script,
            cli_args=[
                "nj",
                "service=jobs",
                "op=list",
                f"db_path={override_db_path}",
                f"[[bulk]]=[[{bulk_path}]]",
            ],
            invoke_job_id=invoke_id,
        )
        dprint(f"PROOF bulk_merge_res: ok={res['parsed']['ok']!r} method={res['parsed']['method']!r}")
        self.assertTrue(res["parsed"]["ok"])
        self.assertEqual(res["parsed"]["method"], "set")

        schedules = self.jobs.list_schedules()
        self.assertIn(schedule_id, schedules, "Merged JSON should overwrite colliding service/op/db_path keys")

    def test_direct_build_command_dry_run(self):
        invoke_id = self._new_id("nj_cli_invoke_build_direct")
        self.active_jobs.add(invoke_id)
        res = CliViaJobs.run_cli_through_jobs(
            jobs=self.jobs,
            script_path=self.nj_script,
            cli_args=["build", "dry_run=true"],
            invoke_job_id=invoke_id,
        )
        status = CliViaJobs._field(res["terminal"], "status")
        dprint(
            f"PROOF build_direct_res: status={status!r} ok={res['parsed']['ok']!r} "
            f"method={res['parsed']['method']!r}"
        )
        self.assertEqual(status, "finished")
        self.assertTrue(res["parsed"]["ok"])
        self.assertEqual(res["parsed"]["method"], "build")
        self.assertIn("Nodejobs.app", res["stdout"])

    def test_admin_build_route_dry_run(self):
        invoke_id = self._new_id("nj_cli_invoke_build_admin_route")
        self.active_jobs.add(invoke_id)
        res = CliViaJobs.run_cli_through_jobs(
            jobs=self.jobs,
            script_path=self.nj_script,
            cli_args=["nj", "service=admin", "op=build", "dry_run=true"],
            invoke_job_id=invoke_id,
        )
        self.assertEqual(CliViaJobs._field(res["terminal"], "status"), "finished")
        self.assertTrue(res["parsed"]["ok"])
        self.assertEqual(res["parsed"]["method"], "build")
        self.assertIn("Nodejobs.app", res["stdout"])

    def test_router_unknown_service_returns_error_envelope(self):
        invoke_id = self._new_id("nj_cli_invoke_unknown_service")
        self.active_jobs.add(invoke_id)
        res = CliViaJobs.run_cli_through_jobs(
            jobs=self.jobs,
            script_path=self.nj_script,
            cli_args=["nj", "service=unknown_service", "op=list"],
            invoke_job_id=invoke_id,
        )
        dprint(f"PROOF unknown_service_res: ok={res['parsed']['ok']!r} error={res['parsed']['error']!r}")
        self.assertFalse(res["parsed"]["ok"])
        self.assertIn("Unknown service", str(res["parsed"]["error"]))

    def test_router_missing_required_arg_raises(self):
        invoke_id = self._new_id("nj_cli_invoke_missing_required")
        self.active_jobs.add(invoke_id)
        res = CliViaJobs.run_cli_through_jobs(
            jobs=self.jobs,
            script_path=self.nj_script,
            cli_args=["nj", "service=jobs"],
            invoke_job_id=invoke_id,
        )
        status = CliViaJobs._field(res["terminal"], "status")
        dprint(f"PROOF missing_required_res: status={status!r} stderr={res['stderr']!r}")
        self.assertIn(status, ("failed_start", "failed"))
        self.assertIn("Missing required argument: op", res["stderr"])


if __name__ == "__main__":
    unittest.main()
