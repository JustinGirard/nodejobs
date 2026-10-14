from __future__ import annotations

import argparse
import os
import re
import sys
import shlex
import time
import xml.etree.ElementTree as ET
from pathlib import Path

from nodejobs.jobs import Jobs, JobRecord
from typing import Callable


def _run_capture_via_jobs(
    suite_name: str,
    command: list[str],
    cwd: Path,
    env: dict[str, str],
    timeout_s: float,
    jobs_db_path: Path,
    live_row_cb: Callable[[str], None] | None = None,
) -> tuple[int, str]:
    jobs = Jobs(db_path=str(jobs_db_path), verbose=False)
    job_id = f"__CLIENT_TEST_RUNNER__{suite_name}__{int(time.time() * 1000)}"
    try:
        jobs.run(command=command, job_id=job_id, cwd=str(cwd), envs=env)
    except Exception as exc:
        return 1, (
            f"[RUN_ERROR] suite={suite_name}\n"
            f"cwd={cwd}\n"
            f"command={' '.join(command)}\n"
            f"error={exc}"
        )
    deadline = time.time() + timeout_s
    terminal = {
        JobRecord.Status.c_finished,
        JobRecord.Status.c_finished_2,
        JobRecord.Status.c_failed,
        JobRecord.Status.c_failed_start,
        JobRecord.Status.c_failed_stop,
        JobRecord.Status.c_stopped,
    }
    timed_out = False
    last_out = ""
    last_err = ""
    out_buf = ""
    err_buf = ""

    def _feed_text(text: str, is_err: bool) -> None:
        nonlocal out_buf, err_buf
        if live_row_cb is None or text == "":
            return
        if is_err:
            err_buf = err_buf + text
            buf = err_buf
        else:
            out_buf = out_buf + text
            buf = out_buf
        parts = buf.splitlines(keepends=True)
        keep = ""
        for part in parts:
            if part.endswith("\n"):
                line = part.rstrip("\n")
                live_row_cb(line)
            else:
                keep = part
        if is_err:
            err_buf = keep
        else:
            out_buf = keep

    while True:
        out_now, err_now = jobs.job_logs(job_id)
        out_now = out_now or ""
        err_now = err_now or ""
        if len(out_now) >= len(last_out):
            delta_out = out_now[len(last_out):]
        else:
            delta_out = out_now
        if len(err_now) >= len(last_err):
            delta_err = err_now[len(last_err):]
        else:
            delta_err = err_now
        _feed_text(delta_out, is_err=False)
        _feed_text(delta_err, is_err=True)
        last_out = out_now
        last_err = err_now

        rec = jobs.get_status(job_id)
        status = rec.status if rec is not None else None
        if status in terminal:
            break
        if time.time() >= deadline:
            timed_out = True
            jobs.stop(job_id=job_id, wait_time=1)
            break
        time.sleep(0.1)

    # Flush partial last lines
    if live_row_cb is not None:
        if out_buf:
            live_row_cb(out_buf)
        if err_buf:
            live_row_cb(err_buf)

    out, err = jobs.job_logs(job_id)
    merged = ((out or "") + ("\n" + err if err else "")).strip()
    if timed_out:
        merged = (
            f"[TIMEOUT] suite={suite_name} exceeded {timeout_s}s\n"
            f"cwd={cwd}\n"
            f"command={' '.join(command)}\n"
            f"{merged}"
        ).strip()
        return 124, merged
    rec = jobs.get_status(job_id)
    status = rec.status if rec is not None else None
    if status in (JobRecord.Status.c_finished, JobRecord.Status.c_finished_2):
        return 0, merged
    return 1, merged


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _parse_pytest_junit(junit_path: Path) -> list[dict]:
    rows: list[dict] = []
    if not junit_path.exists():
        return rows
    root = ET.parse(junit_path).getroot()
    for tc in root.findall(".//testcase"):
        name = tc.attrib.get("classname", "")
        test_name = tc.attrib.get("name", "")
        full_name = f"{name}::{test_name}" if name else test_name
        failure = tc.find("failure")
        error = tc.find("error")
        skipped = tc.find("skipped")
        if failure is not None:
            rows.append({"name": full_name, "status": "FAIL", "dump": (failure.text or "").strip()})
        elif error is not None:
            rows.append({"name": full_name, "status": "FAIL", "dump": (error.text or "").strip()})
        elif skipped is not None:
            rows.append({"name": full_name, "status": "SKIP", "dump": (skipped.text or "").strip()})
        else:
            rows.append({"name": full_name, "status": "ok", "dump": ""})
    return rows


def _parse_node_tap(tap_text: str) -> list[dict]:
    rows: list[dict] = []
    cur_fail: dict | None = None
    ok_pat = re.compile(r"^ok\s+\d+\s+-\s+(.*)$")
    not_ok_pat = re.compile(r"^not ok\s+\d+\s+-\s+(.*)$")
    for raw in tap_text.splitlines():
        line = raw.rstrip("\\n")
        m_ok = ok_pat.match(line)
        if m_ok:
            rows.append({"name": m_ok.group(1).strip(), "status": "ok", "dump": ""})
            cur_fail = None
            continue
        m_not_ok = not_ok_pat.match(line)
        if m_not_ok:
            cur_fail = {"name": m_not_ok.group(1).strip(), "status": "FAIL", "dump": ""}
            rows.append(cur_fail)
            continue
        if cur_fail is not None and line.startswith("  "):
            cur_fail["dump"] = (cur_fail["dump"] + "\n" + line).strip()
    return rows


def _parse_trx(trx_path: Path) -> list[dict]:
    rows: list[dict] = []
    if not trx_path.exists():
        return rows
    root = ET.parse(trx_path).getroot()
    ns = {"t": "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"}
    for ut in root.findall(".//t:UnitTestResult", ns):
        name = ut.attrib.get("testName", "")
        outcome = (ut.attrib.get("outcome", "") or "").lower()
        if outcome == "passed":
            rows.append({"name": name, "status": "ok", "dump": ""})
            continue
        if outcome == "notexecuted":
            rows.append({"name": name, "status": "SKIP", "dump": ""})
            continue
        err = ut.find("t:Output/t:ErrorInfo", ns)
        msg = ""
        if err is not None:
            msg_text = err.findtext("t:Message", default="", namespaces=ns)
            stack = err.findtext("t:StackTrace", default="", namespaces=ns)
            msg = (msg_text + "\n" + stack).strip()
        rows.append({"name": name, "status": "FAIL", "dump": msg})
    return rows


def _mk_live_row_printer(title: str, show_proof: bool = False) -> Callable[[str], None]:
    seen: set[tuple[str, str]] = set()
    py_pat = re.compile(r"^(\S+::\S+)\s+(PASSED|FAILED|ERROR|SKIPPED|XFAIL|XPASS)\b")
    js_ok = re.compile(r"^ok\s+\d+\s+-\s+(.*)$")
    js_not_ok = re.compile(r"^not ok\s+\d+\s+-\s+(.*)$")
    cs_pat = re.compile(r"^\s*(Passed|Failed|Skipped)\s+(.+?)(?:\s+\[[^\]]+\])?\s*$")

    def _emit(name: str, status: str) -> None:
        key = (name, status)
        if key in seen:
            return
        seen.add(key)
        print(f"  {name} - {status}")

    def _cb(line: str) -> None:
        stripped = line.strip()
        if show_proof and "PROOF " in stripped:
            print(f"  {stripped}")
            return
        m_py = py_pat.match(stripped)
        if m_py:
            st_map = {"PASSED": "ok", "FAILED": "FAIL", "ERROR": "FAIL", "SKIPPED": "SKIP", "XFAIL": "SKIP", "XPASS": "ok"}
            _emit(m_py.group(1), st_map[m_py.group(2)])
            return
        m_js_ok = js_ok.match(line.strip())
        if m_js_ok:
            _emit(m_js_ok.group(1).strip(), "ok")
            return
        m_js_not_ok = js_not_ok.match(line.strip())
        if m_js_not_ok:
            _emit(m_js_not_ok.group(1).strip(), "FAIL")
            return
        m_cs = cs_pat.match(line.strip())
        if m_cs:
            st = m_cs.group(1).lower()
            st_map = {"passed": "ok", "failed": "FAIL", "skipped": "SKIP"}
            _emit(m_cs.group(2).strip(), st_map[st])
            return

    return _cb


def _print_suite(title: str, rows: list[dict]) -> None:
    print(f"{title}:")
    if not rows:
        print("  (no tests discovered)")
        return
    for row in rows:
        print(f"  {row['name']} - {row['status']}")
        if row["status"] == "FAIL" and row["dump"]:
            print("  TRACEBACK / DUMP")
            for ln in row["dump"].splitlines():
                print(f"    {ln}")


def _suite_failed(rows: list[dict], exit_code: int) -> bool:
    if exit_code != 0:
        return True
    for row in rows:
        if row["status"] == "FAIL":
            return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    verbose = bool(args.verbose)
    out_dir = repo_root / "test-results" / "client-tests"
    out_dir.mkdir(parents=True, exist_ok=True)
    runner_jobs_db = out_dir / "runner_jobs_db"

    js_dir = repo_root / "clients" / "javascript"
    cs_dir = repo_root / "clients" / "csharp" / "Nodejobs.Client.Tests"

    py_junit = repo_root / "test-results" / "pytest-client.xml"
    cs_trx = cs_dir / "test-results" / "csharp-client.trx"

    if py_junit.exists():
        py_junit.unlink()
    if cs_trx.exists():
        cs_trx.unlink()

    py_cmd = [sys.executable, "-m", "pytest", "tests/client", "--junitxml=test-results/pytest-client.xml"]
    if verbose:
        py_cmd.extend(["-vv", "-s", "-rA"])
    else:
        py_cmd.append("-q")

    py_env = os.environ.copy()
    if verbose:
        py_env["CLIENT_TEST_VERBOSE"] = "1"
    print("Python:")
    try:
        py_code, py_out = _run_capture_via_jobs(
            suite_name="python",
            command=py_cmd,
            cwd=repo_root,
            env=py_env,
            timeout_s=240.0,
            jobs_db_path=runner_jobs_db,
            live_row_cb=_mk_live_row_printer("Python", show_proof=verbose),
        )
    except KeyboardInterrupt:
        print("\nAGGREGATE - FAIL (interrupted)")
        print(f"Logs: {out_dir}")
        return 130
    _write_text(out_dir / "python.log", py_out)
    py_rows = _parse_pytest_junit(py_junit)

    child_env = os.environ.copy()
    child_env["PYTHON"] = sys.executable
    if verbose:
        child_env["CLIENT_TEST_VERBOSE"] = "1"

    print("JS:")
    try:
        js_code, js_out = _run_capture_via_jobs(
            suite_name="javascript",
            command=["node", "--test", "--test-reporter=tap", "test/client.test.js", "test/client.lifecycle.test.js"],
            cwd=js_dir,
            env=child_env,
            timeout_s=180.0,
            jobs_db_path=runner_jobs_db,
            live_row_cb=_mk_live_row_printer("JS", show_proof=verbose),
        )
    except KeyboardInterrupt:
        print("\nAGGREGATE - FAIL (interrupted)")
        print(f"Logs: {out_dir}")
        return 130
    _write_text(out_dir / "javascript.log", js_out)
    js_rows = _parse_node_tap(js_out)

    print("C#:")
    cs_cmd = ["dotnet", "test", "--logger", "trx;LogFileName=csharp-client.trx", "--results-directory", "test-results"]
    if verbose:
        cs_cmd.extend(["--logger", "console;verbosity=detailed"])
    else:
        cs_cmd.extend(["--logger", "console;verbosity=normal"])
    try:
        cs_code, cs_out = _run_capture_via_jobs(
            suite_name="csharp",
            command=cs_cmd,
            cwd=cs_dir,
            env=child_env,
            timeout_s=240.0,
            jobs_db_path=runner_jobs_db,
            live_row_cb=_mk_live_row_printer("C#", show_proof=verbose),
        )
    except KeyboardInterrupt:
        print("\nAGGREGATE - FAIL (interrupted)")
        print(f"Logs: {out_dir}")
        return 130
    _write_text(out_dir / "csharp.log", cs_out)
    cs_rows = _parse_trx(cs_trx)

    _print_suite("Python (final)", py_rows)
    _print_suite("JS (final)", js_rows)
    _print_suite("C# (final)", cs_rows)

    failed = 0
    failed += 1 if _suite_failed(py_rows, py_code) else 0
    failed += 1 if _suite_failed(js_rows, js_code) else 0
    failed += 1 if _suite_failed(cs_rows, cs_code) else 0

    print("")
    if failed:
        print(f"AGGREGATE - FAIL ({failed} suite(s) failed)")
        print(f"Logs: {out_dir}")
        return 1

    print("AGGREGATE - ok")
    print(f"Logs: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
