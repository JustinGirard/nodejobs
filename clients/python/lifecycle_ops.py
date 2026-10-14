import argparse
import os
import json
import socket
import time

from nodejobs.jobs import Jobs, JobRecord


def _start(db_path: str, job_id: str, command_json: str, cwd: str | None):
    command = json.loads(command_json)
    jobs = Jobs(db_path=db_path, verbose=False)
    jobs.stop(job_id=job_id, wait_time=1)
    rec = jobs.run(command=command, job_id=job_id, cwd=cwd)
    return {"status": rec.status, "job_id": job_id}


def _wait(db_path: str, job_id: str, host: str, port: int, timeout_s: float):
    jobs = Jobs(db_path=db_path, verbose=False)
    deadline = time.time() + max(0.1, float(timeout_s))
    while time.time() < deadline:
        rec = jobs.get_status(job_id)
        st = rec.status if rec is not None else None
        if st == JobRecord.Status.c_running:
            break
        if st in (JobRecord.Status.c_failed, JobRecord.Status.c_failed_start):
            out, err = jobs.job_logs(job_id)
            raise Exception(f"server failed start: status={st}\nstdout:\n{out}\nstderr:\n{err}")
        time.sleep(0.05)

    rec = jobs.get_status(job_id)
    st = rec.status if rec is not None else None
    if st != JobRecord.Status.c_running:
        out, err = jobs.job_logs(job_id)
        raise Exception(f"server not running: status={st}\nstdout:\n{out}\nstderr:\n{err}")

    while time.time() < deadline:
        try:
            s = socket.create_connection((host, int(port)), timeout=0.2)
            s.close()
            return {"status": "ok", "job_id": job_id}
        except OSError:
            time.sleep(0.05)
    raise Exception("server socket did not become ready in time")


def _stop(db_path: str, job_id: str, wait_time: int):
    jobs = Jobs(db_path=db_path, verbose=False)
    jobs.stop(job_id=job_id, wait_time=wait_time)
    return {"status": "ok", "job_id": job_id}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", required=True, choices=["start", "wait", "stop"])
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--job-id")
    parser.add_argument("--job-id-env", default="NODEJOBS_LIFECYCLE_JOB_ID")
    parser.add_argument("--cwd")
    parser.add_argument("--command-json")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--timeout-s", type=float)
    parser.add_argument("--wait-time", type=int, default=1)
    args = parser.parse_args()

    resolved_job_id = args.job_id
    if args.action == "start":
        if resolved_job_id is None:
            resolved_job_id = os.environ.get(args.job_id_env)
        if resolved_job_id is None:
            raise Exception("job_id_required_for_start")
    else:
        if resolved_job_id is None:
            raise Exception("job_id_required")

    if args.action == "start":
        if args.command_json is None:
            raise Exception("command_json_required")
        out = _start(args.db_path, resolved_job_id, args.command_json, args.cwd)
    elif args.action == "wait":
        if resolved_job_id is None:
            raise Exception("job_id_required")
        if args.host is None:
            raise Exception("host_required")
        if args.port is None:
            raise Exception("port_required")
        if args.timeout_s is None:
            raise Exception("timeout_s_required")
        out = _wait(args.db_path, resolved_job_id, args.host, args.port, args.timeout_s)
    else:
        out = _stop(args.db_path, resolved_job_id, args.wait_time)

    print(json.dumps(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
