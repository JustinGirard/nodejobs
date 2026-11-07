import json
import os

from nodejobs.jobdb import JobFilter, JobRecord
from nodejobs.jobs import Jobs

from ..types.ui_rows import UiJobRow
from .normalizers import normalize_job_row


class JobsAdapter:
    def __init__(self, jobs_manager):
        self.jobs_manager = jobs_manager

    @classmethod
    def from_db_path(cls, db_path: str):
        return cls(Jobs(db_path=db_path))

    def list_jobs(self, filter: dict | None = None) -> list[UiJobRow]:
        rows_by_id = self.jobs_manager.list_status(filter=filter)
        rows = []
        for raw in rows_by_id.values():
            payload = self._to_payload(raw)
            payload = self._with_command_fallback(payload)
            rows.append(normalize_job_row(payload))
        return rows

    def stop_job(self, job_id: str, wait_time: int = 1):
        return self.jobs_manager.stop(job_id=job_id, wait_time=wait_time)

    def run_job(
        self,
        command: list[str],
        job_id: str,
        cwd: str | None = None,
        envs: dict | None = None,
        tags: dict | None = None,
    ):
        return self.jobs_manager.run(command=command, job_id=job_id, cwd=cwd, envs=envs, tags=tags)

    def delete_job(self, job_id: str):
        return self.jobs_manager.delete_job(job_id=job_id)

    def create_job_draft(
        self,
        command: list[str],
        job_id: str,
        cwd: str | None = None,
        envs: dict | None = None,
        tags: dict | None = None,
    ):
        draft_id = str(job_id or "").strip()
        if not draft_id:
            raise ValueError("job_id is required")
        return self.jobs_manager.create_job(
            command=command, job_id=draft_id, cwd=cwd, envs=envs, tags=tags
        )

    def upsert_job_draft(
        self,
        command: list[str],
        job_id: str,
        cwd: str | None = None,
        envs: dict | None = None,
        tags: dict | None = None,
        status: str | None = None,
    ):
        draft_id = str(job_id or "").strip()
        if not draft_id:
            raise ValueError("job_id is required")
        return self._save_job_draft(
            command=command,
            job_id=draft_id,
            cwd=cwd,
            envs=envs,
            tags=tags,
            status=status,
        )

    def _save_job_draft(
        self,
        command: list[str],
        job_id: str,
        cwd: str | None,
        envs: dict | None,
        tags: dict | None,
        status: str | None,
    ):
        draft_id = str(job_id or "").strip()
        command_parts = self._normalize_command(command)
        if len(command_parts) == 0:
            raise ValueError("command is required")

        cwd_value = str(cwd or "").strip() or os.getcwd()
        env_map = envs if isinstance(envs, dict) else {}
        existing_tags = None
        existing_rows = self.jobs_manager.jobdb.list_status(filter={JobFilter.self_id: draft_id})
        if draft_id in existing_rows:
            existing_tags = existing_rows[draft_id].get(JobRecord.tags)
        tags_map = self.jobs_manager.apply_tags_patch(existing_tags, tags)
        status_value = str(status or "").strip().lower() or JobRecord.Status.c_stopped
        logdir = os.path.join(self.jobs_manager.db_path, "job_logs")
        os.makedirs(logdir, exist_ok=True)

        record = JobRecord(
            {
                JobRecord.self_id: draft_id,
                JobRecord.last_pid: -1,
                JobRecord.dirname: draft_id,
                JobRecord.cwd: cwd_value,
                JobRecord.logdir: logdir,
                JobRecord.logfile: draft_id,
                JobRecord.command: command_parts,
                JobRecord.envs: env_map,
                JobRecord.tags: tags_map,
                JobRecord.status: status_value,
            }
        )
        self.jobs_manager.jobdb.update_status(record)
        return record

    def get_job_logs(self, job_id: str):
        return self.jobs_manager.job_logs(job_id=job_id)

    def get_job_record(self, job_id: str) -> dict:
        rec = self.jobs_manager.get_status(job_id=job_id)
        if rec is None:
            raise ValueError(f"job not found: {job_id}")
        payload = self._to_payload(rec)
        return self._with_command_fallback(payload)

    def _to_payload(self, rec) -> dict:
        if hasattr(rec, "to_safe_dict_LLMS_DONT_USE"):
            return rec.to_safe_dict_LLMS_DONT_USE()
        if isinstance(rec, dict):
            return dict(rec)
        return dict(rec)

    def _normalize_command(self, value) -> list[str]:
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            return [p for p in text.split() if p]
        return []

    def _with_command_fallback(self, payload: dict) -> dict:
        normalized_command = self._normalize_command(payload.get("command"))
        payload["command"] = normalized_command
        has_command = len(normalized_command) > 0
        has_envs = isinstance(payload.get("envs"), dict)
        if has_command and has_envs:
            return payload
        job_id = str(payload.get("self_id") or payload.get("id") or "").strip()
        logdir = str(payload.get("logdir") or "").strip()
        if not job_id or not logdir:
            if not isinstance(payload.get("envs"), dict):
                payload["envs"] = {}
            return payload
        spec_path = os.path.join(logdir, f"{job_id}.json")
        if not os.path.isfile(spec_path):
            if not isinstance(payload.get("envs"), dict):
                payload["envs"] = {}
            return payload
        try:
            with open(spec_path, "r") as f:
                spec = json.load(f)
        except Exception:
            if not isinstance(payload.get("envs"), dict):
                payload["envs"] = {}
            return payload
        if not has_command:
            spec_command = self._normalize_command(spec.get("command"))
            if len(spec_command) > 0:
                payload["command"] = spec_command
        if not has_envs:
            spec_envs = spec.get("envs")
            payload["envs"] = spec_envs if isinstance(spec_envs, dict) else {}
        if not isinstance(payload.get("envs"), dict):
            payload["envs"] = {}
        if not isinstance(payload.get("tags"), dict):
            payload["tags"] = {}
        return payload
