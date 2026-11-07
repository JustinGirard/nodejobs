from nodejobs.dependencies.BaseData import BaseData
from nodejobs.dependencies.BaseService import BaseService
from nodejobs.jobs import Jobs


class JobsServiceResponse(BaseData):
    ok: bool
    method: str
    result: (object, None)
    error: (str, None)


class JobsService(BaseService):
    @classmethod
    def get_command_map(cls):
        return {
            "run": {"required_args": ["job_id", "command"], "method": cls.run_job},
            "create": {"required_args": ["job_id", "command"], "method": cls.create_job},
            "stop": {"required_args": ["job_id"], "method": cls.stop_job},
            "status": {"required_args": ["job_id"], "method": cls.status_job},
            "list": {"required_args": [], "method": cls.list_jobs},
            "logs": {"required_args": ["job_id"], "method": cls.logs_job},
        }

    @classmethod
    def _jobs(cls, db_path=None, verbose=False):
        return Jobs(db_path=db_path, verbose=bool(verbose))

    @classmethod
    def _ok(cls, method: str, result):
        return JobsServiceResponse({"ok": True, "method": method, "result": result, "error": None})

    @classmethod
    def _err(cls, method: str, error: Exception):
        return JobsServiceResponse({"ok": False, "method": method, "result": None, "error": str(error)})

    @classmethod
    def create_job(cls, job_id, command, cwd=None, envs=None, tags=None, db_path=None, verbose=False, **kwargs):
        try:
            if isinstance(command, str):
                raise ValueError("create.command must be list-style; use command.1=... or command=[[file.json]]")
            jobs_manager = cls._jobs(db_path=db_path, verbose=verbose)
            draft_id = str(job_id or "").strip()
            result = jobs_manager.create_job(command=command, job_id=draft_id, cwd=cwd, envs=envs, tags=tags)
            return cls._ok("create", result)
        except Exception as e:
            return cls._err("create", e)

    @classmethod
    def run_job(cls, job_id, command, cwd=None, envs=None, tags=None, db_path=None, verbose=False, **kwargs):
        try:
            if isinstance(command, str):
                raise ValueError("run.command must be list-style; use command.1=... or command=[[file.json]]")
            result = cls._jobs(db_path=db_path, verbose=verbose).run(
                command=command, job_id=job_id, cwd=cwd, envs=envs, tags=tags
            )
            return cls._ok("run", result)
        except Exception as e:
            return cls._err("run", e)

    @classmethod
    def stop_job(cls, job_id, wait_time=1, db_path=None, verbose=False, **kwargs):
        try:
            result = cls._jobs(db_path=db_path, verbose=verbose).stop(
                job_id=job_id, wait_time=int(wait_time)
            )
            return cls._ok("stop", result)
        except Exception as e:
            return cls._err("stop", e)

    @classmethod
    def status_job(cls, job_id, db_path=None, verbose=False, **kwargs):
        try:
            result = cls._jobs(db_path=db_path, verbose=verbose).get_status(job_id=job_id)
            return cls._ok("status", result)
        except Exception as e:
            return cls._err("status", e)

    @classmethod
    def list_jobs(cls, filter=None, db_path=None, verbose=False, **kwargs):
        try:
            result = cls._jobs(db_path=db_path, verbose=verbose).list_status(filter=filter)
            return cls._ok("list", result)
        except Exception as e:
            return cls._err("list", e)

    @classmethod
    def logs_job(cls, job_id, db_path=None, verbose=False, **kwargs):
        try:
            result = cls._jobs(db_path=db_path, verbose=verbose).job_logs(job_id=job_id)
            return cls._ok("logs", result)
        except Exception as e:
            return cls._err("logs", e)


if __name__ == "__main__":
    JobsService.run_cli()
