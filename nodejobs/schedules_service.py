from nodejobs.dependencies.BaseData import BaseData
from nodejobs.dependencies.BaseService import BaseService
from nodejobs.jobs import Jobs


class SchedulesServiceResponse(BaseData):
    ok: bool
    method: str
    result: (object, None)
    error: (str, None)


class SchedulesService(BaseService):
    @classmethod
    def get_command_map(cls):
        return {
            "set": {"required_args": ["schedule"], "method": cls.set_schedule_cmd},
            "list": {"required_args": [], "method": cls.list_schedules_cmd},
            "remove": {"required_args": ["schedule_id"], "method": cls.remove_schedule_cmd},
        }

    @classmethod
    def _jobs(cls, db_path=None, verbose=False):
        return Jobs(db_path=db_path, verbose=bool(verbose))

    @classmethod
    def _ok(cls, method: str, result):
        return SchedulesServiceResponse({"ok": True, "method": method, "result": result, "error": None})

    @classmethod
    def _err(cls, method: str, error: Exception):
        return SchedulesServiceResponse({"ok": False, "method": method, "result": None, "error": str(error)})

    @classmethod
    def set_schedule_cmd(cls, schedule, db_path=None, verbose=False, **kwargs):
        try:
            result = cls._jobs(db_path=db_path, verbose=verbose).set_schedule(schedule=schedule)
            return cls._ok("set", result)
        except Exception as e:
            return cls._err("set", e)

    @classmethod
    def list_schedules_cmd(cls, filter=None, db_path=None, verbose=False, **kwargs):
        try:
            result = cls._jobs(db_path=db_path, verbose=verbose).list_schedules(filter=filter)
            return cls._ok("list", result)
        except Exception as e:
            return cls._err("list", e)

    @classmethod
    def remove_schedule_cmd(cls, schedule_id, db_path=None, verbose=False, **kwargs):
        try:
            result = cls._jobs(db_path=db_path, verbose=verbose).remove_schedule(schedule_id=schedule_id)
            return cls._ok("remove", result)
        except Exception as e:
            return cls._err("remove", e)


if __name__ == "__main__":
    SchedulesService.run_cli()
