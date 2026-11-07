from nodejobs.jobs import Jobs
from nodejobs.jobdb import ScheduleFilter

from ..types.ui_rows import UiScheduleRow
from .normalizers import normalize_schedule_row


class SchedulesAdapter:
    def __init__(self, jobs_manager):
        self.jobs_manager = jobs_manager

    @classmethod
    def from_db_path(cls, db_path: str):
        return cls(Jobs(db_path=db_path))

    def list_schedules(self, filter: dict | None = None) -> list[UiScheduleRow]:
        rows_by_id = self.jobs_manager.list_schedules(filter=filter)
        return [normalize_schedule_row(raw) for raw in rows_by_id.values()]

    def get_schedule(self, schedule_id: str) -> UiScheduleRow:
        rows_by_id = self.jobs_manager.list_schedules(
            filter={ScheduleFilter.schedule_id: schedule_id}
        )
        if schedule_id not in rows_by_id:
            raise ValueError(f"schedule not found: {schedule_id}")
        return normalize_schedule_row(rows_by_id[schedule_id])

    def list_job_ids(self) -> list[str]:
        rows_by_id = self.jobs_manager.list_status(filter={})
        return sorted({str(job_id).strip() for job_id in rows_by_id.keys() if str(job_id).strip()})

    def set_schedule(self, schedule: dict):
        return self.jobs_manager.set_schedule(schedule)

    def remove_schedule(self, schedule_id: str):
        return self.jobs_manager.remove_schedule(schedule_id)
