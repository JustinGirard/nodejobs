from ..types.ui_rows import UiJobRow


class ApiAdapter:
    def fetch_jobs(self) -> list[UiJobRow]:
        return [
            UiJobRow(
                {
                    UiJobRow.id: "job-101",
                    UiJobRow.status: "running",
                    UiJobRow.last_pid: 50001,
                    UiJobRow.last_update: "2026-07-15T11:01:00Z",
                }
            ),
            UiJobRow(
                {
                    UiJobRow.id: "job-102",
                    UiJobRow.status: "stopped",
                    UiJobRow.last_pid: 49987,
                    UiJobRow.last_update: "2026-07-15T10:54:22Z",
                }
            ),
        ]

    def fetch_jobs_with_error(self) -> list[UiJobRow]:
        raise RuntimeError("simulated adapter failure")
