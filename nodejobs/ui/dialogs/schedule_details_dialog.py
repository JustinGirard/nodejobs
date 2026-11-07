import json

from PyQt6.QtWidgets import QDialog, QDialogButtonBox, QPlainTextEdit, QVBoxLayout

from ..types.ui_rows import UiScheduleRow


class ScheduleDetailsDialog(QDialog):
    def __init__(self, record: UiScheduleRow | dict, parent=None):
        super().__init__(parent)
        payload = self._prioritize_fields(self._to_mapping(record))
        schedule_id = str(payload.get("id") or payload.get("schedule_id") or "").strip()
        self.setWindowTitle(
            f"Schedule Details: {schedule_id}" if schedule_id else "Schedule Details"
        )
        self.resize(700, 440)

        layout = QVBoxLayout(self)
        details = QPlainTextEdit(self)
        details.setReadOnly(True)
        details.setPlainText(json.dumps(payload, indent=2, sort_keys=False, default=str))
        layout.addWidget(details)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, self)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _to_mapping(self, record: UiScheduleRow | dict) -> dict:
        if not record:
            return {}
        if isinstance(record, dict):
            return dict(record)
        try:
            return {str(k): record[k] for k in record.keys()}
        except Exception:
            try:
                return dict(record.items())
            except Exception:
                return {"value": str(record)}

    def _prioritize_fields(self, payload: dict) -> dict:
        payload = dict(payload)
        payload.pop("command", None)
        ordered = {}
        for key in (
            "envs",
            "id",
            "schedule_id",
            "job_id",
            "enabled",
            "next_run_at",
            "interval_days",
            "interval_hours",
            "interval_minutes",
            "interval_sec",
            "last_update",
            "last_error",
            "attempt_count",
        ):
            if key in payload:
                ordered[key] = payload[key]
        for key, value in payload.items():
            if key not in ordered:
                ordered[key] = value
        return ordered
