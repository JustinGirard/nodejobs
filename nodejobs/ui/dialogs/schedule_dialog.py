import json

from PyQt6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLineEdit,
)

from ..components import DateTimePicker
from ..types.ui_rows import UiScheduleRow


class ScheduleDialog(QDialog):
    def __init__(self, record, parent=None, job_id_choices: list[str] | None = None):
        super().__init__(parent)
        self.setWindowTitle("Schedule")
        self._record = record or {}

        def _read(key, default=None):
            if isinstance(self._record, dict):
                return self._record.get(key, default)
            try:
                return self._record[key]
            except Exception:
                return getattr(self._record, key, default)

        layout = QFormLayout(self)
        self.schedule_id_input = QLineEdit(str(_read(UiScheduleRow.id, "") or ""), self)
        current_job_id = str(_read(UiScheduleRow.job_id, "") or "").strip()
        self.job_id_input = QComboBox(self)
        self.job_id_input.setEditable(True)
        choices = []
        for choice in (job_id_choices or []):
            text = str(choice or "").strip()
            if not text or text in choices:
                continue
            choices.append(text)
        self.job_id_input.addItems(choices)
        if current_job_id and current_job_id not in choices:
            self.job_id_input.addItem(current_job_id)
        self.job_id_input.setCurrentText(current_job_id)

        self.next_run_picker = DateTimePicker(self)
        self.next_run_picker.set_iso_text(str(_read(UiScheduleRow.next_run_at, "") or ""))

        interval_val = _read(UiScheduleRow.interval_sec, "")
        interval_text = ""
        if interval_val not in (None, 0, ""):
            interval_text = str(interval_val)
        self.interval_input = QLineEdit(interval_text, self)

        interval_days_val = _read(UiScheduleRow.interval_days, "")
        interval_days_text = ""
        if interval_days_val not in (None, 0, ""):
            interval_days_text = str(interval_days_val)
        self.interval_days_input = QLineEdit(interval_days_text, self)

        interval_hours_val = _read(UiScheduleRow.interval_hours, "")
        interval_hours_text = ""
        if interval_hours_val not in (None, 0, ""):
            interval_hours_text = str(interval_hours_val)
        self.interval_hours_input = QLineEdit(interval_hours_text, self)

        interval_minutes_val = _read(UiScheduleRow.interval_minutes, "")
        interval_minutes_text = ""
        if interval_minutes_val not in (None, 0, ""):
            interval_minutes_text = str(interval_minutes_val)
        self.interval_minutes_input = QLineEdit(interval_minutes_text, self)

        cwd_value = _read(UiScheduleRow.cwd, "")
        self.cwd_input = QLineEdit(str(cwd_value or ""), self)

        envs_value = _read(UiScheduleRow.envs, {})
        if isinstance(envs_value, dict):
            envs_text = json.dumps(envs_value, separators=(",", ":"))
        else:
            envs_text = str(envs_value or "")
        self.envs_input = QLineEdit(envs_text, self)

        tags_value = _read(UiScheduleRow.tags, {})
        if isinstance(tags_value, dict):
            tags_text = json.dumps(tags_value, separators=(",", ":"))
        else:
            tags_text = "{}"
        self.tags_input = QLineEdit(tags_text, self)

        self.enabled_input = QCheckBox(self)
        self.enabled_input.setChecked(bool(_read(UiScheduleRow.enabled, True)))

        layout.addRow("schedule_id", self.schedule_id_input)
        layout.addRow("job_id", self.job_id_input)
        layout.addRow("next_run_at", self.next_run_picker)
        layout.addRow("interval_days", self.interval_days_input)
        layout.addRow("interval_hours", self.interval_hours_input)
        layout.addRow("interval_minutes", self.interval_minutes_input)
        layout.addRow("interval_sec", self.interval_input)
        layout.addRow("cwd", self.cwd_input)
        layout.addRow("envs_json", self.envs_input)
        layout.addRow("tags_json", self.tags_input)
        layout.addRow("enabled", self.enabled_input)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel,
            self,
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addRow(buttons)

    def get_payload(self):
        interval_text = str(self.interval_input.text() or "").strip()
        interval_days_text = str(self.interval_days_input.text() or "").strip()
        interval_hours_text = str(self.interval_hours_input.text() or "").strip()
        interval_minutes_text = str(self.interval_minutes_input.text() or "").strip()
        cwd_text = str(self.cwd_input.text() or "").strip()
        envs_text = str(self.envs_input.text() or "").strip()
        tags_text = str(self.tags_input.text() or "").strip()
        return {
            UiScheduleRow.id: str(self.schedule_id_input.text() or "").strip(),
            UiScheduleRow.job_id: str(self.job_id_input.currentText() or "").strip(),
            UiScheduleRow.next_run_at: self.next_run_picker.iso_text(),
            UiScheduleRow.interval_days: interval_days_text if interval_days_text else "",
            UiScheduleRow.interval_hours: interval_hours_text if interval_hours_text else "",
            UiScheduleRow.interval_minutes: interval_minutes_text if interval_minutes_text else "",
            UiScheduleRow.interval_sec: interval_text if interval_text else "",
            UiScheduleRow.cwd: cwd_text if cwd_text else "",
            UiScheduleRow.envs: envs_text if envs_text else "",
            UiScheduleRow.tags: tags_text if tags_text else "{}",
            UiScheduleRow.enabled: bool(self.enabled_input.isChecked()),
        }
