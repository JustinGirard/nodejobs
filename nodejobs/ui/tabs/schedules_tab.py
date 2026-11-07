import datetime as dt
import json

from PyQt6.QtCore import QSettings, QTimer
from PyQt6.QtWidgets import QHBoxLayout, QLabel, QPushButton, QVBoxLayout, QWidget

from nodejobs.jobdb import ScheduleRecord

from ..adapters import SchedulesAdapter
from ..components import (
    ActionToolbar,
    NotificationCenter,
    SearchFilterBar,
    TableViewPanel,
)
from ..controllers import AppState, PolicyController, RefreshController, SelectionController
from ..dialogs import ModalDialogs, ScheduleDetailsDialog, ScheduleDialog
from ..services import ErrorPresenter, TaskRunner
from ..types.ui_rows import UiScheduleRow


class SchedulesTab(QWidget):
    def __init__(self, parent=None, app_state: AppState | None = None):
        super().__init__(parent)
        self.layout = QVBoxLayout(self)
        self.search = SearchFilterBar(self)
        self.toolbar = ActionToolbar(
            self, actions=["refresh", "create", "edit", "enable", "disable", "details", "delete"]
        )
        delete_btn = self.toolbar._buttons.get("delete")
        if delete_btn is not None:
            delete_btn.setText("Remove")
        self.notice = NotificationCenter(self)
        self.table = TableViewPanel(
            [
                UiScheduleRow.id,
                UiScheduleRow.job_id,
                UiScheduleRow.tags_text,
                UiScheduleRow.enabled,
                UiScheduleRow.next_run_at,
                UiScheduleRow.interval_days,
                UiScheduleRow.interval_hours,
                UiScheduleRow.interval_minutes,
                UiScheduleRow.interval_sec,
                UiScheduleRow.last_update,
                UiScheduleRow.seconds_since_last_update,
            ],
            [],
            self,
        )
        self.meta_row = QHBoxLayout()
        self.tag_pills_label = QLabel("Tags:", self)
        self.tag_pills_widget = QWidget(self)
        self.tag_pills_layout = QHBoxLayout(self.tag_pills_widget)
        self.tag_pills_layout.setContentsMargins(0, 0, 0, 0)
        self.tag_pills_layout.setSpacing(6)
        self.last_pull_label = QLabel("Last pull: never", self)
        self.pull_age_label = QLabel("Age: --", self)
        self.last_pull_label.setStyleSheet("font-size: 11px; color: #666;")
        self.pull_age_label.setStyleSheet("font-size: 11px; color: #666;")
        self.meta_row.addWidget(self.tag_pills_label)
        self.meta_row.addWidget(self.tag_pills_widget)
        self.meta_row.addStretch(1)
        self.meta_row.addWidget(self.last_pull_label)
        self.meta_row.addWidget(self.pull_age_label)
        self.selection = SelectionController(self, row_factory=UiScheduleRow)
        self.runner = TaskRunner()
        self.errors = ErrorPresenter()
        self.app_state = app_state if app_state is not None else AppState.from_env()
        self.schedules_adapter = None
        self.adapter_mode = "disconnected"
        self._rows_cache: list[UiScheduleRow] = []
        self._settings = QSettings("nodejobs", "nodejobs-ui")
        saved_tags = self._settings.value("ui/schedules/selected_tags", [])
        if isinstance(saved_tags, str):
            saved_tags = [saved_tags]
        if not isinstance(saved_tags, (list, tuple, set)):
            saved_tags = []
        self._selected_tags: set[str] = {
            str(tag).strip() for tag in saved_tags if str(tag).strip()
        }
        self._tag_pill_buttons: dict[str, QPushButton] = {}
        self._refresh_controller: RefreshController | None = None
        self._refresh_in_flight = False
        self._refresh_pending = False

        self.table.row_selected.connect(self.selection.set_selected)
        self.search.query_changed.connect(self.table.set_query)
        self.toolbar.action_triggered.connect(self._on_toolbar_action)

        self.layout.addWidget(self.search)
        self.layout.addWidget(self.toolbar)
        self.layout.addWidget(self.notice)
        self.layout.addWidget(self.table)
        self.layout.addLayout(self.meta_row)

        self._set_mutating_enabled(False)
        self._clear_rows()
        self._last_pull_utc: dt.datetime | None = None
        self._freshness_timer = QTimer(self)
        self._freshness_timer.setInterval(1000)
        self._freshness_timer.timeout.connect(self._update_freshness_labels)
        self._freshness_timer.start()
        self._clear_freshness()

        if self.app_state.connected and self.app_state.has_db_path():
            try:
                self.rebind_backend(self.app_state.db_path)
            except Exception as ex:
                self.disconnect_backend(self.errors.to_user_message(ex))
        else:
            self.notice.show_warning("backend not connected")

    def rebind_backend(self, db_path: str) -> None:
        path = str(db_path or "").strip()
        if not path:
            raise ValueError("db_path is required")
        self.schedules_adapter = SchedulesAdapter.from_db_path(path)
        self.adapter_mode = "real"
        self._set_mutating_enabled(True)
        self.notice.show_info(f"backend connected mode={self.adapter_mode}")
        self._load_schedules(notify=False)

    def register_refresh_triggers(self, refresh_controller: RefreshController) -> None:
        self._refresh_controller = refresh_controller
        refresh_controller.register("refresh.tick", self._on_refresh_tick)
        refresh_controller.register("refresh.schedules.now", self._on_refresh_now)
        refresh_controller.register("refresh.all.now", self._on_refresh_now)

    def _on_refresh_tick(self) -> None:
        self._load_schedules(notify=False)

    def _on_refresh_now(self) -> None:
        self._load_schedules(notify=False)

    def disconnect_backend(self, reason: str = "backend disconnected") -> None:
        self.schedules_adapter = None
        self.adapter_mode = "disconnected"
        self._set_mutating_enabled(False)
        self._clear_rows()
        self._clear_freshness()
        if str(reason or "").strip():
            self.notice.show_warning(reason)

    def _set_mutating_enabled(self, enabled: bool) -> None:
        self.toolbar.set_actions_enabled(
            ["create", "edit", "enable", "disable", "details", "delete"], enabled
        )

    def _clear_rows(self) -> None:
        self._rows_cache = []
        self._clear_tag_pills(reset_selection=False)
        self.table.update_rows_preserving_state([], row_key=UiScheduleRow.id)

    def _on_toolbar_action(self, action_id: str) -> None:
        if action_id == "refresh":
            self._load_schedules(notify=True)
            return
        if action_id == "create":
            self._open_create_dialog()
            return
        if action_id == "edit":
            self._open_edit_dialog()
            return
        if action_id == "enable":
            self._set_selected_schedule_enabled(True)
            return
        if action_id == "disable":
            self._set_selected_schedule_enabled(False)
            return
        if action_id == "details":
            self._show_selected_schedule_details()
            return
        if action_id == "delete":
            self._remove_selected_schedule()

    def _load_schedules(self, notify: bool = True) -> None:
        if self.schedules_adapter is None:
            if notify:
                self._clear_rows()
                self.notice.show_warning("backend not connected")
            return
        if self._refresh_in_flight:
            self._refresh_pending = True
            return

        self._refresh_in_flight = True

        def _on_loaded(rows: list[UiScheduleRow]) -> None:
            self._refresh_in_flight = False
            self._on_schedules_loaded(rows, notify=notify)
            self._flush_pending_refresh()

        def _on_failed(ex: Exception) -> None:
            self._refresh_in_flight = False
            if notify:
                self._on_error(ex)
            self._flush_pending_refresh()

        PolicyController.apply_refresh_policy(
            self.runner,
            self._fetch_schedules,
            _on_loaded,
            _on_failed,
        )

    def _fetch_schedules(self) -> list[UiScheduleRow]:
        return self.schedules_adapter.list_schedules()

    def _on_schedules_loaded(self, rows: list[UiScheduleRow], notify: bool = True) -> None:
        self._rows_cache = list(rows)
        self._rebuild_tag_pills(rows)
        self._apply_row_filters(rows)
        self._mark_pull_complete()
        if not notify:
            return
        self.notice.show_info(
            PolicyController.format_refresh_success("schedules", len(self.table.model.rows), self.adapter_mode)
        )

    def _flush_pending_refresh(self) -> None:
        if not self._refresh_pending:
            return
        self._refresh_pending = False
        self._load_schedules(notify=False)

    def _mark_pull_complete(self) -> None:
        self._last_pull_utc = dt.datetime.now(dt.timezone.utc)
        self._update_freshness_labels()

    def _clear_freshness(self) -> None:
        self._last_pull_utc = None
        self.last_pull_label.setText("Last pull: never")
        self.pull_age_label.setText("Age: --")

    def _update_freshness_labels(self) -> None:
        if self._last_pull_utc is None:
            return
        stamp = (
            self._last_pull_utc.astimezone(dt.timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )
        age = max(0, int((dt.datetime.now(dt.timezone.utc) - self._last_pull_utc).total_seconds()))
        self.last_pull_label.setText(f"Last pull: {stamp}")
        self.pull_age_label.setText(f"Age: {age}s")

    def _row_tag_keys(self, row: UiScheduleRow) -> set[str]:
        tags_map = row.tags if isinstance(row.tags, dict) else {}
        return {str(key).strip() for key in tags_map.keys() if str(key).strip()}

    def _clear_tag_pills(self, reset_selection: bool) -> None:
        for button in self._tag_pill_buttons.values():
            self.tag_pills_layout.removeWidget(button)
            button.deleteLater()
        self._tag_pill_buttons = {}
        if reset_selection:
            self._selected_tags = set()

    def _rebuild_tag_pills(self, rows: list[UiScheduleRow]) -> None:
        discovered = sorted(
            {
                tag
                for row in rows
                for tag in self._row_tag_keys(row)
            }
        )
        prior_selected = set(self._selected_tags)
        discovered_set = set(discovered)
        self._selected_tags = {tag for tag in self._selected_tags if tag in discovered_set}
        if self._selected_tags != prior_selected:
            self._settings.setValue("ui/schedules/selected_tags", sorted(self._selected_tags))
        self._clear_tag_pills(reset_selection=False)
        for tag in discovered:
            button = QPushButton(tag, self.tag_pills_widget)
            button.setCheckable(True)
            button.setChecked(tag in self._selected_tags)
            button.setStyleSheet(
                "QPushButton { border: 1px solid #b5b5b5; border-radius: 10px; padding: 2px 10px; }"
                "QPushButton:checked { background-color: #d9ecff; border-color: #5f8ec2; }"
            )
            button.toggled.connect(lambda checked, tag_name=tag: self._toggle_tag_filter(tag_name, checked))
            self.tag_pills_layout.addWidget(button)
            self._tag_pill_buttons[tag] = button

    def _toggle_tag_filter(self, tag_name: str, checked: bool) -> None:
        if checked:
            self._selected_tags.add(tag_name)
        else:
            self._selected_tags.discard(tag_name)
        self._settings.setValue("ui/schedules/selected_tags", sorted(self._selected_tags))
        self._apply_row_filters()

    def _apply_row_filters(self, rows: list[UiScheduleRow] | None = None) -> None:
        source_rows = self._rows_cache if rows is None else list(rows)
        if len(self._selected_tags) > 0:
            source_rows = [
                row for row in source_rows if self._selected_tags.issubset(self._row_tag_keys(row))
            ]
        self.table.update_rows_preserving_state(source_rows, row_key=UiScheduleRow.id)

    def _read_field(self, record, key: str, default=None):
        if record is None:
            return default
        if isinstance(record, dict):
            return record.get(key, default)
        try:
            return record[key]
        except Exception:
            return getattr(record, key, default)

    def _selected_schedule_id(self) -> str:
        selected = self.selection.get_selected()
        schedule_id = str(self._read_field(selected, UiScheduleRow.id, "")).strip()
        if not PolicyController.apply_edit_policy(
            schedule_id,
            self.notice.show_warning,
            "schedule",
        ):
            return ""
        return schedule_id

    def _show_selected_schedule_details(self) -> None:
        selected = self.selection.get_selected()
        schedule_id = str(self._read_field(selected, UiScheduleRow.id, "")).strip()
        if not PolicyController.apply_edit_policy(
            schedule_id,
            self.notice.show_warning,
            "schedule",
        ):
            return
        dialog = ScheduleDetailsDialog(selected, self)
        dialog.exec()
        self.notice.show_info(
            PolicyController.format_action_success("details opened", "schedule", schedule_id)
        )

    def _open_create_dialog(self) -> None:
        now_utc = dt.datetime.utcnow()
        suffix = now_utc.strftime("%Y%m%d-%H%M%S")
        next_run_at = (now_utc + dt.timedelta(seconds=10)).isoformat(timespec="milliseconds") + "Z"
        job_choices = self._job_id_choices()
        default_job_id = job_choices[0] if len(job_choices) > 0 else ""
        draft = {
            UiScheduleRow.id: f"sched-{suffix}",
            UiScheduleRow.job_id: default_job_id,
            UiScheduleRow.next_run_at: next_run_at,
            UiScheduleRow.interval_days: "",
            UiScheduleRow.interval_hours: "",
            UiScheduleRow.interval_minutes: "",
            UiScheduleRow.interval_sec: "",
            UiScheduleRow.cwd: "",
            UiScheduleRow.envs: "{}",
            UiScheduleRow.tags: "{}",
            UiScheduleRow.enabled: True,
        }
        dialog = ScheduleDialog(draft, self, job_id_choices=self._job_id_choices())
        if not dialog.exec():
            self.notice.show_info(PolicyController.format_edit_cancelled("schedule"))
            return
        self._persist_schedule(dialog.get_payload(), "create")

    def _open_edit_dialog(self) -> None:
        selected = self.selection.get_selected()
        selected_id = str(self._read_field(selected, UiScheduleRow.id, "")).strip()
        if not PolicyController.apply_edit_policy(
            selected_id,
            self.notice.show_warning,
            "schedule",
        ):
            return
        if self.schedules_adapter is None:
            self.notice.show_warning("backend not connected")
            return
        try:
            selected = self.schedules_adapter.get_schedule(selected_id)
        except Exception as ex:
            self._on_error(ex)
            return
        dialog = ScheduleDialog(selected, self, job_id_choices=self._job_id_choices())
        if not dialog.exec():
            self.notice.show_info(PolicyController.format_edit_cancelled("schedule"))
            return
        payload = dialog.get_payload()
        payload[UiScheduleRow.attempt_count] = self._read_field(
            selected, UiScheduleRow.attempt_count, 0
        )
        self._persist_schedule(payload, "edit")

    def _job_id_choices(self) -> list[str]:
        if self.schedules_adapter is None:
            return []
        try:
            return self.schedules_adapter.list_job_ids()
        except Exception as ex:
            self.notice.show_warning(self.errors.to_user_message(ex))
            return []

    def _set_selected_schedule_enabled(self, enabled: bool) -> None:
        selected = self.selection.get_selected()
        schedule_id = self._selected_schedule_id()
        if not schedule_id:
            return
        current_enabled = bool(self._read_field(selected, UiScheduleRow.enabled, True))
        if current_enabled is enabled:
            state_text = "enabled" if enabled else "disabled"
            self.notice.show_info(f"schedule already {state_text}")
            return
        record = {
            UiScheduleRow.id: schedule_id,
            UiScheduleRow.job_id: self._read_field(selected, UiScheduleRow.job_id, ""),
            UiScheduleRow.next_run_at: self._read_field(selected, UiScheduleRow.next_run_at, ""),
            UiScheduleRow.interval_days: self._read_field(selected, UiScheduleRow.interval_days, ""),
            UiScheduleRow.interval_hours: self._read_field(selected, UiScheduleRow.interval_hours, ""),
            UiScheduleRow.interval_minutes: self._read_field(selected, UiScheduleRow.interval_minutes, ""),
            UiScheduleRow.interval_sec: self._read_field(selected, UiScheduleRow.interval_sec, ""),
            UiScheduleRow.cwd: self._read_field(selected, UiScheduleRow.cwd, ""),
            UiScheduleRow.envs: self._read_field(selected, UiScheduleRow.envs, {}),
            UiScheduleRow.tags: self._read_field(selected, UiScheduleRow.tags, {}),
            UiScheduleRow.enabled: enabled,
        }
        self._persist_schedule(record, "enable" if enabled else "disable")

    def _to_bool(self, value) -> bool:
        if isinstance(value, bool):
            return value
        text = str(value or "").strip().lower()
        if text in ("1", "true", "yes", "y", "on"):
            return True
        if text in ("0", "false", "no", "n", "off"):
            return False
        raise ValueError("enabled must be true/false")

    def _to_non_negative_int_or_none(self, value, field_name: str):
        text = str(value or "").strip()
        if not text:
            return None
        parsed = int(text)
        if parsed < 0:
            raise ValueError(f"{field_name} must be >= 0 when set")
        return parsed

    def _to_dict_or_none(self, value, field_name: str):
        if value is None:
            return None
        if isinstance(value, dict):
            return value
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = json.loads(text)
        except Exception:
            raise ValueError(f"{field_name} must be valid JSON object when set")
        if not isinstance(parsed, dict):
            raise ValueError(f"{field_name} must be JSON object when set")
        return parsed

    def _to_tags_dict(self, value):
        parsed = self._to_dict_or_none(value, "tags")
        if parsed is None:
            return {}
        if not isinstance(parsed, dict):
            return {}
        out = {}
        for key, value in parsed.items():
            key_text = str(key or "").strip()
            if not key_text:
                continue
            if value is None:
                out[key_text] = None
            elif isinstance(value, str):
                out[key_text] = value.strip()
            else:
                out[key_text] = str(value).strip()
        return out

    def _to_set_schedule_payload(self, record) -> dict:
        schedule_id = str(self._read_field(record, UiScheduleRow.id, "")).strip()
        job_id = str(self._read_field(record, UiScheduleRow.job_id, "")).strip()
        next_run_at = str(self._read_field(record, UiScheduleRow.next_run_at, "")).strip()
        if not schedule_id:
            raise ValueError("schedule_id is required")
        if not job_id:
            raise ValueError("job_id is required")
        if not next_run_at:
            raise ValueError("next_run_at is required")

        payload = {
            ScheduleRecord.schedule_id: schedule_id,
            ScheduleRecord.job_id: job_id,
            ScheduleRecord.next_run_at: next_run_at,
            ScheduleRecord.enabled: self._to_bool(self._read_field(record, UiScheduleRow.enabled, True)),
        }
        attempt_count = self._to_non_negative_int_or_none(
            self._read_field(record, UiScheduleRow.attempt_count, 0),
            "attempt_count",
        )
        payload[ScheduleRecord.attempt_count] = 0 if attempt_count is None else attempt_count
        interval_days = self._to_non_negative_int_or_none(
            self._read_field(record, UiScheduleRow.interval_days, None),
            "interval_days",
        )
        interval_hours = self._to_non_negative_int_or_none(
            self._read_field(record, UiScheduleRow.interval_hours, None),
            "interval_hours",
        )
        interval_minutes = self._to_non_negative_int_or_none(
            self._read_field(record, UiScheduleRow.interval_minutes, None),
            "interval_minutes",
        )
        interval_sec = self._to_non_negative_int_or_none(
            self._read_field(record, UiScheduleRow.interval_sec, None),
            "interval_sec",
        )

        payload[ScheduleRecord.interval_days] = interval_days
        payload[ScheduleRecord.interval_hours] = interval_hours
        payload[ScheduleRecord.interval_minutes] = interval_minutes
        payload[ScheduleRecord.interval_sec] = interval_sec

        if any(v is not None for v in (interval_days, interval_hours, interval_minutes, interval_sec)):
            total = (
                (interval_days or 0) * 86400
                + (interval_hours or 0) * 3600
                + (interval_minutes or 0) * 60
                + (interval_sec or 0)
            )
            if total <= 0:
                raise ValueError(
                    "combined interval must be > 0 when interval_days/hours/minutes/sec are set"
                )

        cwd = str(self._read_field(record, UiScheduleRow.cwd, "") or "").strip()
        payload[ScheduleRecord.cwd] = cwd if cwd else None

        envs = self._to_dict_or_none(self._read_field(record, UiScheduleRow.envs, None), "envs")
        payload[ScheduleRecord.envs] = envs
        tags = self._to_tags_dict(self._read_field(record, UiScheduleRow.tags, None))
        payload[ScheduleRecord.tags] = tags
        return payload

    def _persist_schedule(self, record, action_name: str) -> None:
        try:
            payload = self._to_set_schedule_payload(record)
        except Exception as ex:
            self._on_error(ex)
            return

        def _run_set():
            return self.schedules_adapter.set_schedule(payload)

        def _on_set_ok(_result):
            self.notice.show_info(
                PolicyController.format_action_success(
                    f"{action_name} saved",
                    "schedule",
                    str(payload[ScheduleRecord.schedule_id]),
                )
            )
            if self._refresh_controller is not None:
                self._refresh_controller.trigger("refresh.schedules.now")
            else:
                self._load_schedules(notify=False)

        self.runner.run(_run_set, _on_set_ok, self._on_error)

    def _remove_selected_schedule(self) -> None:
        schedule_id = self._selected_schedule_id()
        if not schedule_id:
            return
        if self.schedules_adapter is None:
            self.notice.show_warning("backend not connected")
            return
        if not ModalDialogs.confirm_remove(self, "schedule", schedule_id):
            self.notice.show_info("remove cancelled")
            return

        def _run_remove():
            return self.schedules_adapter.remove_schedule(schedule_id)

        def _on_remove_ok(_result):
            self.notice.show_info(
                PolicyController.format_action_success("removed", "schedule", schedule_id)
            )
            if self._refresh_controller is not None:
                self._refresh_controller.trigger("refresh.schedules.now")
            else:
                self._load_schedules(notify=False)

        self.runner.run(_run_remove, _on_remove_ok, self._on_error)

    def _on_error(self, ex: Exception) -> None:
        self.notice.show_error(PolicyController.format_error(ex, self.errors))
