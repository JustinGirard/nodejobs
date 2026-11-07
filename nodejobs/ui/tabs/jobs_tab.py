import datetime as dt
import os

from PyQt6.QtCore import QSettings, QTimer, QUrl
from PyQt6.QtGui import QDesktopServices
from PyQt6.QtWidgets import QCheckBox, QHBoxLayout, QLabel, QPushButton, QVBoxLayout, QWidget

from nodejobs.jobs import JobDeleteDetails, JobDeleteError, JobDeleteErrorData

from ..adapters import JobsAdapter
from ..components import (
    ActionToolbar,
    NotificationCenter,
    SearchFilterBar,
    TableViewPanel,
)
from ..controllers import AppState, PolicyController, RefreshController, SelectionController
from ..dialogs import JobDetailsDialog, JobLogsDialog, ModalDialogs, RunJobDialog
from ..services import ErrorPresenter, TaskRunner
from ..types.ui_rows import UiJobRow


class JobsTab(QWidget):
    ACTIVE_STATUSES = {"starting", "running", "stopping"}

    def __init__(self, parent=None, app_state: AppState | None = None):
        super().__init__(parent)
        self.layout = QVBoxLayout(self)
        self.search = SearchFilterBar(self)
        self.toolbar = ActionToolbar(
            self, actions=["refresh", "create", "edit", "run", "details", "logs", "open", "stop", "delete"]
        )
        delete_btn = self.toolbar._buttons.get("delete")
        if delete_btn is not None:
            delete_btn.setText("Remove")
        self.notice = NotificationCenter(self)
        self.table = TableViewPanel(
            [
                UiJobRow.id,
                UiJobRow.status,
                UiJobRow.tags_text,
                UiJobRow.command_text,
                UiJobRow.last_pid,
                UiJobRow.last_update,
                UiJobRow.seconds_since_last_update,
            ],
            [],
            self,
        )
        self.meta_row = QHBoxLayout()
        self.running_only_checkbox = QCheckBox("Running only", self)
        self.running_only_checkbox.setChecked(False)
        self.tag_pills_label = QLabel("Tags:", self)
        self.tag_pills_widget = QWidget(self)
        self.tag_pills_layout = QHBoxLayout(self.tag_pills_widget)
        self.tag_pills_layout.setContentsMargins(0, 0, 0, 0)
        self.tag_pills_layout.setSpacing(6)
        self.last_pull_label = QLabel("Last pull: never", self)
        self.pull_age_label = QLabel("Age: --", self)
        self.last_pull_label.setStyleSheet("font-size: 11px; color: #666;")
        self.pull_age_label.setStyleSheet("font-size: 11px; color: #666;")
        self.meta_row.addWidget(self.running_only_checkbox)
        self.meta_row.addWidget(self.tag_pills_label)
        self.meta_row.addWidget(self.tag_pills_widget)
        self.meta_row.addStretch(1)
        self.meta_row.addWidget(self.last_pull_label)
        self.meta_row.addWidget(self.pull_age_label)
        self.selection = SelectionController(self)
        self.runner = TaskRunner()
        self.errors = ErrorPresenter()
        self.app_state = app_state if app_state is not None else AppState.from_env()
        self.jobs_adapter = None
        self.adapter_mode = "disconnected"
        self._rows_cache: list[UiJobRow] = []
        self._settings = QSettings("nodejobs", "nodejobs-ui")
        saved_tags = self._settings.value("ui/jobs/selected_tags", [])
        if isinstance(saved_tags, str):
            saved_tags = [saved_tags]
        if not isinstance(saved_tags, (list, tuple, set)):
            saved_tags = []
        self._selected_tags: set[str] = {
            str(tag).strip() for tag in saved_tags if str(tag).strip()
        }
        self._tag_pill_buttons: dict[str, QPushButton] = {}
        self._live_log_dialogs: dict[str, JobLogsDialog] = {}
        self._refresh_controller: RefreshController | None = None
        self._refresh_in_flight = False
        self._refresh_pending = False

        self.table.row_selected.connect(self.selection.set_selected)
        self.search.query_changed.connect(self.table.set_query)
        self.running_only_checkbox.toggled.connect(lambda _checked: self._apply_running_filter())
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
        self.jobs_adapter = JobsAdapter.from_db_path(path)
        self.adapter_mode = "real"
        self._set_mutating_enabled(True)
        self.notice.show_info(f"backend connected mode={self.adapter_mode}")
        self._load_rows(notify=False)

    def register_refresh_triggers(self, refresh_controller: RefreshController) -> None:
        self._refresh_controller = refresh_controller
        refresh_controller.register("refresh.tick", self._on_refresh_tick)
        refresh_controller.register("refresh.jobs.now", self._on_refresh_now)
        refresh_controller.register("refresh.all.now", self._on_refresh_now)

    def _on_refresh_tick(self) -> None:
        self._load_rows(notify=False)

    def _on_refresh_now(self) -> None:
        self._load_rows(notify=False)

    def disconnect_backend(self, reason: str = "backend disconnected") -> None:
        self.jobs_adapter = None
        self.adapter_mode = "disconnected"
        self._set_mutating_enabled(False)
        self._clear_rows()
        self._clear_freshness()
        if str(reason or "").strip():
            self.notice.show_warning(reason)

    def _set_mutating_enabled(self, enabled: bool) -> None:
        self.toolbar.set_actions_enabled(
            ["create", "edit", "run", "details", "logs", "open", "stop", "delete"], enabled
        )

    def _clear_rows(self) -> None:
        self._rows_cache = []
        self._clear_tag_pills(reset_selection=False)
        self.table.update_rows_preserving_state([], row_key=UiJobRow.id)

    def _on_toolbar_action(self, action_id: str) -> None:
        if action_id == "refresh":
            self._load_rows(notify=True)
            return
        if action_id == "create":
            self._create_job_draft()
            return
        if action_id == "edit":
            self._open_edit_dialog()
            return
        if action_id == "run":
            self._run_new_job()
            return
        if action_id == "details":
            self._show_selected_job_details()
            return
        if action_id == "stop":
            self._stop_selected_job()
            return
        if action_id == "delete":
            self._remove_selected_job()
            return
        if action_id == "logs":
            self._load_selected_job_logs()
            return
        if action_id == "open":
            self._open_selected_job_dir()

    def _load_rows(self, notify: bool = True) -> None:
        if self.jobs_adapter is None:
            if notify:
                self._clear_rows()
                self.notice.show_warning("backend not connected")
            return
        if self._refresh_in_flight:
            self._refresh_pending = True
            return

        self._refresh_in_flight = True

        def _on_loaded(rows: list[UiJobRow]) -> None:
            self._refresh_in_flight = False
            self._on_rows_loaded(rows, notify=notify)
            self._flush_pending_refresh()

        def _on_failed(ex: Exception) -> None:
            self._refresh_in_flight = False
            if notify:
                self._on_error(ex)
            self._flush_pending_refresh()

        PolicyController.apply_refresh_policy(
            self.runner,
            self._fetch_rows,
            _on_loaded,
            _on_failed,
        )

    def _fetch_rows(self) -> list[UiJobRow]:
        return self.jobs_adapter.list_jobs()

    def _on_rows_loaded(self, rows: list[UiJobRow], notify: bool = True) -> None:
        self._rows_cache = list(rows)
        self._rebuild_tag_pills(rows)
        self._apply_running_filter(rows)
        self._mark_pull_complete()
        if not notify:
            return
        shown = len(self.table.model.rows)
        if self.running_only_checkbox.isChecked():
            self.notice.show_info(
                PolicyController.format_refresh_success(
                    "jobs", shown, f"{self.adapter_mode}; running-only filter"
                )
            )
            return
        self.notice.show_info(
            PolicyController.format_refresh_success("jobs", shown, self.adapter_mode)
        )

    def _flush_pending_refresh(self) -> None:
        if not self._refresh_pending:
            return
        self._refresh_pending = False
        self._load_rows(notify=False)

    def _apply_running_filter(self, rows: list[UiJobRow] | None = None) -> None:
        source_rows = self._rows_cache if rows is None else list(rows)
        if self.running_only_checkbox.isChecked():
            source_rows = [
                row for row in source_rows if str(row.status).strip().lower() in self.ACTIVE_STATUSES
            ]
        if len(self._selected_tags) > 0:
            source_rows = [
                row for row in source_rows if self._selected_tags.issubset(self._row_tag_keys(row))
            ]
        self.table.update_rows_preserving_state(source_rows, row_key=UiJobRow.id)

    def _row_tag_keys(self, row: UiJobRow) -> set[str]:
        tags_map = row.tags if isinstance(row.tags, dict) else {}
        return {str(key).strip() for key in tags_map.keys() if str(key).strip()}

    def _clear_tag_pills(self, reset_selection: bool) -> None:
        for button in self._tag_pill_buttons.values():
            self.tag_pills_layout.removeWidget(button)
            button.deleteLater()
        self._tag_pill_buttons = {}
        if reset_selection:
            self._selected_tags = set()

    def _rebuild_tag_pills(self, rows: list[UiJobRow]) -> None:
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
            self._settings.setValue("ui/jobs/selected_tags", sorted(self._selected_tags))
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
        self._settings.setValue("ui/jobs/selected_tags", sorted(self._selected_tags))
        self._apply_running_filter()

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

    def _selected_job_id(self) -> str:
        selected = self.selection.get_selected()
        job_id = str(selected.id).strip()
        if not PolicyController.apply_edit_policy(job_id, self.notice.show_warning, "job"):
            return ""
        return job_id

    def _show_selected_job_details(self) -> None:
        job_id = self._selected_job_id()
        if not job_id:
            return
        def _run_details():
            if self.jobs_adapter is None:
                raise RuntimeError("details require real backend mode")
            return self.jobs_adapter.get_job_record(job_id)

        def _on_details_ok(record):
            dialog = JobDetailsDialog(record, self)
            dialog.exec()
            self.notice.show_info(
                PolicyController.format_action_success("details opened", "job", job_id)
            )

        self.runner.run(_run_details, _on_details_ok, self._on_error)

    def _run_new_job(self) -> None:
        seed = self._selected_job_seed()
        if not seed["job_id"]:
            seed["job_id"] = self._default_job_id()
        if len(seed["command"]) == 0:
            seed["command"] = self._default_command()
        dialog = RunJobDialog(self, title="Run Job", submit_label="Run", initial=seed)
        if not dialog.exec():
            self.notice.show_info("run cancelled")
            return
        try:
            payload = dialog.get_payload()
        except Exception as ex:
            self._on_error(ex)
            return

        job_id = str(payload.get("job_id", "")).strip()
        command = payload.get("command") or []
        if not job_id:
            self.notice.show_warning("job_id is required")
            return
        if not isinstance(command, list) or len(command) == 0:
            self.notice.show_warning("command is required")
            return
        cwd = str(payload.get("cwd", "") or "").strip() or None
        envs = payload.get("envs")
        tags = payload.get("tags")

        def _run_job():
            if self.jobs_adapter is None:
                raise RuntimeError("run requires real backend mode")
            return self.jobs_adapter.run_job(
                command=command,
                job_id=job_id,
                cwd=cwd,
                envs=envs,
                tags=tags,
            )

        def _on_job_ok(_result):
            self.notice.show_info(PolicyController.format_action_success("run requested", "job", job_id))
            self._open_live_logs(job_id)
            if self._refresh_controller is not None:
                self._refresh_controller.trigger("refresh.jobs.now")
            else:
                self._load_rows(notify=False)

        self.runner.run(_run_job, _on_job_ok, self._on_error)

    def _create_job_draft(self) -> None:
        seed = {
            "job_id": self._default_job_id(),
            "command": self._default_command(),
            "cwd": os.getcwd(),
            "envs": {},
            "tags": {},
        }
        dialog = RunJobDialog(
            self,
            title="Create Job Draft",
            submit_label="Create Draft",
            initial=seed,
        )
        if not dialog.exec():
            self.notice.show_info("create cancelled")
            return
        try:
            payload = dialog.get_payload()
        except Exception as ex:
            self._on_error(ex)
            return

        self._persist_job_draft(
            payload,
            action_name="draft created",
            require_new=True,
            default_status="stopped",
        )

    def _open_edit_dialog(self) -> None:
        selected = self.selection.get_selected()
        selected_id = str(selected.id or "").strip()
        if not PolicyController.apply_edit_policy(selected_id, self.notice.show_warning, "job"):
            return
        if self.jobs_adapter is None:
            self.notice.show_warning("backend not connected")
            return
        try:
            canonical = self.jobs_adapter.get_job_record(selected_id)
        except Exception as ex:
            self._on_error(ex)
            return

        status_text = str(canonical.get(UiJobRow.status, "") or "").strip().lower()
        if not PolicyController.apply_job_edit_status_policy(status_text, self.notice.show_warning):
            return

        canonical_tags = canonical.get(UiJobRow.tags, {})
        if not isinstance(canonical_tags, dict):
            canonical_tags = {}
        seed = {
            "job_id": selected_id,
            "command": canonical.get(UiJobRow.command, []),
            "cwd": canonical.get(UiJobRow.cwd, os.getcwd()),
            "envs": canonical.get(UiJobRow.envs, {}),
            "tags": canonical_tags,
        }
        dialog = RunJobDialog(
            self,
            title="Edit Job Draft",
            submit_label="Save",
            initial=seed,
            job_id_read_only=True,
        )
        if not dialog.exec():
            self.notice.show_info(PolicyController.format_edit_cancelled("job"))
            return
        try:
            payload = dialog.get_payload()
        except Exception as ex:
            self._on_error(ex)
            return
        self._persist_job_draft(
            payload,
            action_name="edit saved",
            require_new=False,
            default_status=status_text or "stopped",
        )

    def _persist_job_draft(
        self,
        payload: dict,
        action_name: str,
        require_new: bool,
        default_status: str = "stopped",
    ) -> None:
        job_id = str(payload.get("job_id", "") or "").strip()
        command = payload.get("command") or []
        if not job_id:
            self.notice.show_warning("job_id is required")
            return
        if not isinstance(command, list) or len(command) == 0:
            self.notice.show_warning("command is required")
            return
        cwd = str(payload.get("cwd", "") or "").strip() or None
        envs = payload.get("envs")
        tags = payload.get("tags")
        status = str(default_status or "").strip().lower() or "stopped"

        def _run_persist():
            if self.jobs_adapter is None:
                raise RuntimeError("save requires real backend mode")
            if require_new:
                return self.jobs_adapter.create_job_draft(
                    command=command,
                    job_id=job_id,
                    cwd=cwd,
                    envs=envs,
                    tags=tags,
                )
            return self.jobs_adapter.upsert_job_draft(
                command=command,
                job_id=job_id,
                cwd=cwd,
                envs=envs,
                tags=tags,
                status=status,
            )

        def _on_persist_ok(_result):
            self.notice.show_info(
                PolicyController.format_action_success(action_name, "job", job_id, status)
            )
            if self._refresh_controller is not None:
                self._refresh_controller.trigger("refresh.jobs.now")
            else:
                self._load_rows(notify=False)

        self.runner.run(_run_persist, _on_persist_ok, self._on_error)

    def _default_job_id(self) -> str:
        return dt.datetime.utcnow().strftime("job-%Y%m%d-%H%M%S")

    def _default_command(self) -> list[str]:
        return ["open", "-a", "Calculator"]

    def _selected_job_seed(self) -> dict:
        selected = self.selection.get_selected()
        selected_id = str(selected.id or "").strip()
        selected_command = selected.command if isinstance(selected.command, list) else []
        selected_command = [str(v).strip() for v in selected_command if str(v).strip()]
        selected_cwd = str(selected.cwd or "").strip() or os.getcwd()
        selected_envs = selected.envs if isinstance(selected.envs, dict) else {}
        selected_tags = selected.tags if isinstance(selected.tags, dict) else {}
        return {
            "job_id": selected_id,
            "command": selected_command,
            "cwd": selected_cwd,
            "envs": selected_envs,
            "tags": selected_tags,
        }

    def _stop_selected_job(self) -> None:
        job_id = self._selected_job_id()
        if not job_id:
            return

        def _run_stop():
            if self.jobs_adapter is None:
                raise RuntimeError("stop requires real backend mode")
            return self.jobs_adapter.stop_job(job_id, wait_time=1)

        def _on_stop_ok(_result):
            self.notice.show_info(
                PolicyController.format_action_success("stop requested", "job", job_id)
            )
            self._trigger_jobs_refresh_now()
            self._queue_stop_feedback_refreshes()

        self.runner.run(_run_stop, _on_stop_ok, self._on_error)

    def _trigger_jobs_refresh_now(self) -> None:
        if self._refresh_controller is not None:
            self._refresh_controller.trigger("refresh.jobs.now")
        else:
            self._load_rows(notify=False)

    def _queue_stop_feedback_refreshes(self) -> None:
        # Fast post-stop convergence: 0.5s, 1s, 3s
        QTimer.singleShot(500, self._trigger_jobs_refresh_now)
        QTimer.singleShot(1000, self._trigger_jobs_refresh_now)
        QTimer.singleShot(3000, self._trigger_jobs_refresh_now)

    def _remove_selected_job(self) -> None:
        job_id = self._selected_job_id()
        if not job_id:
            return
        if self.jobs_adapter is None:
            self.notice.show_warning("backend not connected")
            return
        if not ModalDialogs.confirm_remove(self, "job", job_id):
            self.notice.show_info("remove cancelled")
            return

        def _run_remove():
            return self.jobs_adapter.delete_job(job_id=job_id)

        def _on_remove_ok(_result):
            self.notice.show_info(
                PolicyController.format_action_success("removed", "job", job_id)
            )
            if self._refresh_controller is not None:
                self._refresh_controller.trigger("refresh.jobs.now")
            else:
                self._load_rows(notify=False)

        self.runner.run(_run_remove, _on_remove_ok, lambda ex: self._on_remove_error(ex, job_id))

    def _to_delete_error_data(self, ex: Exception, fallback_job_id: str) -> JobDeleteErrorData:
        if isinstance(ex, JobDeleteError):
            return ex.error
        return JobDeleteErrorData(
            {
                JobDeleteErrorData.code: "job_delete_failed",
                JobDeleteErrorData.job_id: str(fallback_job_id or "").strip(),
                JobDeleteErrorData.message: PolicyController.format_error(ex, self.errors),
                JobDeleteErrorData.details: JobDeleteDetails(
                    {
                        JobDeleteDetails.status: None,
                        JobDeleteDetails.schedule_ids: [],
                    }
                ),
            }
        )

    def _on_remove_error(self, ex: Exception, job_id: str) -> None:
        error_data = self._to_delete_error_data(ex, job_id)
        self.notice.show_error(error_data.message)

        details = [
            f"code: {error_data.code}",
            f"job_id: {error_data.job_id}",
        ]
        status_text = str(error_data.details.status or "").strip()
        if len(status_text) > 0:
            details.append(f"status: {status_text}")

        schedule_ids = []
        for sid in error_data.details.schedule_ids:
            sid_text = str(sid).strip()
            if len(sid_text) > 0:
                schedule_ids.append(sid_text)
        if len(schedule_ids) > 0:
            details.append(f"schedule_ids: {', '.join(schedule_ids)}")

        ModalDialogs.show_blocking_error(
            self,
            "Cannot Remove Job",
            error_data.message,
            "\n".join(details),
        )

    def _load_selected_job_logs(self) -> None:
        job_id = self._selected_job_id()
        if not job_id:
            return
        self._open_live_logs(job_id)
        self.notice.show_info(PolicyController.format_action_success("logs opened", "job", job_id))

    def _fetch_job_log_snapshot(self, job_id: str) -> dict:
        if self.jobs_adapter is None:
            raise RuntimeError("logs require real backend mode")
        record = self.jobs_adapter.get_job_record(job_id)
        status = str(record.get(UiJobRow.status) or "").strip().lower()
        stdlog, errlog = self.jobs_adapter.get_job_logs(job_id)
        return {
            "status": status,
            "stdlog": stdlog,
            "errlog": errlog,
        }

    def _open_live_logs(self, job_id: str) -> None:
        key = str(job_id or "").strip()
        if not key:
            return
        existing = self._live_log_dialogs.get(key)
        if existing is not None and existing.isVisible():
            existing.raise_()
            existing.activateWindow()
            return

        dialog = JobLogsDialog(
            key,
            "",
            "",
            self,
            fetch_snapshot=lambda _job_id=key: self._fetch_job_log_snapshot(_job_id),
            poll_interval_ms=1200,
        )
        dialog.finished.connect(
            lambda _code, _job_id=key: self._live_log_dialogs.pop(_job_id, None)
        )
        self._live_log_dialogs[key] = dialog
        dialog.show()
        dialog.raise_()
        dialog.activateWindow()

    def _open_selected_job_dir(self) -> None:
        selected = self.selection.get_selected()
        candidates = [str(selected.cwd).strip(), str(selected.logdir).strip(), str(selected.dirname).strip()]
        target = ""
        for value in candidates:
            if value and os.path.isdir(value):
                target = value
                break
        if not target:
            self.notice.show_warning("selected job has no openable directory")
            return
        opened = QDesktopServices.openUrl(QUrl.fromLocalFile(target))
        if not opened:
            self.notice.show_error(f"failed to open directory: {target}")
            return
        self.notice.show_info(PolicyController.format_action_success("opened", "job dir", str(selected.id)))

    def _on_error(self, ex: Exception) -> None:
        self.notice.show_error(PolicyController.format_error(ex, self.errors))
