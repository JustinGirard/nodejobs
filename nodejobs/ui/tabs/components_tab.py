import os

from PyQt6.QtWidgets import QLabel, QVBoxLayout, QWidget

from ..components import (
    ActionToolbar,
    DetailPane,
    NotificationCenter,
    SearchFilterBar,
    TableViewPanel,
)
from ..controllers import RefreshController, SelectionController
from ..dialogs import EditDialog
from ..services import ErrorPresenter, TaskRunner
from ..adapters import ApiAdapter, JobsAdapter
from ..types.ui_rows import UiJobRow


class ComponentsTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.layout = QVBoxLayout(self)
        self.stage_label = QLabel("Current Plan Stage: Stage 3", self)
        self.placeholder = QLabel("Components demo mount point", self)
        self.layout.addWidget(self.stage_label)
        self.layout.addWidget(self.placeholder)
        self.mount_stage4_demo()
        self.mount_stage5_demo()
        self.mount_stage6_demo()
        self.mount_stage7_demo()

    def set_stage_label(self, text: str) -> None:
        self.stage_label.setText(text)

    def mount_demo_widget(self, widget: QWidget) -> None:
        self.layout.addWidget(widget)

    def mount_stage4_demo(self) -> None:
        self.search_bar = SearchFilterBar(self)
        self.toolbar = ActionToolbar(self)
        delete_btn = self.toolbar._buttons.get("delete")
        if delete_btn is not None:
            delete_btn.setText("Error")
        self.notice = NotificationCenter(self)
        self.last_event = QLabel("Stage 4 demo ready", self)

        self.search_bar.query_changed.connect(
            lambda q: self.last_event.setText(f"search query={q}")
        )
        self.toolbar.action_triggered.connect(
            lambda aid: self.notice.show_info(f"toolbar action={aid}")
        )

        self.layout.addWidget(self.search_bar)
        self.layout.addWidget(self.toolbar)
        self.layout.addWidget(self.notice)
        self.layout.addWidget(self.last_event)

    def mount_stage5_demo(self) -> None:
        rows = [
            UiJobRow(
                {
                    UiJobRow.id: "job-001",
                    UiJobRow.status: "running",
                    UiJobRow.last_pid: 44121,
                    UiJobRow.last_update: "2026-07-15T10:00:00Z",
                }
            ),
            UiJobRow(
                {
                    UiJobRow.id: "job-002",
                    UiJobRow.status: "stopped",
                    UiJobRow.last_pid: 44110,
                    UiJobRow.last_update: "2026-07-15T09:58:11Z",
                }
            ),
            UiJobRow(
                {
                    UiJobRow.id: "job-003",
                    UiJobRow.status: "starting",
                    UiJobRow.last_pid: -1,
                    UiJobRow.last_update: "2026-07-15T10:00:03Z",
                }
            ),
        ]
        self.table_panel = TableViewPanel(
            [UiJobRow.id, UiJobRow.status, UiJobRow.last_pid, UiJobRow.last_update],
            rows,
            self,
        )
        self.row_echo = QLabel("selected row: none", self)
        self.search_bar.query_changed.connect(self.table_panel.set_query)
        self.table_panel.row_selected.connect(
            lambda row: self.row_echo.setText(f"selected row: {row.id}")
        )
        self.layout.addWidget(self.table_panel)
        self.layout.addWidget(self.row_echo)

    def mount_stage6_demo(self) -> None:
        self.selection_controller = SelectionController(self)
        self.detail_pane = DetailPane(self)
        self.edit_echo = QLabel("edit result: none", self)

        self.table_panel.row_selected.connect(self.selection_controller.set_selected)
        self.selection_controller.selection_changed.connect(self.detail_pane.set_record)

        self.edit_button = QLabel("Use Toolbar Edit to open dialog", self)
        self.toolbar.action_triggered.connect(self._handle_stage6_action)

        self.layout.addWidget(self.detail_pane)
        self.layout.addWidget(self.edit_echo)
        self.layout.addWidget(self.edit_button)

    def _handle_stage6_action(self, action_id: str) -> None:
        if action_id != "edit":
            return
        record = self.selection_controller.get_selected()
        if not record or not str(record.id).strip():
            self.notice.show_warning("no selection for edit")
            return
        dlg = EditDialog(record, self)
        if dlg.exec():
            payload = dlg.get_payload()
            self.selection_controller.set_selected(payload)
            self.edit_echo.setText(f"edit result: saved id={payload.id}")
            self.notice.show_info("edit saved")
        else:
            self.notice.show_info("edit cancelled")

    def mount_stage7_demo(self) -> None:
        self.runner = TaskRunner()
        self.error_presenter = ErrorPresenter()
        self.mock_adapter = ApiAdapter()
        self.jobs_adapter = None
        self.adapter_mode = "mock"
        db_path = os.environ.get("NODEJOBS_UI_DB_PATH", "").strip()
        if db_path:
            try:
                self.jobs_adapter = JobsAdapter.from_db_path(db_path)
                self.adapter_mode = "real"
            except Exception as ex:
                msg = self.error_presenter.to_user_message(ex)
                self.notice.show_warning(f"real adapter disabled: {msg}")
        self.refresh = RefreshController(3000, self)
        self.async_echo = QLabel(f"async state: idle mode={self.adapter_mode}", self)

        self.refresh.register("refresh.tick", self._run_async_fetch_ok)
        self.refresh.start()

        self.toolbar.action_triggered.connect(self._handle_stage7_actions)
        self.layout.addWidget(self.async_echo)

    def _handle_stage7_actions(self, action_id: str) -> None:
        if action_id == "refresh":
            self.refresh.trigger_once()
            return
        if action_id == "delete":
            self._run_async_fetch_error()

    def _fetch_jobs(self) -> list[UiJobRow]:
        if self.jobs_adapter is not None:
            return self.jobs_adapter.list_jobs()
        return self.mock_adapter.fetch_jobs()

    def _fetch_jobs_with_error(self) -> list[UiJobRow]:
        if self.jobs_adapter is not None:
            raise RuntimeError("simulated adapter failure")
        return self.mock_adapter.fetch_jobs_with_error()

    def _run_async_fetch_ok(self) -> None:
        self.async_echo.setText(f"async state: loading mode={self.adapter_mode}")
        self.runner.run(
            self._fetch_jobs,
            self._on_async_success,
            self._on_async_error,
        )

    def _run_async_fetch_error(self) -> None:
        self.async_echo.setText(f"async state: loading error path mode={self.adapter_mode}")
        self.runner.run(
            self._fetch_jobs_with_error,
            self._on_async_success,
            self._on_async_error,
        )

    def _on_async_success(self, rows: list[UiJobRow]) -> None:
        self.table_panel.model.beginResetModel()
        self.table_panel.model.rows = rows
        self.table_panel.model.endResetModel()
        self.async_echo.setText(f"async state: ok rows={len(rows)} mode={self.adapter_mode}")
        self.notice.show_info("async fetch ok")

    def _on_async_error(self, ex: Exception) -> None:
        msg = self.error_presenter.to_user_message(ex)
        self.async_echo.setText("async state: error")
        self.notice.show_error(msg)
