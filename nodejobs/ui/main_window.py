from nodejobs.jobs import default_db_path
from nodejobs.db_registry import KnownDbRegistry

from PyQt6.QtGui import QGuiApplication
from PyQt6.QtWidgets import (
    QLabel,
    QMainWindow,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from .components import ConnectionBar
from .controllers import AppState, RefreshController
from .tabs import ComponentsTab, JobsTab, SchedulesTab


class MainWindow(QMainWindow):
    def __init__(
        self,
        stage_label: str = "Current Plan Stage: Stage 2",
        open_tab: str = "",
        db_path: str = "",
    ):
        super().__init__()
        self.setWindowTitle("Nodejobs UI")
        screen = QGuiApplication.primaryScreen()
        if screen is not None:
            available = screen.availableGeometry()
            width = min(980, max(480, available.width() - 40))
            height = min(680, max(360, available.height() - 80))
            self.resize(width, height)
        else:
            self.resize(980, 680)
        self.setMinimumSize(480, 360)

        root = QWidget(self)
        layout = QVBoxLayout(root)

        self.stage_banner = QLabel(stage_label, root)
        self.connection_bar = ConnectionBar(root)
        self.known_db_registry = KnownDbRegistry.from_home(default_db_path())
        known_paths = self.known_db_registry.load_paths()
        self.connection_bar.set_known_paths(known_paths)
        self.tabs = QTabWidget(root)
        self.app_state = AppState.from_env()
        if self.app_state.is_dev_mode() and "stage 11" in str(stage_label).lower():
            self.stage_banner.setText(f"{stage_label} | Stage 11 Policy PASS")
        self.jobs_tab = JobsTab(root, app_state=self.app_state)
        self.schedules_tab = SchedulesTab(root, app_state=self.app_state)
        self.components_tab = ComponentsTab(root)
        self.components_tab.set_stage_label(stage_label.replace("Stage 2", "Stage 3"))
        self.refresh_controller = RefreshController(interval_ms=10000, parent=self)
        self.jobs_tab.register_refresh_triggers(self.refresh_controller)
        self.schedules_tab.register_refresh_triggers(self.refresh_controller)
        self.refresh_controller.start()

        self.tabs.addTab(self.jobs_tab, "Jobs")
        self.tabs.addTab(self.schedules_tab, "Schedules")
        self.tabs.addTab(self.components_tab, "Components")
        self.select_tab_by_name(open_tab)

        self.connection_bar.connection_requested.connect(self.connect_backend)
        self.connection_bar.disconnect_requested.connect(self.disconnect_backend)

        layout.addWidget(self.stage_banner)
        layout.addWidget(self.connection_bar)
        layout.addWidget(self.tabs)
        self.setCentralWidget(root)

        initial_db_path = str(db_path or self.app_state.db_path or "").strip()
        if not initial_db_path and len(known_paths) > 0:
            initial_db_path = known_paths[0]
        if initial_db_path:
            self.connection_bar.set_path(initial_db_path)
            self.connect_backend(initial_db_path)
        else:
            self.disconnect_backend("set DB path and connect")

    def connect_backend(self, db_path: str) -> None:
        path = str(db_path or "").strip()
        if not path:
            self.disconnect_backend("db path is required")
            return
        try:
            self.jobs_tab.rebind_backend(path)
            self.schedules_tab.rebind_backend(path)
        except Exception as ex:
            self.jobs_tab.disconnect_backend("backend disconnected")
            self.schedules_tab.disconnect_backend("backend disconnected")
            self.app_state.disconnect_db(str(ex))
            self.connection_bar.set_connected(False, str(ex))
            return

        known_warning = ""
        try:
            known_paths = self.known_db_registry.remember(path)
        except Exception as ex:
            known_warning = f"known-db save failed: {ex}"
            known_paths = self.known_db_registry.load_paths()

        self.connection_bar.set_known_paths(known_paths)
        self.app_state.connect_db(path)
        self.connection_bar.set_path(path)
        status_message = path if not known_warning else f"{path} | {known_warning}"
        self.connection_bar.set_connected(True, status_message)

    def disconnect_backend(self, reason: str = "disconnected") -> None:
        self.jobs_tab.disconnect_backend(reason)
        self.schedules_tab.disconnect_backend(reason)
        self.app_state.disconnect_db(reason)
        self.connection_bar.set_connected(False, reason)

    def select_tab_by_name(self, name: str) -> None:
        n = str(name or "").strip().lower()
        if n in ("jobs", "job"):
            self.tabs.setCurrentIndex(0)
            return
        if n in ("schedules", "schedule"):
            self.tabs.setCurrentIndex(1)
            return
        if n in ("components", "component"):
            self.tabs.setCurrentIndex(2)
