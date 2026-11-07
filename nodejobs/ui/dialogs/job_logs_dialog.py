import datetime as dt

from PyQt6.QtGui import QTextCursor
from PyQt6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QPlainTextEdit,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from ..controllers import PolicyController, RefreshController
from ..services import TaskRunner


class JobLogsDialog(QDialog):
    ACTIVE_STATUSES = {"starting", "running", "stopping"}

    def __init__(
        self,
        job_id: str,
        stdlog: str,
        errlog: str,
        parent=None,
        fetch_snapshot=None,
        poll_interval_ms: int = 1500,
    ):
        super().__init__(parent)
        self.setWindowTitle(f"Logs: {job_id}")
        self.resize(860, 540)
        self._job_id = str(job_id or "").strip()
        self._fetch_snapshot = fetch_snapshot if callable(fetch_snapshot) else None
        self._std_cache = str(stdlog or "")
        self._err_cache = str(errlog or "")
        self._last_pull_utc: dt.datetime | None = None
        self._poll_in_flight = False
        self._poll_pending = False

        self._runner = TaskRunner()
        interval = int(poll_interval_ms) if int(poll_interval_ms) > 0 else 1500
        self._refresh = RefreshController(interval, self)
        self._refresh.register("refresh.tick", self._poll_if_needed)

        layout = QVBoxLayout(self)
        self.tabs = QTabWidget(self)

        out_widget = QWidget(self)
        out_layout = QVBoxLayout(out_widget)
        self.out_text = QPlainTextEdit(out_widget)
        self.out_text.setReadOnly(True)
        self.out_text.setPlainText(self._std_cache)
        out_layout.addWidget(self.out_text)

        err_widget = QWidget(self)
        err_layout = QVBoxLayout(err_widget)
        self.err_text = QPlainTextEdit(err_widget)
        self.err_text.setReadOnly(True)
        self.err_text.setPlainText(self._err_cache)
        err_layout.addWidget(self.err_text)

        self.tabs.addTab(out_widget, "stdout")
        self.tabs.addTab(err_widget, "stderr")
        layout.addWidget(self.tabs)

        meta_row = QHBoxLayout()
        self.status_label = QLabel("Status: unknown", self)
        self.last_pull_label = QLabel("Last pull: never", self)
        self.pull_age_label = QLabel("Age: --", self)
        self.last_pull_label.setStyleSheet("font-size: 11px; color: #666;")
        self.pull_age_label.setStyleSheet("font-size: 11px; color: #666;")
        meta_row.addWidget(self.status_label)
        meta_row.addStretch(1)
        meta_row.addWidget(self.last_pull_label)
        meta_row.addWidget(self.pull_age_label)
        layout.addLayout(meta_row)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, self)
        buttons.rejected.connect(self.reject)
        buttons.accepted.connect(self.accept)
        layout.addWidget(buttons)

        if self._fetch_snapshot is not None:
            self._refresh.start()
            self._refresh.trigger_once()

    def showEvent(self, event) -> None:
        super().showEvent(event)
        if self._fetch_snapshot is None:
            return
        self._refresh.start()
        self._refresh.trigger_once()

    def closeEvent(self, event) -> None:
        self._refresh.stop()
        super().closeEvent(event)

    def _poll_if_needed(self) -> None:
        if self._fetch_snapshot is None:
            return
        if not self.isVisible():
            return
        if self._poll_in_flight:
            self._poll_pending = True
            return
        self._poll_in_flight = True
        PolicyController.apply_refresh_policy(
            self._runner,
            self._fetch_snapshot,
            self._on_poll_ok,
            self._on_poll_error,
        )

    def _on_poll_ok(self, snapshot) -> None:
        self._poll_in_flight = False
        if not isinstance(snapshot, dict):
            snapshot = {}

        status = str(snapshot.get("status") or "").strip().lower()
        stdlog = str(snapshot.get("stdlog") or "")
        errlog = str(snapshot.get("errlog") or "")

        self._std_cache = self._apply_tail_text(self.out_text, self._std_cache, stdlog)
        self._err_cache = self._apply_tail_text(self.err_text, self._err_cache, errlog)
        self._mark_pull_complete()
        self.status_label.setText(f"Status: {status if status else 'unknown'}")

        if status not in self.ACTIVE_STATUSES:
            self._refresh.stop()
        self._flush_pending_poll()

    def _on_poll_error(self, ex: Exception) -> None:
        self._poll_in_flight = False
        self.status_label.setText(f"Status: poll error ({type(ex).__name__})")
        self._refresh.stop()

    def _flush_pending_poll(self) -> None:
        if not self._poll_pending:
            return
        self._poll_pending = False
        self._poll_if_needed()

    @staticmethod
    def _apply_tail_text(widget: QPlainTextEdit, previous_text: str, next_text: str) -> str:
        prev = str(previous_text or "")
        nxt = str(next_text or "")
        bar = widget.verticalScrollBar()
        at_bottom = bar.value() >= (bar.maximum() - 2)

        if nxt.startswith(prev):
            delta = nxt[len(prev):]
            if delta:
                cursor = widget.textCursor()
                cursor.movePosition(QTextCursor.MoveOperation.End)
                cursor.insertText(delta)
        else:
            widget.setPlainText(nxt)

        if at_bottom:
            bar.setValue(bar.maximum())
        return nxt

    def _mark_pull_complete(self) -> None:
        self._last_pull_utc = dt.datetime.now(dt.timezone.utc)
        self._update_freshness_labels()

    def _update_freshness_labels(self) -> None:
        if self._last_pull_utc is None:
            self.last_pull_label.setText("Last pull: never")
            self.pull_age_label.setText("Age: --")
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
