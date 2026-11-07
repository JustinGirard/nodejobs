from PyQt6.QtCore import pyqtSignal
from PyQt6.QtWidgets import (
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QComboBox,
    QPushButton,
    QSizePolicy,
    QWidget,
)


class ConnectionBar(QWidget):
    connection_requested = pyqtSignal(str)
    disconnect_requested = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QHBoxLayout(self)

        self.path_label = QLabel("DB Path", self)
        self.path_input = QComboBox(self)
        self.path_input.setEditable(True)
        self.path_input.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.path_input.setSizeAdjustPolicy(
            QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon
        )
        self.path_input.setMinimumContentsLength(24)
        line_edit = self.path_input.lineEdit()
        if line_edit is not None:
            line_edit.setPlaceholderText("Select nodejobs DB directory")
        self.browse_button = QPushButton("Browse", self)
        self.connect_button = QPushButton("Connect", self)
        self.disconnect_button = QPushButton("Disconnect", self)
        self.status_label = QLabel("Disconnected", self)
        self.status_label.setMinimumWidth(0)
        self.status_label.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)

        layout.addWidget(self.path_label)
        layout.addWidget(self.path_input, 1)
        layout.addWidget(self.browse_button)
        layout.addWidget(self.connect_button)
        layout.addWidget(self.disconnect_button)
        layout.addWidget(self.status_label, 1)

        self.browse_button.clicked.connect(self._choose_directory)
        self.connect_button.clicked.connect(self._emit_connect)
        self.disconnect_button.clicked.connect(self.disconnect_requested.emit)
        self.status_label.setToolTip("")

    def _choose_directory(self) -> None:
        selected = QFileDialog.getExistingDirectory(self, "Select nodejobs DB directory")
        if not selected:
            return
        self.set_path(selected)

    def _emit_connect(self) -> None:
        self.connection_requested.emit(self.db_path())

    def db_path(self) -> str:
        return str(self.path_input.currentText() or "").strip()

    def set_known_paths(self, paths: list[str]) -> None:
        current = self.db_path()
        known = []
        for raw in paths:
            text = str(raw or "").strip()
            if text and text not in known:
                known.append(text)
        self.path_input.blockSignals(True)
        self.path_input.clear()
        self.path_input.addItems(known)
        if current:
            if self.path_input.findText(current) < 0:
                self.path_input.addItem(current)
            self.path_input.setCurrentText(current)
        self.path_input.blockSignals(False)

    def set_path(self, db_path: str) -> None:
        text = str(db_path or "").strip()
        if text and self.path_input.findText(text) < 0:
            self.path_input.addItem(text)
        self.path_input.setCurrentText(text)

    def set_connected(self, connected: bool, message: str = "") -> None:
        state = "Connected" if connected else "Disconnected"
        raw = str(message or "").strip()
        self.status_label.setToolTip(raw)
        if not raw:
            self.status_label.setText(state)
            return
        if len(raw) > 72:
            raw = "..." + raw[-69:]
        self.status_label.setText(f"{state}: {raw}")
