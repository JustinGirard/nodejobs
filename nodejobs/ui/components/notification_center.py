from PyQt6.QtWidgets import QHBoxLayout, QLabel, QPushButton, QSizePolicy, QWidget


class NotificationCenter(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        self.label = QLabel("", self)
        self.label.setMinimumWidth(0)
        self.label.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        self.clear_button = QPushButton("Clear", self)
        self.clear_button.clicked.connect(self.clear)
        layout.addWidget(self.label)
        layout.addWidget(self.clear_button)

    def show_info(self, text: str) -> None:
        self.label.setText(f"INFO: {text}")

    def show_warning(self, text: str) -> None:
        self.label.setText(f"WARN: {text}")

    def show_error(self, text: str) -> None:
        self.label.setText(f"ERROR: {text}")

    def clear(self) -> None:
        self.label.setText("")
