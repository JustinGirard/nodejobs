from PyQt6.QtWidgets import QLabel, QVBoxLayout, QWidget

from ..types.ui_rows import UiJobRow


class DetailPane(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        self.title = QLabel("Details", self)
        self.body = QLabel("No selection", self)
        self.body.setWordWrap(True)
        layout.addWidget(self.title)
        layout.addWidget(self.body)

    def set_record(self, record: UiJobRow | dict) -> None:
        if not record:
            self.body.setText("No selection")
            return
        pairs = [f"{k}: {v}" for k, v in record.items()]
        self.body.setText("\n".join(pairs))
