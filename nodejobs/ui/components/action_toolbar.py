from PyQt6.QtCore import pyqtSignal
from PyQt6.QtWidgets import QHBoxLayout, QPushButton, QWidget


class ActionToolbar(QWidget):
    action_triggered = pyqtSignal(str)

    def __init__(self, parent=None, actions: list[str] | None = None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        self._buttons = {}
        action_ids = actions if actions is not None else ["refresh", "create", "edit", "delete"]
        for action_id in action_ids:
            btn = QPushButton(action_id.capitalize(), self)
            btn.clicked.connect(
                lambda _checked=False, a=action_id: self.action_triggered.emit(a)
            )
            self._buttons[action_id] = btn
            layout.addWidget(btn)

    def set_actions_enabled(self, action_ids: list[str], enabled: bool) -> None:
        for action_id in action_ids:
            btn = self._buttons.get(action_id)
            if btn is None:
                continue
            btn.setEnabled(bool(enabled))
