from PyQt6.QtCore import QObject, pyqtSignal

from ..types.ui_rows import UiJobRow


class SelectionController(QObject):
    selection_changed = pyqtSignal(object)

    def __init__(self, parent=None, row_factory=UiJobRow):
        super().__init__(parent)
        self.row_factory = row_factory
        self._selected = self.row_factory({})

    def set_selected(self, record) -> None:
        self._selected = self.row_factory(record or {})
        self.selection_changed.emit(self._selected)

    def get_selected(self):
        return self.row_factory(self._selected)
