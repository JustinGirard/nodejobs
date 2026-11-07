from PyQt6.QtCore import pyqtSignal
from PyQt6.QtWidgets import QAbstractItemView, QTableView, QVBoxLayout, QWidget
from nodejobs.dependencies.BaseData import BaseData

from ..models import RecordFilterProxy, RecordTableModel


class TableViewPanel(QWidget):
    row_selected = pyqtSignal(object)

    def __init__(self, columns: list[str], rows: list[BaseData], parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        self.model = RecordTableModel(columns, rows, self)
        self.proxy = RecordFilterProxy(self)
        self.proxy.setSourceModel(self.model)
        self.view = QTableView(self)
        self.view.setModel(self.proxy)
        self.view.setSortingEnabled(True)
        self.view.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.view.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.view.selectionModel().selectionChanged.connect(self._on_selection_changed)
        layout.addWidget(self.view)

    def set_query(self, query: str) -> None:
        self.proxy.set_query(query)

    def update_rows_preserving_state(self, rows: list[BaseData], row_key: str = "id") -> None:
        selected_key = self._selected_row_key(row_key)
        selected_col = self._selected_column()
        had_focus = self.view.hasFocus()
        v_scroll = self.view.verticalScrollBar().value()
        h_scroll = self.view.horizontalScrollBar().value()
        did_reset = self.model.update_rows(rows)
        if not did_reset:
            idx = self.view.currentIndex()
            if idx.isValid():
                source_index = self.proxy.mapToSource(idx)
                row_i = source_index.row() if source_index.isValid() else -1
                if 0 <= row_i < len(self.model.rows):
                    self.row_selected.emit(self.model.rows[row_i])
        if did_reset:
            self._restore_selected_row(row_key, selected_key, selected_col)
        self.view.verticalScrollBar().setValue(v_scroll)
        self.view.horizontalScrollBar().setValue(h_scroll)
        if had_focus:
            self.view.setFocus()

    def _selected_column(self) -> int:
        idx = self.view.currentIndex()
        if not idx.isValid():
            return 0
        return max(0, idx.column())

    def _selected_row_key(self, row_key: str) -> str:
        idx = self.view.currentIndex()
        if not idx.isValid():
            return ""
        source_index = self.proxy.mapToSource(idx)
        if not source_index.isValid():
            return ""
        row_i = source_index.row()
        if row_i < 0 or row_i >= len(self.model.rows):
            return ""
        row = self.model.rows[row_i]
        try:
            return str(row[row_key] or "").strip()
        except Exception:
            return ""

    def _restore_selected_row(self, row_key: str, selected_key: str, selected_col: int = 0) -> None:
        key = str(selected_key or "").strip()
        if not key:
            return
        col = selected_col
        if col < 0 or col >= self.model.columnCount():
            col = 0
        for row_i, row in enumerate(self.model.rows):
            try:
                candidate = str(row[row_key] or "").strip()
            except Exception:
                continue
            if candidate != key:
                continue
            source_index = self.model.index(row_i, col)
            proxy_index = self.proxy.mapFromSource(source_index)
            if not proxy_index.isValid():
                return
            self.view.selectRow(proxy_index.row())
            self.view.setCurrentIndex(proxy_index)
            return

    def _on_selection_changed(self, selected, _deselected) -> None:
        indexes = selected.indexes()
        if not indexes:
            return
        source_index = self.proxy.mapToSource(indexes[0])
        row = self.model.rows[source_index.row()]
        self.row_selected.emit(row)
