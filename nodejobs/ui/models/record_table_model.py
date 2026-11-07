from nodejobs.dependencies.BaseData import BaseData
from PyQt6.QtCore import QAbstractTableModel, QModelIndex, Qt


class RecordTableModel(QAbstractTableModel):
    def __init__(self, columns: list[str], rows: list[BaseData], parent=None):
        super().__init__(parent)
        self.columns = columns
        self.rows = rows

    def rowCount(self, parent=QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self.rows)

    def columnCount(self, parent=QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self.columns)

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        if orientation == Qt.Orientation.Horizontal and 0 <= section < len(self.columns):
            return self.columns[section]
        return None

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid() or role != Qt.ItemDataRole.DisplayRole:
            return None
        row = self.rows[index.row()]
        key = self.columns[index.column()]
        value = row[key]
        if value is None:
            return ""
        return str(value)

    def update_rows(self, rows: list[BaseData]) -> bool:
        if len(rows) == len(self.rows):
            self.rows = rows
            if len(rows) == 0 or len(self.columns) == 0:
                self.layoutChanged.emit()
                return False
            top_left = self.index(0, 0)
            bottom_right = self.index(len(rows) - 1, len(self.columns) - 1)
            self.dataChanged.emit(top_left, bottom_right, [Qt.ItemDataRole.DisplayRole])
            return False
        self.beginResetModel()
        self.rows = rows
        self.endResetModel()
        return True
