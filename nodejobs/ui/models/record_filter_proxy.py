from PyQt6.QtCore import QSortFilterProxyModel


class RecordFilterProxy(QSortFilterProxyModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._query = ""
        self.setDynamicSortFilter(True)

    def set_query(self, query: str) -> None:
        self._query = (query or "").lower().strip()
        self.invalidateFilter()

    def filterAcceptsRow(self, source_row: int, source_parent) -> bool:
        if not self._query:
            return True
        model = self.sourceModel()
        cols = model.columnCount()
        for c in range(cols):
            idx = model.index(source_row, c, source_parent)
            val = model.data(idx)
            if self._query in str(val).lower():
                return True
        return False
