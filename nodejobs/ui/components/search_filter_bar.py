from PyQt6.QtCore import pyqtSignal
from PyQt6.QtWidgets import QHBoxLayout, QLineEdit, QPushButton, QWidget


class SearchFilterBar(QWidget):
    query_changed = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        self.search_input = QLineEdit(self)
        self.search_input.setPlaceholderText("Search...")
        self.clear_button = QPushButton("Clear", self)
        layout.addWidget(self.search_input)
        layout.addWidget(self.clear_button)
        self.search_input.textChanged.connect(self.query_changed.emit)
        self.clear_button.clicked.connect(self.search_input.clear)
