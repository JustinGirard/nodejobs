from PyQt6.QtCore import QDateTime, Qt
from PyQt6.QtWidgets import QDateTimeEdit, QHBoxLayout, QLabel, QWidget


class DateTimePicker(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        self.editor = QDateTimeEdit(self)
        self.editor.setCalendarPopup(True)
        self.editor.setDisplayFormat("yyyy-MM-dd HH:mm:ss.zzz")
        self.editor.setTimeSpec(Qt.TimeSpec.UTC)
        self.editor.setDateTime(QDateTime.currentDateTimeUtc())
        self.utc_label = QLabel("UTC", self)
        layout.addWidget(self.editor)
        layout.addWidget(self.utc_label)

    def set_iso_text(self, value: str) -> None:
        raw = str(value or "").strip()
        if not raw:
            self.editor.setDateTime(QDateTime.currentDateTimeUtc())
            return
        normalized = raw.replace("Z", "+00:00")
        parsed = QDateTime.fromString(normalized, Qt.DateFormat.ISODateWithMs)
        if not parsed.isValid():
            parsed = QDateTime.fromString(normalized, Qt.DateFormat.ISODate)
        if not parsed.isValid():
            parsed = QDateTime.fromString(raw, Qt.DateFormat.ISODateWithMs)
        if not parsed.isValid():
            parsed = QDateTime.fromString(raw, Qt.DateFormat.ISODate)
        if not parsed.isValid():
            self.editor.setDateTime(QDateTime.currentDateTimeUtc())
            return
        self.editor.setDateTime(parsed.toUTC())

    def iso_text(self) -> str:
        text = self.editor.dateTime().toUTC().toString(Qt.DateFormat.ISODateWithMs)
        if text.endswith("+00:00"):
            text = text[:-6] + "Z"
        return text
