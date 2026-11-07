from PyQt6.QtWidgets import QDialog, QDialogButtonBox, QFormLayout, QLineEdit

class EditDialog(QDialog):
    def __init__(self, record, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Edit Record")
        self._fields = {}
        self._record_type = record.__class__ if record is not None else dict
        layout = QFormLayout(self)
        for key, value in (record or {}).items():
            field = QLineEdit(str(value), self)
            self._fields[key] = field
            layout.addRow(key, field)
        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel,
            self,
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addRow(buttons)

    def get_payload(self):
        payload = {k: field.text() for k, field in self._fields.items()}
        try:
            return self._record_type(payload)
        except Exception:
            return payload
