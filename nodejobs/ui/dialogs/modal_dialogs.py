from PyQt6.QtWidgets import QMessageBox, QWidget


class ModalDialogs:
    @staticmethod
    def confirm_remove(parent: QWidget | None, entity: str, item_id: str) -> bool:
        entity_text = str(entity or "item").strip()
        id_text = str(item_id or "").strip()
        box = QMessageBox(parent)
        box.setIcon(QMessageBox.Icon.Warning)
        box.setWindowTitle("Confirm Remove")
        box.setText(f"Remove {entity_text}?")
        if len(id_text) > 0:
            box.setInformativeText(f"This action permanently removes {entity_text} id={id_text}.")
        else:
            box.setInformativeText(f"This action permanently removes the selected {entity_text}.")
        box.setStandardButtons(
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        box.setDefaultButton(QMessageBox.StandardButton.No)
        return box.exec() == QMessageBox.StandardButton.Yes

    @staticmethod
    def show_blocking_error(
        parent: QWidget | None,
        title: str,
        message: str,
        details: str = "",
    ) -> None:
        box = QMessageBox(parent)
        box.setIcon(QMessageBox.Icon.Critical)
        box.setWindowTitle(str(title or "Error"))
        box.setText(str(message or "unknown error"))
        details_text = str(details or "").strip()
        if len(details_text) > 0:
            box.setDetailedText(details_text)
        box.exec()
