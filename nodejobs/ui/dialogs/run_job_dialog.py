import json
import os

from PyQt6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLineEdit,
)


class RunJobDialog(QDialog):
    def __init__(
        self,
        parent=None,
        title: str = "Run Job",
        submit_label: str = "Run",
        initial: dict | None = None,
        job_id_read_only: bool = False,
    ):
        super().__init__(parent)
        self.setWindowTitle(str(title or "Run Job"))
        seed = initial if isinstance(initial, dict) else {}

        layout = QFormLayout(self)
        self.job_id_input = QLineEdit(str(seed.get("job_id") or "").strip(), self)
        self.command_input = QLineEdit(self._command_to_text(seed.get("command")), self)
        seed_cwd = str(seed.get("cwd") or "").strip() or os.getcwd()
        self.cwd_input = QLineEdit(seed_cwd, self)
        if bool(job_id_read_only):
            self.job_id_input.setReadOnly(True)
            self.job_id_input.setToolTip("job_id is immutable in edit mode")
        self.envs_input = QLineEdit(self._envs_to_text(seed.get("envs")), self)
        self.tags_input = QLineEdit(self._tags_to_text(seed.get("tags")), self)

        self.job_id_input.setPlaceholderText("job_id")
        self.command_input.setPlaceholderText("command (space separated or JSON list)")
        self.envs_input.setPlaceholderText("envs JSON object (optional)")
        self.tags_input.setPlaceholderText("tags JSON object (value==key sets, null/\"\" deletes)")

        layout.addRow("job_id", self.job_id_input)
        layout.addRow("command", self.command_input)
        layout.addRow("cwd", self.cwd_input)
        layout.addRow("envs_json", self.envs_input)
        layout.addRow("tags_json", self.tags_input)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel,
            self,
        )
        save_btn = buttons.button(QDialogButtonBox.StandardButton.Save)
        if save_btn is not None:
            save_btn.setText(str(submit_label or "Run"))
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addRow(buttons)

    def _command_to_text(self, value) -> str:
        if isinstance(value, list):
            return " ".join([str(v).strip() for v in value if str(v).strip()])
        return str(value or "").strip()

    def _envs_to_text(self, value) -> str:
        if not isinstance(value, dict) or len(value) == 0:
            return ""
        return json.dumps(value, separators=(",", ":"))

    def _tags_to_text(self, value) -> str:
        if not isinstance(value, dict):
            return "{}"
        return json.dumps(value, separators=(",", ":"))

    def _parse_command(self, text: str) -> list[str]:
        raw = str(text or "").strip()
        if not raw:
            return []
        if raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = None
            if isinstance(parsed, list):
                return [str(v).strip() for v in parsed if str(v).strip()]
        return [part for part in raw.split(" ") if part]

    def _parse_envs(self, text: str):
        raw = str(text or "").strip()
        if not raw:
            return None
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError("envs_json must be a JSON object")
        return parsed

    def _parse_tags(self, text: str) -> dict:
        raw = str(text or "").strip()
        if not raw:
            return {}
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError("tags_json must be a JSON object")
        out = {}
        for key, val in parsed.items():
            key_text = str(key or "").strip()
            if not key_text:
                continue
            if val is None:
                out[key_text] = None
            elif isinstance(val, str):
                out[key_text] = val.strip()
            else:
                out[key_text] = str(val).strip()
        return out

    def get_payload(self) -> dict:
        return {
            "job_id": str(self.job_id_input.text() or "").strip(),
            "command": self._parse_command(self.command_input.text()),
            "cwd": str(self.cwd_input.text() or "").strip(),
            "envs": self._parse_envs(self.envs_input.text()),
            "tags": self._parse_tags(self.tags_input.text()),
        }
