from collections import defaultdict

from PyQt6.QtCore import QObject, QTimer


class RefreshController(QObject):
    def __init__(self, interval_ms: int = 4000, parent=None):
        super().__init__(parent)
        self._listeners = defaultdict(set)
        self.timer = QTimer(self)
        self.timer.setInterval(interval_ms)
        self.timer.timeout.connect(self._on_tick)

    def _on_tick(self) -> None:
        self.trigger("refresh.tick")

    def register(self, topic: str, callback) -> None:
        name = str(topic or "").strip()
        if not name:
            return
        self._listeners[name].add(callback)

    def unregister(self, topic: str, callback) -> None:
        name = str(topic or "").strip()
        if not name:
            return
        callbacks = self._listeners.get(name)
        if not callbacks:
            return
        callbacks.discard(callback)
        if len(callbacks) == 0:
            del self._listeners[name]

    def trigger(self, topic: str) -> None:
        name = str(topic or "").strip()
        if not name:
            return
        callbacks = list(self._listeners.get(name, set()))
        for cb in callbacks:
            cb()

    def start(self) -> None:
        self.timer.start()

    def stop(self) -> None:
        self.timer.stop()

    def trigger_once(self) -> None:
        self.trigger("refresh.tick")
