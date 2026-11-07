from concurrent.futures import ThreadPoolExecutor

from PyQt6.QtCore import QObject, pyqtSignal


class _TaskSignals(QObject):
    success = pyqtSignal(object)
    error = pyqtSignal(object)


class TaskRunner:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=2)

    def run(self, fn, on_success, on_error) -> None:
        signals = _TaskSignals()
        signals.success.connect(on_success)
        signals.error.connect(on_error)
        future = self.executor.submit(fn)

        def _done(fut):
            try:
                result = fut.result()
                signals.success.emit(result)
            except Exception as ex:
                signals.error.emit(ex)

        future.add_done_callback(_done)
