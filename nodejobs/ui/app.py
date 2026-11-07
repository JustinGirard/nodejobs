import argparse
import os
import sys
from PyQt6.QtWidgets import QApplication

from nodejobs.jobs import default_db_path
from .main_window import MainWindow


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage-label", default="Current Plan Stage: Stage 2")
    parser.add_argument("--open-tab", default="")
    parser.add_argument("--db-path", default=os.environ.get("NODEJOBS_UI_DB_PATH", default_db_path()))
    raw_args = list(argv) if argv is not None else sys.argv[1:]
    opts = parser.parse_args(raw_args)

    app = QApplication(sys.argv)
    window = MainWindow(
        stage_label=opts.stage_label,
        open_tab=opts.open_tab,
        db_path=opts.db_path,
    )
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
