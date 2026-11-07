import argparse

from .app import main as app_main


def run_stage_main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", required=True)
    parser.add_argument("--open-tab", default="components")
    parser.add_argument("--db-path", default="")
    opts = parser.parse_args(argv)
    stage_label = f"Current Plan Stage: Stage {opts.stage}"
    args = ["--stage-label", stage_label, "--open-tab", opts.open_tab]
    if str(opts.db_path or "").strip():
        args.extend(["--db-path", opts.db_path])
    return app_main(args)


if __name__ == "__main__":
    raise SystemExit(run_stage_main())
