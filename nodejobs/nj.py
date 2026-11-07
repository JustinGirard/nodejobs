import importlib.util
import json
import os
import shutil
import subprocess
import sys

from nodejobs.dependencies.BaseData import BaseData
from nodejobs.dependencies.BaseService import BaseService
from nodejobs.jobs_service import JobsService
from nodejobs.schedules_service import SchedulesService


class NJServiceResponse(BaseData):
    ok: bool
    method: str
    result: (object, None)
    error: (str, None)


class NJService(BaseService):
    @classmethod
    def get_command_map(cls):
        return {
            "nj": {"required_args": ["service", "op"], "method": cls.route_nj},
            "build": {"required_args": [], "method": cls.build},
        }

    @classmethod
    def _ok(cls, method: str, result):
        return NJServiceResponse(
            {"ok": True, "method": method, "result": result, "error": None}
        )

    @classmethod
    def _err(cls, method: str, error: Exception):
        return NJServiceResponse(
            {"ok": False, "method": method, "result": None, "error": str(error)}
        )

    @classmethod
    def _as_bool(cls, value) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value or "").strip().lower()
        return text in ("1", "true", "yes", "on")

    @classmethod
    def _json_safe(cls, value):
        if isinstance(value, BaseData):
            value = dict(value)
        if isinstance(value, dict):
            return {str(k): cls._json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [cls._json_safe(v) for v in value]
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        return str(value)

    @classmethod
    def _augment_build_response_for_humans(cls, envelope: dict) -> dict:
        method = str(envelope.get("method") or "").strip().lower()
        if method != "build":
            return envelope

        result = envelope.get("result")
        result = result if isinstance(result, dict) else {}

        app_name = str(result.get("app_name") or "Nodejobs")
        app_path = str(result.get("app_path") or "")
        project_root = str(result.get("project_root") or "")
        entry_module = str(result.get("entry_module") or "")
        launcher_path = str(result.get("launcher_path") or "")
        executed = cls._as_bool(result.get("executed"))
        ok = cls._as_bool(envelope.get("ok"))

        if ok and executed:
            status = "success"
            message = "Build completed successfully. App bundle created."
        elif ok and not executed:
            status = "dry_run"
            message = "Build dry-run completed. No files were written."
        else:
            status = "error"
            message = "Build failed. See error for details."

        open_cmd = f'open "{app_path}"' if app_path else ""
        copy_cmd = f'cp -R "{app_path}" "/Applications/{app_name}.app"' if app_path else ""

        envelope["status"] = status
        envelope["message"] = message
        envelope["artifacts"] = {
            "app_bundle_path": app_path,
            "project_root": project_root,
            "entry_module": entry_module,
            "launcher_path": launcher_path,
        }
        envelope["how_to_use"] = {
            "summary": "Launch the .app bundle directly, optionally copy to /Applications, then keep it in Dock.",
            "launch_command": open_cmd,
            "install_to_applications_command": copy_cmd,
            "dock_steps": [
                "Launch the app once.",
                "Right-click the app icon in Dock.",
                "Choose Options -> Keep in Dock.",
            ],
        }
        if isinstance(result.get("command"), list):
            envelope["how_to_use"]["build_command_used"] = result.get("command")
        return envelope

    @classmethod
    def run_cli(cls):
        kwargs = cls.parse_args()
        result = cls.run(**kwargs)
        envelope = cls._json_safe(result)
        if not isinstance(envelope, dict):
            envelope = {
                "ok": True,
                "method": "unknown",
                "result": envelope,
                "error": None,
            }
        envelope = cls._augment_build_response_for_humans(envelope)
        print(json.dumps(envelope, ensure_ascii=True, indent=2))

    @classmethod
    def build(cls, dry_run=False, **kwargs):
        try:
            dry_run = cls._as_bool(dry_run)
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            app_name = "Nodejobs"
            dist_dir = os.path.join(project_root, "dist")
            work_dir = os.path.join(project_root, "build", "pyinstaller")
            spec_dir = os.path.join(work_dir, "spec")
            app_path = os.path.join(dist_dir, f"{app_name}.app")
            entry_module = "nodejobs.ui.app"
            if importlib.util.find_spec(entry_module) is None:
                raise RuntimeError(f"UI entry module not found: {entry_module}")
            launcher_path = os.path.join(work_dir, "_nodejobs_ui_launcher.py")

            command = [
                sys.executable,
                "-m",
                "PyInstaller",
                "--noconfirm",
                "--clean",
                "--windowed",
                "--name",
                app_name,
                "--distpath",
                dist_dir,
                "--workpath",
                work_dir,
                "--specpath",
                spec_dir,
                launcher_path,
            ]

            result = {
                "app_name": app_name,
                "app_path": app_path,
                "project_root": project_root,
                "command": command,
                "executed": False,
                "entry_module": entry_module,
                "launcher_path": launcher_path,
            }
            if dry_run:
                return cls._ok("build", result)

            if importlib.util.find_spec("PyInstaller") is None:
                raise RuntimeError("PyInstaller not installed. Install with: pip install pyinstaller")

            for target in (os.path.join(dist_dir, app_name), app_path, work_dir):
                if os.path.isdir(target):
                    shutil.rmtree(target, ignore_errors=True)
                elif os.path.exists(target):
                    os.remove(target)

            os.makedirs(work_dir, exist_ok=True)
            os.makedirs(spec_dir, exist_ok=True)
            launcher_src = (
                "from nodejobs.ui.app import main\n\n"
                "if __name__ == '__main__':\n"
                "    raise SystemExit(main())\n"
            )
            with open(launcher_path, "w", encoding="utf-8") as f:
                f.write(launcher_src)

            proc = subprocess.run(
                command,
                cwd=project_root,
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                stdout_tail = (proc.stdout or "").strip()[-1200:]
                stderr_tail = (proc.stderr or "").strip()[-1200:]
                details = "\n".join(p for p in (stdout_tail, stderr_tail) if p).strip()
                raise RuntimeError(
                    f"Build failed (exit={proc.returncode}). {details}"
                )
            if not os.path.isdir(app_path):
                raise RuntimeError(f"Build finished but app missing: {app_path}")

            result["executed"] = True
            result["return_code"] = proc.returncode
            return cls._ok("build", result)
        except Exception as ex:
            return cls._err("build", ex)

    @classmethod
    def route_nj(cls, service, op, **kwargs):
        route = str(service).strip().lower()
        op_id = str(op).strip().lower()
        child_kwargs = dict(kwargs)
        child_kwargs["__command"] = [str(op).strip()]
        if route == "jobs":
            return JobsService.run(**child_kwargs)
        if route == "schedules":
            return SchedulesService.run(**child_kwargs)
        if route == "admin" and op_id == "build":
            return cls.build(**kwargs)
        return NJServiceResponse({
            "ok": False,
            "method": "nj",
            "result": None,
            "error": f"Unknown service: {service}",
        })


if __name__ == "__main__":
    NJService.run_cli()
