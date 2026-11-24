from typing import Any, Dict
import json
import os
import shutil

from nodejobs.dependencies.raw_command import RawCommand


class PythonCommand(RawCommand):
    def _get_default_fields(self):
        default_fields = {
            "command": {"type": "list"},
            "data_dir": {"type": "str"},
            "cwd": {"type": "str"},
            "job_id": {"type": "str", "default": "python_command_job"},
            "overwrite": {"type": "bool", "default": True},
            "script": {
                "type": "file_editor",
                "c_args": {"default_filename": "script.py"},
            },
            "run_mode": {"type": "str", "default": "same_process"},
        }
        return default_fields

    def create_default(self, default_filename: str = "script.py") -> Dict[str, Any]:
        content = (
            "import json\n"
            "import argparse\n"
            "\n"
            "def main(message: str) -> None:\n"
            "    print(json.dumps({'message': message}))\n"
            "\n"
            "if __name__ == '__main__':\n"
            "    parser = argparse.ArgumentParser()\n"
            "    parser.add_argument('--message', default='')\n"
            "    args = parser.parse_args()\n"
            "    main(message=args.message)\n"
        )
        session = self.data_session
        cache_root = os.path.join(session.get_root(), self.get_rel_cache_path())
        os.makedirs(cache_root, exist_ok=True)
        path = os.path.join(cache_root, default_filename)
        if not os.path.exists(path):
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
        return {"path": path}

    def run_command(self, **kwargs_in: Dict[str, Any]):
        script_value = kwargs_in["script"]
        assert "job_id" in kwargs_in and len(kwargs_in["job_id"]) > 0, (
            "Job id is required now"
        )
        session = self.data_session
        cache_root = os.path.join(session.get_root(), self.get_rel_cache_path())
        os.makedirs(cache_root, exist_ok=True)
        script_path = os.path.join(cache_root, "script.py")

        if "input_path" in kwargs_in:
            kwargs_in["input_path"] = self.resolve_session_paths(kwargs_in["input_path"])

        if os.path.exists(script_value) and os.path.isfile(script_value):
            if os.path.abspath(script_value) != os.path.abspath(script_path):
                shutil.copyfile(script_value, script_path)
        else:
            with open(script_path, "w", encoding="utf-8") as f:
                f.write(script_value)
        run_mode = kwargs_in.get("run_mode", "same_process")
        if run_mode == "same_process":
            import importlib.util

            spec = importlib.util.spec_from_file_location(
                "python_command_mod", script_path
            )
            if spec is None or spec.loader is None:
                raise Exception(f"Cannot load script: {script_path}")
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            if not hasattr(mod, "main"):
                raise Exception(f"Entrypoint 'main' not found in {script_path}")
            try:
                return mod.main(**kwargs_in)
            except Exception as e:
                msg = f"[PythonCommand.same_process] error={e} args={kwargs_in}"
                print(msg)
                raise Exception(msg) from e
        run_query = kwargs_in.copy()
        run_query["command"] = [script_path]
        run_query["script"] = ""
        # print(f"RUNNING {run_query}")
        res = super().run_command(**run_query)
        # print(f"-res {res}")
        return res
