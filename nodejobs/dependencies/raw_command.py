from typing import Any, List, Dict, Optional
import json, hashlib
from nodejobs.jobs import Jobs, default_db_path
from nodejobs.jobdb import JobRecord
from nodejobs.dependencies.field_set import FieldSet
import sys
import time, shlex, os

class RawCommand(FieldSet): 

    def _get_default_fields(self):
        default_fields = {
            "data_dir": {"type": "str"},
            "cwd": {"type": "str"},
            "command": {"type": "list"},  # list of argv items
            "job_id": {"type": "str"},
            "overwrite": {"type": "bool","default":True},

        }
        return default_fields

    def get_status(
        self, **kwargs_in) -> bool:
        """
        Return the status of a job
        """
        kwargs = self.fill_optional_defaults(kwargs_in)   
        self.jobs = Jobs(db_path=kwargs["data_dir"])
        return self.jobs.get_status(job_id=kwargs["job_id"])    

    def stop_command(
        self, **kwargs_in) -> bool:
        """
        Return the status of a job
        """
        kwargs = self.fill_optional_defaults(kwargs_in)   
        self.jobs = Jobs(db_path=kwargs["data_dir"])
        return self.jobs.stop(job_id=kwargs["job_id"])    
        
    def _wait_for_status(
        self, job_id: str, desired_status: str, timeout: float = 5.0
    ) -> bool:
        """
        Poll list_status() until the job’s status matches desired_status
        or until timeout (in seconds) elapses.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            all_jobs = self.jobs.list_status()
            if job_id in all_jobs:
                rec = JobRecord(all_jobs[job_id])  # runtime type‐check
                if rec.status == desired_status:
                    return True
            time.sleep(0.4)
        return False


    def fill_optional_defaults(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        merged: Dict[str, Any] = dict(kwargs or {})
        if "overwrite" not in merged or merged["overwrite"] is None:
            merged["overwrite"] = False
        # job_id → hash(command + all originally passed args, excluding job_id itself)
        if "job_id" not in merged or merged["job_id"] is None or len(merged["job_id"])==0:
            payload: Dict[str, Any] = {
                "command": merged.get("command"),
                "args": {k: v for k, v in merged.items() if k != "job_id"},
            }
            digest = hashlib.sha256(
                json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()
            merged["job_id"] = digest

        # cwd → session cache dir
        if "cwd" not in merged or merged["cwd"] is None or len(merged["cwd"])==0:
            session = self.data_session
            merged["cwd"] = str(os.path.join(session.get_root(), self.get_rel_cache_path()))
            #print(f" merged[cwd] { merged["cwd"] }")
        # data_dir -> nodejobs canonical default db root
        if "data_dir" not in merged or merged["data_dir"] is None or len(merged["data_dir"])==0 :
            merged["data_dir"] = default_db_path()

        return merged


    def run_command(self, **kwargs_in: Dict[str, Any]):
        assert 'job_id' in kwargs_in and len(kwargs_in["job_id"]) > 0, "job id required now"
        kwargs = self.fill_optional_defaults(kwargs_in)
        # 
        fields = self.get_fields()
        for k in fields.keys():
            if "optional" not in fields[k] or fields[k]["optional"] != True:
                assert k in kwargs, f"Missing required field {k}"
        
        #
        data_dir = kwargs['data_dir']
        command = kwargs['command']
        job_id = kwargs['job_id']
        #print(f"was passed job id {job_id}")
        cwd = kwargs['cwd']
        if isinstance(command,str):
            command = [command]
        assert   data_dir != None, "job db data dir is null"
        assert   command != None, "command is null"
        assert   job_id != None, "job id is null"
        assert   cwd != None, "cwd is null"
        #assert len(command[0]) > 3, "command seems dangerous"
        #
        dec = self._decorate_args(self.get_fields(),kwargs)
        command = list(command)
        command.extend(dec)  
        cmd0 = command[0]
        if isinstance(cmd0, str) and cmd0.lower().endswith(".py"):
            command.insert(0, sys.executable)        
        #return command
        res = self.load_cache_path(job_id)
        if (res!= None and kwargs["overwrite"]==False):
            return res["data"]
        
        npm_bin = "/Users/computercomputer/.nvm/versions/node/v20.11.1/bin"

        envs = {}
        envs["PATH"] = f"{npm_bin}:" + envs.get("PATH", "")


        if sys.platform.startswith("darwin") or sys.platform.startswith("linux"):
            # Minimal system paths so `sh`, `node`, etc. are visible
            envs["PATH"] += ":/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
            envs["SHELL"] = "/bin/sh"
            # Optional but usually harmless:
            envs["HOME"] = os.path.expanduser("~")
        elif os.name == "nt":
            # Windows: make sure cmd.exe can be found
            envs["PATH"] += ";C:\\Windows\\System32;C:\\Windows"
            envs["ComSpec"] = r"C:\Windows\System32\cmd.exe"

        self.jobs = Jobs(db_path=data_dir)
        result = self.jobs.run(
            command=command, 
            job_id=job_id,
            cwd=cwd,
            envs=envs
        )
        result = JobRecord(result)  # runtime type‐check

        time.sleep(1)
        all_jobs = self.jobs.list_status()
        #self.assertIn("t1", all_jobs)
        rec = JobRecord(all_jobs[job_id])
        finished = self._wait_for_status(job_id,
                                         JobRecord.Status.c_finished,
                                         timeout=25.0)
        stdout_text, stderr_text = self.jobs.job_logs(job_id=job_id)
        try:
            out_fields = json.loads(stdout_text)
            res = {
                **out_fields,
                "__finished":finished,
                "__stdout":stdout_text,
                "__stderr":stderr_text,
                "__parse_ex":""

                }

        except Exception as e:
            res = {
                    "__finished":finished,
                    "__stdout":stdout_text,
                    "__stderr":stderr_text,
                    "__parse_ex":str(e)
                    }
        res = self.save_cache_path(job_id,{
            "data":res,
            "query":kwargs_in
        })
        #print(f"Saving job id {job_id}")
        return self.load_cache_path(job_id)["data"]
        
    def _decorate_args(self, fields: Dict[str, Any], kwargs: Dict[str, Any]) -> List[str]:
        cmd: List[str] = []

        for name, spec in fields.items():
            if name in self._get_default_fields():
                continue

            arg_format = spec.get("arg_format")
            if not arg_format:
                continue # Cant add arg without format guidence

            if not name in kwargs:
                continue # Skip formating if the arg is not passed
            value = kwargs[name]

            # flag template (no <{value}>): emit only when truthy
            if "<{value}>" not in arg_format:
                if value:
                    rendered = arg_format.replace("<{argname}>", name)
                    cmd.extend(shlex.split(rendered))
                continue

            # value template: expand list values as repeated args
            if isinstance(value, list):
                for item in value:
                    rendered = (
                        arg_format
                        .replace("<{argname}>", name)
                        .replace("<{value}>", str(item))
                    )
                    cmd.extend(shlex.split(rendered))
            else:
                rendered = (
                    arg_format
                    .replace("<{argname}>", name)
                    .replace("<{value}>", str(value))
                )
                cmd.extend(shlex.split(rendered))

        return cmd
