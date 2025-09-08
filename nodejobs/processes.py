import subprocess
import psutil
import threading
import os
from typing import Dict
import time
import sys
import json
import signal

class Processes:
    def __init__(self, job_db=None, verbose=False):
        self.verbose = verbose
        self._processes: Dict[str, subprocess.Popen] = {}
        threading.Thread(target=self._reap_loop, daemon=True).start()

    def _reap_loop(self):
        while True:
            if self.verbose is True:
                print("reaping ... ", end="")
            for jid, proc in list(self._processes.items()):
                if self.verbose is True:
                    print(f",  {jid}", end="")
                if proc.poll() is not None:
                    proc.wait()  # reap
                    # optional: update your JobDB here, e.g.
                    # self.jobdb.update_status(jid, proc.returncode)
                    del self._processes[jid]
            if self.verbose is True:
                print(".. reaped")

            time.sleep(1)

    def build_run_job_command(
            self,
            job_id: str,
            command: list,
            cwd: str = None,
            envs: dict = None,
            logdir: str = ".") -> list:
        assert type(command) is list, f"Only support list based commands re: {command}. Please adopt a list of strings"
        """
        Service function for tests: writes a {job_id}.json spec into logdir
        and returns the list of arguments to invoke run_job.py.
        """
        if envs is None:
            envs = {}
        # envs["JOB_ID"] = job_id

        os.makedirs(logdir, exist_ok=True)
        spec = {
            "command": command,
            "cwd": cwd,
            "job_id": job_id,
            "envs": envs
        }
        spec_path = os.path.join(logdir, f"{job_id}.json")
        with open(spec_path, "w") as f:
            json.dump(spec, f)

        wrapper = os.path.join(os.path.dirname(__file__), "run_job.py")
        assert os.path.exists(wrapper), f"Cant find the run job kernel {wrapper}"
        cmd = [sys.executable, wrapper, "--job_id", job_id, "--json_path", spec_path]

        # print("===== DEBUG RUN INFO =====")
        # print(" Job ID       :", job_id)
        # print(" Wrapper      :", wrapper, "exists?", os.path.exists(wrapper))
        # print(" Spec JSON    :", spec_path,    "exists?", os.path.exists(spec_path))
        # print(" Working dir  :", cwd,           "exists?", os.path.isdir(cwd))
        # print(" Log dir      :", logdir,        "exists?", os.path.isdir(logdir))
        # print(" Env vars     :", envs)
        # print(" Full command :", command)
        # print("===========================")
        return cmd

    def run(
        self,
        command: list,
        job_id: str,
        envs: dict = None,
        cwd: str = None,
        logdir: str = None,
        logfile: str = None,
    ):

        assert (
            len(job_id) > 0
        ), "Job id is too short. It should be long enough to be unique"
        if envs is None:
            envs = {}

        os.makedirs(logdir, exist_ok=True)
        out_path = f"{logdir}/{logfile}_out.txt"
        err_path = f"{logdir}/{logfile}_errors.txt"
        for p in (out_path, err_path):
            if os.path.exists(p):
                os.remove(p)

        out_f = open(out_path, "a")
        err_f = open(err_path, "a")

        command = self.build_run_job_command(
                            job_id=job_id,
                            command=command,
                            cwd=cwd,
                            envs=envs,
                            logdir=logdir)

        process = subprocess.Popen(
            command,
            shell=False,
            cwd=cwd,
            env=envs,
            stdout=out_f,
            stderr=err_f,
            preexec_fn=os.setsid,
            stdin=subprocess.DEVNULL,          
        )
        try:
            self._processes
        except Exception:
            self._processes = {}
        self._processes[job_id] = process

        out_f.close()
        err_f.close()
        return process

    def find(self, job_id):
        for proc in psutil.process_iter(["pid", "cmdline"]):
           cmdline = proc.info.get("cmdline") or []
           if job_id in cmdline:
               return proc
        return None


    # def stop(self, job_id):
    #     proc = self.find(job_id)
    #     if not proc:
    #         return False
    #     try:
    #         print(f"killing main proc ... {proc}")
    #         print(proc)
    #         os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    #     except Exception as e:
    #         print(f"killpg error: {e}")
    #         pass        
    #     try: 
    #         proc.wait(timeout=1)
    #     except Exception as e:
    #         print(f" proc.wait error: {e}")

    #     # 1) kill descendants first
    #     children = proc.children(recursive=True)
    #     for c in children:
    #         print(f"killing child: {c}")
    #         try: 
    #             c.terminate()
    #         except Exception as e:
    #             print(f"killing child failed: {e}")
    #     gone, alive = psutil.wait_procs(children, timeout=1)
    #     for c in alive:
    #         c.kill()

    #     # 2) kill the wrapper itself
    #     try:
    #         proc.terminate()
    #         proc.wait(timeout=1)
    #     except Exception as e:
    #         print(f"terminate error: {e}")
    #         pass        
    #     if proc.is_running():
    #         try:
    #             os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    #             proc.wait(timeout=1)
    #         except Exception:
    #             try:
    #                 proc.kill()
    #             except Exception as e:
    #                 print(f"kill error: {e}")
    #                 pass        
    #     return True

    # def list(self):
    #     procs = []
    #     for proc in psutil.process_iter(['pid', 'cmdline']):
    #         try:
    #             cmd = ' '.join(proc.info.get('cmdline') or [])
    #             if 'run_job.py' in cmd:
    #                 procs.append(proc)
    #         except (psutil.NoSuchProcess, psutil.AccessDenied):
    #             continue
    #     return procs

    def list(self):
        procs = []
        for proc in psutil.process_iter(['pid', 'cmdline']):
            try:
                parts = proc.info.get('cmdline') or []
                whole_cmd = ' '.join(parts)
                if 'run_job.py' in whole_cmd and '--job_id' in parts:
                    if proc.info.get('status') == psutil.STATUS_ZOMBIE:
                        continue  # <- ignore defunct entries                    
                    idx = parts.index('--job_id')
                    if idx + 1 < len(parts):
                        proc.job_id = parts[idx + 1]
                        procs.append(proc)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return procs

    #
    #
    #
    #
    #

    def _sigterm_then_sigkill_proc(self, proc: psutil.Process, label: str, verbose: bool, grace: float = 3.0):
        try:
            if verbose: print(f"[{label}] a. SIGTERM pid={proc.pid}")
            proc.terminate()
        except Exception as e:
            if verbose: print(f"[{label}] b. SIGTERM failed pid={getattr(proc,'pid','?')} err={e}")
        try:
            proc.wait(timeout=grace)
            if verbose: print(f"[{label}] c. exited rc={getattr(proc, 'returncode', '?')}")
            return
        except Exception:
            pass

        # escalate
        try:
            if verbose: print(f"[{label}] d. SIGKILL pid={proc.pid}")
            proc.kill()
        except Exception as e:
            if verbose: print(f"[{label}] e. SIGKILL failed pid={getattr(proc,'pid','?')} err={e}")
        try:
            proc.wait(timeout=grace)
            if verbose: print(f"[{label}] f. killed rc={getattr(proc, 'returncode', '?')}")
        except Exception as e:
            if verbose: print(f"[{label}] g. STILL ALIVE after SIGKILL err={e}")

    def _sigterm_then_sigkill_pgid(self, pid: int, label: str, verbose: bool, grace: float = 2.0):
        try:
            pgid = os.getpgid(pid)
        except Exception as e:
            if verbose: print(f"[{label}] h. getpgid failed pid={pid} err={e}")
            return
        try:
            if verbose: print(f"[{label}] i. SIGTERM pgid={pgid}")
            os.killpg(pgid, signal.SIGTERM)
        except Exception as e:
            if verbose: print(f"[{label}] j. SIGTERM pgid failed pgid={pgid} err={e}")
        time.sleep(grace)
        # escalate
        try:
            if verbose: print(f"[{label}] k. SIGKILL pgid={pgid}")
            os.killpg(pgid, signal.SIGKILL)
        except Exception as e:
            if verbose: print(f"[{label}] l. SIGKILL pgid failed pgid={pgid} err={e}")

    def stop(self, job_id, verbose: bool = None):
        '''
        Totally psycho stop method that may be ugly, and repetitive, but is extremely easy to debug.
        '''
        #verbose = True
        verbose = self.verbose if verbose is None else verbose
        proc = self.find(job_id)
        if not proc:
            if verbose: print(f"[ m. stop:{job_id}] no wrapper proc found")
            return False

        # Snapshot & log
        try:
            cmd = ' '.join(proc.cmdline() or [])
        except Exception:
            cmd = str(proc)
        if verbose: print(f"[ n. stop:{job_id}] wrapper pid={proc.pid} cmd='{cmd}'")

        # 1) Children-first (avoid orphaning). One pass + re-fetch for late spawns.
        def _kill_children(pass_idx: int):
            try:
                kids = proc.children(recursive=True)
            except Exception:
                kids = []
            if verbose:
                print(f"[o. stop:{job_id}] pass={pass_idx} children={ [k.pid for k in kids] }")
            for k in kids:
                try:
                    self._sigterm_then_sigkill_proc(k, label=f"stop:{job_id}:child:{k.pid}:p{pass_idx}", verbose=verbose)
                except psutil.NoSuchProcess:
                    if verbose: print(f"[ p. stop:{job_id}:child:{k.pid}:p{pass_idx}] already gone")
                except Exception as e:
                    if verbose: print(f"[ q. stop:{job_id}:child:{k.pid}:p{pass_idx}] error={e}")
            return kids

        _kill_children(1)
        time.sleep(0.1)
        _kill_children(2)

        # 2) Wrapper (parent) last
        try:
            if proc.is_running():
                self._sigterm_then_sigkill_proc(proc, label=f"stop:{job_id}:parent:{proc.pid}", verbose=verbose)
        except psutil.NoSuchProcess:
            if verbose: print(f"[r. stop:{job_id}] parent already exited")
        except Exception as e:
            if verbose: print(f"[s. stop:{job_id}] parent kill error={e}")

        # 3) Catch-all: process-group sweep (handles any stragglers in same PGID)
        try:
            self._sigterm_then_sigkill_pgid(proc.pid, label=f"t. stop:{job_id}:pg", verbose=verbose)
        except Exception as e:
            if verbose: print(f"[u. stop:{job_id}] pg sweep error={e}")

        # 4) Verify
        stuck = []
        try:
            if proc.is_running():
                stuck.append(proc.pid)
        except psutil.NoSuchProcess:
            pass
        try:
            for k in proc.children(recursive=True):
                try:
                    if k.is_running():
                        stuck.append(k.pid)
                except psutil.NoSuchProcess:
                    pass
        except Exception:
            pass

        if verbose:
            if stuck:
                print(f"[v. stop:{job_id}] WARNING: still running pids={stuck}")
            else:
                print(f"[w. stop:{job_id}] all processes terminated")

        # cleanup local registry (best-effort)
        # try:
        #     self._processes.pop(job_id, None)
        # except Exception:
        #     pass
        ph = self._processes.pop(job_id, None)
        if ph is not None:
            try:
                ph.wait(timeout=1)
            except Exception:
                pass
        return len(stuck) == 0
