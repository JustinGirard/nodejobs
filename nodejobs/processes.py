import subprocess
import psutil, threading
import os
try:
    from nodejobs.jobdb import JobDB, JobFilter,JobRecord
except:
    from .jobdb import JobDB, JobFilter,JobRecord

from typing import Dict,List
import signal  # add at top of file
import time

class Processes:
    def __init__(self,job_db):
        assert type(job_db) == JobDB
        self.jobdb = job_db
        self._processes: Dict[str, subprocess.Popen] = {}
        threading.Thread(target=self._reap_loop, daemon=True).start()
    
    def _reap_loop(self):
        while True:
            for jid, proc in list(self._processes.items()):
                if proc.poll() is not None:
                    proc.wait()  # reap
                    # optional: update your JobDB here, e.g.
                    # self.jobdb.update_status(jid, proc.returncode)
                    del self._processes[jid]
            time.sleep(1)    

    def run(self, command: str, job_id: str,
            envs: dict = None, cwd: str = None,
            logdir: str = None, logfile: str = None):
        
        #signal.signal(signal.SIGCHLD, signal.SIG_IGN)  # add as first line inside run()
        assert len(job_id) > 0, "Job id is too short. It should be long enough to be unique"
        if envs is None:
            envs = {}
        envs["JOB_ID"] = job_id

        os.makedirs(logdir, exist_ok=True)
        out_path = f"{logdir}/{logfile}_out.txt"
        err_path = f"{logdir}/{logfile}_errors.txt"
        for p in (out_path, err_path):
            if os.path.exists(p):
                os.remove(p)

        # Open log files before launching; OS will keep writing after this function returns
        out_f = open(out_path, "a")
        err_f = open(err_path, "a")
        '''
./Users/computercomputer/.pyenv/versions/3.11.6/lib/python3.11/subprocess.py:1127: ResourceWarning: subprocess 56988 is still running
  _warn("subprocess %s is still running" % self.pid,
ResourceWarning: Enable tracemalloc to get the object allocation traceback        
        
        '''
        process = subprocess.Popen(
            command,
            shell=True,
            cwd=cwd,
            env=envs,
            stdout=out_f,
            stderr=err_f,
            preexec_fn=os.setsid
        )
        try:
            self._processes
        except:
            self._processes = {}    
        self._processes[job_id] = process   
    
        out_f.close()
        err_f.close()
        return  process
    
    
    def find(self,job_id):
        filter = JobFilter({JobFilter.self_id:job_id})
        jobs:Dict[str,JobRecord] = self.jobdb.list_status(filter=filter)
        if len(jobs) == 0 or job_id not in list(jobs.keys()):
            print(f"Process.find found no job record for {job_id}")
            return None
        
        job = JobRecord(jobs[job_id])
        try:
            assert JobRecord.last_pid in job and job.last_pid != None, f"Found a job with a missing pid {job}"
        except:
            print( f"Found a job with a missing pid: {job}")
            return None
        for proc in psutil.process_iter(['pid', 'name', 'environ']):
            if proc.pid == job.last_pid:
                return proc
        return None
        
    def stop(self, job_id):
        for i in [1,2]:
            # Since we run in shell mode, we will have to clean up the shell, too
            proc = self.find(job_id)
            #print("stopping ",proc)
            if proc:
                proc.terminate()
                proc.wait()  
        return True
    

    def list(self):
        """Lists all processes that are currently being managed and their status."""
        filter = JobFilter({})
        jobs:Dict[str,JobRecord] = self.jobdb.list_status(filter=filter)
        jobs_list:List[JobRecord] = list(jobs.values())
        pid_jobs:Dict[int,JobRecord] = {}
        for j in jobs_list:
            j = JobRecord(j)
            pid_jobs[j.last_pid] = j
        procs = []
        for proc in psutil.process_iter(['pid', 'name', 'environ']):
            if  proc.info['pid'] in list(pid_jobs.keys()):
                pid = proc.info['pid']
                proc.job_id = pid_jobs[pid][JobRecord.self_id]
                procs.append(proc)

        return procs
