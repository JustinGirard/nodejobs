import subprocess
import psutil
import os
class Processes:
    def __init__(self):
        pass

    def run(self, command:str,job_name:str,job_id:str=None,envs:dict=None,cwd=None,logdir=None,logfile=None):
        """Starts a new process based on the given command."""
        if job_id == None:
            job_id = job_name
        if envs == None:
            envs = {}
        envs["JOB_NAME"] = job_name
        envs["JOB_ID"] = job_id
        prefix = ""
        proc = self.find(job_id)
        if proc: 
            return proc
        if os.path.exists(f"{logdir}/{logfile}_out.txt"):
            os.remove(f"{logdir}/{logfile}_out.txt")
        if os.path.exists(f"{logdir}/{logfile}_errors.txt"):
            os.remove(f"{logdir}/{logfile}_errors.txt")
        if logfile and logdir:
            command = f" {command} >> {logdir}/{logfile}_out.txt 2>> {logdir}/{logfile}_errors.txt "        
        if cwd:
            print("running command with cwd "+ command + " in "+cwd)            
            process = subprocess.Popen(command, shell=True, text=True, env=envs, cwd=cwd,preexec_fn=os.setsid)
        else:
            process = subprocess.Popen(command, shell=True, text=True, env=envs,preexec_fn=os.setsid)
        
        proc = self.find(job_id)
        if proc: 
            return proc
        return None
    
    def find(self,job_id):
        for proc in psutil.process_iter(['pid', 'name', 'environ']):
            if 'environ' in proc.info and proc.info['environ']:
                if proc.info['environ'].get('JOB_ID') == job_id:
                    return proc
        return None
        
    def stop(self, job_id):
        for i in [1,2]:
            # Since we run in shell mode, we will have to clean up the shell, too
            proc = self.find(job_id)
            print("stopping ",proc)
            if proc:
                proc.terminate()
                proc.wait()  
        return True
        
    def list(self):
        """Lists all processes that are currently being managed and their status."""
        procs = []
        for proc in psutil.process_iter(['pid', 'name', 'environ']):
            if 'environ' in proc.info and proc.info['environ']:            
                jid = proc.info['environ'].get('JOB_ID')
                if jid != None and jid != "":
                    proc.job_id = proc.info['environ'].get('JOB_ID')
                    proc.job_name = proc.info['environ'].get('JOB_NAME')
                    procs.append(proc)
        return procs
