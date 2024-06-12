import subprocess
import psutil
import os
class Processes:
    def __init__(self):
        pass

    def run(self, command:str,job_name:str,job_id:str=None,envs:dict=None):
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
        #for key,value in envs.items():
        #    prefix = prefix + f"{key}={value} "
        #print("start() "+prefix+command)
        #process = subprocess.Popen(prefix+command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,env=envs)
         
        proc = self.find(job_id)
        if proc: 
            return proc
        return None
    
    def find(self,job_id):
        for proc in psutil.process_iter(['pid', 'name', 'environ']):
            if proc.info['environ'].get('JOB_ID') == job_id:
                return proc
        return None
        
    def stop(self, job_id):
        proc = self.find(job_id)
        print("stopping",proc)
        if proc:
            proc.terminate()
            proc.wait()  
        return True
        
    def list(self):
        """Lists all processes that are currently being managed and their status."""
        procs = []
        for proc in psutil.process_iter(['pid', 'name', 'environ']):
            jid = proc.info['environ'].get('JOB_ID')
            if jid != None and jid != "":
                proc.job_id = proc.info['environ'].get('JOB_ID')
                proc.job_name = proc.info['environ'].get('JOB_NAME')
                procs.append(proc)
        return procs
