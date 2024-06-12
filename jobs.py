'''
assert proc
assert len(processes.list()) == 1
assert processes.find("sleep_test") 
assert processes.stop("sleep_test")  == True
assert processes.find("sleep_test") == None
assert processes.stop("sleep_test")  == True
assert len(processes.list()) == 0
print("all done")

'''
# - [ ] Write log file for output
# - [ ] Read logfile success input
# - [ ] Read logfile error input
# - [ ] Tail ongoing output
# - [ ] update process status in DB 

from processes import Processes
from jobdb import JobDB
class Jobs():
    def __init__(self):
        self.processes = Processes()
        self.jobdb = JobDB()
        
    def run(self,command:str,job_name:str,job_id:str=None):
        if job_id == None:
            job_id = job_name
        res = self.jobdb.update_status(
                    {'self_id':job_id,
                    'logdir':"empyy",
                    'logfile':"empy",
                    'status':'starting'})
        proc = self.processes.run(command=command,
                             job_name=job_name,
                             job_id=job_id) 
        if proc:
            result = {'self_id':job_id,'status':'running'}
        else:
            result = {'self_id':job_id,'status':'failed_start'}
        db_res = self.jobdb.update_status(result)
        return result
    
    def stop(self,job_id:str):
        res = self.jobdb.update_status(
                    {'self_id':job_id,
                    'logdir':"empyy",
                    'logfile':"empy",
                    'status':'stopping'})
        success = self.processes.stop(job_id=job_id) 
        if success:
            result = {'self_id':job_id,'status':'stopped'}
        else:
            result = {'self_id':job_id,'status':'failed_stop'}
        db_res = self.jobdb.update_status(result)  
        return result
        
    def _update_status(self):
        running_jobs = {}
        for proc in self.processes.list():
            running_jobs[proc.job_id ] = proc 

        # UPDATE new jobs
        running_ids = list(running_jobs.keys())
        for actually_running_id in running_ids:
            res = self.jobdb.update_status({'self_id':actually_running_id,'status':'running'})

        # RETIRE old jobs
        db_running_list = self.jobdb.list_status({'status':"running"})    
        for job_id in db_running_list.keys():
            if job_id not in running_ids:
                # TODO - Review reason for stop to assign correct final status
                res = self.jobdb.update_status(
                                {'self_id':job_id,
                                'logdir':"empyy",
                                'logfile':"empy",
                                'status':'finished'})
    
    def list_status(self,filter=None):
        self._update_status()
        return self.jobdb.list_status(filter)    

    