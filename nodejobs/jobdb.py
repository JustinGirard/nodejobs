import datetime
import sys
sys.path.append("../")
try:
    from decelium_wallet.decelium_wallet.databases.nosqlite import nosqlite
except:
    from decelium_wallet.databases.nosqlite import nosqlite

from decelium_wallet.commands.BaseData import BaseData

class JobRecord(BaseData):
    def __init__(self, in_dict,trim=False):
        if not 'last_update' in in_dict:
            in_dict['last_update'] = datetime.datetime.utcnow()     
        super().__init__(in_dict,trim)    
    
    def get_keys(self):
        required = {'self_id':str,
                    'status':str,
                    'last_update':datetime.datetime,
                    }
        optional = {
                    'dirname':str,
                    'cwd':str,
                    'logdir':str,
                    'logfile':str,
        }
        return required,optional

class JobFilter(BaseData):
    def get_keys(self):
        optional = {'self_id':str,
                     'dirname':str,
                    'logdir':str,
                    'logfile':str,
                    'status':str,
                    'cwd':str,                    
                    'last_update':datetime.datetime,
                    }
        return {},optional

class JobDB():
    JobRecord = JobRecord
    JobFilter = JobFilter
    
    def __init__(self):    
        self.jobdb = nosqlite("/app/database/jobs.db")      
    
    def update_status(self,job):
        clean_job = JobRecord(job)   
        resp = self.jobdb.execute(qtype='upsert', 
                                     source='process_status', 
                                     filterval={"self_id":clean_job['self_id']}, 
                                     setval=clean_job, 
                                     limit=None, 
                                     offset=None, 
                                     field=None)
        return resp
    
    def job_logs(self,self_id):
        resp = self.jobdb.execute(qtype='find', 
                                 source='process_status', 
                                 filterval={'self_id':self_id}, 
                                 setval=None, 
                                 limit=None, 
                                 offset=None, 
                                 field=None)  
        # {logdir}/{logfile}_out.txt 2>> {logdir}/{logfile}_errors.txt "  
        stdlogs = ""
        errlogs = ""
        if len(resp) <= 0:
            stdlogs,errlogs
        doc = resp[0]
        if not 'logdir' in doc:
            return "error: no log dir found", "error: no log dir found"
        if not 'logfile' in doc:
            return "error: no log file found", "error: no log file found"
        logdir = doc['logdir']
        logfile = doc['logfile']
        
        try:
            with open(f"{logdir}/{logfile}_out.txt",'r') as f:
                stdlogs = f.read()
        except:
            stdlogs = "error: could not open "+ f"{logdir}/{logfile}_out.txt"
        try:
            with open(f"{logdir}/{logfile}_errors.txt",'r') as f:
                errlogs = f.read()
        except:
            errlogs = "error: could not open "+ f"{logdir}/{logfile}_errors.txt"
        
        return    stdlogs, errlogs    
        
        
    def list_status(self,filter=None):
        if filter == None:
            clean_filter = {}
        else:
            clean_filter = JobFilter(filter,trim=True)   
        resp = self.jobdb.execute(qtype='find', 
                                 source='process_status', 
                                 filterval=clean_filter, 
                                 setval=None, 
                                 limit=None, 
                                 offset=None, 
                                 field=None)
        jobs_by_id = {}
        for j in resp:
            jobs_by_id[j['self_id']] = j
        return jobs_by_id



