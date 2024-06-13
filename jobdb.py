import datetime
import sys
sys.path.append("../")
try:
    from decelium_wallet.decelium_wallet.databases.nosqlite import nosqlite
except:
    from decelium_wallet.databases.nosqlite import nosqlite

from propagator.type.BaseData import BaseData

class JobRecord(BaseData):
    def __init__(self, in_dict,trim=False):
        if not 'last_update' in in_dict:
            in_dict['last_update'] = datetime.datetime.utcnow()     
        if not 'logdir' in in_dict:
            in_dict['logdir'] = '/app/database/jobs/'
        if not 'logfile' in in_dict:
            in_dict['logfile'] = 'job_status.'+in_dict['self_id']+".txt"
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



