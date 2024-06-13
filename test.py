#from processes import Processes
#from jobdb import JobDB
from jobs import Jobs
import datetime
import sys
sys.path.append("../")

import time
import pprint
if __name__ == "__main__":
    jobs = Jobs()

    result = jobs.run(command="sleep 3",job_name="sleep_testy")
    assert result['self_id'] == "sleep_testy"
    assert result['status'] == "running"
    result = jobs.list_status()
    #pprint.pprint(result)
    assert 'sleep_testy' in result
    assert result['sleep_testy']['status'] == 'running'
    time.sleep(3)
    result = jobs.list_status()
    #pprint.pprint(result)
    assert 'sleep_testy' in result
    assert result['sleep_testy']['status'] == 'finished'