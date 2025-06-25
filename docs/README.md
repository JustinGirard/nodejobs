<p align="center">
<img width="632" alt="image" src="https://github.com/user-attachments/assets/4cf8dc4e-6daf-4e63-87d8-2ab31f1ada9a" />
</p>

[![CI](https://github.com/JustinGirard/nodejobs/actions/workflows/python-package.yml/badge.svg?branch=master)](https://github.com/JustinGirard/nodejobs/actions/workflows/python-package.yml)

The `nodejobs` repository runs, tracks, and logs data about external commands without requiring a persistent daemon. This means if you have one or more tools relying on backround jobs, they can each collaborate to start up, run, and manage those jobs without some third party process. You likely only ever need to use the 'Jobs' class, however the complete system is quite simple.

### Install
```python
pip install git+https://github.com/JustinGirard/nodejobs/@master
```

### Use
```python
from nodejobs import Jobs, JobRecord

# Create a Jobs manager with a specified database path
jobs_manager = Jobs(db_path="./some/tracking/location")
# * If db_path is not passed, a hard coded directory in the current users home directory will be chosen
job_record = jobs_manager.run(command="python script.py", job_id="job_001")
job_record:JobRecord = jobs_manager.get_status(job_id="job_001")
assert job_record.status == JobRecord.Status.c_finished

stdout:str, stder:str = jobs_manager.job_logs(job_id="job_001")
jobs_manager.stop(job_id="job_001")

```
<p align="center">
<img width="699" alt="image" src="https://github.com/user-attachments/assets/926e70a9-9629-4ca2-8ad2-dba467b19048" />
</p>
It offers a simple API to spawn subprocesses, update job statuses, and persist metadata and logs in flat files that can be cracked open in a pinch, (simply in `JOB_DB/jobname/stdout*.txt` files!)  If you just need a tiny library to run and check on a few jobs along with your script, you might just use `nodejobs`.

As for the code, its clean and extensible-- it has been for us at least. Core components such as `BaseData`, `JobRecord`, and `JobFilter` define and validate the schema for each job records, helping to prevent key-mismatch errors and ensuring data consistency. Status updates occur whenever you invoke a job command, so there’s no background service to manage. Common use cases include automated install scripts, deployment tasks, and data-transfer operations. Designed for minimalism and extensibility, nodejobs can function as a standalone utility or as the foundation for a bespoke job-management solution. If you are looking for a small job running to build on top of, this might be a good fit. Its large enough to have structure, and safety, but small enough you can choose what you want to add in.


### Motivation

It felt silly to write yet another job runner, however I always felt like I needed something more than subprocess, but something way less complex than a full on task managent solution. Importantly, I write code that works on edge devices, and so working towards pi and micropython support is important for me as well. 

These are the motivations:

1. *Light and Lazy by Design*. Most job runners require a daemon running outside python. Running yet another process that raises the complexity of your application. `nodejobs` does not; when you start and stop a nodejob command, it updates the status of all jobs alonside. This means out of the box, with no runtime dependencies, nodejobs can manage and investigate long running processes. 
2. *Typical Use*: We use noejobs to run install scripts, deploy projects, and run data transfer operations: We can simply run a backup command, and if a backup command is fresh, we can skip it. Best is, other programs that know about the job_names can also share the output.
3. **Simple*: Many times job dispatch is part of small utility applications, like a user update script, a patch deployment over servers. Nodejobs allows one to have a small history of all commands run with a few lines of code.
4. *Extensible*: Since nodejobs is so small, it can serve as a good base class or template for a job management utility. 
5. *Observable*: We use it to run jobs on docker, or on servers -- if our python scripts fail, we can easily investigate these logs by connecting remotely and looking at raw stdout and stderr files. All the logs are plaintext, so you dont even need special tools to query the jobs -- just eyes!

### It is not
1. nodejobs is not a process manager. It doesnt work well to manage, restart, or otherwise watch ongoing processes. nodejobs is mainly for dispatch and reporting.
2. nodejobs is not a job database. nodejobs does not handle annotations, or a rich database. 
3. nodejobs is not an event dispatcher. It does not run in the background, and can not notify you or send events when something changes proactively. 

### Job Lifecycle
  When a new job is initiated via `jobs.py.run()`, it triggers `processes.py` to spawn a subprocess. The resulting process ID and metadata are stored in the database (`jobdb.py`) as a `JobRecord`. Status transitions (e.g., queued, running, finished, failed) are updated accordingly, as you query `jobs.get_logs(job_name)`. Logs are written to disk, with file paths stored in job records.

### Job Management Interface (`jobs.py`)

  Serving as the main API layer, this module offers high-level methods for job lifecycle management:
  - `run(command, job_id)-> JobRecord` to spawn new jobs.
  - `stop(job_id) -> None` to send a terminate command to a running processes.
  - `get_status(job_id)-> JobRecord` and `list_status()` for monitoring.
  - `job_logs(job_id)-> List[str,str]` to retrieve process logs.
  
   It coordinates with the database module to update job statuses and track metadata, and leverages data schema classes (`JobRecord`, `JobFilter`) for validation internally and externally. This module acts as the bridge between process control and data persistence.

---
### Detailed Overview

Below are some example use cases you can likely copy and paste into your application to get started. Sometimes the hardest part of getting started with something is to get the tool set up and running.

# 1. **Initializing the Job Management System**

```python
from nodejobs import Jobs

# Initialize the Jobs manager with the database directory
jobs_manager = Jobs(db_path="/path/to/job/database")
```

This sets up the environment for managing jobs, ensuring all job records are stored and retrieved from the specified path.

---

# 2. **Starting a New Job**

```python
# Define the command to run and assign a unique job ID
command = "python my_script.py --arg value"
job_id = "job_12345"

# Launch the job
job_record = jobs_manager.run(command=command, job_id=job_id)

# Access and print job details
print(f"Started job with ID: {job_record.self_id}")
print(f"Initial status: {job_record.status}")
print(f"Process ID: {job_record.last_pid}")
```

This demonstrates how to initiate a job, assign a custom ID, and retrieve initial metadata.

---

# 3. **Checking the Status of a Job**

```python
# Retrieve current status of the job
status_record = jobs_manager.get_status(job_id=job_id)
print(f"Job {status_record.self_id} is {status_record.status}")
```

Allows monitoring of individual job progress and state.

---

### 4. **Listing and Filtering Jobs**

```python
from nodejobs.jobdb import JobRecord

# Filter to find all jobs with status 'running'
filter_criteria = {JobRecord.status: JobRecord.Status.c_running}
running_jobs = jobs_manager.list(filter=filter_criteria)

for job_id, job_info in running_jobs.items():
    print(f"Running job ID: {job_id}, Status: {job_info.status}")
```

Enables batch retrieval of jobs based on criteria like status, self ID patterns, or other fields.

---

### 5. **Retrieving Job Logs**

```python
# Fetch stdout and stderr logs for the job
stdout_log, stderr_log = jobs_manager.job_logs(job_id=job_id)

print("Standard Output:")
print(stdout_log)

print("Standard Error:")
print(stderr_log)
```

Facilitates debugging and auditing by accessing runtime logs.

---

### 6. **Stopping a Running Job**

```python
# Send stop signal to the job
stopped_record = jobs_manager.stop(job_id=job_id)
print(f"Job {stopped_record.self_id} stopped with status: {stopped_record.status}")
```

Provides control over job execution, allowing manual interruption.

---

### 7. **Monitoring and Updating Job Status in a Loop**

```python
import time

while True:
    status = jobs_manager.get_status(job_id=job_id)
    print(f"Current status: {status.status}")
    if status.status in [status.Status.c_finished, status.Status.c_stopped, status.Status.c_failed]:
        break
    time.sleep(5)
```

Supports real-time monitoring and dynamic decision-making based on job state.

---

### 8. **Handling Non-Existent Jobs Gracefully**

```python
# Attempt to get status of a job that doesn't exist
non_existent_status = jobs_manager.get_status("unknown_job_id")
if non_existent_status is None:
    print("Job not found.")
```

Ensures robustness against invalid references and missing records.

---
Enjoy!

