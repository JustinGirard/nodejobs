# Automating Database Backups with nodejobs

## Introduction
This example demonstrates how to orchestrate multiple database backup jobs using the `nodejobs` module. It showcases how to spawn individual backup processes with unique job names, monitor their progress, and aggregate their logs upon completion. The approach emphasizes best practices such as using `nodejobs.Job`, `JobRecord`, and related classes with named arguments, ensuring robust and maintainable job management.

## Step-by-step Walkthrough

### 1. Key Stage: Initialize Job Manager
The first step sets up the job management environment by creating an instance of `nodejobs.Jobs`. This object manages all job executions, providing methods to spawn, monitor, and control jobs.

```python
# Initialize the job manager with a directory for the database
self.jobs = nodejobs.Jobs(db_path=os.path.dirname(self.report_path))
```
This initializes the `Jobs` class with a specified path, preparing the system to run and track jobs.

---

### 2. Key Stage: Generate Unique Job Name
To prevent conflicts during concurrent executions, each backup job is assigned a unique name composed of a prefix, the current date, and a specific job identifier.

```python
# Generate a unique job name based on prefix, date, and job ID
current_date = datetime.utcnow().strftime("%Y%m%d")
unique_job_name = f"{self.backup_job_prefix}_{job_id}_{current_date}"
```
This ensures each job has a distinct identifier.

---

### 3. Key Stage: Spawn Backup Jobs
Using `nodejobs.Job.run()`, the script launches a new backup process with a specified command and the generated unique job name. This returns a `JobRecord` that can be used to monitor the job's status.

```python
# Spawn a new backup job with a unique job name
job_record = self.jobs.run(
    command=self.db_backup_command,
    job_id=unique_job_name
)
```
This method call uses named arguments to maintain clarity and consistency, following best practices.

---

### 4. Key Stage: Monitor Job Status
Once jobs are launched, it's essential to verify they are running and then wait for their completion. The script polls their status periodically, checking for `c_finished` or `c_failed` statuses, with a timeout to prevent indefinite waiting.

```python
# Poll each job until it completes or fails, with a timeout
start_time = datetime.utcnow()
while True:
    status_response = self.jobs.get_status(
        job_id=job_record.f_self_id
    )
    current_status = status_response.f_status
    if current_status in (JobRecord.Status.c_finished, JobRecord.Status.c_failed):
        break
    elapsed_time = (datetime.utcnow() - start_time).total_seconds()
    if elapsed_time > 300:
        self.jobs.stop(
            job_id=job_record.f_self_id
        )
        break
    time.sleep(0.5)
```
This ensures robust monitoring and graceful handling of long-running jobs.

---

### 5. Key Stage: Retrieve and Aggregate Logs
After jobs complete, their standard output logs are retrieved using `Job.job_logs()`. These logs are then collected for report generation.

```python
# Retrieve logs for each completed job
stdout_log, _ = self.jobs.job_logs(
    job_id=job_record.f_self_id
)
self.backup_outputs.append(stdout_log)
```
Logs from all jobs are stored in a list for later aggregation.

---

### 6. Key Stage: Compile and Save Report
Finally, all collected logs are written into a report file, providing a consolidated view of backup outputs.

```python
# Write all logs into the report file
with open(file=self.report_path, mode='w') as report_file:
    for output in self.backup_outputs:
        report_file.write(output + "\n\n")
```
This step consolidates the backup outputs for review.

---

### 7. Full Class Implementation and Usage

```python
import os
from datetime import datetime
from nodejobs import Job, JobRecord
from nodejobs.job import JobRecord

class DatabaseBackupAutomation:
    def __init__(self, db_backup_command: str, backup_job_prefix: str, job_ids: list, report_path: str):
        self.db_backup_command = db_backup_command
        self.backup_job_prefix = backup_job_prefix
        self.job_ids = job_ids
        self.report_path = report_path
        self.jobs = None
        self.job_records = []
        self.backup_outputs = []

    def generate_unique_job_name(self, job_id: str) -> str:
        current_date = datetime.utcnow().strftime("%Y%m%d")
        return f"{self.backup_job_prefix}_{job_id}_{current_date}"

    def start_backup_jobs(self):
        self.jobs = Job(db_path=os.path.dirname(self.report_path))
        self.job_records = []

        for job_id in self.job_ids:
            unique_job_name = self.generate_unique_job_name(job_id=job_id)
            job_record = self.jobs.run(
                command=self.db_backup_command,
                job_id=unique_job_name
            )
            self.job_records.append(job_record)

    def wait_for_jobs_completion(self, timeout_seconds: float = 300.0):
        for job_record in self.job_records:
            self._wait_for_job_completion(
                job_id=job_record.f_self_id,
                timeout=timeout_seconds
            )

    def _wait_for_job_completion(self, job_id: str, timeout: float):
        start_time = datetime.utcnow()
        while True:
            status_response = self.jobs.get_status(
                job_id=job_id
            )
            current_status = status_response.f_status
            if current_status in (JobRecord.Status.c_finished, JobRecord.Status.c_failed):
                break
            elapsed_time = (datetime.utcnow() - start_time).total_seconds()
            if elapsed_time > timeout:
                self.jobs.stop(
                    job_id=job_id
                )
                break
            time.sleep(0.5)

    def collect_job_logs(self):
        for job_record in self.job_records:
            stdout, _ = self.jobs.job_logs(
                job_id=job_record.f_self_id
            )
            self.backup_outputs.append(stdout)

    def compile_report(self):
        with open(file=self.report_path, mode='w') as report_file:
            for output in self.backup_outputs:
                report_file.write(output + "\n\n")

    def execute_backup_sequence(self):
        self.start_backup_jobs()
        self.wait_for_jobs_completion()
        self.collect_job_logs()
        self.compile_report()
```

## Overall Summary:
This structured, class-based pattern ensures that users can reliably spawn, monitor, and control multiple backup jobs using `nodejobs`. It adheres to best practices such as using **named arguments** for all method calls, leveraging `nodejobs.Job` and `JobRecord`, and managing job lifecycle events robustly. This example serves as a comprehensive template for automating complex, concurrent job workflows with clarity and maintainability.