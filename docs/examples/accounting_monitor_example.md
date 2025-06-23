# Title: Implementation of an Persistent Accounting Summary Job Monitor Using nodejobs

## Short Demonstration:
This example illustrates the design of an object-oriented monitor that manages a background accounting summary job using the nodejobs.Job class. The monitor performs an initial check for existing stdout logs associated with the job. If logs are present, it assumes the summary has already run and remains idle. If logs are absent, it launches the job via the `run()` method. The monitor then continuously polls the job status every 60 seconds, restarting the job if it is not active, and logs health status messages at each check.

```python
import time
import datetime
from nodejobs import Job, JobRecord, JobFilter

class AccountingSummaryMonitor:
    def __init__(self, job_name_pattern: str, job_template: str):
        self.job_name_pattern = job_name_pattern
        self.job_template = job_template
        self.job_instance = None

    def check_existing_logs(self) -> bool:
        # Check for prior stdout logs indicating prior run
        filter_obj = JobFilter()
        filter_obj[self_job_filter.f_self_id] = self.job_name_pattern
        existing_jobs = list(self.jobdb.list_status(filter=filter_obj))
        for job in existing_jobs:
            if self.job_name_pattern in job[self_job_record.f_self_id]:
                # Retrieve stdout log content
                try:
                    stdout_content, _ = self.job_instance.job_logs(job_id=job[self_job_record.f_self_id])
                except Exception:
                    stdout_content = ""
                if stdout_content:
                    return True
        return False

    def run_job(self):
        # Instantiate and run the accounting summary job
        self.job_instance = Job()
        new_job_record = self.job_instance.run(
            command=self.job_template,
            job_id=self.job_name_pattern
        )
        print(f"Started accounting_summary job: {self.job_name_pattern}")

    def monitor(self):
        # Initial check for existing logs
        if not self.check_existing_logs():
            self.run_job()

        while True:
            time.sleep(60)
            filter_obj = JobFilter()
            filter_obj[self_job_filter.f_self_id] = self.job_name_pattern
            filtered_jobs = list(self.jobdb.list_status(filter=filter_obj))
            active_job = None
            for job in filtered_jobs:
                if self.job_name_pattern in job[self_job_record.f_self_id]:
                    active_job = job
                    break

            if active_job and active_job[self_job_record.f_status] == JobRecord.Status.c_running:
                print(f"{self.job_name_pattern} is healthy at {datetime.datetime.utcnow()}")
            else:
                # Restart job if not actively running
                try:
                    self.run_job()
                except Exception as e:
                    print(f"Job {self.job_name_pattern} unexpectedly stopped: {str(e)}")

# Main execution
def main():
    monitor = AccountingSummaryMonitor(
        job_name_pattern="accounting_summary_2023-10-01",
        job_template="python3 /path/to/accounting_script.py"
    )
    monitor.monitor()
```

## Discussion:
This implementation employs a class-based approach to encapsulate the monitoring logic, leveraging `nodejobs.Job`, `JobRecord`, and `JobFilter` classes to manage job lifecycle, status querying, and log retrieval. The key concept is to prevent duplicate job runs by inspecting existing stdout logs before launching a new job. The monitor then uses periodic polling (`time.sleep(60)`) to check job status via `list_status()` filtered by `job_name_pattern`. If the job is found inactive, it is restarted using `run()` with explicit named arguments. Throughout, logging provides real-time insights into the health and status of the accounting summary process, enhancing resilience and transparency.

This pattern highlights the importance of using the nodejobs API functions correctly: filtering with `JobFilter`, listing status with `list_status()`, launching jobs with `run()`, and retrieving logs via `job_logs()`, all with named arguments to ensure clarity and correctness. The design ensures the accounting summary job remains active indefinitely, automatically restarting if it terminates unexpectedly, with continuous status reporting.```python
backup_job_record = nodejobs.Job.run(command=backup_command, job_id=generate_unique_job_name(job_id))
```
This line calls `nodejobs.Job.run()` with named arguments: `command=backup_command` (the shell command string to execute) and `job_id=generate_unique_job_name(job_id)` (a unique identifier generated to prevent job ID collisions). It spawns a new backup job process, tracks it, and returns a `JobRecord` object assigned to `backup_job_record`.

```python
active_jobs = nodejobs.Job.list(filter=dict(status=nodejobs.JobRecord.Status.c_running))
```
This line invokes `nodejobs.Job.list()` with a `filter` argument as a dictionary: `status=nodejobs.JobRecord.Status.c_running`. This filters the list to include only jobs with status "running". It returns a dictionary `active_jobs` with job IDs as keys and `JobRecord` instances as values, representing all currently executing jobs.

```python
status_of_job = nodejobs.Job.get_status(job_id=backup_job_record.f_self_id)
```
This line uses `nodejobs.Job.get_status()` with the named argument `job_id=backup_job_record.f_self_id` (the unique ID of the specific job). It retrieves the latest status of the specified job and returns an updated `JobRecord`, stored in `status_of_job`.

```python
stop_result = nodejobs.Job.stop(job_id=backup_job_record.f_self_id)
```
This line calls `nodejobs.Job.stop()` with `job_id=backup_job_record.f_self_id`, signaling the process to terminate. It attempts a graceful shutdown and returns a `JobRecord` indicating the final status, assigned to `stop_result`.

```python
stdout_log, stderr_log = nodejobs.Job.job_logs(job_id=backup_job_record.f_self_id)
```
This line invokes `nodejobs.Job.job_logs()` with `job_id=backup_job_record.f_self_id`. It reads the log files associated with this job, returning a tuple: `stdout_log` and `stderr_log`, containing the captured standard output and error logs respectively.

---

This sequence demonstrates the consistent use of `nodejobs.Job` class methods with **all** function calls utilizing **named arguments**. It shows how to spawn, list, check status, stop, and retrieve logs of jobs, adhering strictly to the design patterns that leverage `nodejobs` module, `Job`, `JobRecord`, and related classes—all essential for managing jobs programmatically.```python
import os
import time
import uuid
from typing import Tuple, Optional
import nodejobs
from nodejobs.job import JobRecord, JobRecord.Status

class JobManagerDemo:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.jobs_manager: nodejobs.Jobs = self.initialize_job_manager()

    def initialize_job_manager(self) -> nodejobs.Jobs:
        os.makedirs(self.db_path, exist_ok=True)
        jobs_manager = nodejobs.Jobs(db_path=self.db_path)
        assert jobs_manager is not None, "Failed to initialize Jobs manager."
        return jobs_manager

    def spawn_job(self, command: str, job_id: str) -> JobRecord:
        job_record = self.jobs_manager.run(command=command, job_id=job_id)
        assert job_record is not None, "Job spawn returned None."
        assert job_record.f_self_id == job_id, "Job ID mismatch after spawn."
        return job_record

    def wait_for_job_status(self, job_id: str, desired_status: str, timeout_seconds: float = 30.0) -> bool:
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            status_record = self.jobs_manager.get_status(job_id)
            if status_record.f_status == desired_status:
                return True
            time.sleep(0.5)
        return False

    def verify_job_is_running(self, job_id: str) -> None:
        status_record = self.jobs_manager.get_status(job_id)
        assert status_record.f_status == Status.c_running, \
            f"Expected job to be running, but status is {status_record.f_status}."

    def retrieve_job_logs(self, job_id: str) -> Tuple[str, str]:
        stdout_log, stderr_log = self.jobs_manager.job_logs(job_id)
        assert isinstance(stdout_log, str), "Stdout log is not a string."
        assert isinstance(stderr_log, str), "Stderr log is not a string."
        return stdout_log, stderr_log

    def stop_job(self, job_id: str) -> None:
        result_record = self.jobs_manager.stop(job_id)
        assert result_record is not None, "Failed to stop job; result is None."
        assert hasattr(result_record, 'f_self_id'), "Result JobRecord missing 'f_self_id'."

    def cleanup(self) -> None:
        # Placeholder for any cleanup actions if needed
        pass

# Main function demonstrating the usage of the JobManagerDemo class
# Stage 1: Initialize job manager
# Stage 2: Spawn a new job
# Stage 3: Verify job is running
# Stage 4: Wait for job completion
# Stage 5: Retrieve logs
# Stage 6: Stop job if necessary
# Stage 7: Cleanup resources

if __name__ == "__main__":
    # Stage 1. Initialize Job Manager
    demo = JobManagerDemo(db_path="./test_job_db")

    # Stage 2. Spawn a new job
    unique_job_id = str(uuid.uuid4())
    command = "echo 'Starting job'; sleep 2; echo 'Job finished'"
    job_record = demo.spawn_job(command=command, job_id=unique_job_id)

    # Stage 3. Verify job is running
    demo.verify_job_is_running(job_id=unique_job_id)

    # Stage 4. Wait for job to reach 'finished' status
    job_completed = demo.wait_for_job_status(job_id=unique_job_id, desired_status=Status.c_finished, timeout_seconds=10)
    assert job_completed, "Job did not reach finished status within timeout."

    # Stage 5. Retrieve logs
    stdout_logs, stderr_logs = demo.retrieve_job_logs(job_id=unique_job_id)
    print(f"Stdout logs:\n{stdout_logs}")
    print(f"Stderr logs:\n{stderr_logs}")

    # Stage 6. Stop the job (if still running)
    demo.stop_job(job_id=unique_job_id)
    final_status = demo.jobs_manager.get_status(unique_job_id).f_status
    assert final_status in (Status.c_stopped, Status.c_finished), \
        f"Unexpected final status: {final_status}"

    # Stage 7. Cleanup resources
    demo.cleanup()
```