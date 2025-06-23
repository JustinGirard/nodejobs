# Automating Database Backups with nodejobs

## Short Demonstration:
This example illustrates how to orchestrate multiple database backup jobs using the `nodejobs` module. Each backup process is initiated as a separate job with a unique `job_name` constructed from a common prefix, the current date, and a specific job identifier, ensuring no collisions occur. After all backup jobs are launched, the script monitors their status and, upon completion, aggregates their stdout logs into a consolidated report.

## Solution Class:

```python
# Import necessary modules
import os
from datetime import datetime
from nodejobs import Job, JobRecord

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
                job_id=job_record[self_job_record.f_self_id],
                timeout=timeout_seconds
            )

    def _wait_for_job_completion(self, job_id: str, timeout: float):
        start_time = datetime.utcnow()
        while True:
            status_response = self.jobs.get_status(
                job_id=job_id
            )
            current_status = status_response[self_job_record.f_status]
            if current_status in (JobRecord.Status.c_finished, JobRecord.Status.c_failed):
                break
            elapsed_time = (datetime.utcnow() - start_time).total_seconds()
            if elapsed_time > timeout:
                self.jobs.stop(
                    job_id=job_id
                )
                break

    def collect_job_logs(self):
        for job_record in self.job_records:
            stdout, _ = self.jobs.job_logs(
                job_id=job_record[self_job_record.f_self_id]
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

## Overall Approach and Key Concepts:

This solution adopts an object-oriented approach, encapsulating the backup orchestration within the `DatabaseBackupAutomation` class. The process involves:

- **Job Initialization:** Using `nodejobs.Job` with the appropriate `db_path` to manage job execution environment.
- **Unique Job Naming:** Generating distinct `job_name` strings using a prefix, the current UTC date, and individual job identifiers to prevent collisions during concurrent executions.
- **Launching Jobs:** Invoking `Job.run()` with the `command` and `job_id`, capturing each `JobRecord` for monitoring.
- **Monitoring Job Status:** Utilizing `Job.get_status()` with `job_id` to poll the current status, checking for `c_finished` or `c_failed`, and implementing a timeout mechanism with `self.jobs.stop()` if necessary.
- **Log Collection:** Retrieving stdout logs via `Job.job_logs()` for each completed job.
- **Report Compilation:** Writing all collected logs into a designated report file, ensuring the output consolidates all backup outputs for review.

This pattern emphasizes the correct use of `nodejobs` classes (`Job`, `JobRecord`) and their methods, adhering to best practices such as named arguments and avoiding manual or naïve handling of job processes. The approach demonstrates robust management of multiple concurrent jobs, status polling, and log aggregation, serving as a comprehensive template for automating database backups with `nodejobs`.

---

This structured, class-based design ensures maintainability, clarity, and adherence to the `nodejobs` module's patterns, providing a clear example for users looking to automate complex job workflows within Python.```python
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