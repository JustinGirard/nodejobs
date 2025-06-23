# Monitoring and Automatic Restart of a Named Job with Email Alert on Persistent Failure

## Short Demonstration:
This example demonstrates a Python object-oriented daemon that utilizes the `nodejobs` module to monitor a specific job by name. The daemon periodically checks whether the job is active using `list_status()`. If the job is not running, it attempts to restart the job via the `Job.run()` method, waits 10 seconds to verify if the job resumes, and if it remains down, retrieves the error logs to notify an administrator via email. The process repeats every 5 minutes, with a counter tracking consecutive failures, and continues to send alert notifications after 20 failed recovery attempts until manual intervention occurs.

## Implementation:
```python
import time
import smtplib
from email.message import EmailMessage
from nodejobs import Job, list_status, JobRecord

class JobMonitorDaemon:
    def __init__(
        self,
        job_name: str,
        restart_command: str,
        admin_email: str,
        check_interval_seconds: int = 300,
        max_failures: int = 20
    ):
        self.job_name = job_name
        self.restart_command = restart_command
        self.admin_email = admin_email
        self.check_interval_seconds = check_interval_seconds
        self.max_failures = max_failures
        self.failure_count = 0

    def send_email(self, subject: str, body: str):
        email_msg = EmailMessage(
            subject=subject,
            from_addr='monitoring@domain.com',
            to_addrs=self.admin_email,
            content=body
        )
        with smtplib.SMTP(host='localhost') as smtp:
            smtp.send_message(msg=email_msg)

    def get_job_record(self) -> JobRecord or None:
        jobs = list_status()
        for job_id, job_record in jobs.items():
            if job_record[JobRecord.f_self_id] == self.job_name:
                return job_record
        return None

    def check_and_restart_job(self):
        job_record = self.get_job_record()
        if job_record is None or job_record[JobRecord.f_status] != JobRecord.Status.c_running:
            # Attempt to restart the job
            new_job = Job.run(command=self.restart_command, job_id=self.job_name)
            # Wait 10 seconds for the job to initialize
            time.sleep(10)
            job_record_after = self.get_job_record()
            if job_record_after is None or job_record_after[JobRecord.f_status] != JobRecord.Status.c_running:
                # Retrieve job logs for error details
                stdout_log, stderr_log = Job.job_logs(job_id=self.job_name)
                error_message = (
                    f"Job {self.job_name} failed to restart.\n"
                    f"Error logs:\n{stderr_log}"
                )
                self.send_email(
                    subject=f"Alert: Job {self.job_name} down",
                    body=error_message
                )
                self.failure_count += 1
            else:
                # Job successfully restarted
                self.failure_count = 0

    def run(self):
        while True:
            self.check_and_restart_job()
            if self.failure_count >= self.max_failures:
                # Send persistent failure alert
                self.send_email(
                    subject=f"Persistent failure: Job {self.job_name} not recovered",
                    body=f"The job {self.job_name} has failed to recover after {self.failure_count} attempts."
                )
            time.sleep(self.check_interval_seconds)
```

---

## Discussion:

This implementation employs an object-oriented approach to continuous job monitoring using the `nodejobs` module. The `JobMonitorDaemon` class encapsulates all core functionalities, including periodic status polling, job restart procedures, log retrieval, and email notifications. Key concepts include:

- **Polling job status:** Using `list_status()` with iteration over returned `JobRecord` objects to identify whether the specified job is active based on `JobRecord.f_self_id`.
- **Job restart operation:** Utilizing `Job.run()` with explicit named arguments (`command` and `job_id`) to initiate a new job process when the target job is down.
- **Verification delay:** Waiting 10 seconds after restart commands to allow job initialization and status stabilization.
- **Log retrieval and error reporting:** Calling `Job.job_logs()` with the `job_id` to extract stderr logs for diagnostic purposes when a restart fails.
- **Failure tracking and alerts:** Maintaining a `failure_count` to monitor consecutive failures, triggering persistent email alerts after exceeding the threshold.
- **Email notifications:** Using `smtplib.SMTP()` and `EmailMessage()` with explicit headers to notify administrators of critical failures or persistent issues.

This design aligns with best practices for `nodejobs` usage, ensuring all job operations invoke the `Job` class, avoiding manual or bypassed process management. The structured, low-nesting approach facilitates maintainability, clarity, and effective monitoring of critical jobs within an automated environment.```python
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